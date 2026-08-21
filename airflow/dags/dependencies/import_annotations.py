#!/usr/bin/env python3
"""
Build per-project Ensembl annotation manifests ({project}.jsonl) from the
projects.ensembl.org `_data/<project>/species.yaml` files and upload them to
gs://prj-ext-prod-biodiv-data-in-annotations/. Consumed downstream by the Beam
metadata pipeline (beam/src/dependencies/my_pipeline.py).
"""
import json
import logging
import time
import xml.etree.ElementTree as ET

import requests
import yaml

logger = logging.getLogger(__name__)

GITHUB_CONTENTS = (
    "https://api.github.com/repos/Ensembl/projects.ensembl.org/contents/"
    "_data/{project}/species.yaml"
)
ENA_XML = "https://www.ebi.ac.uk/ena/browser/api/xml/{accession}"
GCS_BASE = "gcs://prj-ext-prod-biodiv-data-in-annotations"

# Output manifest name -> source `_data` project directories.
PROJECT_SOURCES = {
    "dtol": ["darwin_tree_of_life"],
    "erga": ["darwin_tree_of_life", "erga_bge", "erga_pilot"],
    "asg": ["asg"],
    "aegis": ["aegis"],
    "gbdp": [
        "darwin_tree_of_life",
        "erga_bge",
        "erga_pilot",
        "asg",
        "aegis",
        "vgp",
        "canadian_biogenome",
    ],
}


def build_record(entry: dict, tax_id: str) -> dict:
    """Map one species.yaml entry + resolved tax_id to an output record."""
    repeat = entry.get("repeat_library")
    return {
        "species": entry.get("species"),
        "accession": entry.get("accession"),
        "tax_id": tax_id,
        "annotation": {
            "GTF": entry.get("annotation_gtf"),
            "GFF3": entry.get("annotation_gff3"),
        },
        "proteins": {"FASTA": entry.get("proteins")},
        "transcripts": {"FASTA": entry.get("transcripts")},
        "softmasked_genome": {"FASTA": entry.get("softmasked_genome")},
        "repeat_library": {"FASTA": repeat} if repeat else None,
        "other_data": {"ftp_dumps": entry.get("ftp_dumps")},
        "view_in_browser": entry.get("beta_link"),
        "annotation_method": entry.get("annotation_method"),
        "busco_score": entry.get("busco_score"),
        "busco_lineage": entry.get("busco_lineage"),
    }


def resolve_tax_id(accession, cache, _get=requests.get, retries=3, sleep_s=0.1):
    """Resolve an accession to its NCBI tax_id via ENA, cached across calls.

    Returns the tax_id string, or None on persistent failure (logged, never
    raised, so one bad accession cannot abort the whole task).
    """
    if accession in cache:
        return cache[accession]
    tax_id = None
    for attempt in range(retries):
        try:
            resp = _get(ENA_XML.format(accession=accession), timeout=(5, 15))
            resp.raise_for_status()
            node = ET.fromstring(resp.content).find(".//TAXON_ID")
            if node is not None and node.text:
                tax_id = node.text.strip()
            break
        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as exc:
            logger.warning(
                "ENA tax_id lookup failed for %s (attempt %d/%d): %s",
                accession, attempt + 1, retries, exc,
            )
            time.sleep(sleep_s)
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status is not None and 400 <= status < 500:
                logger.warning("ENA returned %s for %s; not retrying", status, accession)
                break
            logger.warning(
                "ENA tax_id lookup failed for %s (attempt %d/%d): %s",
                accession, attempt + 1, retries, exc,
            )
            time.sleep(sleep_s)
        except ET.ParseError as exc:
            logger.warning("ENA returned unparseable XML for %s: %s", accession, exc)
            break
    if tax_id is None:
        logger.warning("No tax_id resolved for %s; skipping its record", accession)
    cache[accession] = tax_id
    time.sleep(sleep_s)
    return tax_id


def fetch_project_yaml(project, token, _get=requests.get):
    """Fetch and parse `_data/<project>/species.yaml` from the (private)
    projects.ensembl.org GitHub repo. Raises on HTTP error or unexpected shape.
    """
    resp = _get(
        GITHUB_CONTENTS.format(project=project),
        headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/vnd.github.raw",
        },
        timeout=60,
    )
    resp.raise_for_status()
    data = yaml.safe_load(resp.content)
    if not isinstance(data, list):
        raise ValueError(
            f"Unexpected species.yaml for {project!r}: "
            f"expected a list, got {type(data).__name__}"
        )
    return data


def build_project(project_name, token, cache, _fetch=fetch_project_yaml,
                  _resolve=resolve_tax_id):
    """Build {tax_id: [record, ...]} for one output manifest, deduping by
    accession across the project's source dirs.

    Raises RuntimeError if accessions were found but none resolved a tax_id
    (a likely ENA outage): writing an empty manifest here would truncate the
    downstream BigQuery/ES annotation data for the whole project.
    """
    seen = set()
    by_tax = {}
    entries_seen = 0
    for source in PROJECT_SOURCES[project_name]:
        for entry in _fetch(source, token):
            accession = entry.get("accession")
            if not accession or accession in seen:
                continue
            seen.add(accession)
            entries_seen += 1
            tax_id = _resolve(accession, cache)
            if tax_id is None:
                continue
            by_tax.setdefault(tax_id, []).append(build_record(entry, tax_id))
    if entries_seen and not by_tax:
        raise RuntimeError(
            f"{project_name}: {entries_seen} accessions found but none resolved "
            f"a tax_id (likely an ENA outage); refusing to write an empty manifest"
        )
    return by_tax


def write_jsonl(project_name, by_tax):
    """Write {project}.jsonl to GCS, one line per tax_id."""
    from airflow.io.path import ObjectStoragePath  # lazy: keeps module import light

    base = ObjectStoragePath(GCS_BASE, conn_id="google_cloud_default")
    base.mkdir(exist_ok=True)
    path = base / f"{project_name}.jsonl"
    with path.open("w") as fh:
        for tax_id, annotations in by_tax.items():
            fh.write(json.dumps({"annotations": annotations, "tax_id": tax_id}) + "\n")


def main(github_token, projects=None):
    """Build and upload annotation manifests.

    `projects` optionally restricts which manifests are built (e.g. ["aegis"]
    so the AEGIS DAG can refresh only its own manifest); defaults to all.
    """
    cache = {}
    for project_name in (projects or PROJECT_SOURCES):
        by_tax = build_project(project_name, github_token, cache)
        write_jsonl(project_name, by_tax)
        logger.info("Wrote %s.jsonl (%d taxa)", project_name, len(by_tax))
