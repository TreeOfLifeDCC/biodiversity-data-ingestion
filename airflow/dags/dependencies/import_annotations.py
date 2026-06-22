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
            resp = _get(ENA_XML.format(accession=accession), timeout=30)
            resp.raise_for_status()
            node = ET.fromstring(resp.content).find(".//TAXON_ID")
            if node is not None and node.text:
                tax_id = node.text.strip()
            break
        except Exception as exc:  # network, HTTP, or XML parse error
            logger.warning(
                "ENA tax_id lookup failed for %s (attempt %d/%d): %s",
                accession, attempt + 1, retries, exc,
            )
            time.sleep(sleep_s)
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
