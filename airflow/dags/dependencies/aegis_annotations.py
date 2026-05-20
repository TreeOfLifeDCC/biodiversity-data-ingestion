"""
Loader + transform for the AEGIS Ensembl-annotation manifest
(`results_filtered.json`).

The manifest is keyed by `Genus_species` and carries one or more assemblies
per species, each with relative paths to genebuild / homology / genome
files. Records are joined onto AEGIS species docs by `taxid` →
`data_portal.taxId`.
"""

import json
import logging
from urllib.request import urlopen

logger = logging.getLogger(__name__)


def load_annotations(path: str) -> dict:
    """Load the annotations JSON from a local path, http(s) URL, or gs:// URI."""
    if path.startswith(("http://", "https://")):
        with urlopen(path, timeout=300) as resp:
            return json.loads(resp.read())
    if path.startswith("gs://"):
        # google-cloud-storage is pre-installed on Composer.
        from google.cloud import storage

        bucket_name, _, object_name = path[len("gs://"):].partition("/")
        client = storage.Client()
        blob = client.bucket(bucket_name).blob(object_name)
        return json.loads(blob.download_as_text())
    with open(path) as f:
        return json.load(f)


def _flatten_files(node: dict | None) -> list[dict]:
    """
    Walk a {category: {filename: relative_path}} tree (one level deep) into
    flat records: [{"category": ..., "name": ..., "path": ...}, ...].
    """
    out: list[dict] = []
    if not isinstance(node, dict):
        return out
    for category, contents in node.items():
        if not isinstance(contents, dict):
            continue
        for name, path in contents.items():
            if isinstance(path, str):
                out.append({"category": category, "name": name, "path": path})
    return out


def build_annotation_records(annotations: dict) -> dict[int, list[dict]]:
    """
    Transform the raw manifest into `{taxId: [annotation_record, ...]}`.

    One record is emitted per (assembly × provider × release) combination so
    the data_portal doc carries a flat list of buildable annotation sets.
    """
    by_tax: dict[int, list[dict]] = {}
    for species_key, rec in (annotations or {}).items():
        raw_tax = rec.get("taxid") or rec.get("species_taxonomy_id")
        try:
            tax_id = int(raw_tax)
        except (TypeError, ValueError):
            logger.warning(
                "Annotation entry '%s' has no usable taxid; skipping",
                species_key,
            )
            continue

        biosample_id = rec.get("biosample_id")
        strain = rec.get("strain")
        strain_type = rec.get("strain_type")
        species_common_name = rec.get("common_name")
        species_scientific_name = rec.get("scientific_name")

        for asm_acc, asm in (rec.get("assemblies") or {}).items():
            assembly_files = _flatten_files(
                (asm.get("assembly") or {}).get("files")
            )

            for prov_name, prov_releases in (asm.get("genebuild_providers") or {}).items():
                for release_id, release in (prov_releases or {}).items():
                    paths = release.get("paths") or {}
                    annotation_files = _flatten_files(
                        (paths.get("genebuild") or {}).get("files")
                    )
                    homology_files = _flatten_files(
                        (paths.get("homologies") or {}).get("files")
                    )

                    record = {
                        "assemblyAccession": asm_acc,
                        "assemblyName": asm.get("name"),
                        "assemblyLevel": asm.get("level"),
                        "biosampleId": biosample_id,
                        "strain": strain,
                        "strainType": strain_type,
                        "speciesKey": species_key,
                        "scientificName": species_scientific_name,
                        "commonName": species_common_name,
                        "provider": prov_name,
                        "release": release.get("release") or release_id,
                        "annotationFiles": annotation_files,
                        "homologyFiles": homology_files,
                        "assemblyFiles": assembly_files,
                    }
                    by_tax.setdefault(tax_id, []).append(record)

    logger.info(
        "Annotations: %d taxa, %d total assembly-release records",
        len(by_tax), sum(len(v) for v in by_tax.values()),
    )
    return by_tax
