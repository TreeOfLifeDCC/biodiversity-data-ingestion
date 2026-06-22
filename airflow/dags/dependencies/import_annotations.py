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
