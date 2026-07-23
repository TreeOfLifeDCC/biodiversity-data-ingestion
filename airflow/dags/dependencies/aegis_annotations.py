"""
Loader for the AEGIS Ensembl-annotation manifest (`aegis.jsonl`).

The manifest is produced by `import_annotations.py` from the
`Ensembl/projects.ensembl.org` GitHub `_data/aegis/species.yaml` file and
uploaded to `gs://prj-ext-prod-biodiv-data-in-annotations/aegis.jsonl` — the
same producer and format used for the dtol/erga/asg/gbdp manifests.

Each line is one JSON object: `{"tax_id": <int|str>, "annotations": [record, ...]}`.
Records are joined onto AEGIS species docs by `tax_id` → `data_portal.taxId`.
"""

import json
import logging
from urllib.request import urlopen

logger = logging.getLogger(__name__)

GCS_MANIFEST = "gs://prj-ext-prod-biodiv-data-in-annotations/aegis.jsonl"


def _read_text(path: str) -> str:
    """Read a text file from a local path, http(s) URL, or gs:// URI."""
    if path.startswith(("http://", "https://")):
        with urlopen(path, timeout=300) as resp:
            return resp.read().decode("utf-8")
    if path.startswith("gs://"):
        # google-cloud-storage is pre-installed on Composer.
        from google.cloud import storage

        bucket_name, _, object_name = path[len("gs://"):].partition("/")
        client = storage.Client()
        blob = client.bucket(bucket_name).blob(object_name)
        return blob.download_as_text()
    with open(path) as f:
        return f.read()


def load_annotation_manifest(path: str = GCS_MANIFEST) -> dict[int, list[dict]]:
    """Load the `aegis.jsonl` manifest into `{taxId(int): [record, ...]}`.

    Lines without a parseable integer `tax_id` are logged and skipped, so a
    single malformed row can't abort the build.
    """
    by_tax: dict[int, list[dict]] = {}
    for line in _read_text(path).splitlines():
        line = line.strip()
        if not line:
            continue
        row = json.loads(line)
        raw_tax = row.get("tax_id")
        try:
            tax_id = int(raw_tax)
        except (TypeError, ValueError):
            logger.warning(
                "Manifest line has no usable tax_id; skipping: %r", raw_tax
            )
            continue
        by_tax.setdefault(tax_id, []).extend(row.get("annotations") or [])

    logger.info(
        "Annotations: %d taxa, %d total records",
        len(by_tax), sum(len(v) for v in by_tax.values()),
    )
    return by_tax
