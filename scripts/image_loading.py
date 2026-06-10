import os
import time
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from dotenv import load_dotenv

load_dotenv()

# Session with retries on transient failures (timeouts, 5xx, connection drops).
session = requests.Session()
retry = Retry(
    total=5,
    backoff_factor=2,          # waits 2s, 4s, 8s, 16s, 32s between retries
    status_forcelist=(500, 502, 503, 504),
    allowed_methods=("GET",),
)
session.mount("https://", HTTPAdapter(max_retries=retry))

# (connect_timeout, read_timeout) — BIA can be slow to start streaming a big page.
REQUEST_TIMEOUT = (10, 120)

ES_INDEX = "data_portal_development_5"
BIA_ACCESSIONS = [
    "S-BIAD2258",
    # Add future accessions here
]

es = Elasticsearch(
    [os.getenv("ES_URL")],
    basic_auth=(os.getenv("ES_USERNAME"), os.getenv("ES_PASSWORD")),
    verify_certs=True,
    request_timeout=60,
)


def fetch_bia_files(accession: str) -> dict:
    url = f"https://www.ebi.ac.uk/biostudies/api/v1/files/{accession}"
    results = {}
    start = 0
    length = 100

    while True:
        resp = session.get(url, params={"start": start, "length": length}, timeout=REQUEST_TIMEOUT)
        resp.raise_for_status()
        data = resp.json()
        entries = data.get("data", [])
        if not entries:
            break

        for entry in entries:
            if entry.get("type") != "file" or not entry.get("name"):
                continue
            full_url = entry.get("BioSamples_ID", "")
            if not full_url:
                continue
            biosample_id = full_url.rstrip("/").split("/")[-1]
            image = {
                "acquisition_location": entry.get("acquisition_location", ""),
                "acquisition_date": entry.get("acquisition_date", ""),
                "name": entry.get("name", ""),
                "tile": entry.get("tile", ""),
            }
            results.setdefault(biosample_id, []).append(image)

        start += length
        if start >= data.get("recordsTotal", 0):
            break

    return results


def get_all_samples() -> list:
    samples = []
    batch_size = 1000
    search_after = None

    while True:
        body = {
            "query": {"match_all": {}},
            "size": batch_size,
            "sort": [{"biosampleId.keyword": "asc"}],
            "_source": ["biosampleId"],
        }
        if search_after:
            body["search_after"] = search_after

        resp = es.search(index=ES_INDEX, body=body)
        hits = resp["hits"]["hits"]
        if not hits:
            break

        for hit in hits:
            samples.append({
                "id": hit["_id"],
                "biosampleId": hit["_source"]["biosampleId"]
            })

        search_after = hits[-1]["sort"]
        print(f"  Fetched {len(samples)} samples so far...", end="\r")

    print()
    return samples


def bulk_update(updates: list):
    """Send bulk update actions to ES."""
    actions = [
        {
            "_op_type": "update",
            "_index": ES_INDEX,
            "_id": u["id"],
            "doc": {"images": u["images"], "has_images": "Yes"},
        }
        for u in updates
    ]
    bulk(es, actions)


def main():
    # fetch all image references from BIA
    print("Fetching image references from BioImage Archive...")
    all_images = {}
    for accession in BIA_ACCESSIONS:
        print(f"  Querying {accession}...")
        images = fetch_bia_files(accession)
        for biosample_id, entries in images.items():
            all_images.setdefault(biosample_id, []).extend(entries)
    print(f"  Found images for {len(all_images)} samples.")

    # fetch all samples from ES
    print("Fetching samples from Elasticsearch...")
    samples = get_all_samples()
    print(f"  Found {len(samples)} samples in index.")

    # bulk update samples that have images
    print("Updating samples with image references...")
    batch = []
    updated = 0
    skipped = 0

    for sample in samples:
        biosample_id = sample["biosampleId"]
        images = all_images.get(biosample_id, [])
        if not images:
            skipped += 1
            continue
        batch.append({"id": sample["id"], "images": images})
        print("Updating ", sample["id"])
        if len(batch) >= 500:
            bulk_update(batch)
            updated += len(batch)
            print(f"  Updated {updated} samples so far...", end="\r")
            batch = []

    # Flush remaining
    if batch:
        bulk_update(batch)
        updated += len(batch)

    print(f"\nDone. Updated: {updated}, Skipped (no images): {skipped}")


if __name__ == "__main__":
    main()
