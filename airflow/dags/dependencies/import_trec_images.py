import logging
from collections import defaultdict

import requests
from elasticsearch import Elasticsearch, helpers
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


BIA_ACCESSIONS = ("S-BIAD2258",)
PAGE_SIZE = 100
BULK_BATCH_SIZE = 500
REQUEST_TIMEOUT = (10, 120)


def normalise_es_host(es_host: str) -> str:
    if es_host.startswith("http://") or es_host.startswith("https://"):
        return es_host
    return f"https://{es_host}"


def get_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=(500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch_bia_files(session: requests.Session, accession: str) -> dict[str, list[dict]]:
    url = f"https://www.ebi.ac.uk/biostudies/api/v1/files/{accession}"
    results = defaultdict(list)
    start = 0

    while True:
        response = session.get(
            url,
            params={"start": start, "length": PAGE_SIZE},
            timeout=REQUEST_TIMEOUT,
        )
        response.raise_for_status()
        data = response.json()
        entries = data.get("data", [])

        if not entries:
            break

        for entry in entries:
            if entry.get("type") != "file" or not entry.get("name"):
                continue

            biosample_url = entry.get("BioSamples_ID")
            if not biosample_url:
                continue

            biosample_id = biosample_url.rstrip("/").split("/")[-1]
            results[biosample_id].append(
                {
                    "acquisition_location": entry.get("acquisition_location", ""),
                    "acquisition_date": entry.get("acquisition_date", ""),
                    "name": entry.get("name", ""),
                    "tile": entry.get("tile", ""),
                }
            )

        start += PAGE_SIZE
        if start >= data.get("recordsTotal", 0):
            break

    return dict(results)


def get_all_samples(es: Elasticsearch, index_name: str) -> list[dict]:
    samples = []
    search_after = None

    while True:
        body = {
            "query": {"match_all": {}},
            "size": 1000,
            "sort": [{"biosampleId.keyword": "asc"}],
            "_source": ["biosampleId"],
        }
        if search_after:
            body["search_after"] = search_after

        response = es.search(index=index_name, body=body, request_timeout=120)
        hits = response["hits"]["hits"]

        if not hits:
            break

        for hit in hits:
            source = hit.get("_source", {})
            biosample_id = source.get("biosampleId")
            if biosample_id:
                samples.append({"id": hit["_id"], "biosampleId": biosample_id})

        search_after = hits[-1]["sort"]

    return samples


def bulk_update_images(es: Elasticsearch, index_name: str, updates: list[dict]) -> None:
    actions = [
        {
            "_op_type": "update",
            "_index": index_name,
            "_id": update["id"],
            "doc": {
                "images": update["images"],
                "has_images": "Yes",
            },
        }
        for update in updates
    ]

    helpers.bulk(
        es,
        actions,
        chunk_size=BULK_BATCH_SIZE,
        request_timeout=120,
    )


def refresh_has_images_flags(es: Elasticsearch, index_name: str) -> dict:
    response = es.update_by_query(
        index=index_name,
        body={
            "script": {
                "source": (
                    "ctx._source.has_images = "
                    "(ctx._source.images != null && ctx._source.images.length > 0) "
                    "? 'Yes' : 'No'"
                ),
                "lang": "painless",
            }
        },
        conflicts="proceed",
        refresh=True,
        request_timeout=120,
    )
    return dict(response)


def main(es_host: str, es_password: str, index_name: str) -> dict:
    logging.info("Connecting to Elasticsearch index %s", index_name)
    es = Elasticsearch(
        [normalise_es_host(es_host)],
        basic_auth=("elastic", es_password),
        request_timeout=60,
    )

    session = get_session()

    all_images = {}
    for accession in BIA_ACCESSIONS:
        logging.info("Fetching TREC images from BioImage Archive accession %s", accession)
        images = fetch_bia_files(session, accession)
        for biosample_id, entries in images.items():
            all_images.setdefault(biosample_id, []).extend(entries)

    logging.info("Found image references for %s BioSamples", len(all_images))

    samples = get_all_samples(es, index_name)
    logging.info("Found %s samples in Elasticsearch index %s", len(samples), index_name)

    batch = []
    updated = 0
    skipped = 0

    for sample in samples:
        images = all_images.get(sample["biosampleId"], [])
        if not images:
            skipped += 1
            continue

        batch.append({"id": sample["id"], "images": images})

        if len(batch) >= BULK_BATCH_SIZE:
            bulk_update_images(es, index_name, batch)
            updated += len(batch)
            batch = []

    if batch:
        bulk_update_images(es, index_name, batch)
        updated += len(batch)

    has_images_response = refresh_has_images_flags(es, index_name)

    summary = {
        "updated": updated,
        "skipped": skipped,
        "has_images_updated": has_images_response.get("updated", 0),
    }
    logging.info("Finished TREC image import: %s", summary)
    return summary
