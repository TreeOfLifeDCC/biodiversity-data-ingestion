from time import sleep
from typing import Iterator

from dateutil import parser
import requests

from dependencies.common_functions import check_field_existence


mandatory_fields = [
    "organism",
    "depth",
    "collection date",
    "altitude",
    "geographic location (latitude)",
    "geographic location (longitude)",
    "geographic location (country and/or sea)",
]

columns_mapping = {
    "collection date": "collection_date",
    "geographic location (latitude)": "lat",
    "geographic location (longitude)": "lon",
    "geographic location (country and/or sea)": "location",
}

BIOSAMPLES_ROOT_URL = "https://www.ebi.ac.uk/biosamples/samples"
REQUEST_TIMEOUT = (10, 120)


def transform_sample(sample: dict) -> dict:
    item: dict[str, object] = {}
    item["customFields"] = []
    for record_name, record in sample.get("characteristics", {}).items():
        values, units, _ = check_field_existence(record)
        if record_name not in mandatory_fields:
            item["customFields"].append(
                {
                    "name": record_name,
                    "value": values,
                    "unit": units,
                }
            )
        else:
            if record_name == "collection date":
                try:
                    values = parser.parse(values)
                except (parser.ParserError, TypeError, ValueError):
                    values = None
            if record_name in [
                "geographic location (latitude)",
                "geographic location (longitude)",
            ]:
                try:
                    values = float(values)
                except (TypeError, ValueError):
                    values = None
            if units:
                values = f"{values} {units}"
            if record_name in columns_mapping:
                item[columns_mapping[record_name]] = values
            else:
                item[record_name] = values
    item["relationships"] = sample.get("relationships", [])
    item["biosampleId"] = sample["accession"]
    return item


def iter_metadata(project_tag: str) -> Iterator[dict]:
    if project_tag != "Traversing European Coastlines (TREC) expedition":
        return

    seen: set[str] = set()

    response = requests.get(
        BIOSAMPLES_ROOT_URL,
        params={"size": 200, "text": project_tag},
        timeout=REQUEST_TIMEOUT,
    )
    response.raise_for_status()
    samples_response = response.json()
    sleep(0.1)

    while "_embedded" in samples_response:
        for sample in samples_response["_embedded"]["samples"]:
            accession = sample["accession"]
            # Guard against any pagination overlap without buffering records.
            if accession in seen:
                continue
            seen.add(accession)
            yield transform_sample(sample)

        next_url = samples_response.get("_links", {}).get("next", {}).get("href")
        if not next_url:
            break
        response = requests.get(next_url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        samples_response = response.json()
        sleep(0.1)


def main(project_tag: str) -> dict[str, dict]:
    return {record["biosampleId"]: record for record in iter_metadata(project_tag)}
