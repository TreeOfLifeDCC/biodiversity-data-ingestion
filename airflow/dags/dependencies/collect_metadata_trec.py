from time import sleep

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


def main(project_tag: str) -> dict[str, dict]:
    """
    Collect TREC metadata from BioSamples for the given project tag.
    """

    samples: dict[str, dict] = {}

    biosamples_root_url = "https://www.ebi.ac.uk/biosamples/samples"

    # collect metadata from the BioSamples
    if project_tag == "Traversing European Coastlines (TREC) expedition":
        samples_response = requests.get(
            biosamples_root_url,
            params={"size": 200, "text": project_tag},
            timeout=3600,
        ).json()
        sleep(0.1)
        while "_embedded" in samples_response:
            for sample in samples_response["_embedded"]["samples"]:
                # tag samples with the project for downstream processing
                sample["project_name"] = project_tag
                samples[sample["accession"]] = sample
            next_url = samples_response.get("_links", {}).get("next", {}).get("href")
            if not next_url:
                break
            samples_response = requests.get(next_url, timeout=3600).json()
            sleep(0.1)

    columns_mapping = {
        "collection date": "collection_date",
        "geographic location (latitude)": "lat",
        "geographic location (longitude)": "lon",
        "geographic location (country and/or sea)": "location",
    }

    for sample_id, sample in samples.items():
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
        item["biosampleId"] = sample_id

        samples[sample_id] = item

    return samples

