import json
from collections import defaultdict
from typing import Any

import requests

from .samples_schema import samples_schema


def main(study_id: str, project_tag: str, project_name: str) -> dict[str, dict]:
    """
    Collect DToL metadata from BioSamples, experiments, assemblies and analyses
    from the ENA
    """
    experiments_aggr: defaultdict[str, list] = defaultdict(list)
    assemblies_aggr: defaultdict[str, list] = defaultdict(list)
    analyses_aggr: defaultdict[str, list] = defaultdict(list)
    samples: dict[str, dict] = {}

    ena_root_url = "https://www.ebi.ac.uk/ena/portal/api/filereport"
    biosamples_root_url = "https://www.ebi.ac.uk/biosamples/samples"

    # Collect required experiments, assemblies and analyses fields
    experiment_fields: list[str | None] = []
    assemblies_fields: list[str | None] = []
    analysis_fields: list[str | None] = []

    for field in samples_schema["fields"]:
        if field["name"] == "experiments":
            for experiment_field in field["fields"]:
                experiment_fields.append(experiment_field["name"])
        elif field["name"] == "assemblies":
            for assemblies_field in field["fields"]:
                assemblies_fields.append(assemblies_field["name"])
        elif field["name"] == "analyses":
            for analysis_field in field["fields"]:
                analysis_fields.append(analysis_field["name"])

    # Collect all experiments
    raw_data = requests.get(
        f"{ena_root_url}?accession={study_id}&result=read_run"
        f"&fields={','.join(experiment_fields)}&format=json&limit=0",
        timeout=60,
    ).json()

    # Collect all assemblies
    assemblies = requests.get(
        f"{ena_root_url}?accession={study_id}&result=assembly"
        f"&fields={','.join(assemblies_fields)}&format=json&limit=0",
        timeout=60,
    ).json()

    # Collect all analyses
    analyses = requests.get(
        f"{ena_root_url}?accession={study_id}&result=analysis"
        f"&fields={','.join(analysis_fields)}&format=json&limit=0",
        timeout=60,
    ).json()

    # aggregate experiments, assemblies and analyses in the
    # dict(key: biosample_id, value: data record)
    for aggr_var, records_data in [
        (experiments_aggr, raw_data),
        (assemblies_aggr, assemblies),
        (analyses_aggr, analyses),
    ]:
        parse_data_records(aggr_var, records_data)

    # collect metadata from the BioSamples
    if project_tag in ["ASG", "DTOL", "ERGA"]:
        first_url = (
            f"{biosamples_root_url}?size=200&filter="
            f"attr%3Aproject%20name%3A{project_tag}"
        )
        samples_response = requests.get(first_url, timeout=60).json()
        while "_embedded" in samples_response:
            for sample in samples_response["_embedded"]["samples"]:
                sample["project_name"] = project_tag
                samples[sample["accession"]] = sample
            if "next" in samples_response["_links"]:
                samples_response = requests.get(
                    samples_response["_links"]["next"]["href"], timeout=60
                ).json()
            else:
                samples_response = requests.get(
                    samples_response["_links"]["last"]["href"], timeout=60
                ).json()

    # join metadata and data records
    for record_type, agg_name in {
        "experiments": experiments_aggr,
        "assemblies": assemblies_aggr,
        "analyses": analyses_aggr,
    }.items():
        join_metadata_and_data(
            record_type, agg_name, project_name, samples, biosamples_root_url
        )

    # check for missing child -> parent relationship records
    additional_samples = dict()
    for sample_id, record in samples.items():
        if "sample derived from" in record["characteristics"]:
            host_sample_id = \
            record["characteristics"]["sample derived from"][0]["text"]
            if (
                    host_sample_id not in samples
                    and host_sample_id not in additional_samples
            ):
                try:
                    additional_samples[host_sample_id] = requests.get(
                        f"{biosamples_root_url}/{host_sample_id}", timeout=60
                    ).json()
                except json.decoder.JSONDecodeError:
                    print(f"json decode error for {host_sample_id}")
                    continue
        elif "sample symbiont of" in record["characteristics"]:
            host_sample_id = record["characteristics"]["sample symbiont of"][0][
                "text"]
            if (
                    host_sample_id not in samples
                    and host_sample_id not in additional_samples
            ):
                try:
                    additional_samples[host_sample_id] = requests.get(
                        f"{biosamples_root_url}/{host_sample_id}", timeout=60
                    ).json()
                except json.decoder.JSONDecodeError:
                    print(f"json decode error for {host_sample_id}")
                    continue
    for sample_id, record in additional_samples.items():
        record["project_name"] = project_tag
        samples[sample_id] = record

    return samples


def parse_data_records(aggr_var: defaultdict[Any, list],
                       records_data: dict) -> None:
    """
    Parse data records from ENA into python dict(key: biosample_id, \
        value: data_record)
    :param aggr_var: variable to aggregate data records
    :param records_data: experiments and assemblies from ENA
    """
    for record in records_data:
        if record["sample_accession"] != "":
            aggr_var[record["sample_accession"]].append(record)


def join_metadata_and_data(
        records_type: str,
        records_data: dict,
        project_name: str,
        samples: dict[str, dict],
        biosamples_root_url: str,
) -> None:
    """
    Join records from BioSamples and ENA into python dict(key: biosample_id, \
        value: record)
    :param records_type: can be of type experiment or assemblies
    :param records_data: experiments and assemblies from ENA
    :param project_name: name of the project to import
    :param samples: existing samples data
    :param biosamples_root_url: biosamples root url

    """
    if records_type not in ["experiments", "assemblies", "analyses"]:
        raise ValueError(
            "records_type must be either 'experiments' or \
                         'assemblies'"
        )
    else:
        for sample_id, data in records_data.items():
            if sample_id not in samples:
                try:
                    response = requests.get(
                        f"{biosamples_root_url}/{sample_id}", timeout=60
                    ).json()
                    if response["status"] == 403:
                        continue
                    samples[sample_id] = response
                    samples[sample_id]["project_name"] = project_name
                    samples[sample_id][records_type] = data
                except requests.exceptions.JSONDecodeError:
                    continue
            else:
                samples[sample_id].setdefault(records_type, [])
                samples[sample_id][records_type].extend(data)
                samples[sample_id]["project_name"] = project_name
