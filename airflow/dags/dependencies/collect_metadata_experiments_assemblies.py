import json
from collections import defaultdict
from typing import Any
from time import sleep
import asyncio
import aiohttp

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
        timeout=3600,
    ).json()

    sleep(0.1)

    # Collect all assemblies
    assemblies = requests.get(
        f"{ena_root_url}?accession={study_id}&result=assembly"
        f"&fields={','.join(assemblies_fields)}&format=json&limit=0",
        timeout=3600,
    ).json()

    sleep(0.1)

    # Collect all analyses
    analyses = requests.get(
        f"{ena_root_url}?accession={study_id}&result=analysis"
        f"&fields={','.join(analysis_fields)}&format=json&limit=0",
        timeout=3600,
    ).json()

    sleep(0.1)

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
        samples_response = requests.get(first_url, timeout=3600).json()
        sleep(0.1)
        while "_embedded" in samples_response:
            for sample in samples_response["_embedded"]["samples"]:
                sample["project_name"] = project_tag
                samples[sample["accession"]] = sample
            if "next" in samples_response["_links"]:
                samples_response = requests.get(
                    samples_response["_links"]["next"]["href"], timeout=3600
                ).json()
                sleep(0.1)
            else:
                samples_response = requests.get(
                    samples_response["_links"]["last"]["href"], timeout=3600
                ).json()
                sleep(0.1)

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
            host_sample_id = record["characteristics"]["sample derived from"][0]["text"]
            if (
                host_sample_id not in samples
                and host_sample_id not in additional_samples
            ):
                try:
                    additional_samples[host_sample_id] = requests.get(
                        f"{biosamples_root_url}/{host_sample_id}", timeout=3600
                    ).json()
                    sleep(0.1)
                except json.decoder.JSONDecodeError:
                    print(f"json decode error for {host_sample_id}")
                    continue
        elif "sample symbiont of" in record["characteristics"]:
            host_sample_id = record["characteristics"]["sample symbiont of"][0]["text"]
            if (
                host_sample_id not in samples
                and host_sample_id not in additional_samples
            ):
                try:
                    additional_samples[host_sample_id] = requests.get(
                        f"{biosamples_root_url}/{host_sample_id}", timeout=3600
                    ).json()
                    sleep(0.1)
                except json.decoder.JSONDecodeError:
                    print(f"json decode error for {host_sample_id}")
                    continue
    for sample_id, record in additional_samples.items():
        record["project_name"] = project_tag
        samples[sample_id] = record

    if project_tag == "AEGIS":
        unique_tax_ids = set()
        for sample in samples.values():
            tax_id = sample.get("taxId")
            if tax_id:
                unique_tax_ids.add(tax_id)

        phylogenies = asyncio.run(fetch_taxonomy_for_samples(unique_tax_ids))

        for sample in samples.values():
            tax_id = sample.get("taxId")
            sample["taxonomy"] = phylogenies.get(tax_id, {})


    return samples


def parse_data_records(aggr_var: defaultdict[Any, list], records_data: dict) -> None:
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
                        f"{biosamples_root_url}/{sample_id}", timeout=3600
                    ).json()
                    sleep(0.1)
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



async def fetch_lineage(session, tax_id):
    url = f"https://www.ebi.ac.uk/ena/taxonomy/rest/tax-id/{tax_id}"
    async with session.get(url) as response:
        data = await response.json(content_type=None)
        lineage_string = data.get("lineage", "")
        names = [n.strip() for n in lineage_string.split(";") if n.strip()]
        return tax_id, names

async def fetch_rank(session, name):
    url = f"https://www.ebi.ac.uk/ena/taxonomy/rest/scientific-name/{name}"
    async with session.get(url) as response:
        data = await response.json(content_type=None)
        if isinstance(data, list) and len(data) > 0:
            return name, data[0].get("rank", None)
        return name, None

async def fetch_taxonomy_for_samples(tax_ids):
    target_ranks = {'kingdom', 'phylum', 'class', 'order', 'family', 'genus'}

    async with aiohttp.ClientSession() as session:
        lineage_results = await asyncio.gather(
            *[fetch_lineage(session, tax_id) for tax_id in tax_ids]
        )
    lineages = dict(lineage_results)

    all_names = set()
    for names in lineages.values():
        all_names.update(names)

    async with aiohttp.ClientSession() as session:
        rank_results = await asyncio.gather(
            *[fetch_rank(session, name) for name in all_names]
        )
    ranks = dict(rank_results)

    phylogenies = {}
    for tax_id, names in lineages.items():
        phylogeny = {}
        for name in names:
            rank = ranks.get(name)
            if rank in target_ranks:
                phylogeny[rank] = name
        phylogenies[tax_id] = phylogeny

    return phylogenies

