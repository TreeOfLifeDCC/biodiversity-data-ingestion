"""
Defines common functions to be used by map_functions
"""

import requests


def check_field_existence(record):
    values = list()
    units = list()
    ontology_terms = list()
    for element in record:
        values.append(element["text"])
        try:
            units.append(element["unit"])
        except KeyError:
            pass
        try:
            ontology_terms.append(element["ontologyTerms"][0])
        except KeyError:
            pass
    return ", ".join(values), ", ".join(units), ", ".join(ontology_terms)


def get_common_name(latin_name):
    url = f"https://www.ebi.ac.uk/ena/taxonomy/rest/scientific-name/{latin_name}"

    resp = None
    for attempt in range(3):
        try:
            resp = requests.get(url, timeout=30)
        except requests.exceptions.RequestException:
            continue  # network error / timeout -> retry
        if resp.status_code == 200:
            break
        # 429 / 5xx are transient -> retry; other 4xx -> give up
        if resp.status_code not in (429, 500, 502, 503, 504):
            return "Not specified"
    if resp is None or resp.status_code != 200:
        return "Not specified"

    body = resp.text.strip()
    if not body or body == "No results.":
        return "Not specified"

    try:
        data = resp.json()
    except requests.exceptions.JSONDecodeError:
        return "Not specified"

    if len(data) != 0 and "commonName" in data[0]:
        return data[0]["commonName"]
    return "Not specified"


def parse_data_records(records):
    experiments = list()
    assemblies = list()
    analyses = list()
    project_names = set()
    images_available = False
    for record in records:
        if "experiments" in record:
            experiments.extend(record["experiments"])
        if "assemblies" in record:
            assemblies.extend(record["assemblies"])
        if "analyses" in record:
            analyses.extend(record["analyses"])
        if "images_available" in record:
            images_available = record["images_available"]
        project_names.add(record["project_name"])
    return experiments, assemblies, analyses, list(project_names), images_available


def parse_data_records_dwh(records):
    experiments = list()
    assemblies = list()
    project_names = set()
    for record in records:
        if "experiments" in record:
            experiments.extend(record["experiments"])
        if "assemblies" in record:
            assmbl_formatted = list()
            for assmbl in record["assemblies"]:
                assmbl_formatted.append(
                    {
                        "accession": assmbl["accession"],
                        "description": assmbl["description"],
                    }
                )
            assemblies.extend(assmbl_formatted)
        project_names.add(record["project_name"])
    return experiments, assemblies, list(project_names)


def remove_duplicated_metadata_records(metadata_records):
    visited_ids = {}
    new_records = {}
    ranks = {
        "Submitted to BioSamples": 1,
        "Raw Data - Submitted": 2,
        "Assemblies - Submitted": 3,
    }
    for sample in metadata_records:
        if sample["accession"] not in visited_ids:
            visited_ids[sample["accession"]] = ranks[sample["trackingSystem"]]
            new_records[sample["accession"]] = sample
        else:
            if ranks[sample["trackingSystem"]] > visited_ids[sample["accession"]]:
                visited_ids[sample["accession"]] = ranks[sample["trackingSystem"]]
                new_records[sample["accession"]] = sample
    return list(new_records.values())


def remove_duplicated_data_records(data_records, accession_name):
    visited_ids = set()
    new_ids = list()
    for item in data_records:
        if item[accession_name] not in visited_ids:
            visited_ids.add(item[accession_name])
            new_ids.append(item)
    return new_ids
