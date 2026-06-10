"""TREC metadata normalisation helpers for Beam/Dataflow."""

from __future__ import annotations

import json
import re
from datetime import datetime
from typing import Iterable

PLACEHOLDERS = {
    "",
    "missing",
    "missing: control sample",
    "na",
    "n/a",
    "nan",
    "none",
    "not applicable",
    "not collected",
    "not provided",
    "null",
}

CUSTOM_FIELD_MAP = {
    "SRA accession": "ena_accession",
    "broad-scale environmental context": "biome",
    "environmental medium": "environmental_medium",
    "local environmental context": "local_environment",
    "sample collection device": "collection_device",
    "sampling platform": "sampling_platform",
}


def parse_trec_json_line(line: str) -> dict:
    record = json.loads(line)
    if not record.get("biosampleId"):
        raise ValueError("TREC record has no biosampleId")
    return record


def clean_value(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if value.lower() in PLACEHOLDERS:
            return None
    return value


def custom_field_lookup(record: dict) -> dict[str, object]:
    values = {}
    for field in record.get("customFields", []):
        if not isinstance(field, dict):
            continue

        name = field.get("name")
        value = clean_value(field.get("value"))
        if name and value is not None:
            values[name] = value

    return values


def parse_number(value):
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        match = re.search(r"[-+]?\d+(?:\.\d+)?", value)
        if match:
            return float(match.group(0))
    return None


def set_geo_location(record: dict) -> None:
    lat = record.get("lat")
    lon = record.get("lon")
    if lat is None or lon is None:
        record.pop("geo_location", None)
    else:
        record["geo_location"] = {"lat": lat, "lon": lon}


def parse_ontology_label(value):
    if not isinstance(value, str):
        return value

    match = re.match(r"^(.*?)(?:\s*\[.*\])?\s*$", value)
    return match.group(1).strip() if match else value.strip()


def parse_environment_type(value):
    if not value:
        return None

    lower = str(value).lower()
    if "marine" in lower or "ocean" in lower or "sea" in lower:
        return "marine"
    if "soil" in lower or "terrestrial" in lower or "land" in lower:
        return "soil"
    if "aerosol" in lower or "air" in lower or "atmosph" in lower:
        return "aerosol"

    return None


def parse_analysis_type(target_analysis, protocol_label):
    if target_analysis:
        lower = str(target_analysis).lower()
        if "metabolom" in lower:
            return "Metabolomics"
        if "imag" in lower or "microscop" in lower:
            return "Imaging"
        if "genom" in lower:
            return "Genomics"

    if protocol_label:
        label = str(protocol_label).strip()
        mapping = {
            "MetaBGT": "Metagenomics",
            "Metagenomics analysis": "Metagenomics",
            "Metabarcoding analysis": "Metagenomics",
            "MB": "Metabolomics",
            "MB320": "Metabolomics",
            "MB033": "Metabolomics",
            "MB20": "Metabolomics",
            "HPF": "Imaging",
            "PK1": "Imaging",
            "Microscopy": "Imaging",
            "Ions": "Ions",
            "ASM": "Metagenomics",
            "eDNA": "Metagenomics",
            "SML-023": "Genomics",
            "SML-CP": "Genomics",
            "SML-320": "Genomics",
            "Biodiversity analysis": "Metagenomics",
        }
        if label in mapping:
            return mapping[label]

        lower = label.lower()
        if "meta" in lower and "gen" in lower:
            return "Metagenomics"
        if "metab" in lower:
            return "Metabolomics"
        if "ion" in lower:
            return "Ions"
        if "micr" in lower or "imag" in lower or "hpf" in lower:
            return "Imaging"

    return None


def build_station_name(country, locality, lat, lon):
    if lat is None or lon is None:
        return None
    if locality and country:
        return f"{locality}, {country}"
    if locality:
        return locality
    if country:
        return (
            f"{country} ({lat:.2f}\N{DEGREE SIGN}N, "
            f"{lon:.2f}\N{DEGREE SIGN}E)"
        )
    return f"{lat:.2f}\N{DEGREE SIGN}N, {lon:.2f}\N{DEGREE SIGN}E"


def is_derived_from_relationship(relationship):
    return "derived from" in str(relationship.get("type") or "").lower()


def parse_collection_date(value):
    if not isinstance(value, str):
        return None

    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def extract_parent_child_relationships(record: dict) -> Iterable[tuple[str, str]]:
    child_id = record.get("biosampleId")
    if not child_id:
        return

    for relationship in record.get("relationships", []):
        if not isinstance(relationship, dict):
            continue
        if not is_derived_from_relationship(relationship):
            continue

        parent_id = relationship.get("target")
        if parent_id:
            yield parent_id, child_id


def normalise_trec_record(
    record: dict,
    derived_sample_ids_by_parent: dict[str, list[str]] | None = None,
) -> dict:
    record = dict(record)
    lookup = custom_field_lookup(record)
    derived_sample_ids_by_parent = derived_sample_ids_by_parent or {}

    for source_field, target_field in CUSTOM_FIELD_MAP.items():
        value = clean_value(lookup.get(source_field))
        if value is not None:
            record[target_field] = parse_ontology_label(value)

    country = clean_value(record.get("country") or record.get("location"))
    if country is not None:
        record["country"] = country

    parsed_date = parse_collection_date(record.get("collection_date"))
    if parsed_date:
        record["collection_date"] = parsed_date.isoformat()
        record["collection_year"] = str(parsed_date.year)
        record["collection_month"] = f"{parsed_date.month:02d}"

    for field in ("lat", "lon"):
        value = parse_number(record.get(field))
        if value is None:
            record.pop(field, None)
        else:
            record[field] = value
    set_geo_location(record)

    for source_field, target_field in [
        ("size-fraction lower threshold", "size_fraction_lower"),
        ("size-fraction upper threshold", "size_fraction_upper"),
    ]:
        value = parse_number(lookup.get(source_field))
        if value is not None:
            record[target_field] = value

    parent_ids = [
        relationship.get("target")
        for relationship in record.get("relationships", [])
        if isinstance(relationship, dict)
           and is_derived_from_relationship(relationship)
           and relationship.get("target")
    ]

    if parent_ids:
        record["parent_sample_id"] = parent_ids[0] if len(parent_ids) == 1 else parent_ids
    else:
        record.pop("parent_sample_id", None)

    record["is_source_sample"] = not parent_ids

    derived_sample_ids = derived_sample_ids_by_parent.get(record["biosampleId"])
    if derived_sample_ids:
        record["derived_sample_ids"] = sorted(derived_sample_ids)
    else:
        record.pop("derived_sample_ids", None)

    has_control = clean_value(lookup.get("has control"))
    if has_control is not None:
        record["control_sample_id"] = has_control

    is_control_of = clean_value(lookup.get("is control of"))
    if is_control_of is not None:
        record["controlled_sample_ids"] = [
            value.strip()
            for value in str(is_control_of).split(",")
            if clean_value(value) is not None
        ]

    analysis_type = parse_analysis_type(
        clean_value(lookup.get("target analysis type")),
        clean_value(lookup.get("protocol label")),
    )
    if analysis_type is None:
        record.pop("analysis_type", None)
    else:
        record["analysis_type"] = analysis_type

    if clean_value(record.get("environmental_medium")) is None:
        record.pop("environmental_medium", None)
    if clean_value(record.get("local_environment")) is None:
        record.pop("local_environment", None)

    station_name = build_station_name(
        country,
        clean_value(lookup.get("geographic location (region and locality)")),
        record.get("lat"),
        record.get("lon"),
    )
    if station_name is None:
        record.pop("station_name", None)
    else:
        record["station_name"] = station_name

    environment_type = parse_environment_type(
        clean_value(lookup.get("broad-scale environmental context"))
    )
    if environment_type is None:
        environment_type = parse_environment_type(record.get("organism"))

    if environment_type is None:
        record.pop("environment_type", None)
    else:
        record["environment_type"] = environment_type

    record["has_ena_data"] = bool(clean_value(record.get("ena_accession")))

    has_images = clean_value(record.get("has_images"))
    record["has_images"] = (
        "Yes" if str(has_images).lower() in {"true", "yes"} else "No"
    )

    return record
