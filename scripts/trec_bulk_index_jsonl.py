"""Bulk index TREC JSONL records into Elasticsearch.

This is a direct, controlled fallback for loading the refreshed TREC metadata
without Dataflow. It reads a local JSONL file produced by the Airflow metadata
task and indexes each record using biosampleId as the document id.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from getpass import getpass
from pathlib import Path
from typing import Iterator
from datetime import datetime

from elasticsearch import Elasticsearch, helpers


LIVE_INDEX = "data_portal_development_4"
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
    "geographic location (region and locality)": "station_name",
    "local environmental context": "local_environment",
    "sample collection device": "collection_device",
    "sampling platform": "sampling_platform",
    "target analysis type": "analysis_type",
}


def normalise_host(host: str) -> str:
    if host.startswith("http://") or host.startswith("https://"):
        return host
    return f"https://{host}"


def read_actions(
    path: Path,
    index_name: str,
    source_sample_ids: set[str],
    derived_sample_ids_by_parent: dict[str, list[str]],
) -> Iterator[dict]:
    with path.open("r", encoding="utf-8") as file:
        for line_number, line in enumerate(file, start=1):
            line = line.strip()
            if not line:
                continue

            record = json.loads(line)
            record_id = record.get("biosampleId")
            if not record_id:
                raise ValueError(f"Line {line_number} has no biosampleId")
            normalise_record(record, source_sample_ids, derived_sample_ids_by_parent)

            yield {
                "_op_type": "index",
                "_index": index_name,
                "_id": record_id,
                "_source": record,
            }


def clean_value(value):
    if value is None:
        return None
    if isinstance(value, str):
        value = value.strip()
        if value.lower() in PLACEHOLDERS:
            return None
    return value


def custom_field_lookup(record: dict) -> dict[str, str]:
    values = {}
    for field in record.get("customFields", []):
        name = field.get("name")
        value = clean_value(field.get("value"))
        if name and value is not None:
            values[name] = value
    return values


def parse_number(value):
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        match = re.search(r"[-+]?\d+(?:\.\d+)?", value)
        if match:
            return float(match.group(0))
    return None


def parse_collection_date(value):
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def normalise_record(
    record: dict,
    source_sample_ids: set[str],
    derived_sample_ids_by_parent: dict[str, list[str]],
) -> None:
    lookup = custom_field_lookup(record)

    for source_field, target_field in CUSTOM_FIELD_MAP.items():
        value = clean_value(lookup.get(source_field))
        if value is not None:
            record[target_field] = value

    country = clean_value(record.get("country") or record.get("location"))
    if country is not None:
        record["country"] = country

    collection_date = record.get("collection_date")
    parsed_date = parse_collection_date(collection_date)
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

    for source_field, target_field in [
        ("size-fraction lower threshold", "size_fraction_lower"),
        ("size-fraction upper threshold", "size_fraction_upper"),
    ]:
        value = parse_number(lookup.get(source_field))
        if value is not None:
            record[target_field] = value

    record_id = record["biosampleId"]
    record["is_source_sample"] = record_id in source_sample_ids
    if record_id in derived_sample_ids_by_parent:
        record["derived_sample_ids"] = sorted(derived_sample_ids_by_parent[record_id])

    parent_ids = [
        relationship.get("target")
        for relationship in record.get("relationships", [])
        if relationship.get("type") == "derived from" and relationship.get("target")
    ]
    if parent_ids:
        record["parent_sample_id"] = parent_ids[0] if len(parent_ids) == 1 else parent_ids

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

    if clean_value(record.get("analysis_type")) is None:
        record.pop("analysis_type", None)
    if clean_value(record.get("environmental_medium")) is None:
        record.pop("environmental_medium", None)
    if clean_value(record.get("local_environment")) is None:
        record.pop("local_environment", None)
    if clean_value(record.get("station_name")) is None:
        record.pop("station_name", None)

    if clean_value(record.get("environment_type")) is None:
        environment_type = clean_value(record.get("environmental_medium"))
        if environment_type is None:
            record.pop("environment_type", None)
        else:
            record["environment_type"] = environment_type

    record["has_ena_data"] = bool(clean_value(record.get("ena_accession")))
    record.setdefault("has_images", "false")


def collect_relationship_fields(path: Path) -> tuple[set[str], dict[str, list[str]]]:
    source_sample_ids = set()
    derived_sample_ids_by_parent: dict[str, list[str]] = {}

    with path.open("r", encoding="utf-8") as file:
        for line in file:
            if not line.strip():
                continue
            record = json.loads(line)
            child_id = record.get("biosampleId")
            for relationship in record.get("relationships", []):
                if relationship.get("type") != "derived from":
                    continue
                parent_id = relationship.get("target")
                if not parent_id:
                    continue
                source_sample_ids.add(parent_id)
                if child_id:
                    derived_sample_ids_by_parent.setdefault(parent_id, []).append(child_id)

    return source_sample_ids, derived_sample_ids_by_parent


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Bulk index refreshed TREC JSONL into Elasticsearch."
    )
    parser.add_argument("--input", required=True, help="Path to local TREC JSONL")
    parser.add_argument("--index", required=True, help="Target Elasticsearch index")
    parser.add_argument("--host", default=os.getenv("TREC_ES_HOST"))
    parser.add_argument("--password", default=os.getenv("TREC_ES_PASSWORD"))
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument(
        "--allow-live-index",
        action="store_true",
        help=f"Allow writing directly to {LIVE_INDEX}. Off by default.",
    )
    args = parser.parse_args()

    input_path = Path(args.input)
    if not input_path.exists():
        raise FileNotFoundError(input_path)

    if args.index == LIVE_INDEX and not args.allow_live_index:
        raise SystemExit(
            f"Refusing to write to live hardcoded index '{LIVE_INDEX}'. "
            "Use a dated/test index, or pass --allow-live-index deliberately."
        )

    if not args.host:
        raise SystemExit("Missing --host or TREC_ES_HOST")
    password = args.password or getpass("Elasticsearch password: ")

    es = Elasticsearch([normalise_host(args.host)], http_auth=("elastic", password))

    if not es.indices.exists(index=args.index):
        raise SystemExit(f"Target index does not exist: {args.index}")

    source_sample_ids, derived_sample_ids_by_parent = collect_relationship_fields(
        input_path
    )
    before_count = es.count(index=args.index)["count"]
    print(f"Target index: {args.index}")
    print(f"Count before: {before_count}")
    print(f"Derived source samples: {len(source_sample_ids)}")

    success_count = 0
    errors = []
    for ok, item in helpers.streaming_bulk(
        es,
        read_actions(
            input_path,
            args.index,
            source_sample_ids,
            derived_sample_ids_by_parent,
        ),
        chunk_size=args.batch_size,
        request_timeout=120,
        raise_on_error=False,
    ):
        if ok:
            success_count += 1
        else:
            errors.append(item)
            if len(errors) <= 5:
                print(f"Bulk error: {json.dumps(item)[:1000]}")

    if args.refresh:
        es.indices.refresh(index=args.index)

    after_count = es.count(index=args.index)["count"]
    print(f"Indexed successfully: {success_count}")
    print(f"Bulk errors: {len(errors)}")
    print(f"Count after: {after_count}")

    if errors:
        raise SystemExit("Bulk indexing completed with errors.")


if __name__ == "__main__":
    main()
