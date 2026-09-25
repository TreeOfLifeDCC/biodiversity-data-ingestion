import json
import re
import time
import requests

from collections import Counter, defaultdict
from datetime import datetime
from elasticsearch import Elasticsearch
from email.utils import parsedate_to_datetime
from google.cloud import bigquery
from typing import Any


# Constants
ACCESSION_RE = re.compile(r"^(GCA_\d+)(?:\.(\d+))?$")
# Assembly classification constants
EXPLICIT_MAIN_ASSEMBLY = "EXPLICIT_MAIN_ASSEMBLY"
HAPLOTYPE_OR_ALTERNATE_ASSEMBLY = "HAPLOTYPE_OR_ALTERNATE_ASSEMBLY"
UNMARKED_ASSEMBLY_CANDIDATE = "UNMARKED_ASSEMBLY_CANDIDATE"
NO_MATCHING_ASSEMBLY_METADATA = "NO_MATCHING_ASSEMBLY_METADATA"
# GTF constants
SELECTED = "SELECTED"
REJECTED_NO_VALID_ANNOTATION_CANDIDATES = (
    "REJECTED_NO_VALID_ANNOTATION_CANDIDATES"
)
REJECTED_MISSING_GTF_URL = "REJECTED_MISSING_GTF_URL"
REJECTED_NO_SELECTABLE_ANNOTATION = "REJECTED_NO_SELECTABLE_ANNOTATION"
# Other constants
NEW_FTP_PATTERN = "NEW_FTP_PATTERN"
OLD_FTP_PATTERN = "OLD_FTP_PATTERN"
PRE_RELEASE_PATTERN = "PRE_RELEASE_PATTERN"
UNKNOWN_GTF_PATTERN = "UNKNOWN_GTF_PATTERN"

NO_UPDATE = "NO_UPDATE"
SKIP_PATTERN_ONLY_SAME_SIZE = "SKIP_PATTERN_ONLY_SAME_SIZE"
UPDATE_ACCESSION_CHANGED = "UPDATE_ACCESSION_CHANGED"
UPDATE_GTF_URL_CHANGED = "UPDATE_GTF_URL_CHANGED"
UPDATE_PRE_RELEASE = "UPDATE_PRE_RELEASE"
UPDATE_PROVENANCE_ONLY = "UPDATE_PROVENANCE_ONLY"
UPDATE_GTF_SIZE_CHANGED = "UPDATE_GTF_SIZE_CHANGED"
UPDATE_GTF_SIZE_UNKNOWN = "UPDATE_GTF_SIZE_UNKNOWN"



# main selection logic
def parse_accession(accession: str | None) -> tuple[str | None, int | None]:
    if not accession:
        return None, None

    match = ACCESSION_RE.fullmatch(accession)
    if not match:
        return None, None

    root = match.group(1)
    version = int(match.group(2)) if match.group(2) is not None else None
    return root, version


def classify_assembly(
    assembly_name: str | None,
    description: str | None,
) -> str:
    text = " ".join(
        value.lower()
        for value in [assembly_name, description]
        if value
    )

    if not text:
        return NO_MATCHING_ASSEMBLY_METADATA

    if any(
        token in text
        for token in ["hap", "haplotype", ".alt", " alt", "alternate"]
    ):
        return HAPLOTYPE_OR_ALTERNATE_ASSEMBLY

    if any(
        token in text
        for token in [".pri", "_pri", " prim", "primary", "_prim"]
    ):
        return EXPLICIT_MAIN_ASSEMBLY

    return UNMARKED_ASSEMBLY_CANDIDATE


def group_candidates_by_root(
    candidates: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    candidates_by_root: dict[str, list[dict[str, Any]]] = {}

    for candidate in candidates:
        candidates_by_root.setdefault(
            candidate["accession_root"],
            [],
        ).append(candidate)

    return candidates_by_root


def choose_highest_version_from_candidates(
    candidates: list[dict[str, Any]],
    selection_reason: str,
) -> dict[str, Any]:
    selected = max(
        candidates,
        key=lambda candidate: candidate["accession_version"],
    )

    return {
        "status": SELECTED,
        "selection_reason": selection_reason,
        **selected,
    }


def choose_by_root_highest_version(
    candidates: list[dict[str, Any]],
    highest_version_reason: str,
    tie_reason: str,
) -> dict[str, Any]:
    candidates_by_root = group_candidates_by_root(candidates)
    root_order = []

    for candidate in candidates:
        root = candidate["accession_root"]
        if root not in root_order:
            root_order.append(root)

    root_summaries = []
    for root, root_candidates in candidates_by_root.items():
        highest_candidate = max(
            root_candidates,
            key=lambda candidate: candidate["accession_version"],
        )
        root_summaries.append(
            {
                "root": root,
                "highest_version": highest_candidate["accession_version"],
                "highest_candidate": highest_candidate,
                "first_seen_order": root_order.index(root),
            }
        )

    max_version = max(summary["highest_version"] for summary in root_summaries)
    best_roots = [
        summary
        for summary in root_summaries
        if summary["highest_version"] == max_version
    ]

    if len(best_roots) == 1:
        selected = best_roots[0]["highest_candidate"]
        return {
            "status": SELECTED,
            "selection_reason": highest_version_reason,
            **selected,
        }

    selected_summary = min(
        best_roots,
        key=lambda summary: summary["first_seen_order"],
    )

    return {
        "status": SELECTED,
        "selection_reason": tie_reason,
        **selected_summary["highest_candidate"],
    }


def choose_hap1_or_fallback(
    candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    hap1_candidates = [
        candidate
        for candidate in candidates
        if "hap1" in " ".join(
            str(value).lower()
            for value in [
                candidate.get("assembly_name"),
                candidate.get("assembly_description"),
            ]
            if value
        )
    ]

    if hap1_candidates:
        return choose_by_root_highest_version(
            hap1_candidates,
            highest_version_reason=(
                "selected_only_non_main_hap1_highest_version"
            ),
            tie_reason="selected_only_non_main_hap1_tie_first_annotation_order",
        )

    return choose_by_root_highest_version(
        candidates,
        highest_version_reason="selected_only_non_main_highest_version",
        tie_reason="selected_only_non_main_tie_first_annotation_order",
    )


def select_latest_main_annotation(source: dict[str, Any]) -> dict[str, Any]:
    annotations = source.get("annotation") or []
    assemblies = source.get("assemblies") or []

    assemblies_by_root = {}
    for assembly in assemblies:
        root, version_from_accession = parse_accession(assembly.get("accession"))
        if not root:
            continue

        assemblies_by_root[root] = {
            **assembly,
            "accession_root": root,
            "assembly_version": assembly.get("version") or version_from_accession,
            "classification": classify_assembly(
                assembly.get("assembly_name"),
                assembly.get("description"),
            ),
        }

    candidates = []

    for annotation in annotations:
        accession = annotation.get("accession")
        accession_root, accession_version = parse_accession(accession)
        annotation_files = annotation.get("annotation") or {}
        gtf_url = annotation_files.get("GTF")

        if not accession_root or accession_version is None:
            continue

        assembly = assemblies_by_root.get(accession_root)

        if assembly:
            classification = assembly["classification"]
            assembly_name = assembly.get("assembly_name")
            assembly_description = assembly.get("description")
        else:
            classification = NO_MATCHING_ASSEMBLY_METADATA
            assembly_name = None
            assembly_description = None

        candidates.append(
            {
                "tax_id": source.get("tax_id"),
                "species": (
                    annotation.get("species")
                    or source.get("organism")
                    or source.get("scientific_name")
                ),
                "accession": accession,
                "accession_root": accession_root,
                "accession_version": accession_version,
                "gtf_url": gtf_url,
                "assembly_name": assembly_name,
                "assembly_description": assembly_description,
                "assembly_classification": classification,
                "view_in_browser": annotation.get("view_in_browser"),
                "annotation_method": annotation.get("annotation_method"),
                "busco_score": annotation.get("busco_score"),
            }
        )

    if not candidates:
        return {
            "status": REJECTED_NO_VALID_ANNOTATION_CANDIDATES,
            "tax_id": source.get("tax_id"),
            "organism": source.get("organism") or source.get("scientific_name"),
        }

    candidates_with_gtf = [
        candidate
        for candidate in candidates
        if candidate.get("gtf_url")
    ]

    if not candidates_with_gtf:
        return {
            "status": REJECTED_MISSING_GTF_URL,
            "tax_id": source.get("tax_id"),
            "candidate_accessions": [
                candidate["accession"]
                for candidate in candidates
            ],
        }

    explicit_main = [
        candidate
        for candidate in candidates_with_gtf
        if candidate["assembly_classification"] == EXPLICIT_MAIN_ASSEMBLY
    ]

    if explicit_main:
        return choose_highest_version_from_candidates(
            explicit_main,
            selection_reason=(
                "selected_explicit_main_highest_accession_version"
            ),
        )

    unmarked = [
        candidate
        for candidate in candidates_with_gtf
        if candidate["assembly_classification"] == UNMARKED_ASSEMBLY_CANDIDATE
    ]

    if unmarked:
        return choose_by_root_highest_version(
            unmarked,
            highest_version_reason="selected_unmarked_assembly_highest_version",
            tie_reason="selected_unmarked_assembly_tie_first_annotation_order",
        )

    no_assembly_metadata = [
        candidate
        for candidate in candidates_with_gtf
        if candidate["assembly_classification"] == NO_MATCHING_ASSEMBLY_METADATA
    ]

    if no_assembly_metadata:
        return choose_by_root_highest_version(
            no_assembly_metadata,
            highest_version_reason=(
                "selected_no_assembly_metadata_highest_version"
            ),
            tie_reason=(
                "selected_no_assembly_metadata_tie_first_annotation_order"
            ),
        )

    non_main = [
        candidate
        for candidate in candidates_with_gtf
        if (
            candidate["assembly_classification"]
            == HAPLOTYPE_OR_ALTERNATE_ASSEMBLY
        )
    ]

    if non_main:
        if len(non_main) == 1:
            return {
                "status": SELECTED,
                "selection_reason": "selected_only_non_main_single_candidate",
                **non_main[0],
            }

        return choose_hap1_or_fallback(non_main)

    return {
        "status": REJECTED_NO_SELECTABLE_ANNOTATION,
        "tax_id": source.get("tax_id"),
        "candidate_accessions": [
            candidate["accession"]
            for candidate in candidates_with_gtf
        ],
    }


def summarize_annotation_selection(
    es_host: str,
    es_password: str,
    index: str = "data_portal",
    page_size: int = 500,
) -> dict:
    host = es_host if es_host.startswith("http") else f"https://{es_host}"

    es = Elasticsearch(
        hosts=[host],
        basic_auth=("elastic", es_password),
        request_timeout=30,
        retry_on_timeout=True,
        max_retries=3,
    )

    status_counts = Counter()
    reason_counts = Counter()
    examples_by_status = defaultdict(list)

    pit_id = es.open_point_in_time(index=index, keep_alive="2m")["id"]

    body = {
        "size": page_size,
        "query": {"term": {"annotation_complete": "Done"}},
        "_source": [
            "tax_id",
            "organism",
            "scientific_name",
            "annotation.accession",
            "annotation.species",
            "annotation.annotation.GTF",
            "annotation.view_in_browser",
            "annotation.annotation_method",
            "annotation.busco_score",
            "assemblies.accession",
            "assemblies.assembly_name",
            "assemblies.description",
            "assemblies.version",
            "assemblies.last_updated",
        ],
        "sort": [{"_shard_doc": "asc"}],
        "pit": {"id": pit_id, "keep_alive": "2m"},
    }

    total = 0

    try:
        while True:
            response = es.search(body=body)
            hits = response["hits"]["hits"]

            if not hits:
                break

            for hit in hits:
                source = hit["_source"]
                result = select_latest_main_annotation(source)

                total += 1
                status = result.get("status", "UNKNOWN")
                reason = result.get("selection_reason", "NO_REASON")

                status_counts[status] += 1
                reason_counts[reason] += 1

                if len(examples_by_status[status]) < 10:
                    examples_by_status[status].append(
                        {
                            "es_id": hit.get("_id"),
                            "tax_id": source.get("tax_id"),
                            "organism": source.get("organism") or source.get("scientific_name"),
                            "result": result,
                        }
                    )

            body["search_after"] = hits[-1]["sort"]
            body["pit"]["id"] = response.get("pit_id", body["pit"]["id"])

    finally:
        es.close_point_in_time(id=body["pit"]["id"])

    summary = {
        "total": total,
        "status_counts": dict(status_counts),
        "reason_counts": dict(reason_counts),
        "examples_by_status": dict(examples_by_status),
    }

    print(json.dumps(summary, indent=2, sort_keys=True))
    return summary


# Helpers for exploration
def inspect_annotation_records(
    es_host: str,
    es_password: str,
    index: str = "data_portal",
    tax_ids: list[str | int] | None = None,
    size: int = 20,
):
    """
    Inspect annotation records in Elasticsearch.

    Joins annotation accessions to assemblies by accession root:
      annotation.accession = GCA_024256425.2
      assemblies.accession = GCA_024256425

    This helps identify whether assembly_name / description can distinguish
    main assemblies from haplotype or alternate assemblies.
    """
    host = es_host if es_host.startswith("http") else f"https://{es_host}"

    es = Elasticsearch(
        hosts=[host],
        basic_auth=("elastic", es_password),
        request_timeout=30,
        retry_on_timeout=True,
        max_retries=3,
    )

    if tax_ids:
        query = {"terms": {"tax_id": [str(t) for t in tax_ids]}}
    else:
        query = {"exists": {"field": "annotation"}}

    response = es.search(
        index=index,
        size=size,
        query=query,
        _source=[
            "tax_id",
            "organism",
            "scientific_name",
            "currentStatus",
            "current_status",
            "annotation.accession",
            "annotation.species",
            "annotation.annotation.GTF",
            "annotation.annotation.GFF3",
            "annotation.view_in_browser",
            "annotation.annotation_method",
            "annotation.busco_score",
            "assemblies.accession",
            "assemblies.assembly_name",
            "assemblies.description",
            "assemblies.version",
            "assemblies.last_updated",
        ],
    )

    for hit in response["hits"]["hits"]:
        source = hit["_source"]
        annotations = source.get("annotation") or []
        assemblies = source.get("assemblies") or []

        assemblies_by_root = {}
        for asm in assemblies:
            root, version_from_accession = parse_accession(asm.get("accession"))
            if not root:
                continue

            assemblies_by_root[root] = {
                **asm,
                "accession_root": root,
                "assembly_version": asm.get("version") or version_from_accession,
            }

        annotation_roots = set()
        for ann in annotations:
            root, _ = parse_accession(ann.get("accession"))
            if root:
                annotation_roots.add(root)

        print("=" * 100)
        print(f"_id: {hit['_id']}")
        print(f"tax_id: {source.get('tax_id')}")
        print(f"organism: {source.get('organism') or source.get('scientific_name')}")
        print(f"status: {source.get('currentStatus') or source.get('current_status')}")
        print(f"annotation_count: {len(annotations)}")
        print(f"assembly_count: {len(assemblies)}")

        print("\nANNOTATIONS JOINED TO ASSEMBLIES BY ACCESSION ROOT")
        for i, ann in enumerate(annotations):
            accession = ann.get("accession")
            accession_root, accession_version = parse_accession(accession)
            annotation_files = ann.get("annotation") or {}

            asm = assemblies_by_root.get(accession_root) or {}
            assembly_classification = classify_assembly(
                asm.get("assembly_name"),
                asm.get("description"),
            )

            marker = (
                "  <-- LAST / current downstream latest"
                if i == len(annotations) - 1
                else ""
            )

            print("-" * 80)
            print(f"[{i}]{marker}")
            print(f"  accession: {accession}")
            print(f"  accession_root: {accession_root}")
            print(f"  accession_version: {accession_version}")
            print(f"  species: {ann.get('species')}")
            print(f"  GTF: {annotation_files.get('GTF')}")
            print(f"  GFF3: {annotation_files.get('GFF3')}")
            print(f"  browser: {ann.get('view_in_browser')}")
            print(f"  method: {ann.get('annotation_method')}")
            print(f"  busco_score: {ann.get('busco_score')}")
            print("  assembly:")
            print(f"    classification: {assembly_classification}")
            print(f"    accession: {asm.get('accession')}")
            print(f"    assembly_name: {asm.get('assembly_name')}")
            print(f"    description: {asm.get('description')}")
            print(f"    version: {asm.get('version')}")
            print(f"    last_updated: {asm.get('last_updated')}")

        unmatched_assemblies = []
        for asm in assemblies:
            root, _ = parse_accession(asm.get("accession"))
            if root and root not in annotation_roots:
                unmatched_assemblies.append(asm)

        if unmatched_assemblies:
            print("\nASSEMBLIES WITHOUT ANNOTATION ENTRY")
            for asm in unmatched_assemblies[:10]:
                classification = classify_assembly(
                    asm.get("assembly_name"),
                    asm.get("description"),
                )
                print("-" * 80)
                print(f"  accession: {asm.get('accession')}")
                print(f"  classification: {classification}")
                print(f"  assembly_name: {asm.get('assembly_name')}")
                print(f"  description: {asm.get('description')}")
                print(f"  version: {asm.get('version')}")
                print(f"  last_updated: {asm.get('last_updated')}")

            if len(unmatched_assemblies) > 10:
                print(f"  ... {len(unmatched_assemblies) - 10} more unmatched assemblies")

        selected = select_latest_main_annotation(source)
        print("\nSELECTED LATEST MAIN ANNOTATION")
        print(json.dumps(selected, indent=2, sort_keys=True))


def inspect_only_non_main_annotations(
    es_host: str,
    es_password: str,
    index: str = "data_portal",
    limit: int = 20,
    page_size: int = 500,
):
    host = es_host if es_host.startswith("http") else f"https://{es_host}"

    es = Elasticsearch(
        hosts=[host],
        basic_auth=("elastic", es_password),
        request_timeout=30,
        retry_on_timeout=True,
        max_retries=3,
    )

    pit_id = es.open_point_in_time(index=index, keep_alive="2m")["id"]

    body = {
        "size": page_size,
        "query": {"term": {"annotation_complete": "Done"}},
        "_source": [
            "tax_id",
            "organism",
            "scientific_name",
            "annotation.accession",
            "annotation.species",
            "annotation.annotation.GTF",
            "annotation.annotation_method",
            "annotation.busco_score",
            "annotation.view_in_browser",
            "assemblies.accession",
            "assemblies.assembly_name",
            "assemblies.description",
            "assemblies.version",
            "assemblies.last_updated",
        ],
        "sort": [{"_shard_doc": "asc"}],
        "pit": {"id": pit_id, "keep_alive": "2m"},
    }

    printed = 0

    try:
        while printed < limit:
            response = es.search(body=body)
            hits = response["hits"]["hits"]

            if not hits:
                break

            for hit in hits:
                source = hit["_source"]
                result = select_latest_main_annotation(source)

                if result.get("status") != "ONLY_NON_MAIN_ANNOTATIONS":
                    continue

                annotations = source.get("annotation") or []
                assemblies = source.get("assemblies") or []

                assemblies_by_root = {}
                for asm in assemblies:
                    root, _ = parse_accession(asm.get("accession"))
                    if not root:
                        continue
                    assemblies_by_root[root] = asm

                print("=" * 100)
                print(f"es_id: {hit.get('_id')}")
                print(f"tax_id: {source.get('tax_id')}")
                print(f"organism: {source.get('organism') or source.get('scientific_name')}")
                print(f"annotation_count: {len(annotations)}")
                print(f"assembly_count: {len(assemblies)}")
                print("candidate_accessions:", result.get("candidate_accessions"))

                for ann in annotations:
                    accession = ann.get("accession")
                    root, version = parse_accession(accession)
                    asm = assemblies_by_root.get(root) or {}
                    classification = classify_assembly(
                        asm.get("assembly_name"),
                        asm.get("description"),
                    )
                    annotation_files = ann.get("annotation") or {}

                    print("-" * 80)
                    print(f"annotation_accession: {accession}")
                    print(f"accession_root: {root}")
                    print(f"accession_version: {version}")
                    print(f"gtf: {annotation_files.get('GTF')}")
                    print(f"annotation_method: {ann.get('annotation_method')}")
                    print(f"busco_score: {ann.get('busco_score')}")
                    print(f"assembly_accession: {asm.get('accession')}")
                    print(f"assembly_name: {asm.get('assembly_name')}")
                    print(f"description: {asm.get('description')}")
                    print(f"assembly_version: {asm.get('version')}")
                    print(f"last_updated: {asm.get('last_updated')}")
                    print(f"classification: {classification}")

                printed += 1

                if printed >= limit:
                    break

            body["search_after"] = hits[-1]["sort"]
            body["pit"]["id"] = response.get("pit_id", body["pit"]["id"])

    finally:
        es.close_point_in_time(id=body["pit"]["id"])


def print_ambiguous_no_assembly_metadata(
    es_host: str,
    es_password: str,
    index: str = "data_portal",
    page_size: int = 500,
):
    host = es_host if es_host.startswith("http") else f"https://{es_host}"

    es = Elasticsearch(
        hosts=[host],
        basic_auth=("elastic", es_password),
        request_timeout=30,
        retry_on_timeout=True,
        max_retries=3,
    )

    pit_id = es.open_point_in_time(index=index, keep_alive="2m")["id"]

    body = {
        "size": page_size,
        "query": {"term": {"annotation_complete": "Done"}},
        "_source": [
            "tax_id",
            "organism",
            "scientific_name",
            "annotation.accession",
            "annotation.annotation.GTF",
            "assemblies.accession",
            "assemblies.assembly_name",
            "assemblies.description",
            "assemblies.version",
        ],
        "sort": [{"_shard_doc": "asc"}],
        "pit": {"id": pit_id, "keep_alive": "2m"},
    }

    try:
        while True:
            response = es.search(body=body)
            hits = response["hits"]["hits"]

            if not hits:
                break

            for hit in hits:
                source = hit["_source"]
                result = select_latest_main_annotation(source)

                if result.get("status") == "AMBIGUOUS_NO_ASSEMBLY_METADATA":
                    print(
                        json.dumps(
                            {
                                "tax_id": source.get("tax_id"),
                                "organism": source.get("organism") or source.get("scientific_name"),
                                "accessions": result.get("candidate_accessions", []),
                                # "candidate_roots": result.get("candidate_roots", []),
                            },
                            sort_keys=True,
                        )
                    )

            body["search_after"] = hits[-1]["sort"]
            body["pit"]["id"] = response.get("pit_id", body["pit"]["id"])

    finally:
        es.close_point_in_time(id=body["pit"]["id"])

# ES and BQ comparison

def fetch_bq_provenance_by_tax_id(
    project_id: str,
    dataset: str,
    table: str = "bp_provenance_metadata",
) -> dict[str, dict]:
    client = bigquery.Client(project=project_id)

    query = f"""
        SELECT
          CAST(tax_id AS STRING) AS tax_id,
          accession,
          GTF,
          Biodiversity_portal,
          Ensembl_browser,
          gbif_url
        FROM `{project_id}.{dataset}.{table}`
        WHERE tax_id IS NOT NULL
    """

    rows = client.query(query).result()

    by_tax_id = {}
    duplicates = {}

    for row in rows:
        record = dict(row.items())
        tax_id = record["tax_id"]

        if tax_id in by_tax_id:
            duplicates.setdefault(tax_id, [by_tax_id[tax_id]]).append(record)
            continue

        by_tax_id[tax_id] = record

    if duplicates:
        print(
            json.dumps(
                {
                    "warning": "duplicate_tax_ids_in_bp_provenance_metadata",
                    "duplicate_count": len(duplicates),
                    "sample_tax_ids": list(duplicates)[:20],
                },
                indent=2,
                sort_keys=True,
            )
        )

    return by_tax_id


def compare_es_annotations_to_bq_provenance(
    es_host: str,
    es_password: str,
    es_index: str,
    bq_project_id: str,
    bq_dataset: str,
    page_size: int = 500,
    limit_updates: int | None = None,
) -> dict:
    bq_by_tax_id = fetch_bq_provenance_by_tax_id(
        project_id=bq_project_id,
        dataset=bq_dataset,
    )

    host = es_host if es_host.startswith("http") else f"https://{es_host}"

    es = Elasticsearch(
        hosts=[host],
        basic_auth=("elastic", es_password),
        request_timeout=30,
        retry_on_timeout=True,
        max_retries=3,
    )

    pit_id = es.open_point_in_time(index=es_index, keep_alive="2m")

    body = {
        "size": page_size,
        "query": {"term": {"annotation_complete": "Done"}},
        "_source": [
            "tax_id",
            "organism",
            "scientific_name",
            "annotation.accession",
            "annotation.species",
            "annotation.annotation.GTF",
            "annotation.view_in_browser",
            "annotation.annotation_method",
            "annotation.busco_score",
            "assemblies.accession",
            "assemblies.assembly_name",
            "assemblies.description",
            "assemblies.version",
            "assemblies.last_updated",
        ],
        "sort": [{"_shard_doc": "asc"}],
        "pit": {"id": pit_id["id"], "keep_alive": "2m"},
    }

    summary = Counter()
    action_counts = Counter()
    update_manifest = []
    gtf_reload_updates = []
    provenance_only_updates = []
    skipped_pattern_only_same_size = []
    rejected_missing_gtf_url = []
    skipped_examples = defaultdict(list)

    try:
        while True:
            response = es.search(body=body)
            hits = response["hits"]["hits"]

            if not hits:
                break

            for hit in hits:
                source = hit["_source"]
                selected = select_latest_main_annotation(source)
                tax_id = str(source.get("tax_id"))

                summary["es_records_scanned"] += 1

                if selected.get("status") != "SELECTED":
                    summary[f"selector_{selected.get('status')}"] += 1

                    if selected.get("status") == REJECTED_MISSING_GTF_URL:
                        rejected_missing_gtf_url.append(
                            {
                                "tax_id": tax_id,
                                "organism": source.get("organism") or source.get("scientific_name"),
                                "selected": selected,
                            }
                        )

                    elif len(skipped_examples[selected.get("status")]) < 10:
                        skipped_examples[selected.get("status")].append(
                            {
                                "tax_id": tax_id,
                                "organism": source.get("organism") or source.get("scientific_name"),
                                "selected": selected,
                            }
                        )
                    continue

                bq_record = bq_by_tax_id.get(tax_id)
                if not bq_record:
                    summary["missing_bq_provenance_row"] += 1
                    continue

                previous_gtf = bq_record.get("GTF")
                new_gtf = selected.get("gtf_url")
                previous_gtf_pattern = classify_gtf_url_pattern(previous_gtf)
                new_gtf_pattern = classify_gtf_url_pattern(new_gtf)

                previous_gtf_metadata = None
                new_gtf_metadata = None
                needs_metadata_check = (
                    bq_record.get("accession") == selected.get("accession")
                    and previous_gtf != new_gtf
                    and previous_gtf_pattern == OLD_FTP_PATTERN
                    and new_gtf_pattern == NEW_FTP_PATTERN
                )

                if needs_metadata_check:
                    summary["gtf_metadata_checks"] += 1
                    previous_gtf_metadata = fetch_gtf_url_metadata(previous_gtf)
                    new_gtf_metadata = fetch_gtf_url_metadata(new_gtf)

                    if previous_gtf_metadata.get("ok") and new_gtf_metadata.get("ok"):
                        summary["gtf_metadata_check_succeeded"] += 1
                    else:
                        summary["gtf_metadata_check_failed"] += 1

                comparison = compare_selected_annotation_to_provenance(
                    selected=selected,
                    provenance=bq_record,
                    previous_gtf_metadata=previous_gtf_metadata,
                    new_gtf_metadata=new_gtf_metadata,
                )

                action = comparison["action"]
                action_counts[action] += 1
                summary[f"action_{action}"] += 1

                if comparison["requires_provenance_update"]:
                    update_manifest.append(comparison)

                if comparison["requires_gtf_reload"]:
                    gtf_reload_updates.append(comparison)

                if action == UPDATE_PROVENANCE_ONLY:
                    provenance_only_updates.append(comparison)

                if action == SKIP_PATTERN_ONLY_SAME_SIZE:
                    skipped_pattern_only_same_size.append(comparison)

                if action == NO_UPDATE:
                    summary["no_update"] += 1

                if limit_updates and len(update_manifest) >= limit_updates:
                    break

            if limit_updates and len(update_manifest) >= limit_updates:
                break

            body["search_after"] = hits[-1]["sort"]
            body["pit"]["id"] = response.get("pit_id", body["pit"]["id"])

    finally:
        es.close_point_in_time(id=body["pit"]["id"])

    result = {
        "summary": dict(summary),
        "action_counts": dict(action_counts),
        "update_manifest_count": len(update_manifest),
        "gtf_reload_update_count": len(gtf_reload_updates),
        "provenance_only_update_count": len(provenance_only_updates),
        "skipped_pattern_only_same_size_count": len(skipped_pattern_only_same_size),
        "rejected_missing_gtf_url_count": len(rejected_missing_gtf_url),
        "update_manifest_sample": update_manifest[:20],
        "gtf_reload_updates_sample": gtf_reload_updates[:20],
        "provenance_only_updates_sample": provenance_only_updates[:20],
        "skipped_pattern_only_same_size_sample": skipped_pattern_only_same_size[:20],
        "rejected_missing_gtf_url_sample": rejected_missing_gtf_url[:20],
        "skipped_examples": dict(skipped_examples),
    }

    print(json.dumps(result, indent=2, sort_keys=True))
    return result


# Fetching FTP metadata

def classify_gtf_url_pattern(url: str | None) -> str:
    if not url:
        return UNKNOWN_GTF_PATTERN

    if "/pub/databases/ensembl/pre-release/" in url:
        return PRE_RELEASE_PATTERN

    if "/pub/ensemblorganisms/GCA/" in url:
        return NEW_FTP_PATTERN

    if "/pub/ensemblorganisms/" in url and "/GCA_" in url:
        return OLD_FTP_PATTERN

    return UNKNOWN_GTF_PATTERN


def compare_selected_annotation_to_provenance(
    selected: dict[str, Any],
    provenance: dict[str, Any],
    previous_gtf_metadata: dict[str, Any] | None = None,
    new_gtf_metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    previous_accession = provenance.get("accession")
    new_accession = selected.get("accession")

    previous_gtf_url = provenance.get("GTF")
    new_gtf_url = selected.get("gtf_url")

    old_ensembl_url = provenance.get("Ensembl_browser")
    new_ensembl_url = selected.get("view_in_browser")

    previous_pattern = classify_gtf_url_pattern(previous_gtf_url)
    new_pattern = classify_gtf_url_pattern(new_gtf_url)

    accession_changed = previous_accession != new_accession
    gtf_url_changed = previous_gtf_url != new_gtf_url
    ensembl_url_changed = old_ensembl_url != new_ensembl_url

    base_result = {
        "tax_id": str(selected.get("tax_id")),
        "species": selected.get("species"),
        "previous_accession": previous_accession,
        "new_accession": new_accession,
        "accession_root": selected.get("accession_root"),
        "accession_version": selected.get("accession_version"),
        "previous_gtf_url": previous_gtf_url,
        "new_gtf_url": new_gtf_url,
        "previous_gtf_url_pattern": previous_pattern,
        "new_gtf_url_pattern": new_pattern,
        "old_ensembl_url": old_ensembl_url,
        "new_ensembl_url": new_ensembl_url,
        "Biodiversity_portal": provenance.get("Biodiversity_portal"),
        "gbif_url": provenance.get("gbif_url"),
        "selection_reason": selected.get("selection_reason"),
        "assembly_classification": selected.get("assembly_classification"),
        "previous_gtf_size_bytes": (
            previous_gtf_metadata or {}
        ).get("size_bytes"),
        "new_gtf_size_bytes": (
            new_gtf_metadata or {}
        ).get("size_bytes"),
        "previous_gtf_last_modified": (
            previous_gtf_metadata or {}
        ).get("last_modified_raw"),
        "new_gtf_last_modified": (
            new_gtf_metadata or {}
        ).get("last_modified_raw"),
    }

    if not accession_changed and not gtf_url_changed and not ensembl_url_changed:
        return {
            **base_result,
            "action": NO_UPDATE,
            "requires_provenance_update": False,
            "requires_gtf_reload": False,
        }

    if accession_changed:
        return {
            **base_result,
            "action": UPDATE_ACCESSION_CHANGED,
            "requires_provenance_update": True,
            "requires_gtf_reload": True,
        }

    if gtf_url_changed and new_pattern == PRE_RELEASE_PATTERN:
        return {
            **base_result,
            "action": UPDATE_PRE_RELEASE,
            "requires_provenance_update": True,
            "requires_gtf_reload": True,
        }

    is_old_to_new_same_accession = (
        not accession_changed
        and previous_pattern == OLD_FTP_PATTERN
        and new_pattern == NEW_FTP_PATTERN
    )

    if gtf_url_changed and is_old_to_new_same_accession:
        previous_size = (previous_gtf_metadata or {}).get("size_bytes")
        new_size = (new_gtf_metadata or {}).get("size_bytes")

        if previous_size is not None and previous_size == new_size:
            if ensembl_url_changed:
                return {
                    **base_result,
                    "action": UPDATE_PROVENANCE_ONLY,
                    "requires_provenance_update": True,
                    "requires_gtf_reload": False,
                }

            return {
                **base_result,
                "action": SKIP_PATTERN_ONLY_SAME_SIZE,
                "requires_provenance_update": False,
                "requires_gtf_reload": False,
            }

        if previous_size is None or new_size is None:
            return {
                **base_result,
                "action": UPDATE_GTF_SIZE_UNKNOWN,
                "requires_provenance_update": True,
                "requires_gtf_reload": True,
            }

        return {
            **base_result,
            "action": UPDATE_GTF_SIZE_CHANGED,
            "requires_provenance_update": True,
            "requires_gtf_reload": True,
        }

    if gtf_url_changed:
        return {
            **base_result,
            "action": UPDATE_GTF_URL_CHANGED,
            "requires_provenance_update": True,
            "requires_gtf_reload": True,
        }

    if ensembl_url_changed:
        return {
            **base_result,
            "action": UPDATE_PROVENANCE_ONLY,
            "requires_provenance_update": True,
            "requires_gtf_reload": False,
        }

    return {
        **base_result,
        "action": NO_UPDATE,
        "requires_provenance_update": False,
        "requires_gtf_reload": False,
    }


def fetch_gtf_url_metadata(
    url: str,
    timeout_seconds: int = 20,
    max_attempts: int = 3,
) -> dict[str, Any]:
    """
    Fetch lightweight metadata for a remote GTF file without downloading it.

    Uses HTTP HEAD against https://ftp.ebi.ac.uk URLs. The returned size is the
    compressed .gtf.gz object size in bytes, matching the file served at the URL.
    """
    last_error = None

    for attempt in range(1, max_attempts + 1):
        try:
            response = requests.head(
                url,
                allow_redirects=True,
                timeout=timeout_seconds,
            )

            if response.status_code == 405:
                response = requests.get(
                    url,
                    headers={"Range": "bytes=0-0"},
                    stream=True,
                    timeout=timeout_seconds,
                )

            response.raise_for_status()

            headers = response.headers
            size_bytes = _parse_content_size(headers)
            last_modified_raw = headers.get("Last-Modified")

            return {
                "url": url,
                "ok": True,
                "status_code": response.status_code,
                "size_bytes": size_bytes,
                "last_modified": _parse_http_datetime(last_modified_raw),
                "last_modified_raw": last_modified_raw,
                "etag": headers.get("ETag"),
                "error": None,
            }

        except requests.RequestException as exc:
            last_error = str(exc)

            if attempt < max_attempts:
                time.sleep(1.5 ** attempt)

    return {
        "url": url,
        "ok": False,
        "status_code": None,
        "size_bytes": None,
        "last_modified": None,
        "last_modified_raw": None,
        "etag": None,
        "error": last_error,
    }


def _parse_content_size(headers: requests.structures.CaseInsensitiveDict) -> int | None:
    content_range = headers.get("Content-Range")
    if content_range and "/" in content_range:
        total_size = content_range.rsplit("/", 1)[-1]
        if total_size.isdigit():
            return int(total_size)

    content_length = headers.get("Content-Length")
    if content_length and content_length.isdigit():
        return int(content_length)

    return None


def _parse_http_datetime(value: str | None) -> datetime | None:
    if not value:
        return None

    try:
        return parsedate_to_datetime(value)
    except (TypeError, ValueError):
        return None
