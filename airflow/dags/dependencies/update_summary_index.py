from elasticsearch import Elasticsearch
from datetime import datetime

DATA_PORTAL_AGGREGATIONS = [
    "biosamples",
    "raw_data",
    "mapped_reads",
    "assemblies_status",
    "annotation_status",
    "annotation_complete",
    "project_name",
    "symbionts_assemblies_status",
    "symbionts_biosamples_status",
    "symbionts_raw_data_status",
]

DATA_PORTAL_AGGREGATIONS_DTOL = ["assemblies_status", "annotation_complete"]


def update_summary_index(host: str, password: str):
    date_prefix = datetime.today().strftime("%Y-%m-%d")
    es = Elasticsearch([f"https://{host}"], http_auth=("elastic", password))
    body = dict()
    body["aggs"] = dict()
    for aggregation_field in DATA_PORTAL_AGGREGATIONS:
        body["aggs"][aggregation_field] = {
            "terms": {"field": aggregation_field, "size": 20}
        }
        body["aggs"]["taxonomies"] = {
            "nested": {"path": f"taxonomies.kingdom"},
            "aggs": {
                "kingdom": {"terms": {"field": f"taxonomies.kingdom.scientificName"}}
            },
        }
    results = es.search(index=f"{date_prefix}_data_portal", body=body)
    names_mapping = {
        "biosamples": "BioSamples - Submitted",
        "raw_data": "Raw Data - Submitted",
        "assemblies_status": "Assemblies - Submitted",
        "annotation_complete": "Annotation Complete - Done",
    }
    summary = dict()
    for key, aggs in results["aggregations"].items():
        try:
            for bucket in aggs["buckets"]:
                if bucket["key"] == "Done":
                    if key in names_mapping:
                        summary.setdefault("status", {})
                        summary["status"][names_mapping[key]] = bucket["doc_count"]
                elif bucket["key"] != "Waiting" and "symbionts" not in key:
                    summary.setdefault("projects", {})
                    summary["projects"][bucket["key"]] = bucket["doc_count"]
                elif bucket["key"] != "Waiting" and "symbionts" in key:
                    summary.setdefault("status", {})
                    summary["status"][f"Symbionts {bucket['key']}"] = bucket[
                        "doc_count"
                    ]
        except KeyError:
            for bucket in aggs["kingdom"]["buckets"]:
                summary.setdefault("phylogeny", {})
                summary["phylogeny"][bucket["key"]] = bucket["doc_count"]
    es.index(index="summary_test", body=summary, id="summary")


def update_summary_index_dtol(host: str, password: str):
    date_prefix = datetime.today().strftime("%Y-%m-%d")
    es = Elasticsearch([f"https://{host}"], http_auth=("elastic", password))
    body = dict()
    body["aggs"] = dict()
    for aggregation_field in DATA_PORTAL_AGGREGATIONS_DTOL:
        body["aggs"][aggregation_field] = {
            "terms": {"field": aggregation_field, "size": 20}
        }
    body["aggs"]["genome_notes"] = {
        "nested": {"path": "genome_notes"},
        "aggs": {
            "genome_count": {
                "reverse_nested": {},  # get to the parent document level
                "aggs": {
                    "distinct_docs": {"cardinality": {"field": "organism.keyword"}}
                },
            }
        },
    }
    results = es.search(index=f"{date_prefix}_data_portal", body=body)
    summary = dict()
    names_mapping = {
        "assemblies_status": "Assemblies - Submitted",
        "annotation_complete": "Annotation Complete",
        "genome_notes": "Genome Notes",
    }
    for key, aggs in results["aggregations"].items():
        try:
            for bucket in aggs["buckets"]:
                if bucket["key"] == "Done":
                    if key in names_mapping:
                        summary[names_mapping[key]] = bucket["doc_count"]
        except KeyError:
            summary["Genome Notes"] = aggs["genome_count"]["distinct_docs"]["value"]
    es.index(index="summary", body=summary, id="summary")
