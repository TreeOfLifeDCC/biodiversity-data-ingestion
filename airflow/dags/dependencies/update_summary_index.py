from elasticsearch import Elasticsearch

DATA_PORTAL_AGGREGATIONS = [
    "biosamples", "raw_data", "mapped_reads", "assemblies_status",
    "annotation_status", "annotation_complete", "project_name",
    "symbionts_assemblies_status", "symbionts_biosamples_status",
    "symbionts_raw_data_status"]


def update_summary_index(host: str, password: str):
    es = Elasticsearch(
        [f"https://{host}"],
        http_auth=("elastic", password))
    body = dict()
    body["aggs"] = dict()
    for aggregation_field in DATA_PORTAL_AGGREGATIONS:
        body["aggs"][aggregation_field] = {
            "terms": {"field": aggregation_field, "size": 20}
        }
        body["aggs"]["taxonomies"] = {
            "nested": {"path": f"taxonomies.kingdom"},
            "aggs": {"kingdom": {
                "terms": {"field": f"taxonomies.kingdom.scientificName"}}
            }
        }
    results = es.search(index="data_portal", body=body)
    names_mapping = {
        "biosamples": "BioSamples - Submitted",
        "raw_data": "Raw Data - Submitted",
        "assemblies_status": "Assebmlies - Submitted",
        "annotation_complete": "Annotation Complete"
    }
    summary = dict()
    for key, aggs in results["aggregations"].items():
        try:
            for bucket in aggs["buckets"]:
                if bucket['key'] == 'Done':
                    if key in names_mapping:
                        summary.setdefault("status", {})
                        summary["status"][names_mapping[key]] = bucket[
                            'doc_count']
                elif bucket['key'] != 'Waiting' and "symbionts" not in key:
                    summary.setdefault("projects", {})
                    summary["projects"][bucket["key"]] = bucket['doc_count']
                elif bucket['key'] != 'Waiting' and "symbionts" in key:
                    summary.setdefault("status", {})
                    summary["status"][f"Symbionts {bucket['key']}"] = bucket[
                        'doc_count']
        except KeyError:
            for bucket in aggs["kingdom"]["buckets"]:
                summary.setdefault("phylogeny", {})
                summary["phylogeny"][bucket['key']] = bucket['doc_count']
    es.index("summary_test", summary, id="summary")
