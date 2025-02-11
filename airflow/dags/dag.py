import pendulum
import json

from datetime import datetime, timedelta
from elasticsearch import Elasticsearch

from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath
from airflow.models import Variable
from airflow.operators.bash import BashOperator

from dependencies.biodiversity_projects import (
    gbdp_projects,
    erga_projects,
    dtol_projects,
    asg_projects,
)

from dependencies.common_functions import start_apache_beam


@task
def update_summary_index(host: str, password: str):
    data_portal_aggregations = [
        "biosamples", "raw_data", "mapped_reads", "assemblies_status",
        "annotation_status", "annotation_complete", "project_name",
        "symbionts_assemblies_status", "symbionts_biosamples_status",
        "symbionts_raw_data_status"]
    es = Elasticsearch(
        [f"https://{host}"],
        http_auth=("elastic", password))
    body = dict()
    body["aggs"] = dict()
    for aggregation_field in data_portal_aggregations:
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


@task
def get_metadata(study_id: str, project_name: str, bucket_name: str,
                 **kwargs) -> None:
    from dependencies import collect_metadata_experiments_assemblies

    if "ERGA" in project_name:
        project_tag = "ERGA"
    else:
        project_tag = project_name
    metadata = collect_metadata_experiments_assemblies.main(
        study_id, project_tag, project_name
    )

    base = ObjectStoragePath(f"gs://google_cloud_default@{bucket_name}")
    base.mkdir(exist_ok=True)
    path = base / f"{study_id}.jsonl"
    with path.open("w") as file:
        for sample_id, record in metadata.items():
            file.write(f"{json.dumps(record)}\n")


@dag(
    schedule=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["biodiversity_metadata_ingestion"],
)
def biodiversity_metadata_ingestion():
    """
    This DAG builds BigQuery tables and ElasticSearch indexes for all
    biodiversity projects
    """
    date_prefix = datetime.today().strftime("%Y-%m-%d")
    yesterday_day_prefix = (datetime.today() - timedelta(days=1)).strftime(
        "%Y-%m-%d")
    for project_name, subprojects in {
        'gbdp': gbdp_projects,
        'erga': erga_projects,
        'dtol': dtol_projects,
        'asg': asg_projects}.items():
        metadata_import_tasks = []
        for study_id, item in subprojects.items():
            subproject_name, bucket_name = item["project_name"], item[
                "bucket_name"]
            metadata_import_tasks.append(
                get_metadata.override(
                    task_id=f"{project_name}_{study_id}_get_metadata")(
                    study_id, subproject_name, bucket_name
                )
            )
        start_ingestion_job = start_apache_beam(project_name)

        # Get Elasticsearch variables
        host = Variable.get(f"{project_name}_elasticsearch_host")
        password = Variable.get(f"{project_name}_elasticsearch_password")
        settings = json.dumps(
            Variable.get("elasticsearch_settings", deserialize_json=True))
        data_portal_mapping = Variable.get(
            f"{project_name}_elasticsearch_data_portal_mapping")
        tracking_status_mapping = Variable.get(
            f"{project_name}_elasticsearch_tracking_status_mapping")
        specimens_mapping = Variable.get(
            f"{project_name}_elasticsearch_specimens_mapping")

        base_url = f"https://elastic:{password}@{host}"

        create_data_portal_index_command = f"curl -X PUT '{base_url}/{date_prefix}_data_portal' -H 'Content-Type: application/json' -d '{settings}'"
        create_tracking_status_index_command = f"curl -X PUT '{base_url}/{date_prefix}_tracking_status' -H 'Content-Type: application/json' -d '{settings}'"
        create_specimens_index_command = f"curl -X PUT '{base_url}/{date_prefix}_specimens' -H 'Content-Type: application/json' -d '{settings}'"

        add_data_portal_mapping_command = f"curl -X PUT '{base_url}/{date_prefix}_data_portal/_mapping' -H 'Content-Type: application/json' -d '{data_portal_mapping}'"
        add_tracking_status_mapping_command = f"curl -X PUT '{base_url}/{date_prefix}_tracking_status/_mapping' -H 'Content-Type: application/json' -d '{tracking_status_mapping}'"
        add_specimens_mapping_command = f"curl -X PUT '{base_url}/{date_prefix}_specimens/_mapping' -H 'Content-Type: application/json' -d '{specimens_mapping}'"

        (BashOperator(
            task_id=f"{project_name}-create-data-portal-index",
            bash_command=create_data_portal_index_command) >>
         BashOperator(
             task_id=f"{project_name}-add-mapping-data-portal-index",
             bash_command=add_data_portal_mapping_command),
         BashOperator(
             task_id=f"{project_name}-create-tracking-status-index",
             bash_command=create_tracking_status_index_command) >>
         BashOperator(
             task_id=f"{project_name}-add-mapping-tracking-status-index",
             bash_command=add_tracking_status_mapping_command),
         BashOperator(
             task_id=f"{project_name}-create-specimens-index",
             bash_command=create_specimens_index_command) >>
         BashOperator(
             task_id=f"{project_name}-add-mapping-specimens-index",
             bash_command=add_specimens_mapping_command)
         ) >> start_ingestion_job

        metadata_import_tasks >> start_ingestion_job

        change_aliases_json = {
            "actions": [
                {
                    "add": {
                        "index": f"{date_prefix}_data_portal",
                        "alias": "data_portal",
                    }
                },
                {
                    "add": {
                        "index": f"{date_prefix}_tracking_status",
                        "alias": "tracking_status",
                    }
                },
                {
                    "add": {
                        "index": f"{date_prefix}_specimens",
                        "alias": "specimens",
                    }
                }
            ]
        }
        change_aliases_command = f"curl -X PUT '{base_url}/_aliases' -H 'Content-Type: application/json' -d '{change_aliases_json}'"
        change_aliases_task = BashOperator(
            task_id=f"{project_name}-change-aliases",
            bash_command=change_aliases_command
        )
        change_aliases_task << start_ingestion_job
        if project_name == "ERGA":
            change_aliases_task >> update_summary_index(host, password)


biodiversity_metadata_ingestion()
