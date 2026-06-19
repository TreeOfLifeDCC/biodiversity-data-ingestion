import json
from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from elasticsearch import Elasticsearch

from dependencies.common_functions import start_apache_beam
from dependencies.trec_project import trec_projects


@task
def get_trec_metadata(project_tag: str, bucket_name: str) -> None:
    """
    Fetch TREC metadata from BioSamples and write it to GCS as JSONL.
    """
    from dependencies import collect_metadata_trec

    metadata = collect_metadata_trec.main(project_tag)
    base = ObjectStoragePath(f"gcs://{bucket_name}", conn_id="google_cloud_default")
    base.mkdir(exist_ok=True)
    path = base / f"{project_tag}.jsonl"
    with path.open("w") as file:
        for _, record in metadata.items():
            file.write(f"{json.dumps(record, default=str)}\n")


@task
def update_data_portal_alias(
    es_host: str, es_password: str, index_name: str, alias_name: str = "data_portal"
) -> None:
    """
    Point the alias at the new index, removing it from any existing indices.
    """
    es = Elasticsearch([es_host], http_auth=("elastic", es_password))
    if es.indices.exists_alias(name=alias_name):
        old_indices = es.indices.get_alias(name=alias_name)
        actions = [
            {"remove": {"index": old_index, "alias": alias_name}}
            for old_index in old_indices.keys()
        ]
        actions.append({"add": {"index": index_name, "alias": alias_name}})
        es.indices.update_aliases(body={"actions": actions})
    else:
        es.indices.put_alias(index=index_name, name=alias_name)


@dag(
    # schedule="0 11 * * *",
    start_date=pendulum.datetime(2025, 7, 2, tz="Europe/London"),
    catchup=False,
    tags=["trec_metadata_ingestion"],
)
def trec_metadata_ingestion():
    """
    This DAG builds TREC metadata JSONL files, runs the Beam ingestion job,
    and manages the Elasticsearch index for the TREC expedition.
    """
    project_cfg = trec_projects["project"]
    project_tag = project_cfg["project_tag"]
    bucket_name = project_cfg["bucket_name"]

    # Create the TREC metadata file in GCS
    metadata_task = get_trec_metadata.override(task_id="trec_get_metadata")(
       project_tag, bucket_name
    )

    # Start Beam / Dataflow ingestion
    start_ingestion_job = start_apache_beam("trec")

    # Get Elasticsearch variables
    host = Variable.get("trec_elasticsearch_host")
    password = Variable.get("trec_elasticsearch_password")
    settings = json.dumps(
        Variable.get("elasticsearch_settings", deserialize_json=True)
    )
    data_portal_mapping = Variable.get("trec_elasticsearch_data_portal_mapping")

    date_prefix = datetime.today().strftime("%Y-%m-%d")
    yesterday_day_prefix = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    two_days_prefix = (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d")

    base_url = f"https://elastic:{password}@{host}"

    create_data_portal_index_command = (
        f"curl -X PUT '{base_url}/{date_prefix}_data_portal' "
        f"-H 'Content-Type: application/json' "
        f"-d '{settings}'"
    )

    add_data_portal_mapping_command = (
        f"curl -X PUT '{base_url}/{date_prefix}_data_portal/_mapping' "
        f"-H 'Content-Type: application/json' "
        f"-d '{data_portal_mapping}'"
    )

    change_aliases_task = update_data_portal_alias.override(
        task_id="trec-change-aliases"
    )(
        es_host=host,
        es_password=password,
        index_name=f"{date_prefix}_data_portal",
    )

    remove_data_portal_index_command = (
        f"curl -X DELETE '{base_url}/{two_days_prefix}_data_portal'"
    )
    remove_data_portal_index_task = BashOperator(
        task_id="trec-remove-old-data-portal-index",
        bash_command=remove_data_portal_index_command,
    )

    # ES index must exist before Beam runs, metadata must be produced before Beam
    (
        BashOperator(
            task_id="trec-create-data-portal-index",
            bash_command=create_data_portal_index_command,
        )
        >> BashOperator(
            task_id="trec-add-mapping-data-portal-index",
            bash_command=add_data_portal_mapping_command,
        )
        >> start_ingestion_job
    )

    (metadata_task >>
    start_ingestion_job)
    (start_ingestion_job >>
    change_aliases_task >>
    remove_data_portal_index_task)


trec_metadata_ingestion()

