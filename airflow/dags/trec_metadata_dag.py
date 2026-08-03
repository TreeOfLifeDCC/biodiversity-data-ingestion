import json
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
import pendulum
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from elasticsearch import Elasticsearch

from dependencies.common_functions import start_apache_beam
from dependencies.trec_project import trec_projects
from dependencies import import_trec_images


@task
def get_trec_metadata(
    project_tag: str, bucket_name: str, blob_name: str | None = None
) -> None:
    """
    Fetch TREC metadata from BioSamples and write it to GCS as JSONL.
    """
    import logging
    import resource
    import time

    from dependencies import collect_metadata_trec
    from google.cloud import storage

    log = logging.getLogger(__name__)

    def _rss_mb() -> float:
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024.0

    client = storage.Client(project="prj-ext-prod-biodiv-data-in")
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name or f"{project_tag}.jsonl")

    started = time.monotonic()

    count = 0
    with blob.open("w", content_type="application/json") as fh:
        for record in collect_metadata_trec.iter_metadata(project_tag):
            fh.write(f"{json.dumps(record, default=str)}\n")
            count += 1
            if count % 200 == 0:
                log.info(
                    "progress: %s records | peak RSS %.0f MB | %.0fs elapsed",
                    count,
                    _rss_mb(),
                    time.monotonic() - started,
                )




@task
def update_data_portal_alias(
    es_host: str, es_password: str, index_name: str, alias_name: str = "data_portal_alias"
) -> None:
    """
    Point the alias at the new index, removing it from any existing indices.
    """
    if not es_host.startswith(("http://", "https://")):
        es_host = f"https://{es_host}"
    es = Elasticsearch([es_host], basic_auth=("elastic", es_password))
    if es.indices.exists_alias(name=alias_name):
        old_indices = es.indices.get_alias(name=alias_name)
        actions = [
            {"remove": {"index": old_index, "alias": alias_name}}
            for old_index in old_indices.keys()
        ]
        actions.append({"add": {"index": index_name, "alias": alias_name}})
        es.indices.update_aliases(body={"actions": actions})
    else:
        if es.indices.exists(index=alias_name):
            raise ValueError(
                f"Cannot create alias '{alias_name}': a concrete index with that "
                f"exact name already exists on this cluster. Either pick a "
                f"different alias via the Airflow Variable "
                f"'trec_data_portal_alias', or remove/reindex the existing "
                f"'{alias_name}' index."
            )
        es.indices.put_alias(index=index_name, name=alias_name)


@dag(
    schedule="0 7 * * *",
    start_date=pendulum.datetime(2026, 7, 22, tz="Europe/London"),
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
    blob_name = "trec.jsonl"

    # Create the TREC metadata file in GCS
    metadata_task = get_trec_metadata.override(task_id="trec_get_metadata")(
        project_tag,
        bucket_name,
        blob_name,
    )

    # Start Beam / Dataflow ingestion
    template_tag = Variable.get(
        "trec_dataflow_template_tag",
        default_var="20260723-115924",
    )
    start_ingestion_job = start_apache_beam(
        "trec",
        template_tag=template_tag,
        job_name="trec-data-ingestion-{{ ts_nodash | replace('T', '-') | lower }}",
        input_path=f"gs://{bucket_name}/{blob_name}",
        output_path=f"gs://{bucket_name}",
    )

    # Get Elasticsearch variables
    host = Variable.get("trec_elasticsearch_host")
    password = Variable.get("trec_elasticsearch_password")
    settings = json.dumps(
        Variable.get("elasticsearch_settings", deserialize_json=True)
    )
    data_portal_mapping = Variable.get("trec_elasticsearch_data_portal_mapping")

    alias_name = Variable.get("trec_data_portal_alias", default_var="data_portal_alias")

    date_prefix = datetime.today().strftime("%Y-%m-%d")
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
        alias_name=alias_name,
    )

    remove_data_portal_index_command = (
        f"curl -X DELETE '{base_url}/{two_days_prefix}_data_portal'"
    )
    remove_data_portal_index_task = BashOperator(
        task_id="trec-remove-old-data-portal-index",
        bash_command=remove_data_portal_index_command,
    )
    import_trec_images_task = PythonOperator(
        task_id="trec-import-images",
        python_callable=import_trec_images.main,
        op_kwargs={
            "es_host": host,
            "es_password": password,
            "index_name": f"{date_prefix}_data_portal",
        },
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

    metadata_task >> start_ingestion_job
    (
        start_ingestion_job
        >> import_trec_images_task
        >> change_aliases_task
        >> remove_data_portal_index_task
    )

trec_metadata_ingestion()
