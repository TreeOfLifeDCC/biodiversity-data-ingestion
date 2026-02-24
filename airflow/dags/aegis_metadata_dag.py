import pendulum
import json

from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath

from dependencies.aegis_projects import aegis_projects
from dependencies.common_functions import start_aegis_beam


@task
def get_aegis_metadata(study_id: str, bucket_name: str) -> None:
    from dependencies import collect_metadata_experiments_assemblies
    from google.cloud import storage

    metadata = collect_metadata_experiments_assemblies.main(
        study_id,
        "AEGIS",
        bucket_name
    )

    client = storage.Client(project="prj-ext-prod-biodiv-data-in")
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(f"{study_id}.jsonl")

    content = ""
    for sample_id, record in metadata.items():
        content += f"{json.dumps(record)}\n"

    blob.upload_from_string(content, content_type="application/json")

@task
def trigger_aegis_beam_pipeline(bucket_name: str) -> None:
    start_aegis_beam(bucket_name)


@dag(
    schedule="0 11 * * *",  # Run daily at 11am UTC
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["aegis_metadata_ingestion"],
)
def aegis_metadata_ingestion():
    aegis_config = aegis_projects["PRJEB80366"]
    study_id = "PRJEB80366"
    bucket_name = aegis_config["bucket_name"]

    metadata_task = get_aegis_metadata(study_id, bucket_name)
    beam_task = trigger_aegis_beam_pipeline(bucket_name)

    # collect metadata first, then trigger beam
    metadata_task >> beam_task


aegis_metadata_ingestion()