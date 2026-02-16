import pendulum
import json

from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath

from dependencies.aegis_projects import aegis_projects
from dependencies.common_functions import start_aegis_beam


@task
def get_aegis_metadata(study_id: str, bucket_name: str) -> None:
    from dependencies import collect_metadata_experiments_assemblies

    metadata = collect_metadata_experiments_assemblies.main(
        study_id,
        "AEGIS",
        bucket_name
    )

    base = ObjectStoragePath(f"gs://google_cloud_default@{bucket_name}")
    base.mkdir(exist_ok=True)
    path = base / f"{study_id}.jsonl"

    with path.open("w") as file:
        for sample_id, record in metadata.items():
            file.write(f"{json.dumps(record)}\n")


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
    """
    This DAG pulls data from ENA (PRJEB80366) and BioSamples and builds
    Elasticsearch indices for AEGIS project
    """

    aegis_config = aegis_projects["PRJEB80366"]
    study_id = "PRJEB80366"
    bucket_name = aegis_config["bucket_name"]

    metadata_task = get_aegis_metadata(study_id, bucket_name)
    beam_task = trigger_aegis_beam_pipeline(bucket_name)

    # collect metadata first, then trigger beam
    metadata_task >> beam_task


aegis_metadata_ingestion()