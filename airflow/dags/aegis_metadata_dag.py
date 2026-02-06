import pendulum
import json

from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath

from dependencies.aegis_projects import aegis_projects
from dependencies.common_functions import start_aegis_beam


@task
def get_aegis_metadata(study_id: str, bucket_name: str) -> None:
    """Collect metadata from ENA and BioSamples and save to GCS"""
    from dependencies import collect_metadata_experiments_assemblies

    # Collect metadata for the study
    metadata = collect_metadata_experiments_assemblies.main(
        study_id,
        "AEGIS",
        bucket_name
    )

    # Set up GCS path
    base = ObjectStoragePath(f"gs://google_cloud_default@{bucket_name}")
    base.mkdir(exist_ok=True)
    path = base / f"{study_id}.jsonl"

    # Write metadata to file (one sample per line)
    with path.open("w") as file:
        for sample_id, record in metadata.items():
            file.write(f"{json.dumps(record)}\n")


@task
def trigger_aegis_beam_pipeline(bucket_name: str) -> None:
    """Trigger the AEGIS Apache Beam pipeline on Dataflow"""
    # This will trigger the aegis_pipeline.py we'll create in Task 2
    start_aegis_beam(bucket_name)


# ← FIX 3: @dag is now at the CORRECT indentation level (not inside a task)
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
    # ← FIX 4: DAG now actually calls tasks and sets dependencies

    # Get AEGIS project config
    aegis_config = aegis_projects["PRJEB80366"]
    study_id = "PRJEB80366"
    bucket_name = aegis_config["bucket_name"]

    # Define task flow
    metadata_task = get_aegis_metadata(study_id, bucket_name)
    beam_task = trigger_aegis_beam_pipeline(bucket_name)

    # Set dependencies: collect metadata first, then trigger beam
    metadata_task >> beam_task


aegis_metadata_ingestion()