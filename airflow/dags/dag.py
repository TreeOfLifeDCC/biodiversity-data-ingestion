import pendulum
import json

from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath
from airflow.providers.google.cloud.operators.dataflow import (
    DataflowStartFlexTemplateOperator
)

from dependencies.biodiversity_projects import (
    gbdp_projects,
    erga_projects,
    dtol_projects,
    asg_projects,
)


@task
def get_metadata(study_id: str, project_name: str, bucket_name: str, **kwargs) -> None:
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


@task
def start_apache_beam_gbdp(**kwargs) -> None:
    print("DONE")


@task
def start_apache_beam_erga(**kwargs) -> None:
    print("DONE")


@task
def start_apache_beam_dtol(**kwargs) -> None:
    print("DONE")


@task
def start_apache_beam_asg(**kwargs) -> None:
    print("DONE")


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

    asg_metadata_import_tasks = []
    for study_id, item in asg_projects.items():
        project_name, bucket_name = item["project_name"], item["bucket_name"]
        asg_metadata_import_tasks.append(
            get_metadata.override(task_id=f"asg_{study_id}_get_metadata")(
                study_id, project_name, bucket_name
            )
        )
    body = {
        "launchParameter": {
            "jobName": "test-2025-01-30",
            "parameters": {
                "input_path": "gs://prj-ext-prod-biodiv-data-in-asg/*jsonl",
                "output_path": "gs://prj-ext-prod-biodiv-data-in-asg",
                "bq_dataset_name": "asg"
            },
            "environment" : {
                "tempLocation": "gs://dataflow-staging-europe-west2-153439618737/tmp",
                "machineType": "e2-medium",
                "stagingLocation": "gs://dataflow-staging-europe-west2-153439618737/staging",
                "sdkContainerImage": "europe-west2-docker.pkg.dev/prj-ext-prod-biodiv-data-in/apache-beam-pipelines/biodiversity_etl:latest"
            },
            "containerSpecGcsPath": "gs://prj-ext-prod-biodiv-data-in_cloudbuild/biodiversity_etl-20250129-124751.json"
        }
    }
    start_template_job = DataflowStartFlexTemplateOperator(
        task_id="start_template_job",
        project_id="prj-ext-prod-biodiv-data-in",
        body=body,
        location="europe-west2",
        wait_until_finished=True,
        )

    asg_metadata_import_tasks >> start_template_job


biodiversity_metadata_ingestion()
