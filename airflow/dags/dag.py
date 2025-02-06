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


def start_apache_beam(biodiversity_project_name):
    gc_project_name = "prj-ext-prod-biodiv-data-in"
    region = "europe-west2"
    body = {
        "launchParameter": {
            "jobName": "biodiversity-ingestion-2025-02-06",
            "parameters": {
                "input_path": f"gs://{gc_project_name}-"
                              f"{biodiversity_project_name}/*jsonl",
                "output_path": f"gs://{gc_project_name}-"
                               f"{biodiversity_project_name}",
                "bq_dataset_name": biodiversity_project_name,
            },
            "environment": {
                "tempLocation": "gs://dataflow-staging-europe-west2-"
                                "153439618737/tmp",
                "machineType": "e2-medium",
                "stagingLocation": "gs://dataflow-staging-europe-west2-"
                                   "153439618737/staging",
                "sdkContainerImage": f"{region}-docker.pkg.dev/"
                                     f"{gc_project_name}/apache-beam-pipelines/"
                                     f"biodiversity_etl:20250206-121022"
            },
            "containerSpecGcsPath": f"gs://{gc_project_name}_cloudbuild/"
                                    f"biodiversity_etl-20250206-121022.json"
        }
    }
    return DataflowStartFlexTemplateOperator(
        task_id=f"start_ingestion_job_{biodiversity_project_name}",
        project_id=gc_project_name,
        body=body,
        location=region,
        wait_until_finished=True,
    )


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
    start_ingestion_job_asg = start_apache_beam("asg")

    asg_metadata_import_tasks >> start_ingestion_job_asg


biodiversity_metadata_ingestion()
