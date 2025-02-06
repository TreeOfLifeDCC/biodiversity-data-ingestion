import pendulum
import json

from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath

from dependencies.biodiversity_projects import (
    gbdp_projects,
    erga_projects,
    dtol_projects,
    asg_projects,
)

from dependencies.common_functions import start_apache_beam


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
        metadata_import_tasks >> start_ingestion_job


biodiversity_metadata_ingestion()
