import pendulum
import json
import asyncio

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from dependencies.biodiversity_projects import (
    gbdp_projects,
    erga_projects,
    dtol_projects,
    asg_projects,
)

from dependencies.common_functions import start_apache_beam
from dependencies import import_tol_qc, import_images, import_annotations


@task
def additional_task(host: str, password: str, project_name: str, **kwargs):
    from dependencies import (
        update_summary_index,
        update_articles_index,
        import_mgnify_data,
    )

    if project_name == "erga":
        update_summary_index.update_summary_index(host=host, password=password)
        update_articles_index.update_articles_index(host=host, password=password)
    elif project_name == "dtol":
        update_summary_index.update_summary_index_dtol(host=host, password=password)
        update_articles_index.update_articles_index(host=host, password=password)
    elif project_name == "gbdp":
        update_articles_index.update_articles_index(host=host, password=password)
    elif project_name == "asg":
        import_mgnify_data.main(host=host, password=password)
        update_articles_index.update_articles_index(host=host, password=password)


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

    base = ObjectStoragePath(f"gcs://{bucket_name}", conn_id="google_cloud_default")
    base.mkdir(exist_ok=True)
    path = base / f"{study_id}.jsonl"
    with path.open("w") as file:
        for sample_id, record in metadata.items():
            file.write(f"{json.dumps(record)}\n")


@task
def get_genome_notes(**kwargs) -> None:
    from dependencies import import_genome_notes

    genome_notes = asyncio.run(import_genome_notes.main())

    base = ObjectStoragePath(
        f"gcs://prj-ext-prod-biodiv-data-in-genome_notes",
        conn_id="google_cloud_default",
    )
    base.mkdir(exist_ok=True)
    path = base / "genome_notes.jsonl"
    with path.open("w") as file:
        for tax_id, genome_note in genome_notes.items():
            body = dict()
            if tax_id == "1594315":
                body["articles"] = [genome_note[0]]
            else:
                body["articles"] = genome_note
            body["tax_id"] = tax_id
            file.write(f"{json.dumps(body)}\n")


@dag(
    schedule="0 7 * * *",
    # schedule_interval=None,
    start_date=pendulum.datetime(2025, 5, 1, tz="Europe/London"),
    catchup=False,
    tags=["biodiversity_metadata_ingestion"],
)
def biodiversity_metadata_ingestion():
    """
    This DAG builds BigQuery tables and ElasticSearch indexes for all
    biodiversity projects
    """
    erga_host = Variable.get("erga_elasticsearch_host")
    erga_password = Variable.get("erga_elasticsearch_password")
    github_token = Variable.get("github_token")
    import_tol_qc_data_task = PythonOperator(
        task_id="import_tol_qc_data_task",
        python_callable=import_tol_qc.main,
        op_kwargs={"es_host": erga_host, "es_password": erga_password},
    )
    import_images_task = PythonOperator(
        task_id="import_images_task",
        python_callable=import_images.main,
        op_kwargs={"es_host": erga_host, "es_password": erga_password},
    )
    import_genome_notes_task = get_genome_notes.override(
        task_id="import_genome_notes_task"
    )()
    import_annotations_task = PythonOperator(
        task_id="import_annotations_task",
        python_callable=import_annotations.main,
        op_kwargs={"github_token": github_token},
    )
    date_prefix = datetime.today().strftime("%Y-%m-%d")
    yesterday_day_prefix = (datetime.today() - timedelta(days=1)).strftime("%Y-%m-%d")
    two_days_prefix = (datetime.today() - timedelta(days=2)).strftime("%Y-%m-%d")
    for project_name, subprojects in {
        "gbdp": gbdp_projects,
        "erga": erga_projects,
        "dtol": dtol_projects,
        "asg": asg_projects,
    }.items():
        metadata_import_tasks = []
        for study_id, item in subprojects.items():
            subproject_name, bucket_name = item["project_name"], item["bucket_name"]
            metadata_import_tasks.append(
                get_metadata.override(
                    task_id=f"{project_name}_{study_id}_get_metadata"
                )(study_id, subproject_name, bucket_name)
            )
        start_ingestion_job = start_apache_beam(project_name)

        # Get Elasticsearch variables
        host = Variable.get(f"{project_name}_elasticsearch_host")
        password = Variable.get(f"{project_name}_elasticsearch_password")
        settings = json.dumps(
            Variable.get("elasticsearch_settings", deserialize_json=True)
        )
        data_portal_mapping = Variable.get(
            f"{project_name}_elasticsearch_data_portal_mapping"
        )
        tracking_status_mapping = Variable.get(
            f"{project_name}_elasticsearch_tracking_status_mapping"
        )
        specimens_mapping = Variable.get(
            f"{project_name}_elasticsearch_specimens_mapping"
        )

        base_url = f"https://elastic:{password}@{host}"

        create_data_portal_index_command = (
            f"curl -X PUT '{base_url}/"
            f"{date_prefix}_data_portal' "
            f"-H 'Content-Type: "
            f"application/json' "
            f"-d '{settings}'"
        )
        create_tracking_status_index_command = (
            f"curl -X PUT '{base_url}/"
            f"{date_prefix}_"
            f"tracking_status' "
            f"-H 'Content-Type: "
            f"application/json' "
            f"-d '{settings}'"
        )
        create_specimens_index_command = (
            f"curl -X PUT '{base_url}/"
            f"{date_prefix}_specimens' "
            f"-H 'Content-Type: "
            f"application/json' -d '{settings}'"
        )

        add_data_portal_mapping_command = (
            f"curl -X PUT '{base_url}/"
            f"{date_prefix}_data_portal/"
            f"_mapping' "
            f"-H 'Content-Type: "
            f"application/json' "
            f"-d '{data_portal_mapping}'"
        )
        add_tracking_status_mapping_command = (
            f"curl -X PUT '{base_url}/"
            f"{date_prefix}_tracking_status/"
            f"_mapping' "
            f"-H 'Content-Type: "
            f"application/json' "
            f"-d '{tracking_status_mapping}'"
        )
        add_specimens_mapping_command = (
            f"curl -X PUT '{base_url}/"
            f"{date_prefix}_specimens/_mapping' "
            f"-H 'Content-Type: application/json' "
            f"-d '{specimens_mapping}'"
        )

        (
            BashOperator(
                task_id=f"{project_name}-create-data-portal-index",
                bash_command=create_data_portal_index_command,
            )
            >> BashOperator(
                task_id=f"{project_name}-add-mapping-data-portal-index",
                bash_command=add_data_portal_mapping_command,
            ),
            BashOperator(
                task_id=f"{project_name}-create-tracking-status-index",
                bash_command=create_tracking_status_index_command,
            )
            >> BashOperator(
                task_id=f"{project_name}-add-mapping-tracking-status-index",
                bash_command=add_tracking_status_mapping_command,
            ),
            BashOperator(
                task_id=f"{project_name}-create-specimens-index",
                bash_command=create_specimens_index_command,
            )
            >> BashOperator(
                task_id=f"{project_name}-add-mapping-specimens-index",
                bash_command=add_specimens_mapping_command,
            ),
        ) >> start_ingestion_job

        metadata_import_tasks >> start_ingestion_job
        import_tol_qc_data_task >> start_ingestion_job
        import_images_task >> start_ingestion_job
        import_genome_notes_task >> start_ingestion_job
        import_annotations_task >> start_ingestion_job

        if project_name == "dtol":
            data_portal_alias_name = "data_portal"
            tracking_status_alias_name = "tracking_status_index"
            specimens_alias_name = "organisms_test"
        else:
            data_portal_alias_name = "data_portal"
            tracking_status_alias_name = "tracking_status"
            specimens_alias_name = "specimens"

        change_aliases_json = {
            "actions": [
                {
                    "add": {
                        "index": f"{date_prefix}_data_portal",
                        "alias": f"{data_portal_alias_name}",
                    }
                },
                {
                    "remove": {
                        "index": f"{yesterday_day_prefix}_data_portal",
                        "alias": f"{data_portal_alias_name}",
                    }
                },
                {
                    "add": {
                        "index": f"{date_prefix}_tracking_status",
                        "alias": f"{tracking_status_alias_name}",
                    }
                },
                {
                    "remove": {
                        "index": f"{yesterday_day_prefix}_tracking_status",
                        "alias": f"{tracking_status_alias_name}",
                    }
                },
                {
                    "add": {
                        "index": f"{date_prefix}_specimens",
                        "alias": f"{specimens_alias_name}",
                    }
                },
                {
                    "remove": {
                        "index": f"{yesterday_day_prefix}_specimens",
                        "alias": f"{specimens_alias_name}",
                    }
                },
            ]
        }
        change_aliases_command = (
            f"curl -X POST '{base_url}/_aliases' "
            f"-H 'Content-Type: application/json' "
            f"-d '{json.dumps(change_aliases_json)}'"
        )
        change_aliases_task = BashOperator(
            task_id=f"{project_name}-change-aliases",
            bash_command=change_aliases_command,
        )

        (
            change_aliases_task
            << additional_task.override(task_id=f"{project_name}-additional-task")(
                host, password, project_name
            )
            << start_ingestion_job
        )

        remove_data_portal_index_command = (
            f"curl -X DELETE '{base_url}/" f"{two_days_prefix}_data_portal'"
        )
        remove_tracking_status_index_command = (
            f"curl -X DELETE '{base_url}/" f"{two_days_prefix}_tracking_status'"
        )
        remove_specimens_index_command = (
            f"curl -X DELETE '{base_url}/" f"{two_days_prefix}_specimens'"
        )
        remove_data_portal_index_task = BashOperator(
            task_id=f"{project_name}-remove-data-portal-index",
            bash_command=remove_data_portal_index_command,
        )
        remove_tracking_status_index_task = BashOperator(
            task_id=f"{project_name}-remove-tracking-status-index",
            bash_command=remove_tracking_status_index_command,
        )
        remove_specimens_index_task = BashOperator(
            task_id=f"{project_name}-remove-specimens-index",
            bash_command=remove_specimens_index_command,
        )
        change_aliases_task >> (
            remove_data_portal_index_task,
            remove_tracking_status_index_task,
            remove_specimens_index_task,
        )


biodiversity_metadata_ingestion()
