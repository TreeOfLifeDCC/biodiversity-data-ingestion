from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator

from biodiv_airflow.annotations_update import (
    build_update_manifest,
    select_current_annotations,
)
from biodiv_airflow.config import load_config
from biodiv_airflow.helpers import validate_config


default_args = {
    "owner": "biodiversity",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

cfg = load_config()

manifest_prefix = f"{cfg.run_prefix}/annotations_update"
selected_annotations_uri = f"{manifest_prefix}/selected_annotations.jsonl"
rejected_missing_gtf_uri = f"{manifest_prefix}/rejected_missing_gtf_url.jsonl"
update_manifest_uri = f"{manifest_prefix}/update_manifest.jsonl"
gtf_reload_updates_uri = f"{manifest_prefix}/gtf_reload_updates.jsonl"
provenance_only_updates_uri = f"{manifest_prefix}/provenance_only_updates.jsonl"
skipped_pattern_only_uri = f"{manifest_prefix}/skipped_pattern_only_same_size.jsonl"


with DAG(
    dag_id="biodiv_annotations_update_dag",
    description="Biodiv: detect genome annotation updates and build manifests",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["biodiv", "annotations", "update"],
) as dag:

    validate = PythonOperator(
        task_id="validate_config",
        python_callable=lambda: validate_config(cfg, require_delete_service=False),
    )

    select_annotations = PythonOperator(
        task_id="select_current_annotations",
        python_callable=select_current_annotations,
        op_kwargs={
            "elastic_host": cfg.elastic_host,
            "elastic_user": cfg.elastic_user,
            "elastic_password": cfg.elastic_password,
            "elastic_index": "data_portal",
            "page_size": int(cfg.elastic_size),
            "selected_annotations_uri": selected_annotations_uri,
            "rejected_missing_gtf_uri": rejected_missing_gtf_uri,
        },
    )

    build_manifests = PythonOperator(
        task_id="build_update_manifest",
        python_callable=build_update_manifest,
        op_kwargs={
            "selected_annotations_uri": selected_annotations_uri,
            "bq_project_id": cfg.gcp_project,
            "bq_dataset": cfg.bq_dataset,
            "update_manifest_uri": update_manifest_uri,
            "gtf_reload_updates_uri": gtf_reload_updates_uri,
            "provenance_only_updates_uri": provenance_only_updates_uri,
            "skipped_pattern_only_uri": skipped_pattern_only_uri,
        },
    )

    validate >> select_annotations >> build_manifests
