from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from biodiv_airflow.annotations_update import (
    build_update_manifest,
    select_current_annotations,
)
from biodiv_airflow.sql_queries import (
    build_create_annotation_update_manifest_stage_sql,
    build_update_provenance_metadata_from_manifest_sql,
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

    create_manifest_stage_table = BigQueryInsertJobOperator(
        task_id="create_annotation_update_manifest_stage_table",
        configuration={
            "query": {
                "query": build_create_annotation_update_manifest_stage_sql(cfg),
                "useLegacySql": False,
            }
        },
    )

    load_provenance_only_manifest_stage = BigQueryInsertJobOperator(
        task_id="load_provenance_only_manifest_stage",
        configuration={
            "load": {
                "sourceUris": [provenance_only_updates_uri],
                "destinationTable": {
                    "projectId": cfg.gcp_project,
                    "datasetId": cfg.bq_dataset,
                    "tableId": "bp_annotation_update_manifest_stage",
                },
                "sourceFormat": "NEWLINE_DELIMITED_JSON",
                "writeDisposition": "WRITE_TRUNCATE",
                "schema": {
                    "fields": [
                        {"name": "tax_id", "type": "STRING"},
                        {"name": "species", "type": "STRING"},
                        {"name": "previous_accession", "type": "STRING"},
                        {"name": "new_accession", "type": "STRING"},
                        {"name": "previous_gtf_url", "type": "STRING"},
                        {"name": "new_gtf_url", "type": "STRING"},
                        {"name": "old_ensembl_url", "type": "STRING"},
                        {"name": "new_ensembl_url", "type": "STRING"},
                        {"name": "Biodiversity_portal", "type": "STRING"},
                        {"name": "gbif_url", "type": "STRING"},
                        {"name": "action", "type": "STRING"},
                        {"name": "requires_gtf_reload", "type": "BOOL"},
                        {"name": "requires_provenance_update", "type": "BOOL"},
                        {"name": "selection_reason", "type": "STRING"},
                        {"name": "assembly_classification", "type": "STRING"},
                        {"name": "previous_gtf_size_bytes", "type": "INT64"},
                        {"name": "new_gtf_size_bytes", "type": "INT64"},
                        {"name": "previous_gtf_last_modified", "type": "STRING"},
                        {"name": "new_gtf_last_modified", "type": "STRING"},
                        {"name": "previous_gtf_url_pattern", "type": "STRING"},
                        {"name": "new_gtf_url_pattern", "type": "STRING"},
                    ]
                },
            }
        },
    )

    update_provenance_metadata = BigQueryInsertJobOperator(
        task_id="update_provenance_metadata",
        configuration={
            "query": {
                "query": build_update_provenance_metadata_from_manifest_sql(cfg),
                "useLegacySql": False,
            }
        },
    )

    (
            validate
            >> select_annotations
            >> build_manifests
            >> create_manifest_stage_table
            >> load_provenance_only_manifest_stage
            >> update_provenance_metadata
    )

