from typing import Any
from biodiv_airflow.config import BiodivConfig


def _base_environment(cfg: BiodivConfig, pipeline_label: str) -> dict[str, Any]:
    return {
        "tempLocation": cfg.df_temp_location,
        "stagingLocation": cfg.df_staging_location,
        "additionalUserLabels": {
            "app": "biodiversity",
            "dag": "biodiv_pipelines_dag",
            "pipeline": pipeline_label,
            "window_start": "{{ ds_nodash }}",
        },
        "serviceAccountEmail": cfg.dataflow_service_account,
    }


def taxonomy_body(cfg: BiodivConfig) -> dict[str, Any]:
    return {
        "launchParameter": {
            "jobName": "biodiv-taxonomy-{{ ds_nodash }}-{{ ts_nodash | lower }}",
            "containerSpecGcsPath": cfg.taxonomy_template,
            "parameters": {
                "pipeline": "taxonomy",
                "host": cfg.elastic_host,
                "user": cfg.elastic_user,
                "password": cfg.elastic_password,
                "index": "data_portal",
                "size": cfg.elastic_size,
                "pages": cfg.elastic_pages,
                "sleep": cfg.ena_sleep_s,
                "output": f"{cfg.run_prefix}/taxonomy/taxonomy",
                "bq_schema": f"{cfg.output_base}/schemas/bq_taxonomy_schema.json",
                "bq_table": f"{cfg.gcp_project}.{cfg.bq_dataset}.bp_taxonomy_validated",
                "bq_gate_table": f"{cfg.gcp_project}.{cfg.bq_dataset}.bp_log_taxonomy",
                "sdk_container_image": cfg.sdk_container_image,
                "experiments": "use_runner_v2",
            },
            "environment": _base_environment(cfg, "taxonomy"),
        }
    }


def occurrences_body(cfg: BiodivConfig) -> dict[str, Any]:
    return {
        "launchParameter": {
            "jobName": "biodiv-occurrences-{{ ds_nodash }}-{{ ts_nodash | lower }}",
            "containerSpecGcsPath": cfg.occurrences_template,
            "parameters": {
                "pipeline": "occurrences",
                "validated_input": cfg.taxonomy_validated,
                "output_dir": cfg.raw_occurrences,
                "limit": cfg.gbif_limit,
                "sleep_seconds": cfg.gbif_sleep_s,
                "retry_delay_seconds": cfg.gbif_retry_delay_s,
                "max_retries": cfg.gbif_max_retries,
                "sdk_container_image": cfg.sdk_container_image,
                "experiments": "use_runner_v2",
            },
            "environment": _base_environment(cfg, "occurrences"),
        }
    }


def cleaning_occs_body(cfg: BiodivConfig) -> dict[str, Any]:
    return {
        "launchParameter": {
            "jobName": "biodiv-occs-cleaning-{{ ds_nodash }}-{{ ts_nodash | lower }}",
            "containerSpecGcsPath": cfg.occs_cleaning_template,
            "parameters": {
                "pipeline": "cleaning_occs",
                "input_glob": f"{cfg.raw_occurrences}/occ_*.jsonl",
                "output_dir": cfg.cleaned_occurrences,
                "land_shapefile": cfg.continental_land_shapefile,
                "centroid_shapefile": cfg.centroids_shapefile,
                "min_uncertainty": "1000",
                "max_uncertainty": "5000",
                "max_centroid_dist": "1000",
                "bq_schema": f"{cfg.output_base}/schemas/bq_gbif_occurrences_schema.json",
                "bq_table": f"{cfg.gcp_project}.{cfg.bq_dataset}.bp_gbif_occurrences",
                "shards": "5",
                "sdk_container_image": cfg.sdk_container_image,
                "experiments": "use_runner_v2",
            },
            "environment": _base_environment(cfg, "occs_cleaning"),
        }
    }


def spatial_annotation_body(cfg: BiodivConfig) -> dict[str, Any]:
    return {
        "launchParameter": {
            "jobName": "biodiv-spatial-annotation-{{ ds_nodash }}-{{ ts_nodash | lower }}",
            "containerSpecGcsPath": cfg.spatial_annotation_template,
            "parameters": {
                "pipeline": "spatial_annotation",
                "input_occs": f"{cfg.cleaned_occurrences}/occ_*.jsonl",
                "annotated_output": f"{cfg.spatial_annotations}/spatial_annotations",
                "summary_output": f"{cfg.spatial_annotations}/spatial_annotations_summary",
                "climate_dir": cfg.climate_layers,
                "biogeo_vector": cfg.ecoregions_vector,
                "bq_schema": f"{cfg.output_base}/schemas/bq_spatial_annotation_summ_schema.json",
                "bq_summary_table": f"{cfg.gcp_project}.{cfg.bq_dataset}.bp_spatial_annotations",
                "sdk_container_image": cfg.sdk_container_image,
                "experiments": "use_runner_v2",
            },
            "environment": _base_environment(cfg, "spatial_annotation"),
        }
    }


def range_estimation_body(cfg: BiodivConfig) -> dict[str, Any]:
    return {
        "launchParameter": {
            "jobName": "biodiv-range-estimation-{{ ds_nodash }}-{{ ts_nodash | lower }}",
            "containerSpecGcsPath": cfg.range_estimates_template,
            "parameters": {
                "pipeline": "range_estimation",
                "input_glob": f"{cfg.cleaned_occurrences}/occ_*.jsonl",
                "bq_schema": f"{cfg.output_base}/schemas/bq_range_estimates_schema.json",
                "bq_table": f"{cfg.gcp_project}.{cfg.bq_dataset}.bp_species_range_estimates",
                "sdk_container_image": cfg.sdk_container_image,
                "experiments": "use_runner_v2",
            },
            "environment": _base_environment(cfg, "range_estimation"),
        }
    }


def data_provenance_body(cfg: BiodivConfig) -> dict[str, Any]:
    return {
        "launchParameter": {
            "jobName": "biodiv-run-data-provenance-{{ ds_nodash }}-{{ ts_nodash | lower }}",
            "containerSpecGcsPath": cfg.data_provenance_template,
            "parameters": {
                "pipeline": "data_provenance",
                "host": cfg.elastic_host,
                "user": cfg.elastic_user,
                "password": cfg.elastic_password,
                "index": "data_portal",
                "min_batch_size": cfg.beam_min_batch_size,
                "max_batch_size": cfg.beam_max_batch_size,
                "taxonomy_path": cfg.taxonomy_validated,
                "output": cfg.data_provenance,
                "bq_schema": f"{cfg.output_base}/schemas/bq_metadata_url_schema.json",
                "bq_table": f"{cfg.gcp_project}.{cfg.bq_dataset}.bp_provenance_metadata",
                "sdk_container_image": cfg.sdk_container_image,
                "experiments": "use_runner_v2",
            },
            "environment": _base_environment(cfg, "data_provenance"),
        }
    }
