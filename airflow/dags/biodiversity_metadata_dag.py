import pendulum
import json
import asyncio

from datetime import datetime, timedelta

from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

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
    # schedule="0 7 * * *",
    schedule_interval=None,
    start_date=pendulum.datetime(2025, 7, 1, tz="Europe/London"),
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
        CREATE_SAMPING_MAP_BASE_VIEW = f"""
        CREATE OR REPLACE VIEW 
        `prj-ext-prod-biodiv-data-in.{project_name}.sampling_map_base` 
        AS WITH base_data AS(SELECT main.current_status, main.tax_id, 
        main.symbionts_status, main.common_name, main.phylogenetic_tree, 
        organism.biosample_id, organism.organism, 
        SAFE_CAST(organism.latitude AS FLOAT64) AS lat, 
        SAFE_CAST(organism.longitude AS FLOAT64) AS lon, 
        COALESCE( NULLIF(TRIM(raw_data_item.library_construction_protocol), ''), 
        'No experiment data') AS library_construction_protocol, 
        CONCAT( SAFE_CAST(organism.latitude AS STRING), ',', 
        SAFE_CAST(organism.longitude AS STRING) ) AS geotag 
        FROM `prj-ext-prod-biodiv-data-in.{project_name}.metadata` AS main, 
        UNNEST(main.organisms) AS organism LEFT JOIN UNNEST(main.raw_data) AS 
        raw_data_item ON TRUE WHERE organism.biosample_id IS NOT NULL AND 
        organism.organism IS NOT NULL AND organism.latitude IS NOT NULL AND 
        organism.longitude IS NOT NULL AND SAFE_CAST(organism.latitude AS FLOAT64) 
        IS NOT NULL AND SAFE_CAST(organism.longitude AS FLOAT64) IS NOT NULL AND 
        SAFE_CAST(organism.latitude AS FLOAT64) BETWEEN -90 AND 90 AND 
        SAFE_CAST(organism.longitude AS FLOAT64) BETWEEN -180 AND 180 ) 
        SELECT biosample_id, organism, current_status, tax_id, symbionts_status, 
        common_name, phylogenetic_tree.kingdom.scientific_name AS Kingdom, 
        lat, lon, geotag, STRING_AGG(DISTINCT library_construction_protocol, ', ') 
        AS experiment_type FROM base_data GROUP BY biosample_id, organism, 
        current_status, tax_id, symbionts_status, common_name, 
        phylogenetic_tree.kingdom.scientific_name, lat, lon, geotag;
        """
        CREATE_SAMPING_MAP_AGGREGATED_VIEW = f"""
        CREATE OR REPLACE VIEW 
        `prj-ext-prod-biodiv-data-in.{project_name}.sampling_map_aggregated` AS 
        SELECT geotag, lat, lon, STRING_AGG(DISTINCT biosample_id, ', ') AS 
        biosample_ids, STRING_AGG(DISTINCT organism, ', ') AS organisms, 
        ARRAY_AGG(DISTINCT Kingdom IGNORE NULLS) AS kingdoms, 
        STRING_AGG(DISTINCT common_name, ', ') AS common_names, 
        STRING_AGG(DISTINCT current_status, ', ') AS current_statuses, 
        STRING_AGG(DISTINCT experiment_type, ', ') AS experiment_types, 
        COUNT(DISTINCT organism) AS record_count FROM 
        `prj-ext-prod-biodiv-data-in.{project_name}.sampling_map_base` GROUP BY 
        geotag, lat, lon;
        """
        sampling_map_base_view = BigQueryInsertJobOperator(
            task_id=f"sampling_map_base_view_job_{project_name}",
            configuration={
                "query": {
                    "query": CREATE_SAMPING_MAP_BASE_VIEW,
                    "useLegacySql": False,
                    "priority": "BATCH",
                }
            },
        )
        sampling_map_aggregated_view = BigQueryInsertJobOperator(
            task_id=f"sampling_map_aggregated_view_job_{project_name}",
            configuration={
                "query": {
                    "query": CREATE_SAMPING_MAP_AGGREGATED_VIEW,
                    "useLegacySql": False,
                    "priority": "BATCH",
                }
            },
        )
        (change_aliases_task >> sampling_map_base_view >> sampling_map_aggregated_view)

        CREATE_METADATA_AGGREGATED_VIEW = f"""
        CREATE OR REPLACE VIEW 
        `prj-ext-prod-biodiv-data-in.{project_name}.metadata_aggregated` AS WITH 
        base_data AS( SELECT main.current_status, main.tax_id, main.symbionts_status, 
        main.common_name, organism.biosample_id, organism.organism, organism.sex, 
        organism.lifestage, organism.habitat FROM 
        `prj-ext-prod-biodiv-data-in.{project_name}.metadata` as main, 
        UNNEST(main.organisms) as organism WHERE organism.biosample_id IS NOT NULL 
        AND organism.organism IS NOT NULL), sex_aggregates AS ( SELECT '{project_name}' as 
        project_name, sex, COUNT(DISTINCT organism) as record_count, 
        COUNT(DISTINCT biosample_id) as biosample_count, STRING_AGG(DISTINCT 
        biosample_id, ',') as sample_biosample_ids, STRING_AGG(DISTINCT organism, ',') 
        as sample_organisms, STRING_AGG(DISTINCT current_status, ',') as 
        sample_statuses FROM base_data WHERE sex IS NOT NULL GROUP BY sex ), 
        lifestage_aggregates AS ( SELECT '{project_name}' as project_name, lifestage, 
        COUNT(DISTINCT organism) as record_count, COUNT(DISTINCT biosample_id) as 
        biosample_count, STRING_AGG(DISTINCT biosample_id, ',') as 
        sample_biosample_ids, STRING_AGG(DISTINCT organism, ',') as sample_organisms, 
        STRING_AGG(DISTINCT current_status, ',') as sample_statuses FROM base_data 
        WHERE lifestage IS NOT NULL GROUP BY lifestage ), habitat_aggregates AS 
        ( SELECT '{project_name}' as project_name, habitat, COUNT(DISTINCT organism) as 
        record_count, COUNT(DISTINCT biosample_id) as biosample_count, 
        STRING_AGG(DISTINCT biosample_id, ',') as sample_biosample_ids, 
        STRING_AGG(DISTINCT organism, ',') as sample_organisms, 
        STRING_AGG(DISTINCT current_status, ',') as sample_statuses FROM base_data 
        WHERE habitat IS NOT NULL GROUP BY habitat ), cross_filter_data AS 
        ( SELECT '{project_name}' as project_name, sex, lifestage, habitat, 
        COUNT(DISTINCT organism) as record_count, COUNT(DISTINCT biosample_id) 
        as biosample_count FROM base_data GROUP BY sex, lifestage, habitat ) 
        SELECT 'sex' as dimension, project_name, sex as value, record_count, 
        biosample_count, sample_biosample_ids, sample_organisms, sample_statuses, 
        CAST(NULL AS STRING) as filter_sex, CAST(NULL AS STRING) as filter_lifestage, 
        CAST(NULL AS STRING) as filter_habitat FROM sex_aggregates UNION ALL SELECT 
        'lifestage' as dimension, project_name, lifestage as value, record_count, 
        biosample_count, sample_biosample_ids, sample_organisms, sample_statuses, 
        CAST(NULL AS STRING) as filter_sex, CAST(NULL AS STRING) as filter_lifestage, 
        CAST(NULL AS STRING) as filter_habitat FROM lifestage_aggregates UNION ALL 
        SELECT 'habitat' as dimension, project_name, habitat as value, record_count, 
        biosample_count, sample_biosample_ids, sample_organisms, sample_statuses, 
        CAST(NULL AS STRING) as filter_sex, CAST(NULL AS STRING) as filter_lifestage, 
        CAST(NULL AS STRING) as filter_habitat FROM habitat_aggregates UNION ALL 
        SELECT 'cross_filter' as dimension, project_name, CONCAT(IFNULL(sex, 'NULL'), 
        '|', IFNULL(lifestage, 'NULL'), '|', IFNULL(habitat, 'NULL')) as value, 
        record_count, biosample_count, CAST(NULL AS STRING) as sample_biosample_ids, 
        CAST(NULL AS STRING) as sample_organisms, CAST(NULL AS STRING) as 
        sample_statuses, sex as filter_sex, lifestage as filter_lifestage, 
        habitat as filter_habitat FROM cross_filter_data;
        """
        CREATE_RAW_DATA_AGGREGATED_VIEW = f"""
        CREATE OR REPLACE VIEW 
        `prj-ext-prod-biodiv-data-in.{project_name}.rawdata_aggregated` AS WITH 
        base_data AS( SELECT main.current_status, main.tax_id, main.symbionts_status, 
        main.common_name, organism.biosample_id, organism.organism, 
        raw_data_item.instrument_platform, raw_data_item.instrument_model, 
        raw_data_item.library_construction_protocol, CASE WHEN 
        raw_data_item.first_public IS NOT NULL THEN SAFE.PARSE_DATE('%Y-%m-%d', 
        raw_data_item.first_public) ELSE NULL END as first_public FROM 
        `prj-ext-prod-biodiv-data-in.{project_name}.metadata` as main, 
        UNNEST(main.organisms) as organism LEFT JOIN UNNEST(main.raw_data) as 
        raw_data_item ON TRUE WHERE organism.biosample_id IS NOT NULL AND 
        organism.organism IS NOT NULL), instrument_platform_aggregates AS 
        ( SELECT '{project_name}' as project_name, instrument_platform, 
        COUNT(DISTINCT organism) as record_count, COUNT(DISTINCT biosample_id) as 
        biosample_count, STRING_AGG(DISTINCT biosample_id, ',') as sample_biosample_ids, 
        STRING_AGG(DISTINCT organism, ',') as sample_organisms, 
        STRING_AGG(DISTINCT current_status, ',') as sample_statuses FROM base_data 
        WHERE instrument_platform IS NOT NULL GROUP BY instrument_platform ), 
        instrument_model_aggregates AS ( SELECT '{project_name}' as project_name, 
        instrument_model, COUNT(DISTINCT organism) as record_count, 
        COUNT(DISTINCT biosample_id) as biosample_count, 
        STRING_AGG(DISTINCT biosample_id, ',') as sample_biosample_ids, 
        STRING_AGG(DISTINCT organism, ',') as sample_organisms, 
        STRING_AGG(DISTINCT current_status, ',') as sample_statuses FROM base_data 
        WHERE instrument_model IS NOT NULL GROUP BY instrument_model ), 
        library_protocol_aggregates AS ( SELECT '{project_name}' as project_name, 
        library_construction_protocol, COUNT(DISTINCT organism) as record_count, 
        COUNT(DISTINCT biosample_id) as biosample_count, 
        STRING_AGG(DISTINCT biosample_id, ',') as sample_biosample_ids, 
        STRING_AGG(DISTINCT organism, ',') as sample_organisms, 
        STRING_AGG(DISTINCT current_status, ',') as sample_statuses FROM 
        base_data WHERE library_construction_protocol IS NOT NULL GROUP BY 
        library_construction_protocol ), time_series_aggregates AS 
        ( SELECT '{project_name}' as project_name, first_public, 
        COUNT(DISTINCT organism) as record_count, COUNT(DISTINCT biosample_id) as 
        biosample_count, STRING_AGG(DISTINCT biosample_id, ',') as 
        sample_biosample_ids, STRING_AGG(DISTINCT organism, ',') as sample_organisms, 
        STRING_AGG(DISTINCT current_status, ',') as sample_statuses FROM base_data 
        WHERE first_public IS NOT NULL GROUP BY first_public ), cross_filter_data 
        AS ( SELECT '{project_name}' as project_name, instrument_platform, 
        instrument_model, library_construction_protocol, first_public, 
        COUNT(DISTINCT organism) as record_count, COUNT(DISTINCT biosample_id) 
        as biosample_count FROM base_data GROUP BY instrument_platform, 
        instrument_model, library_construction_protocol, first_public ) 
        SELECT 'instrument_platform' as dimension, project_name, 
        instrument_platform as value, record_count, biosample_count, 
        sample_biosample_ids, sample_organisms, sample_statuses, 
        CAST(NULL AS STRING) as filter_platform, CAST(NULL AS STRING) as 
        filter_model, CAST(NULL AS STRING) as filter_protocol, CAST(NULL AS DATE) as 
        filter_date FROM instrument_platform_aggregates UNION ALL SELECT 
        'instrument_model' as dimension, project_name, instrument_model as value, 
        record_count, biosample_count, sample_biosample_ids, sample_organisms, 
        sample_statuses, CAST(NULL AS STRING) as filter_platform, 
        CAST(NULL AS STRING) as filter_model, CAST(NULL AS STRING) as filter_protocol, 
        CAST(NULL AS DATE) as filter_date FROM instrument_model_aggregates 
        UNION ALL SELECT 'library_construction_protocol' as dimension, project_name, 
        library_construction_protocol as value, record_count, biosample_count, 
        sample_biosample_ids, sample_organisms, sample_statuses, CAST(NULL AS STRING) 
        as filter_platform, CAST(NULL AS STRING) as filter_model, CAST(NULL AS STRING) 
        as filter_protocol, CAST(NULL AS DATE) as filter_date FROM 
        library_protocol_aggregates UNION ALL SELECT 'time_series' as 
        dimension, project_name, CAST(first_public AS STRING) as value, 
        record_count, biosample_count, sample_biosample_ids, sample_organisms, 
        sample_statuses, CAST(NULL AS STRING) as filter_platform, CAST(NULL AS STRING) 
        as filter_model, CAST(NULL AS STRING) as filter_protocol, CAST(NULL AS DATE) 
        as filter_date FROM time_series_aggregates UNION ALL SELECT 'cross_filter' 
        as dimension, project_name, CONCAT( IFNULL(instrument_platform, 'NULL'), '|', 
        IFNULL(instrument_model, 'NULL'), '|', IFNULL(library_construction_protocol, 
        'NULL'), '|', IFNULL(CAST(first_public AS STRING), 'NULL') ) as value, 
        record_count, biosample_count, CAST(NULL AS STRING) as sample_biosample_ids, 
        CAST(NULL AS STRING) as sample_organisms, CAST(NULL AS STRING) as 
        sample_statuses, instrument_platform as filter_platform, instrument_model 
        as filter_model, library_construction_protocol as filter_protocol, 
        first_public as filter_date FROM cross_filter_data;
        """

        CREATE_TABLE_DATA_VIEW = f"""
        CREATE OR REPLACE VIEW `prj-ext-prod-biodiv-data-in.{project_name}.table_data` 
        AS WITH base_data AS( SELECT '{project_name}' as project_name, 
        main.current_status, main.tax_id, main.symbionts_status, 
        main.common_name, organism.biosample_id, organism.organism, organism.sex, 
        organism.lifestage, organism.habitat, raw_data_item.instrument_platform, 
        raw_data_item.instrument_model, raw_data_item.library_construction_protocol, 
        CASE WHEN raw_data_item.first_public IS NOT NULL 
        THEN SAFE.PARSE_DATE('%Y-%m-%d', raw_data_item.first_public) 
        ELSE NULL END as first_public FROM 
        `prj-ext-prod-biodiv-data-in.{project_name}.metadata` 
        as main, UNNEST(main.organisms) as organism LEFT JOIN UNNEST(main.raw_data) 
        as raw_data_item ON TRUE WHERE organism.biosample_id IS NOT NULL AND 
        organism.organism IS NOT NULL), grouped_data AS ( SELECT project_name, 
        current_status, tax_id, symbionts_status, common_name, biosample_id, organism, 
        sex, lifestage, habitat, instrument_platform, instrument_model, 
        library_construction_protocol, first_public FROM base_data GROUP BY 
        project_name, biosample_id, current_status, tax_id, symbionts_status, 
        common_name, organism, sex, lifestage, habitat, instrument_platform, 
        instrument_model, library_construction_protocol, first_public ) 
        SELECT DISTINCT project_name, organism, common_name, current_status, 
        symbionts_status, sex, lifestage, habitat, instrument_platform, 
        instrument_model, library_construction_protocol, first_public FROM grouped_data;
        """
        metadata_aggregated_view = BigQueryInsertJobOperator(
            task_id=f"metadata_aggregated_view_job_{project_name}",
            configuration={
                "query": {
                    "query": CREATE_METADATA_AGGREGATED_VIEW,
                    "useLegacySql": False,
                    "priority": "BATCH",
                }
            },
        )
        rawdata_aggregated_view = BigQueryInsertJobOperator(
            task_id=f"rawdata_aggregated_view_job_{project_name}",
            configuration={
                "query": {
                    "query": CREATE_RAW_DATA_AGGREGATED_VIEW,
                    "useLegacySql": False,
                    "priority": "BATCH",
                }
            },
        )
        table_data_view = BigQueryInsertJobOperator(
            task_id=f"table_data_view_job_{project_name}",
            configuration={
                "query": {
                    "query": CREATE_TABLE_DATA_VIEW,
                    "useLegacySql": False,
                    "priority": "BATCH",
                }
            },
        )
        (
            change_aliases_task
            >> metadata_aggregated_view
            >> rawdata_aggregated_view
            >> table_data_view
        )


biodiversity_metadata_ingestion()
