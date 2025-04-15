from airflow.providers.google.cloud.operators.dataflow import (
    DataflowStartFlexTemplateOperator
)


def start_apache_beam(biodiversity_project_name):
    gc_project_name = "prj-ext-prod-biodiv-data-in"
    region = "europe-west2"
    body = {
        "launchParameter": {
            "jobName": "biodiversity-ingestion-2025-03-18",
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
                                     f"biodiversity_etl:20250318-081117"
            },
            "containerSpecGcsPath": f"gs://{gc_project_name}_cloudbuild/"
                                    f"biodiversity_etl-20250318-081117.json"
        }
    }
    return DataflowStartFlexTemplateOperator(
        task_id=f"start_ingestion_job_{biodiversity_project_name}",
        project_id=gc_project_name,
        body=body,
        location=region,
        wait_until_finished=True
    )
