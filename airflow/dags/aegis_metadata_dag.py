import pendulum
import json

from airflow.decorators import dag, task

from dependencies.aegis_projects import aegis_projects


@task
def get_aegis_metadata(study_id: str, bucket_name: str) -> None:
    from dependencies import collect_metadata_experiments_assemblies

    metadata = collect_metadata_experiments_assemblies.main(study_id, "AEGIS",
                                                            bucket_name)
    base = ObjectStoragePath(f"gs://google_cloud_default@{bucket_name}")
    base.mkdir(exist_ok=True)
    path = base / f"{study_id}.jsonl"
    with path.open("w") as file:
        for sample_id, record in metadata.items():
            file.write(f"{json.dumps(record)}\n")

    @dag(
        schedule="0 11 * * *",
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        tags=["aegis_metadata_ingestion"],
    )
    def aegis_metadata_ingestion():
        """
        Thi DAG pulls data from ENA (PRJEB80366) and BioSamples and builds
        Elasticsearch indices
        """
        pass
