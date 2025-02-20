import pendulum
import json
import gzip
import io
import requests
import csv

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
from dependencies import import_tol_qc, import_images


@task
def ingest_gtf(url: str) -> None:
    base = ObjectStoragePath(
        "gs://google_cloud_default@prj-ext-prod-biodiv-data-in-gbdp")
    base.mkdir(exist_ok=True)
    gca_accession = url.split("/")[7]
    path = base / f"{gca_accession}.jsonl"
    gtf_gz_file = requests.get(url, stream=True, timeout=30).content
    f = io.BytesIO(gtf_gz_file)
    with gzip.GzipFile(fileobj=f) as fh:
        output = path.open("w")
        reader = csv.reader(io.TextIOWrapper(fh, "utf-8"))
        for row in reader:
            row = row[0]
            if row.startswith("#"):
                continue
            data = row.split("\t")
            formatted_row = {
                "accession": gca_accession,
                "record_type": data[2],
                "info": data[8]
            }
            output.write(json.dumps(formatted_row) + "\n")
        output.close()


@dag(
    schedule="* * * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["biodiversity_annotations_ingestion"],
)
def biodiversity_annotations_ingestion():
    """
    This DAG downloads GTF files, format them into json files and upload to GCS
    """
    ingest_gtf.override(task_id=f"test_ingest_gtf")("https://ftp.ensembl.org/pub/rapid-release/species/Bathymodiolus_brooksi/GCA_963680875.1/ensembl/geneset/2024_01/Bathymodiolus_brooksi-GCA_963680875.1-2024_01-genes.gtf.gz")

biodiversity_annotations_ingestion()