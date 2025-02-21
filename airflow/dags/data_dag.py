import pendulum
import json
import gzip
import io
import requests
import csv

from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath


@task
def ingest_gtf(url: str, file_id: str) -> None:
    base = ObjectStoragePath(
        "gs://google_cloud_default@prj-ext-prod-biodiv-data-in-gbdp/gtf_files")
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
    schedule_interval=None,
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    tags=["biodiversity_annotations_ingestion"],
)
def biodiversity_annotations_ingestion():
    """
    This DAG downloads GTF files, format them into json files and upload to GCS
    """
    offset = 0
    gbdp_response = requests.get(
        f"https://www.ebi.ac.uk/biodiversity/api/data_portal?limit=1000&offset={offset}"
        f"&filter=annotation_complete:Done").json()
    results_length = len(gbdp_response["results"])
    annotations_data = gbdp_response["results"]
    while results_length != 0:
        offset += 1000
        gbdp_response = requests.get(
            f"https://www.ebi.ac.uk/biodiversity/api/data_portal?limit=1000&offset="
            f"{offset}&filter=annotation_complete:Done").json()
        results_length = len(gbdp_response["results"])
        annotations_data.extend(gbdp_response["results"])
    for record in annotations_data:
        for annotation in record["_source"]["annotation"]:
            url = annotation["annotation"]["GTF"]
            ingestion_id = url.split("/")[-1]
            ingest_gtf.override(task_id=f"{ingestion_id}_ingest_gtf")(
                url=url, file_id=ingestion_id)


biodiversity_annotations_ingestion()
