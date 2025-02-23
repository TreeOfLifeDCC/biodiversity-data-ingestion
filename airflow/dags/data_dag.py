import pendulum
import json
import gzip
import io
import requests
import csv

from elasticsearch import Elasticsearch

from airflow.decorators import dag, task
from airflow.io.path import ObjectStoragePath
from airflow.models import Variable


@task
def generate_annotations_list(offset: int):
    annotations_list = []
    gbdp_host = Variable.get("gbdp_elasticsearch_host")
    gbdp_password = Variable.get("gbdp_elasticsearch_password")
    es_client = Elasticsearch(
        [f"https://{gbdp_host}"],
        http_auth=("elastic", gbdp_password),
        verify_certs=True,
    )
    # TODO: add pagination to search
    search_body = {
        "size": 500,
        "query": {
            "bool": {
                "filter": [{"terms": {"currentStatus": ["Annotation Complete"]}}]
            }
        }
    }
    annotations_data = es_client.search(index="data_portal", body=search_body,
                                        from_=offset)
    for record in annotations_data["hits"]["hits"]:
        for annotation in record["_source"]["annotation"]:
            url = annotation["annotation"]["GTF"]
            file_id = url.split("/")[-1]
            annotations_list.append({"url": url, "file_id": file_id})
    return annotations_list


@task
def ingest_gtf(arg) -> None:
    url, file_id = arg["url"], arg["file_id"]
    base = ObjectStoragePath(
        "gs://google_cloud_default@prj-ext-prod-biodiv-data-in-gbdp/gtf_files")
    base.mkdir(exist_ok=True)
    gca_accession = url.split("/")[7]
    path = base / f"{file_id}.jsonl"
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
    ingest_gtf.expand(arg=generate_annotations_list(0))


biodiversity_annotations_ingestion()
