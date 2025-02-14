#!/usr/bin/env python3
# import_images.py

"""
Synchronously get data from BioImage archive and put in ERGA API
"""
import logging
import requests
from collections import defaultdict
from elasticsearch import Elasticsearch, helpers


def main(es_host: str, es_password: str) -> None:
    logging.info("Connecting to Elasticsearch")
    es = Elasticsearch(hosts=[f"https://{es_host}"],
                       http_auth=("elastic", es_password))

    logging.info("Fetching BioImage archive data")
    images = requests.get(
        "https://ftp.ebi.ac.uk/biostudies/fire/S-BIAD/588/S-BIAD588/Files/"
        "09052024_dtol_reupload_file_list.json", timeout=60).json()

    images_data = defaultdict(list)
    actions = []
    logging.info("Aggregating images under nhmuk_id")
    for image in images:
        url = f"https://www.ebi.ac.uk/biostudies/files/S-BIAD588/{image['path']}"
        for record in image['attributes']:
            if record['name'] == 'NHMUK Barcode':
                images_data[record['value']].append(url)

    logging.info("Generating Elasticsearch actions for bulk update")
    for nhmuk_id, images in images_data.items():
        body = {"images": images}
        actions.append({
            "_op_type": "index",
            "_index": "images",
            "_id": nhmuk_id,
            "_source": body
        })
    try:
        logging.info("Starting bulk update")
        helpers.bulk(es, actions)
    except helpers.BulkIndexError as e:
        logging.warning(e)
