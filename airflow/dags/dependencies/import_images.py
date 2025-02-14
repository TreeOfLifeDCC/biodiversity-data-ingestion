#!/usr/bin/env python3
# import_images.py

"""
Synchronously get data from BioImage archive and put in ERGA API
"""
import requests
from collections import defaultdict
from elasticsearch import Elasticsearch, helpers


def main(es_host: str, es_password: str):
    es = Elasticsearch(hosts=[f"https://{es_host}"],
                       http_auth=("elastic", es_password))

    images = requests.get(
        "https://ftp.ebi.ac.uk/biostudies/fire/S-BIAD/588/S-BIAD588/Files/"
        "09052024_dtol_reupload_file_list.json").json()
    images_data = defaultdict(list)
    actions = []
    for image in images:
        url = f"https://www.ebi.ac.uk/biostudies/files/S-BIAD588/{image['path']}"
        for record in image['attributes']:
            if record['name'] == 'NHMUK Barcode':
                images_data[record['value']].append(url)
    for nhmuk_id, images in images_data.items():
        body = {"images": images}
        actions.append({
            "_op_type": "index",
            "_index": "images",
            "_id": nhmuk_id,
            "_source": body
        })
    try:
        helpers.bulk(es, actions)
    except helpers.BulkIndexError as e:
        print(e)
