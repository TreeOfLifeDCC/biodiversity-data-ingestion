#!/usr/bin/env python3
# import_tol_qc.py

"""
Synchronously get tol qc data and put in ERGA API
"""
import requests
import logging

from collections import defaultdict
from elasticsearch import Elasticsearch, helpers
from urllib3.exceptions import ReadTimeoutError


def main(es_host: str, es_password: str) -> None:
    logging.info("Connecting to Elasticsearch")
    es = Elasticsearch(hosts=[f"https://{es_host}"],
                       http_auth=("elastic", es_password))

    logging.info("Fetching tolqc data")
    try:
        tolqc_data = requests.get("https://tolqc.cog.sanger.ac.uk/data.json", timeout=60).json()
    except ReadTimeoutError:
        logging.warning("Timeout while fetching tolqc data")
        return

    tolqc_dict = defaultdict(list)
    actions = []

    logging.info("Aggregating tol qc links under tax_id")
    for record in tolqc_data:
        link = (f"https://tolqc.cog.sanger.ac.uk/{record['group']}/"
                f"{record['_name']}")
        tolqc_dict[record["taxon"]].append(link)

    logging.info("Generating Elasticsearch actions for bulk update")
    for tax_id, links in tolqc_dict.items():
        body = {"tol_qc_links": links}
        actions.append({
            "_op_type": "index",
            "_index": "tol_qc",
            "_id": tax_id,
            "_source": body
        })
    try:
        logging.info("Starting bulk update")
        helpers.bulk(es, actions)
    except helpers.BulkIndexError as e:
        logging.warning(e)
