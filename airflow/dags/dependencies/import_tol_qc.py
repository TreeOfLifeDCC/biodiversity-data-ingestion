#!/usr/bin/env python3
# import_tol_qc.py

"""
Synchronously get tol qc data and put in ERGA API
"""
import requests
from collections import defaultdict
from elasticsearch import Elasticsearch, helpers


def main(es_host: str, es_password: str):
    es = Elasticsearch(hosts=[f"https://{es_host}"],
                       http_auth=("elasticsearch", es_password))

    tolqc_data = requests.get("https://tolqc.cog.sanger.ac.uk/data.json").json()
    tolqc_dict = defaultdict(list)
    actions = []
    for record in tolqc_data:
        link = (f"https://tolqc.cog.sanger.ac.uk/{record['group']}/"
                f"{record['_name']}")
        tolqc_dict[record["taxon"]].append(link)
    for tax_id, links in tolqc_dict.items():
        body = {"tol_qc_links": links}
        actions.append({
            "_op_type": "index",
            "_index": "tol_qc",
            "_id": tax_id,
            "_source": body
        })
    helpers.bulk(es, actions)
