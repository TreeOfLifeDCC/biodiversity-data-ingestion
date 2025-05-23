import requests

from elasticsearch import Elasticsearch
from datetime import datetime


def update_articles_index(host: str, password: str):
    date_prefix = datetime.today().strftime("%Y-%m-%d")
    es = Elasticsearch([f"https://{host}"], http_auth=("elastic", password))
    data_portal = get_samples(f"{date_prefix}_data_portal", es)
    articles = list()
    for tax_id, record in data_portal.items():
        print(
            f"{list(data_portal.keys()).index(tax_id) / len(data_portal) * 100}\r",
            end="",
            flush=True,
        )
        if "genome_notes" in record and len(record["genome_notes"]) > 0:
            for article in record["genome_notes"]:
                article_response = requests.get(
                    f"https://www.ebi.ac.uk/europepmc/webservices/rest/"
                    f"search?query={article['study_id']}&format=json"
                ).json()
                if len(article_response["resultList"]["result"]) > 0:
                    pub_year = article_response["resultList"]["result"][0]["pubYear"]
                    article["pub_year"] = pub_year
                    article["pubYear"] = pub_year
                else:
                    article["pub_year"] = None
                    article["pubYear"] = None
                article["id"] = article["study_id"]
                article["articleType"] = "Genome Note"
                article["journalTitle"] = "Wellcome Open Res"
                article["organism_name"] = record["organism"]
                articles.append(
                    {"index": {"_index": "articles", "_id": article["study_id"]}}
                )
                articles.append(article)
    for i in range(0, len(articles), 10000):
        print(f"Working on {i}: {i + 10000}")
        _ = es.bulk(body=articles[i : i + 10000])


def get_samples(index_name, es):
    samples = dict()
    search_body = {"size": 10000, "sort": [{"tax_id": "asc"}]}
    response = es.search(index=index_name, body=search_body)
    while len(response["hits"]["hits"]) != 0:
        for sample in response["hits"]["hits"]:
            samples[sample["_id"]] = sample["_source"]
        search_body["search_after"] = response["hits"]["hits"][-1]["sort"]
        response = es.search(index=index_name, body=search_body)
    return samples
