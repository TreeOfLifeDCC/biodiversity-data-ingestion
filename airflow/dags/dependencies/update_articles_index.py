import time

import requests

from elasticsearch import Elasticsearch
from datetime import datetime

# Page size for paginated ES reads. 10000 (the index.max_result_window default)
# pulls a large payload per request and can exceed the client socket timeout.
PAGE_SIZE = 1000
# Generous per-request timeout (seconds) so heavy search/bulk calls don't time out.
REQUEST_TIMEOUT = 120


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
                # EuropePMC occasionally returns an empty/non-JSON body or a
                # transient 5xx/429; retry a few times and skip pub_year on
                # persistent failure rather than aborting the whole task.
                pub_year = None
                for attempt in range(3):
                    try:
                        resp = requests.get(
                            "https://www.ebi.ac.uk/europepmc/webservices/rest/search",
                            params={"query": article["study_id"], "format": "json"},
                            timeout=REQUEST_TIMEOUT,
                        )
                        resp.raise_for_status()
                        results = resp.json()["resultList"]["result"]
                        if results:
                            pub_year = results[0].get("pubYear")
                        break
                    except (requests.RequestException, ValueError, KeyError) as exc:
                        print(
                            f"EuropePMC lookup failed for {article['study_id']} "
                            f"(attempt {attempt + 1}/3): {exc}"
                        )
                        time.sleep(1)
                article["pub_year"] = pub_year
                article["pubYear"] = pub_year
                article["id"] = article["study_id"]
                article["articleType"] = "Genome Note"
                article["journalTitle"] = "Wellcome Open Res"
                article["organism_name"] = record["organism"]
                articles.append(
                    {"index": {"_index": "articles", "_id": article["study_id"]}}
                )
                articles.append(article)
    for i in range(0, len(articles), PAGE_SIZE):
        print(f"Working on {i}: {i + PAGE_SIZE}")
        _ = es.bulk(body=articles[i : i + PAGE_SIZE], request_timeout=REQUEST_TIMEOUT)


def get_samples(index_name, es):
    samples = dict()
    # search_after needs a total ordering. _shard_doc is a heap-free tiebreaker
    # that is only available inside a Point-In-Time. Sorting on _id (the previous
    # approach) requires fielddata on _id, which is disabled by default and loads
    # every doc's _id onto the JVM heap - dangerous on a small node. The PIT also
    # makes the scan a consistent snapshot, so concurrent writes can't cause
    # skips/duplicates across page boundaries.
    # Only request the two fields update_articles_index() actually reads, to keep
    # the per-page payload and server-side fetch heap small.
    pit_id = es.open_point_in_time(index=index_name, keep_alive="2m")["id"]
    search_body = {
        "size": PAGE_SIZE,
        "_source": ["genome_notes", "organism"],
        "sort": [{"tax_id": "asc"}, {"_shard_doc": "asc"}],
        "pit": {"id": pit_id, "keep_alive": "2m"},
    }
    try:
        response = es.search(body=search_body, request_timeout=REQUEST_TIMEOUT)
        while len(response["hits"]["hits"]) != 0:
            for sample in response["hits"]["hits"]:
                samples[sample["_id"]] = sample["_source"]
            search_body["search_after"] = response["hits"]["hits"][-1]["sort"]
            # The PIT id can be refreshed on each response; carry it forward.
            pit_id = response.get("pit_id", pit_id)
            search_body["pit"]["id"] = pit_id
            response = es.search(body=search_body, request_timeout=REQUEST_TIMEOUT)
    finally:
        es.close_point_in_time(id=pit_id)
    return samples
