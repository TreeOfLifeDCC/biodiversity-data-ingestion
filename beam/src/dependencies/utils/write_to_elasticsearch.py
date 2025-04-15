import apache_beam as beam

from datetime import datetime
from elasticsearch import Elasticsearch, helpers
from elasticsearch.helpers import BulkIndexError
from google.cloud import secretmanager


class WriteToElasticsearchDoFn(beam.DoFn):

    def __init__(self, index, project_name):
        super().__init__()
        self.index = f"{datetime.today().strftime("%Y-%m-%d")}_{index}"
        self.project_name = project_name
        self.es = None
        self.actions = None

    def setup(self):
        client = secretmanager.SecretManagerServiceClient()
        host = client.access_secret_version(request={
            "name": f"projects/153439618737/secrets/"
                    f"{self.project_name}_elasticsearch_host/versions/latest"}
        ).payload.data.decode("UTF-8")
        password = client.access_secret_version(request={
            "name": f"projects/153439618737/secrets/"
                    f"{self.project_name}_elasticsearch_password/versions/"
                    f"latest"}).payload.data.decode("UTF-8")
        self.es = Elasticsearch([host], http_auth=("elastic", password))

    def start_bundle(self):
        self.actions = []

    def process(self, element):
        record: dict
        record_id: str
        if "tracking_status" in self.index:
            record = dict()
            record['organism'] = element['organism']
            record['commonName'] = element['commonName']
            record['biosamples'] = 'Done'
            record['biosamples_date'] = None
            record['ena_date'] = None
            record['annotation_date'] = None
            record['raw_data'] = "Done" if "experiment" in element and len(
                element["experiment"]) > 0 else "Waiting"
            record['mapped_reads'] = element['raw_data']
            record['assemblies'] = "Done" if "assemblies" in element and len(
                element["assemblies"]) > 0 else "Waiting"
            record["assemblies_status"] = record['assemblies']
            record['annotation'] = 'Waiting'
            record['annotation_complete'] = "Done" if element[
                                                          "currentStatus"] == "Annotation Complete" else "Waiting"
            record['trackingSystem'] = [
                {'name': 'biosamples', 'status': 'Done', 'rank': 1},
                {'name': 'mapped_reads', 'status': record['mapped_reads'],
                 'rank': 2},
                {'name': 'assemblies', 'status': record['assemblies'],
                 'rank': 3},
                {'name': 'raw_data', 'status': record['raw_data'], 'rank': 4},
                {'name': 'annotation', 'status': 'Waiting', 'rank': 5},
                {'name': 'annotation_complete',
                 'status': record['annotation_complete'], 'rank': 6}
            ]
            for field_name in ["taxonomies", "symbionts_records",
                               "symbionts_assemblies", "symbionts_status",
                               "symbionts_experiments",
                               "symbionts_biosamples_status",
                               "symbionts_assemblies_status",
                               "metagenomes_records", "metagenomes_experiments",
                               "metagenomes_assemblies",
                               "metagenomes_biosamples_status",
                               "metagenomes_assemblies_status"]:
                try:
                    record[field_name] = element[field_name]
                except KeyError:
                    continue
            record["project_name"] = element["project_name"]
            if self.project_name in ["erga", "gbdp"]:
                record_id = element["tax_id"]
            else:
                record_id = element['organism']
        elif "specimens" in self.index:
            record = element
            record_id = element["accession"]
        else:
            if self.project_name == "asg":
                record = element
            else:
                record = {k: v for k, v in element.items() if
                          k not in ["analyses", "metagenomes_analyses"]}
            if self.project_name in ["erga", "gbdp"]:
                record_id = element["tax_id"]
            else:
                record_id = element['organism']
        self.actions.append({"index": {"_index": self.index, "_id": record_id}})
        self.actions.append(record)

    def finish_bundle(self):
        if self.actions:
            for i in range(0, len(self.actions), 1000):
                _ = self.es.bulk(body=self.actions[i:i+1000], request_timeout=60)
            self.actions = []
