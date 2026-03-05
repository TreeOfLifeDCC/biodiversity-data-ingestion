import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from dependencies.aegis_transforms import transform_to_aegis_format
from datetime import datetime
from elasticsearch import Elasticsearch
from google.cloud import secretmanager
from pathlib import Path


def aegis_etl(
    input_path: str,
    output_path: str,
    project_name: str,
    pipeline_options_args: list[str],
) -> beam.Pipeline:

    pipeline_options = PipelineOptions(pipeline_options_args)

    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    if not google_cloud_options.project:
        google_cloud_options.project = 'prj-ext-prod-biodiv-data-in'

    pipeline = beam.Pipeline(options=pipeline_options)

    samples = (
        pipeline
        | "Read JSONL files" >> beam.io.ReadFromText(input_path)
        | "Parse JSON" >> beam.Map(lambda line: json.loads(line))
    )

    samples_by_taxid = (
        samples
        | "Key by tax_id" >> beam.Map(lambda sample: (sample.get("taxId", "unknown"), sample))
        | "Group by tax_id" >> beam.GroupByKey()
    )

    aegis_records = (
        samples_by_taxid
        | "Transform to AEGIS format" >> beam.Map(transform_to_aegis_format)
        | "Filter None" >> beam.Filter(lambda x: x is not None)
    )

    aegis_records | "Write to Elasticsearch" >> beam.ParDo(
        WriteToAegisElasticsearchDoFn(project_name=project_name)
    )

    return pipeline



class WriteToAegisElasticsearchDoFn(beam.DoFn):

    def __init__(self, project_name):
        super().__init__()
        self.project_name = project_name
        self.index = f"{datetime.today().strftime('%Y-%m-%d')}_data_portal"
        self.es = None
        self.actions = None

    def setup(self):

        client = secretmanager.SecretManagerServiceClient()

        host = client.access_secret_version(request={
            "name": f"projects/153439618737/secrets/"
                    f"{self.project_name}_elasticsearch_host/versions/latest"
        }).payload.data.decode("UTF-8")

        password = client.access_secret_version(request={
            "name": f"projects/153439618737/secrets/"
                    f"{self.project_name}_elasticsearch_password/versions/latest"
        }).payload.data.decode("UTF-8")

        self.es = Elasticsearch([host], http_auth=("elastic", password))


        if not self.es.indices.exists(index=self.index):
            module_dir = Path(__file__).parent
            settings_file = module_dir / "elasticsearch_settings" / "aegis_settings.json"
            with open(settings_file, "r") as f:
                settings = json.load(f)

            self.es.indices.create(index=self.index, **settings)

            mappings_file = module_dir / "elasticsearch_settings" / "data_portal.mapping.json"
            with open(mappings_file, "r") as f:
                mappings = json.load(f)

            self.es.indices.put_mapping(index=self.index, **mappings)

        # alias
        alias_name = "data_portal"
        if self.es.indices.exists_alias(name=alias_name):
            old_indices = self.es.indices.get_alias(name=alias_name)
            actions = []
            for old_index in old_indices:
                actions.append({"remove": {"index": old_index, "alias": alias_name}})
            actions.append({"add": {"index": self.index, "alias": alias_name}})
            self.es.indices.update_aliases(body={"actions": actions})
        else:
            self.es.indices.put_alias(index=self.index, name=alias_name)

    def start_bundle(self):
        self.actions = []

    def process(self, element):
        record = element
        record_id = element["taxId"]

        # prepare bulk action
        self.actions.append({
            "index": {
                "_index": self.index,
                "_id": record_id
            }
        })
        self.actions.append(record)

    def finish_bundle(self):
        if self.actions:
            for i in range(0, len(self.actions), 1000):
                self.es.bulk(
                    body=self.actions[i:i + 1000],
                    request_timeout=60
                )
            self.actions = []

if __name__ == "__main__":
    pass