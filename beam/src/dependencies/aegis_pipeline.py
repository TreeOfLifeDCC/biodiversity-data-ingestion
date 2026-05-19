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
    annotation_path: str | None = None,
    skip_alias_update: bool = False,
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

    # Optional annotation enrichment.
    # When annotation_path is given, read the annotation JSONL and pass it to
    # the transform as a dict side input keyed by gca_accession. When it is not
    # given, transform_kwargs stays empty and the pipeline behaves exactly as
    # it did before annotation support was added.
    transform_kwargs = {}
    if annotation_path:
        annotations = (
            pipeline
            | "Read annotation JSONL" >> beam.io.ReadFromText(annotation_path)
            | "Parse annotation JSON" >> beam.Map(lambda line: json.loads(line))
            | "Key annotations by accession" >> beam.Map(
                lambda record: (record["gca_accession"], record)
            )
        )
        transform_kwargs["annotations"] = beam.pvalue.AsDict(annotations)

    aegis_records = (
        samples_by_taxid
        | "Transform to AEGIS format" >> beam.Map(
            transform_to_aegis_format, **transform_kwargs
        )
        | "Filter None" >> beam.Filter(lambda x: x is not None)
    )

    aegis_records | "Write to Elasticsearch" >> beam.ParDo(
        WriteToAegisElasticsearchDoFn(
            project_name=project_name,
            skip_alias_update=skip_alias_update,
        )
    )

    return pipeline



class WriteToAegisElasticsearchDoFn(beam.DoFn):

    def __init__(self, project_name, skip_alias_update=False):
        super().__init__()
        self.project_name = project_name
        self.skip_alias_update = skip_alias_update
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
        # When skip_alias_update is set (e.g. a local test run), the index is
        # still created and filled, but the live "data_portal" alias is left
        # pointing at whatever index it currently points at -- so AEGIS users
        # keep seeing the existing data.
        if self.skip_alias_update:
            print(
                f"skip_alias_update is set: index '{self.index}' will be "
                f"created and filled, but the 'data_portal' alias will NOT "
                f"be changed."
            )
        else:
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