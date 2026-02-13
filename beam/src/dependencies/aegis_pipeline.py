import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, GoogleCloudOptions
from dependencies.aegis_transforms import transform_to_aegis_format
from datetime import datetime
from elasticsearch import Elasticsearch
from google.cloud import secretmanager


def aegis_etl(
    input_path: str,
    output_path: str,
    project_name: str,
    pipeline_options_args: list[str],
) -> beam.Pipeline:
    """
    Creates and returns the AEGIS ETL pipeline

    Args:
        input_path: GCS path to input JSONL files (e.g., gs://bucket/*.jsonl)
        output_path: GCS path for output (not used yet, but kept for consistency)
        project_name: Project name ("aegis")
        pipeline_options_args: Additional Beam pipeline arguments

    Returns:
        Configured Beam pipeline
    """

    pipeline_options = PipelineOptions(pipeline_options_args)

    # Explicitly set Google Cloud options to ensure project is configured
    google_cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    if not google_cloud_options.project:
        google_cloud_options.project = 'prj-ext-prod-biodiv-data-in'

    pipeline = beam.Pipeline(options=pipeline_options)

    # Step 1: Read input data
    samples = (
        pipeline
        | "Read JSONL files" >> beam.io.ReadFromText(input_path)
        | "Parse JSON" >> beam.Map(lambda line: json.loads(line))
    )

    # Step 2: Group samples by tax_id
    samples_by_taxid = (
        samples
        | "Key by tax_id" >> beam.Map(lambda sample: (sample.get("taxId", "unknown"), sample))
        | "Group by tax_id" >> beam.GroupByKey()
    )

    # Step 3: Transform to AEGIS format (we'll implement this in Sub-task 2.2)
    aegis_records = (
        samples_by_taxid
        | "Transform to AEGIS format" >> beam.Map(transform_to_aegis_format)
        | "Filter None" >> beam.Filter(lambda x: x is not None)
    )

    # Step 4: Write to Elasticsearch (we'll implement this in Sub-task 2.4)
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
            # Write in batches of 1000 (2000 items in actions list = 1000 records)
            for i in range(0, len(self.actions), 1000):
                self.es.bulk(
                    body=self.actions[i:i + 1000],
                    request_timeout=60
                )
            self.actions = []

# Entry point that will be called by launcher.py
if __name__ == "__main__":
    # This will be handled by a launcher similar to the biodiversity project
    pass