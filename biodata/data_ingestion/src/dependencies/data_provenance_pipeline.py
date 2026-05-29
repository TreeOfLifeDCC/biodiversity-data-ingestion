import argparse
import json

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions

from dependencies.utils.helpers import convert_dict_to_table_schema, to_provenance_request
from dependencies.utils.transforms import FetchProvenanceByTaxIdBatchFn


def data_provenance_pipeline(args, beam_args):
    options = PipelineOptions(beam_args)

    with beam.Pipeline(options=options) as p:
        requests = (
            p
            | "ReadValidatedTaxonomy" >> beam.io.ReadFromText(args.taxonomy_path)
            | "ParseValidatedJson" >> beam.Map(json.loads)
            | "ToProvenanceRequest" >> beam.Map(to_provenance_request)
            | "ReshuffleBeforeBatch" >> beam.Reshuffle()
            | "BatchRequests" >> beam.BatchElements(
                min_batch_size=args.min_batch_size,
                max_batch_size=args.max_batch_size,
            )
        )

        rows = (
            requests
            | "FetchProvenanceFromES" >> beam.ParDo(
                FetchProvenanceByTaxIdBatchFn(
                    host=args.host,
                    user=args.user,
                    password=args.password,
                    index=args.index,
                )
            )
        )

        # Optional JSONL output
        if args.output:
            (
                rows
                | "ToJsonl" >> beam.Map(json.dumps)
                | "WriteJsonl" >> beam.io.WriteToText(
                    file_path_prefix=args.output,
                    file_name_suffix=".jsonl",
                    num_shards=1,
                    shard_name_template="",
                )
            )

        # BigQuery append
        if args.bq_table and args.bq_schema and args.temp_location:
            with FileSystems.open(args.bq_schema) as f:
                schema_dict = json.load(f)
                table_schema = convert_dict_to_table_schema(schema_dict)

            (
                rows
                | "WriteProvenanceToBQ" >> beam.io.WriteToBigQuery(
                    table=args.bq_table,
                    schema=table_schema,
                    method="FILE_LOADS",
                    custom_gcs_temp_location=args.temp_location,
                    write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                    create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                )
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch provenance metadata for validated taxonomy (incremental).")

    # Input
    parser.add_argument("--taxonomy_path", required=True, help="Path to taxonomy_validated.jsonl")
    parser.add_argument("--output", required=False, default="out/provenance", help="Output prefix for JSONL (if enabled)")

    # ES
    parser.add_argument("--host", required=True, help="Elasticsearch host URL")
    parser.add_argument("--user", required=True, help="Elasticsearch username")
    parser.add_argument("--password", required=True, help="Elasticsearch password")
    parser.add_argument("--index", required=True, help="Elasticsearch index name")

    # Batch controls
    parser.add_argument("--min_batch_size", type=int, default=50)
    parser.add_argument("--max_batch_size", type=int, default=200)

    # BigQuery
    parser.add_argument("--bq_table", help="BigQuery table (project.dataset.bp_provenance_metadata)")
    parser.add_argument("--temp_location", help="GCS temp path for BQ file loads")
    parser.add_argument("--bq_schema", help="Path to bq_metadata_url_schema.json")

    args, beam_args = parser.parse_known_args()
    data_provenance_pipeline(args, beam_args)
