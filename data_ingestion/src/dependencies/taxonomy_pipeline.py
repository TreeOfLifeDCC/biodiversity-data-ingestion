import argparse
import json
import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions

from dependencies.utils.helpers import convert_dict_to_table_schema
from dependencies.utils.transforms import FetchESFn, ENATaxonomyFn, ValidateNamesFn


def taxonomy_pipeline(args, beam_args):
    options = PipelineOptions(beam_args)

    with beam.Pipeline(options=options) as p:

        # Fetch species records from ElasticSearch
        es_records = (
            p
            | "Start" >> beam.Create([None])
            | "FetchFromES" >> beam.ParDo(
                FetchESFn(
                    host=args.host,
                    user=args.user,
                    password=args.password,
                    index=args.index,
                    page_size=args.size,
                    max_pages=args.pages
                )
            )
        )

        # Enrich from ENA API (with retry + optional delay)
        enriched = (
            es_records
            | "ReshuffleBeforeENA" >> beam.Reshuffle()
            | "FetchENATaxonomy" >> beam.ParDo(ENATaxonomyFn(
                sleep_seconds=args.sleep,
                include_lineage=True
            ))
            | "ReshuffleAfterENA" >> beam.Reshuffle()
        )

        # Validate species names against GBIF
        validated_output = (
            enriched
            | "ValidateGBIF" >> beam.ParDo(ValidateNamesFn()).with_outputs(ValidateNamesFn.TO_CHECK, main=ValidateNamesFn.VALIDATED)
        )

        validated = validated_output.validated
        unmatched = validated_output.to_check

        # Write validated records
        (
            validated
            | "ToJSONValidated" >> beam.Map(json.dumps)
            | "WriteValidated" >> beam.io.WriteToText(
                file_path_prefix=args.output + "_validated",
                file_name_suffix=".jsonl",
                num_shards=1,
                shard_name_template=""
            )
        )

        # Write unmatched/fuzzy/synonym records
        (
            unmatched
            | "ToJSONUnmatched" >> beam.Map(json.dumps)
            | "WriteUnmatched" >> beam.io.WriteToText(
                file_path_prefix=args.output + "_tocheck",
                file_name_suffix=".jsonl",
                num_shards=1,
                shard_name_template=""
            )
        )

        # Export to BigQuery
        if args.bq_table and args.bq_schema and args.temp_location:
            with FileSystems.open(args.bq_schema) as f:
                schema_dict = json.load(f)
                table_schema = convert_dict_to_table_schema(schema_dict)

            (
                    validated
                    | "PrepareBQRecords" >> beam.Map(lambda x: x)  # Already dicts
                    | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                        table=args.bq_table,
                        schema=table_schema,
                        method="FILE_LOADS",
                        custom_gcs_temp_location=args.temp_location,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
                    )
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and validate species taxonomy")

    # For ES conn
    parser.add_argument("--host", required=True, help="Elasticsearch host URL")
    parser.add_argument("--user", required=True, help="Elasticsearch username")
    parser.add_argument("--password", required=True, help="Elasticsearch password")
    parser.add_argument("--index", required=True, help="Elasticsearch index name")
    parser.add_argument("--size", type=int, default=10, help="Page size from ES")
    parser.add_argument("--pages", type=int, default=1, help="Max pages to fetch from ES")

    # ENA throttling
    parser.add_argument("--sleep", type=float, default=0.25, help="Delay (in seconds) between ENA requests")

    # Output file
    parser.add_argument("--output", required=True, help="Output path prefix (no extension)")

    # BigQuery options
    parser.add_argument("--bq_table", help="BigQuery table (project.dataset.table)")
    parser.add_argument("--temp_location", help="GCS temp path for BQ file loads", required=False)
    parser.add_argument("--bq_schema", help="Path to BQ schema JSON")

    args, beam_args = parser.parse_known_args()
    taxonomy_pipeline(args, beam_args)
