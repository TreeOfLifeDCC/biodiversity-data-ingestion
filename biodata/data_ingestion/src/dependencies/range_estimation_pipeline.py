import argparse
import json

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.io.gcp.bigquery_tools import parse_table_schema_from_json

from dependencies.utils.transforms import EstimateRangeFn


def range_estimation_pipeline(args, beam_args):
    options = PipelineOptions(beam_args)

    with beam.Pipeline(options=options) as p:
        rows = (
            p
            | "MatchInputFiles" >> fileio.MatchFiles(args.input_glob)
            | "ReadFiles" >> fileio.ReadMatches()
            | "EstimateRangeArea" >> beam.ParDo(EstimateRangeFn())
        )

        if args.bq_table and args.bq_schema and args.temp_location:
            with FileSystems.open(args.bq_schema) as f:
                schema_dict = json.load(f)
                schema_wrapped = json.dumps({"fields": schema_dict})
                bq_schema = parse_table_schema_from_json(schema_wrapped)

            _ = (
                rows
                | "WriteToBigQuery" >> WriteToBigQuery(
                    table=args.bq_table,
                    schema=bq_schema,
                    method="FILE_LOADS",
                    custom_gcs_temp_location=args.temp_location,
                    write_disposition=BigQueryDisposition.WRITE_APPEND,
                    create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
                )
            )

        # TODO: Add text output file to persist range estimates in GCS if needed.


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Estimate species range size from occurrence data")

    parser.add_argument("--input_glob", required=True, help="Glob pattern to cleaned occurrences .jsonl files")
    parser.add_argument("--bq_table", required=False, help="BigQuery table name: project.dataset.table")
    parser.add_argument("--bq_schema", required=False, help="Path to BigQuery schema JSON file")
    parser.add_argument("--temp_location", required=False, help="GCS path for temporary BQ file loads")

    args, beam_args = parser.parse_known_args()
    range_estimation_pipeline(args, beam_args)
