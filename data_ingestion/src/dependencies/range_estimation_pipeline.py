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

    if args.bq_schema:
        with FileSystems.open(args.bq_schema) as f:
            schema_dict = json.load(f)
            schema_wrapped = json.dumps({"fields": schema_dict})
            bq_schema = parse_table_schema_from_json(schema_wrapped)

    with beam.Pipeline(options=options) as p:
        (
            p
            | "MatchInputFiles" >> fileio.MatchFiles(args.input_glob)
            | "ReadFiles" >> fileio.ReadMatches()
            | "EstimateRangeArea" >> beam.ParDo(EstimateRangeFn())
            | "WriteToBigQuery" >> WriteToBigQuery(
                table=args.bq_table,
                schema=bq_schema,
                method="FILE_LOADS",
                custom_gcs_temp_location=args.temp_location,
                write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
            )
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Estimate species range size from occurrence data")

    parser.add_argument("--input_glob", required=True, help="Glob pattern to cleaned occurrences .jsonl files")
    parser.add_argument("--bq_table", required=False, help="BigQuery table name: project.dataset.table")
    parser.add_argument("--bq_schema", required=False, help="Path to BigQuery schema JSON file")
    parser.add_argument("--temp_location", required=False, help="GCS path for temporary BQ file loads")

    args, beam_args = parser.parse_known_args()
    range_estimation_pipeline(args, beam_args)
