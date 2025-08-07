"""
it launches the pipeline by passing its specific command line arguments.
"""

from dependencies.range_estimation_pipeline import range_estimation_pipeline
import argparse

def run(argv):
    parser = argparse.ArgumentParser()

    parser.add_argument("--input_glob", required=True, help="Glob pattern to cleaned occurrences .jsonl files")
    parser.add_argument("--bq_table", required=True, help="BigQuery table name: project.dataset.table")
    parser.add_argument("--bq_schema", required=True, help="Path to BigQuery schema JSON file")
    parser.add_argument("--temp_location", required=True, help="GCS path for temporary BQ file loads")

    args, beam_args = parser.parse_known_args(argv)
    range_estimation_pipeline(args, beam_args)