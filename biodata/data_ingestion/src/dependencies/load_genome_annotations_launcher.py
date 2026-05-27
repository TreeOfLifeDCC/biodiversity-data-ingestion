"""
it launches the pipeline by passing its specific command line arguments.
"""

from dependencies.load_genome_annotations_pipeline import load_genome_annotations_pipeline
import argparse


def run(argv):
    parser = argparse.ArgumentParser()

    parser.add_argument("--gtf_path", required=True, help="Path to GTF file manifest gtf_gcs_paths.jsonl")
    parser.add_argument("--output", required=True, help="Output path for bq_stats.jsonl")
    parser.add_argument("--bq_table", required=True, help="BigQuery table name: project.dataset.table")
    parser.add_argument("--bq_schema", required=True, help="Path to BigQuery schema JSON file")
    parser.add_argument("--temp_location", required=False, help="GCS temp path for BigQuery file loads")

    args, beam_args = parser.parse_known_args(argv)

    load_genome_annotations_pipeline(args, beam_args)
