"""
it launches the pipeline by passing its specific command line arguments.
"""

from dependencies.data_provenance_pipeline import data_provenance_pipeline
import argparse

def run(argv):
    parser = argparse.ArgumentParser()

    parser.add_argument("--host", required=True, help="Elasticsearch host URL")
    parser.add_argument("--user", required=True, help="Elasticsearch username")
    parser.add_argument("--password", required=True, help="Elasticsearch password")
    parser.add_argument("--index", required=True, help="Elasticsearch index to query")
    parser.add_argument("--output", required=True, help="Output file prefix (no extension)")
    parser.add_argument("--page_size", type=int, default=100, help="Page size for Elasticsearch query")
    parser.add_argument("--max_pages", type=int, default=10, help="Max number of pages to fetch")
    parser.add_argument("--taxonomy_path", required=True, help="Path to validated taxonomy JSONL file")
    parser.add_argument("--bq_table", help="BigQuery table in format project:dataset.table")
    parser.add_argument("--bq_schema", help="Path to BigQuery schema JSON")
    parser.add_argument("--temp_location", help="GCS temp path for BigQuery file loads")

    args, beam_args = parser.parse_known_args(argv)
    data_provenance_pipeline(args, beam_args)