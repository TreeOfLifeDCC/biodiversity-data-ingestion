"""
Launches the Ensembl stats pipeline by passing its specific command line arguments.
"""
import argparse

from dependencies.ensembl_stats_pipeline import ensembl_stats_pipeline


def run(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--accessions_file", required=True)
    parser.add_argument("--output_jsonl", required=False)
    parser.add_argument("--errors_jsonl", required=False)
    parser.add_argument("--bq_table", required=False)
    parser.add_argument("--bq_schema", required=False)
    parser.add_argument("--temp_location", required=False)
    parser.add_argument("--ensembl_api_delay_seconds", default=0.0, type=float)

    args, beam_args = parser.parse_known_args(argv)
    ensembl_stats_pipeline(args, beam_args)
