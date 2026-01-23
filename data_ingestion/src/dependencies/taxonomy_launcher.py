"""
it launches the pipeline by passing its specific command line arguments.
"""

from dependencies.taxonomy_pipeline import taxonomy_pipeline
import argparse


def run(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True)
    parser.add_argument("--user", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--index", required=True)
    parser.add_argument("--size", type=int, default=10)
    parser.add_argument("--pages", type=int, default=1)
    parser.add_argument("--sleep", type=float, default=0.25)
    parser.add_argument("--output", required=True)
    parser.add_argument("--bq_table", required=False)
    parser.add_argument("--bq_schema", required=False)
    parser.add_argument("--temp_location", required=False)

    args, beam_args = parser.parse_known_args(argv)
    taxonomy_pipeline(args, beam_args)