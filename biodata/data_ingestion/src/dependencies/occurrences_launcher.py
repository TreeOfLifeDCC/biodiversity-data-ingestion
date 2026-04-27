"""
it launches the pipeline by passing its specific command line arguments.
"""

from dependencies.occurrences_pipeline import occurrences_pipeline
import argparse


def run(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--validated_input", required=True, help="Path to validated taxonomy JSONL file")
    parser.add_argument("--output_dir", required=True, help="Directory to store occurrence files")
    parser.add_argument("--limit", type=int, default=300, help="Number of GBIF occurrences per species. Max. 300")

    parser.add_argument("--sleep_seconds", type=float, default=0.25, help="Delay before each GBIF request")
    parser.add_argument( "--retry_delay_seconds", type=float, default=1.5, help="Delay before retrying a failed GBIF request")
    parser.add_argument( "--max_retries", type=int, default=1, help="Number of retries after the first failed GBIF request")


    args, beam_args = parser.parse_known_args(argv)
    occurrences_pipeline(args, beam_args)