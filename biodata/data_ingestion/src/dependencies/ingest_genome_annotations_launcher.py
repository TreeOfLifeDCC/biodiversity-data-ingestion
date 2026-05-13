"""
it launches the pipeline by passing its specific command line arguments.
"""

from dependencies.ingest_genome_annotations_pipeline import ingest_genome_annotations_pipeline
import argparse


def run(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", required=True, help="Elasticsearch host URL")
    parser.add_argument("--user", required=True, help="Elasticsearch username")
    parser.add_argument("--password", required=True, help="Elasticsearch password")
    parser.add_argument("--index", required=True, help="Elasticsearch index name")

    parser.add_argument("--taxonomy_path", required=True, help="Path to the taxonomy JSON file")
    parser.add_argument("--manifest_path", required=True,
                        help="Output path for GTF files manifests: downloads status and gtf_gcs_paths")

    parser.add_argument("--gtf_staging_path", required=True, help="Path to the GTF staging directory")

    args, beam_args = parser.parse_known_args(argv)
    ingest_genome_annotations_pipeline(args, beam_args)