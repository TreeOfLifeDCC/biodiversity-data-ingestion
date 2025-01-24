"""Defines command line arguments for the pipeline defined in the package."""

import argparse

from dependencies import my_pipeline


def run(argv: list[str] | None = None):
    """Parses the parameters provided on the command line and runs the pipeline."""
    parser = argparse.ArgumentParser(
        description='ETL for biodiversity data, updates indices in '
                    'Elasticsearch and tables in BigQuery')
    parser.add_argument("--input_path", required=True,
                        help="Input path with files to process")
    parser.add_argument("--output_path", required=True,
                        help="Output path for results")
    parser.add_argument('--bq_dataset_name', required=True,
                        help='BigQuery dataset name')

    pipeline_args, other_args = parser.parse_known_args(argv)

    pipeline = my_pipeline.biodiversity_etl(
        pipeline_args.bq_dataset_name, pipeline_args.input_path,
        pipeline_args.output_path, other_args
    )

    pipeline.run()
