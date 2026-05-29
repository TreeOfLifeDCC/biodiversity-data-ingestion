"""
it launches the pipeline by passing its specific command line arguments.
"""

from dependencies.cleaning_occs_pipeline import cleaning_occs_pipeline
import argparse


def run(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument('--input_glob', required=True,
                        help='Glob for raw JSONL files (e.g., out/occurrences_raw/*.jsonl)')
    parser.add_argument('--output_dir', required=True, help='Directory for cleaned output JSONL files')
    parser.add_argument('--land_shapefile', required=True, help='Path to Natural Earth land shapefile')
    parser.add_argument('--centroid_shapefile', required=True, help='Path to admin-0 label points shapefile')
    parser.add_argument('--min_uncertainty', type=float, default=1000, help='Min coordinate uncertainty in meters')
    parser.add_argument('--max_uncertainty', type=float, default=5000, help='Max coordinate uncertainty in meters')
    parser.add_argument('--max_centroid_dist', type=float, default=5000, help='Max distance to centroid in meters')
    # If a consolidated file needed for inspection
    parser.add_argument('--output_consolidated', required=False, help='Optional consolidated output path prefix')
    # BigQuery parameters
    parser.add_argument('--bq_table', required=False, help='BigQuery table in the format project:dataset.table')
    parser.add_argument('--bq_schema', required=False, help='Path to BigQuery schema JSON (optional if table exists)')
    parser.add_argument('--temp_location', required=False, help='GCS temp path for BigQuery load jobs')
    parser.add_argument('--shards', type=int, default=5, help='Sharding factor to avoid skew in GroupByKey')

    args, beam_args = parser.parse_known_args(argv)

    cleaning_occs_pipeline(args, beam_args)