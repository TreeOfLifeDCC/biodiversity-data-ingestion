"""
it launches the pipeline by passing its specific command line arguments.
"""

from dependencies.spatial_annotation_pipeline import spatial_annotation_pipeline
import argparse

def run(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--input_occs", required=True, help="Glob for cleaned JSONL files (quoted)")
    parser.add_argument("--climate_dir", required=True, help="Path to CHELSA dataset")
    parser.add_argument("--biogeo_vector", required=True, help="Path to vector dataset (e.g., ecoregions)")
    parser.add_argument("--annotated_output", required=True, help="Output path for full annotated records")
    parser.add_argument("--summary_output", required=True, help="Output path for the joined spatial summary")
    parser.add_argument("--bq_summary_table", required=False, help="BigQuery table to upload joined summary")
    parser.add_argument("--bq_schema", required=False, help="Path to BigQuery schema JSON for summary")
    parser.add_argument("--temp_location", required=False, help="GCS temp path for BigQuery file loads")

    args, beam_args = parser.parse_known_args(argv)
    spatial_annotation_pipeline(args, beam_args)