"""
Defines a pipeline to ingest biodiversity data and update Elasticsearch indices
and BigQuery tables.
"""

import json

import apache_beam as beam
from dependencies.utils.common_functions import classify_samples


def biodiversity_etl(bq_dataset_name: str, input_path: str, output_path: str,
                     pipeline_options_args: list[str]) -> beam.Pipeline:
    """
    Instantiates and returns a Beam pipeline object
    """
    pipeline_options = beam.options.pipeline_options.PipelineOptions(
        pipeline_options_args
    )

    pipeline = beam.Pipeline(options=pipeline_options)
    classified_samples = (
            pipeline
            | "Read data from JSON file" >> beam.io.ReadFromText(input_path)
            | "Parse Json" >> beam.Map(lambda sample: json.loads(sample))
            | "Classify Samples" >> beam.Map(classify_samples).with_outputs(
        "Symbionts", "Metagenomes", main="Specimens")
    )
    classified_samples.Metagenomes | "Write to GCS" >> beam.io.WriteToText(
        output_path)

    return pipeline
