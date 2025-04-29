"""
Defines a pipeline to ingest biodiversity data and update Elasticsearch indices
and BigQuery tables.
"""

import json

import apache_beam as beam

from dependencies.utils.map_functions import (
    classify_samples,
    process_specimens_for_elasticsearch,
    process_samples_for_dwh,
    build_data_portal_record,
    build_dwh_record,
)
from dependencies.utils.write_to_elasticsearch import WriteToElasticsearchDoFn
from dependencies.utils.schemas import bq_metadata_schema


def biodiversity_etl(
    bq_dataset_name: str,
    input_path: str,
    output_path: str,
    pipeline_options_args: list[str],
) -> beam.Pipeline:
    """
    Instantiates and returns a Beam pipeline object
    """

    pipeline_options = beam.options.pipeline_options.PipelineOptions(
        pipeline_options_args
    )

    pipeline = beam.Pipeline(options=pipeline_options)
    genome_notes = (
        pipeline
        | "Read genome_notes from JSON file"
        >> beam.io.ReadFromText(
            "gs://prj-ext-prod-biodiv-data-in-genome_notes/genome_notes.jsonl"
        )
        | "Parse genome_notes JSON" >> beam.Map(lambda sample: json.loads(sample))
    )
    classified_samples = (
        pipeline
        | "Read data from JSON file" >> beam.io.ReadFromText(input_path)
        | "Parse Json" >> beam.Map(lambda sample: json.loads(sample))
        | "Classify Samples"
        >> beam.Map(classify_samples).with_outputs(
            "Symbionts", "Metagenomes", "Errors", main="Specimens"
        )
    )

    specimens_collection = classified_samples.Specimens
    symbionts_collection = classified_samples.Symbionts
    metagenomes_collection = classified_samples.Metagenomes

    specimens_collection | "Build specimens Elasticsearch records" >> beam.Map(
        process_specimens_for_elasticsearch
    ) | "Write specimens Elasticsearch records to Elasticsearch" >> beam.ParDo(
        WriteToElasticsearchDoFn(index="specimens", project_name=bq_dataset_name)
    )

    dwh_specimens_collection = (
        specimens_collection
        | "Create dwh specimens records tuple"
        >> beam.Map(process_samples_for_dwh, "specimens").with_outputs(
            "Errors", main="Normal"
        )
    )

    dwh_symbionts_collection = (
        symbionts_collection
        | "Create dwh symbionts record tuple"
        >> beam.Map(process_samples_for_dwh, "symbionts").with_outputs(
            "Errors", main="Normal"
        )
    )

    dwh_metagenomes_collection = (
        metagenomes_collection
        | "Create dwh metagenomes record tuple"
        >> beam.Map(process_samples_for_dwh, "metagenomes").with_outputs(
            "Errors", main="Normal"
        )
    )

    merged_collection = {
        "specimens": dwh_specimens_collection.Normal,
        "symbionts": dwh_symbionts_collection.Normal,
        "metagenomes": dwh_metagenomes_collection.Normal,
    } | "Merge samples" >> beam.CoGroupByKey()

    data_portal_collection = (
        merged_collection
        | "Build data portal dump"
        >> beam.Map(
            build_data_portal_record, bq_dataset_name, beam.pvalue.AsList(genome_notes)
        ).with_outputs()
    )

    data_portal_collection.normal | "Write data portal dump to Elasticsearch" >> beam.ParDo(
        WriteToElasticsearchDoFn(index="data_portal", project_name=bq_dataset_name)
    )

    data_portal_collection.normal | "Write tracking status dump to Elasticsearch" >> beam.ParDo(
        WriteToElasticsearchDoFn(index="tracking_status", project_name=bq_dataset_name)
    )

    dwh_collection = (
        merged_collection
        | "Build data portal dump for dwh"
        >> beam.Map(build_dwh_record, bq_dataset_name).with_outputs()
    )

    dwh_collection.normal | "Write dwh dump to BigQuery" >> beam.io.WriteToBigQuery(
        table=f"prj-ext-prod-biodiv-data-in:{bq_dataset_name}.metadata",
        schema=bq_metadata_schema,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
    )

    return pipeline
