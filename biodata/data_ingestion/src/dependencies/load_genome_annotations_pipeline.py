import argparse
import json
import apache_beam as beam

from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions
from dependencies.utils.helpers import convert_dict_to_table_schema
from dependencies.utils.transforms import ParseGTFDoFn


def load_genome_annotations_pipeline(args, beam_args):

    options = PipelineOptions(beam_args)

    with beam.Pipeline(options=options) as p:

        gtf_files = (
                p
                | "ReadPathManifest" >> beam.io.ReadFromText(args.gtf_path)
                | "ParseJSON" >> beam.Map(json.loads)
        )

        parsed = (
                gtf_files
                | "Parse GTF" >> beam.ParDo(ParseGTFDoFn()).with_outputs(
                    "file_stats",
                    main="rows",
                )
        )

        parsed_rows = parsed.rows
        file_stats = parsed.file_stats

        if args.bq_table and args.bq_schema and args.temp_location:

            with FileSystems.open(args.bq_schema) as f:
                schema_dict = json.load(f)
                table_schema = convert_dict_to_table_schema(schema_dict)

            (
                    parsed_rows
                    | "WriteToBigQuery" >> beam.io.WriteToBigQuery(
                        table=args.bq_table,
                        schema=table_schema,
                        method=beam.io.WriteToBigQuery.Method.FILE_LOADS,
                        write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
                        custom_gcs_temp_location=args.temp_location,
                        additional_bq_parameters={
                            "clustering": {
                                "fields": ["accession", "record_type", "gene_biotype"]
                            }
                        },
                    )
            )

        if args.output:
            (
                file_stats
                | "FormatStats" >> beam.Map(json.dumps)
                | "WriteStats" >> beam.io.WriteToText(
                    file_path_prefix=args.output + "/bq_ingestion",
                    file_name_suffix=".jsonl",
                    shard_name_template="",
                )
            )

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--gtf_path", required=True, help="Path to GTF file manifest gtf_gcs_paths.jsonl")
    parser.add_argument("--output", required=True, help="Output path for bq_ingestion.jsonl")
    parser.add_argument("--bq_table", required=True, help="BigQuery table name: project.dataset.table")
    parser.add_argument("--bq_schema", required=True, help="Path to BigQuery schema JSON file")
    parser.add_argument("--temp_location", required=False, help="GCS temp path for BigQuery file loads")

    args, beam_args = parser.parse_known_args()

    load_genome_annotations_pipeline(args, beam_args)