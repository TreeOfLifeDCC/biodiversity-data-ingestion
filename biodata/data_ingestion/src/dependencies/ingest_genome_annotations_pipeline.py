import argparse
import json
import apache_beam as beam

from apache_beam.options.pipeline_options import PipelineOptions
from dependencies.utils.transforms import LookupGTFUrlBatchFn, DownloadGTFDoFn


def ingest_genome_annotations_pipeline(args, beam_args):

    options = PipelineOptions(beam_args)

    with beam.Pipeline(options=options) as p:

        taxonomy_records = (
                p
                | "ReadTaxonomyValidated" >> beam.io.ReadFromText(args.taxonomy_path)
                | "ParseTaxonomyJSON" >> beam.Map(json.loads)
        )

        records_to_process = (
                taxonomy_records
                | "BatchTaxonomyRecords" >> beam.BatchElements(
                    min_batch_size=10,
                    max_batch_size=100
                )
                | "LookupGTFUrlBatch" >> beam.ParDo(
                    LookupGTFUrlBatchFn(
                        host=args.host,
                        user=args.user,
                        password=args.password,
                        index=args.index,
                    )
                )
        )

        downloaded = (
                records_to_process
                | "DownloadGTF" >> beam.ParDo(
                        DownloadGTFDoFn(args.gtf_staging_path)
                ).with_outputs(
                    "file_stats",
                    main="main"
                )
        )

        if args.output:
            (
                    downloaded.main
                    | "FormatGTFPaths" >> beam.Map(json.dumps)
                    | "WriteGTFPaths" >> beam.io.WriteToText(
                        file_path_prefix=args.output + "gtf_gcs_paths",
                        file_name_suffix=".jsonl",
                        shard_name_template="",
                    )
            )

            (
                    downloaded.file_stats
                    | "FormatStats" >> beam.Map(json.dumps)
                    | "WriteStats" >> beam.io.WriteToText(
                        file_path_prefix=args.output + "download_status",
                        file_name_suffix=".jsonl",
                        shard_name_template="",
                    )
            )


if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("--host", required=True, help="Elasticsearch host URL")
    parser.add_argument("--user", required=True, help="Elasticsearch username")
    parser.add_argument("--password", required=True, help="Elasticsearch password")
    parser.add_argument("--index", required=True, help="Elasticsearch index name")

    parser.add_argument("--taxonomy_path", required=True, help="Path to the taxonomy JSON file")
    parser.add_argument("--output", required=True, help="Output path for GTF files manifests: downloads status and gtf_gcs_paths")
    parser.add_argument("--gtf_staging_path", required=True, help="Path to the GTF staging directory")

    args, beam_args = parser.parse_known_args()

    ingest_genome_annotations_pipeline(args, beam_args)