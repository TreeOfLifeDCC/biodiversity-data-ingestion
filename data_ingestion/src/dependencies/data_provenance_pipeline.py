import argparse
import json
import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition, parse_table_schema_from_json
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToText

from dependencies.utils.helpers import merge_gbif_url
from dependencies.utils.transforms import FetchProvenanceMetadataFn


def data_provenance_pipeline(args, beam_args):
    options = PipelineOptions(beam_args)

    with beam.Pipeline(options=options) as p:
        # Fetching urls from ES Biodiversity Portal
        provenance_kv = (
            p
            | "Start" >> beam.Create([None])
            | "FetchProvenanceMetadata" >> beam.ParDo(FetchProvenanceMetadataFn(
                host=args.host,
                user=args.user,
                password=args.password,
                index=args.index,
                page_size=args.page_size,
                max_pages=args.max_pages
            ))
            | "KeyProvenanceByAccession" >> beam.Map(lambda r: (r["accession"], r))
        )

        # Loading GBIF usage key from the validated taxonomy
        taxonomy_kv = (
            p
            | "ReadTaxonomy" >> beam.io.ReadFromText(args.taxonomy_path)
            | "ParseTaxonomy" >> beam.Map(json.loads)
            | "KeyTaxonomyByAccession" >> beam.Map(lambda r: (r["accession"], r))
        )

        # Joining ES and taxonomy by accession
        joined = (
            {"provenance": provenance_kv, "taxonomy": taxonomy_kv}
            | "JoinOnAccession" >> beam.CoGroupByKey()
        )

        # Merge and enrich records with GBIF url
        enriched = joined | "AddGbifUrlFromJoin" >> beam.FlatMap(merge_gbif_url)

        filtered = (
                enriched
                | "FilterMissingGbifURL" >> beam.Filter(lambda r: r.get("gbif_url") is not None)
        )

        # Write output
        (
            filtered
            | "ToJSON" >> beam.Map(json.dumps)
            | "WriteToText" >> WriteToText(
                file_path_prefix=args.output,
                file_name_suffix=".jsonl",
                num_shards=1,
                shard_name_template=""
            )
        )

        if args.bq_table and args.temp_location:
            with FileSystems.open(args.bq_schema) as f:
                schema_dict = json.load(f)
                schema_wrapped = json.dumps({"fields": schema_dict})  # Beam expects: {"fields": schema}
                bq_schema = parse_table_schema_from_json(schema_wrapped)

            (
                    filtered
                    | "WriteToBigQuery" >> WriteToBigQuery(
                        table=args.bq_table,
                        schema=bq_schema,
                        method='FILE_LOADS',
                        custom_gcs_temp_location=args.temp_location,
                        write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
                    )
            )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch provenance metadata from Elasticsearch and add GBIF URL")

    parser.add_argument("--host", required=True, help="Elasticsearch host URL")
    parser.add_argument("--user", required=True, help="Elasticsearch username")
    parser.add_argument("--password", required=True, help="Elasticsearch password")
    parser.add_argument("--index", required=True, help="Elasticsearch index to query")
    parser.add_argument("--output", required=True, help="Output file prefix (no extension)")
    parser.add_argument("--page_size", type=int, default=100, help="Page size for Elasticsearch query")
    parser.add_argument("--max_pages", type=int, default=10, help="Max number of pages to fetch")
    parser.add_argument("--taxonomy_path", required=True, help="Path to validated taxonomy JSONL file")
    parser.add_argument("--bq_table", help="BigQuery table in format project:dataset.table")
    parser.add_argument("--bq_schema", help="Path to BigQuery schema JSON")
    parser.add_argument("--temp_location", help="GCS temp path for BigQuery file loads")

    args, beam_args = parser.parse_known_args()
    data_provenance_pipeline(args, beam_args)
