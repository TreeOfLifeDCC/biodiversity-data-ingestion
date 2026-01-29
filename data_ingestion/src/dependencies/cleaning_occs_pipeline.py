import argparse
import json
import random

import apache_beam as beam
from apache_beam.io.filesystems import FileSystems
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import fileio, WriteToBigQuery, BigQueryDisposition
from dependencies.utils import cleaning_occs as cl
from dependencies.utils.helpers import extract_species_name, write_species_file, convert_dict_to_table_schema


def cleaning_occs_pipeline(args, beam_args):
    options = PipelineOptions(beam_args)

    if args.bq_schema:
        with FileSystems.open(args.bq_schema) as f:
            schema_dict = json.load(f)
            table_schema = convert_dict_to_table_schema(schema_dict)

    with beam.Pipeline(options=options) as p:
        # Side inputs for coordinate validation: land and country centroids
        land_si = (
                p
                | 'CreateLandPath' >> beam.Create([args.land_shapefile])
                | 'LoadLandGDF' >> beam.Map(cl.load_land_gdf)
        )
        centroids_si = (
                p
                | 'CreateCentroidPath' >> beam.Create([args.centroid_shapefile])
                | 'LoadCentroidList' >> beam.Map(cl.load_centroid_list)
        )

        # Loading occurrence files
        raw_lines = (
                p
                | 'MatchFiles' >> fileio.MatchFiles(args.input_glob)
                | 'ReadFiles' >> fileio.ReadMatches()
                | 'ExtractLinesWithFilename' >> beam.FlatMap(
                     lambda file: [(file.metadata.path, line) for line in file.read_utf8().splitlines()]
                )
        )

        # Clean occurrence records
        cleaned = (
                raw_lines
                | 'ParseAndAttachSpecies' >> beam.Map(lambda kv: (extract_species_name(kv[0]), json.loads(kv[1])))
                | 'FilterZeroCoords' >> beam.Filter(lambda kv: cl.filter_zero_coords(kv[1]) is not None)
                | 'FilterInvalidCoords' >> beam.Filter(lambda kv: cl.filter_invalid_coords(kv[1]) is not None)
                | 'FilterHighUncertainty' >> beam.Filter(
                    lambda kv: cl.filter_high_uncertainty(
                        kv[1],
                        min_uncertainty=args.min_uncertainty,
                        max_uncertainty=args.max_uncertainty
                    ) is not None
                )
                | 'FilterSea' >> beam.Filter(
                    lambda kv, land: cl.filter_sea(kv[1], land) is not None,
                    land=beam.pvalue.AsSingleton(land_si)
                )
                | 'FilterCentroid' >> beam.Filter(
                    lambda kv, cents: cl.filter_centroid(kv[1], cents, args.max_centroid_dist) is not None,
                    cents=beam.pvalue.AsSingleton(centroids_si)
                )
                | 'KeyBySpeciesAndCoords' >> beam.Map(
                    lambda kv: ((kv[0], kv[1]['decimalLatitude'], kv[1]['decimalLongitude']), kv[1])
                )
                | 'GroupDuplicates' >> beam.GroupByKey()
                | 'Deduplicate' >> beam.Map(lambda kv: (kv[0][0], cl.select_best_record(list(kv[1]))))
        )

        # Write cleaned output per species
        _ = (
                cleaned
                | 'ToJSON' >> beam.Map(lambda kv: (kv[0], kv[1]))
                | 'AddShardedKey' >> beam.Map(lambda kv: ((kv[0], random.randint(0, args.shards - 1)), kv[1]))
                | 'GroupByShardedKey' >> beam.GroupByKey()
                | 'ReshuffleBalanceLoad' >> beam.Reshuffle()
                | 'DropShardKey' >> beam.FlatMap(lambda kv: [(kv[0][0], record) for record in kv[1]])
                | 'GroupBySpecies' >> beam.GroupByKey()
                | 'WritePerSpecies' >> beam.ParDo(lambda kv: write_species_file(kv, args.output_dir))
        )

        # Write consolidated output
        if args.output_consolidated:
            _ = (
                    cleaned
                    | 'ToJSON2' >> beam.Map(lambda kv: (kv[0], json.dumps(kv[1])))
                    | 'ExtractCleanedDict' >> beam.Map(lambda kv: kv[1])
                    | 'WriteConsolidatedJSON' >> beam.io.WriteToText(
                        file_path_prefix=args.output_consolidated,
                        file_name_suffix=".jsonl",
                        num_shards=1,
                        shard_name_template=""
                    )
            )

        # Write to BigQuery
        if args.bq_table and args.temp_location:
            _ = (
                    cleaned
                    | 'PrepareBQRows' >> beam.Map(lambda kv: kv[1])
                    | 'WriteToBigQuery' >> WriteToBigQuery(
                        table=args.bq_table,
                        schema=table_schema,
                        method='FILE_LOADS',
                        custom_gcs_temp_location=args.temp_location,
                        write_disposition=BigQueryDisposition.WRITE_TRUNCATE,
                        create_disposition=BigQueryDisposition.CREATE_IF_NEEDED
                    )
            )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Batch clean GBIF occurrence files')

    parser.add_argument('--input_glob', required=True,
                        help='Glob for raw JSONL files (e.g., out/occurrences_raw/*.jsonl)')
    parser.add_argument('--output_dir', required=True, help='Directory for cleaned output JSONL files')
    parser.add_argument('--land_shapefile', required=True, help='Path to Natural Earth land shapefile')
    parser.add_argument('--centroid_shapefile', required=True, help='Path to admin-0 label points shapefile')
    parser.add_argument('--min_uncertainty', type=float, default=1000, help='Min coordinate uncertainty in meters')
    parser.add_argument('--max_uncertainty', type=float, default=5000, help='Max coordinate uncertainty in meters')
    parser.add_argument('--max_centroid_dist', type=float, default=5000, help='Max distance to centroid in meters')
    # If consolidated file needed for inspection
    parser.add_argument('--output_consolidated', required=False, help='Optional consolidated output path prefix')
    # BigQuery parameters
    parser.add_argument('--bq_table', required=False, help='BigQuery table in the format project:dataset.table')
    parser.add_argument('--bq_schema', required=False, help='Path to BigQuery schema JSON (optional if table exists)')
    parser.add_argument('--temp_location', required=False, help='GCS temp path for BigQuery load jobs')
    parser.add_argument('--shards', type=int, default=5, help='Sharding factor to avoid skew in GroupByKey')

    args, beam_args = parser.parse_known_args()
    cleaning_occs_pipeline(args, beam_args)
