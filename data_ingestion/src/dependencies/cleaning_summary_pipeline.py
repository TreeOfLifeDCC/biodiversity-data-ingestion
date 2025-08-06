import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.filesystems import FileSystems
import re


def extract_species_info_from_filename(path):
    match = re.search(r'(occ_(.+?)\.jsonl)$', path)
    if not match:
        return None
    filename = match.group(1)
    species = match.group(2).replace('_', ' ')
    return {
        'file_path': path,
        'file_name': filename,
        'species': species
    }


def format_summary(species, grouped_data):
    total = grouped_data.get('total', [0])
    retained = grouped_data.get('retained', [0])

    total = total[0] if total else 0
    retained = retained[0] if retained else 0

    percent = (retained / total * 100) if total > 0 else 0.0

    return json.dumps({
        'species': species,
        'total_records': total,
        'retained_records': retained,
        'percent_retained': round(percent, 2)
    })


def run_summary_pipeline(args, beam_args):
    options = PipelineOptions(beam_args)

    # Extract metadata only from cleaned files
    match_results = FileSystems.match([args.cleaned_glob])[0].metadata_list
    species_files = [extract_species_info_from_filename(f.path) for f in match_results]
    species_files = [s for s in species_files if s]

    with beam.Pipeline(options=options) as p:
        metadata = (
            p
            | 'CreateSpeciesMetadata' >> beam.Create(species_files)
            | 'InitMetadataKV' >> beam.Map(lambda d: (d['species'], d))
        )

        # Retained counts from cleaned files
        retained_counts = (
            p
            | 'ReadCleaned' >> beam.io.ReadFromText(args.cleaned_glob)
            | 'ParseCleanedJSON' >> beam.Map(json.loads)
            | 'KeyBySpeciesRetained' >> beam.Map(lambda r: (r['species'], 1))
            | 'CountRetained' >> beam.CombinePerKey(sum)
        )

        # Total counts from raw files
        total_counts = (
            p
            | 'ReadRaw' >> beam.io.ReadFromText(args.raw_glob)
            | 'ParseRawJSON' >> beam.Map(json.loads)
            | 'KeyBySpeciesTotal' >> beam.Map(lambda r: (r['species'], 1))
            | 'CountTotal' >> beam.CombinePerKey(sum)
        )

        summary = (
            {'meta': metadata, 'total': total_counts, 'retained': retained_counts}
            | 'MergeAll' >> beam.CoGroupByKey()
            | 'FormatSummary' >> beam.Map(lambda kv: format_summary(kv[0], kv[1]))
            | 'WriteSummary' >> beam.io.WriteToText(
                file_path_prefix=args.output_path,
                file_name_suffix=".jsonl",
                num_shards=1,
                shard_name_template=""
            )
        )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Summarize cleaned GBIF occurrences per species with metadata')

    parser.add_argument('--cleaned_glob', required=True, help='Glob pattern for cleaned JSONL files (e.g. out/occurrences_clean/*.jsonl)')
    parser.add_argument('--raw_glob', required=True, help='Glob pattern for raw input JSONL files (e.g. out/occurrences_raw/*.jsonl)')
    parser.add_argument('--output_path', required=True, help='Output path prefix for the summary file')

    args, beam_args = parser.parse_known_args()
    run_summary_pipeline(args, beam_args)
