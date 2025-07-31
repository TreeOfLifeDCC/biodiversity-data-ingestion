import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from src.dependencies.utils.transforms import ClimateSummaryFn


def climate_summary_pipeline(args, beam_args):
    options = PipelineOptions(beam_args)

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadInput" >> beam.io.ReadFromText(args.input)
            | "ParseJSON" >> beam.Map(json.loads)
            | "KeyByAccession" >> beam.Map(lambda r: (r["accession"], r))
            | "GroupByAccession" >> beam.GroupByKey()
            | "ComputeSummary" >> beam.ParDo(ClimateSummaryFn())
            | "WriteSummary" >> beam.io.WriteToText(
                file_path_prefix=args.output,
                file_name_suffix=".jsonl",
                num_shards=1,
                shard_name_template=""
            )
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Summarize climate variables per species")

    parser.add_argument("--input", required=True, help="Path to climate-annotated JSONL input")
    parser.add_argument("--output", required=True, help="Output prefix for summary JSONL")

    args, beam_args = parser.parse_known_args()
    climate_summary_pipeline(args, beam_args)
