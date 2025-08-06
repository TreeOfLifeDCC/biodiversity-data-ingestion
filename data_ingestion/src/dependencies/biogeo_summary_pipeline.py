import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from src.dependencies.utils.transforms import BiogeoSummaryNestedFn

def run(args, beam_args):
    options = PipelineOptions(beam_args)

    with beam.Pipeline(options=options) as p:
        (
            p
            | "ReadAnnotated" >> beam.io.ReadFromText(args.input)
            | "ParseJSON" >> beam.Map(json.loads)
            | "KeyBySpecies" >> beam.Map(lambda r: (r["species"], r))
            | "GroupBySpecies" >> beam.GroupByKey()
            | "SummarizeBiogeoNest" >> beam.ParDo(BiogeoSummaryNestedFn(dict_key=args.dict_key))
            | "WriteSummary" >> beam.io.WriteToText(
                file_path_prefix=args.output,
                file_name_suffix=".jsonl",
                num_shards=1,
                shard_name_template=""
            )
        )

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Summarize biogeographic areas per species")

    parser.add_argument("--input", required=True, help="Path to JSONL file annotated with biogeo regions")
    parser.add_argument("--output", required=True, help="Output prefix for the summary JSONL")
    parser.add_argument("--dict_key", default="biogeo_Ecoregion", help="Key for nested region data")

    args, beam_args = parser.parse_known_args()
    run(args, beam_args)
