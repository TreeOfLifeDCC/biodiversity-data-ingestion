import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions

from dependencies.utils.transforms import WriteSpeciesOccurrencesFn



def occurrences_pipeline(args, beam_args):
    pipeline_options = PipelineOptions(beam_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        records = (
            p
            | "ReadValidatedSpecies" >> beam.io.ReadFromText(args.validated_input)
            | "ParseJSON" >> beam.Map(json.loads)
        )

        results = (
                records
                | "FetchAndWriteOccurrences" >> beam.ParDo(
                    WriteSpeciesOccurrencesFn(
                        output_dir=args.output_dir,
                        max_records=args.limit,
                        sleep_seconds=args.sleep_seconds,
                        retry_delay_seconds=args.retry_delay_seconds,
                        max_retries=args.max_retries,
                )
            ).with_outputs("dead", main="success")
        )

        success = results.success
        dead = results.dead

        _ = (
            dead
            | "FilterDeadRecords" >> beam.Filter(lambda r: r is not None)
            | "DeadToJSON" >> beam.Map(json.dumps)
            | "WriteDeadLetters" >> beam.io.WriteToText(
                file_path_prefix=args.output_dir + "/dead_records",
                file_name_suffix=".jsonl",
                num_shards=1,
                shard_name_template="",
                skip_if_empty=True
            )
        )

        summary = (
            (
                success
                | "SuccessMetrics" >> beam.Map(
                    lambda r: {
                        "species_succeeded": 1,
                        "species_failed": 0,
                        "occurrences_written": r.get("n_occurrences", 0),
                    }
                ),
                dead
                | "FailureMetrics" >> beam.Map(
                    lambda r: {
                        "species_succeeded": 0,
                        "species_failed": 1,
                        "occurrences_written": 0,
                    }
                )
            )
            | "FlattenSummaryInputs" >> beam.Flatten()
            | "AggregateSummary" >> beam.CombineGlobally(
                lambda rows: {
                    "species_succeeded": sum(r["species_succeeded"] for r in rows),
                    "species_failed": sum(r["species_failed"] for r in rows),
                    "occurrences_written": sum(r["occurrences_written"] for r in rows),
                }
            )
        )

        _ = (
            summary
            | "SummaryToJSON" >> beam.Map(json.dumps)
            | "WriteSummary" >> beam.io.WriteToText(
                file_path_prefix=args.output_dir + "/summary_occ_download",
                file_name_suffix=".jsonl",
                num_shards=1,
                shard_name_template=""
            )
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and write GBIF occurrences per species")

    parser.add_argument("--validated_input", required=True, help="Path to validated taxonomy JSONL file")
    parser.add_argument("--output_dir", required=True, help="Directory to store occurrence files")
    parser.add_argument("--limit", type=int, default=150, help="Max GBIF occurrences per species")

    parser.add_argument("--sleep_seconds", type=float, default=0.25, help="Delay before each GBIF request")
    parser.add_argument( "--retry_delay_seconds", type=float, default=1.5, help="Delay before retrying a failed GBIF request")
    parser.add_argument( "--max_retries", type=int, default=1, help="Number of retries after the first failed GBIF request")

    args, beam_args = parser.parse_known_args()
    occurrences_pipeline(args, beam_args)
