import argparse
import json
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, SetupOptions
from apache_beam.metrics import MetricsFilter
from apache_beam.io.filesystems import FileSystems

from src.dependencies.utils.transforms import WriteSpeciesOccurrencesFn


def occurrences_pipeline(args, beam_args):
    pipeline_options = PipelineOptions(beam_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True

    with beam.Pipeline(options=pipeline_options) as p:
        # Read validated species input
        records = (
            p
            | "ReadValidatedSpecies" >> beam.io.ReadFromText(args.validated_input)
            | "ParseJSON" >> beam.Map(json.loads)
        )

        # Fetch and write occurrences per species with side output for dead records
        results = (
            records
            | "FetchAndWriteOccurrences" >> beam.ParDo(
                WriteSpeciesOccurrencesFn(
                    output_dir=args.output_dir,
                    max_records=args.limit
                )
            ).with_outputs("dead", main="success")
        )

        # Write dead-letter records only if any
        _ = (
            results.dead
            | "FilterDeadRecords" >> beam.Filter(lambda r: r is not None)
            | "WriteDeadLetters" >> beam.io.WriteToText(
                file_path_prefix=args.output_dir + "/dead_records",
                file_name_suffix=".jsonl",
                num_shards=1,
                shard_name_template="",
                skip_if_empty=True
            )
        )

    # Post-pipeline: extract metrics and write summary
    result = p.run()
    result.wait_until_finish()

    query = result.metrics().query(MetricsFilter().with_namespace("WriteSpeciesOccurrencesFn"))

    success = skipped = failures = 0
    for counter in query['counters']:
        name = counter.key.metric.name
        value = counter.committed
        if name == "SUCCESS":
            success = value
        elif name == "SKIPPED":
            skipped = value
        elif name == "FAILURES":
            failures = value

    summary = {
        "SUCCESS": success,
        "SKIPPED": skipped,
        "FAILURES": failures
    }

    with FileSystems.create(args.output_dir + "/summary_occ_download.jsonl") as f:
        f.write(json.dumps(summary).encode("utf-8"))
        f.write(b"\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fetch and write GBIF occurrences per species")

    parser.add_argument("--validated_input", required=True, help="Path to validated taxonomy JSONL file")
    parser.add_argument("--output_dir", required=True, help="Directory to store occurrence files")
    parser.add_argument("--limit", type=int, default=150, help="Max GBIF occurrences per species")

    args, beam_args = parser.parse_known_args()
    occurrences_pipeline(args, beam_args)
