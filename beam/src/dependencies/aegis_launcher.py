import argparse
from dependencies import aegis_pipeline


def run(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(
        description='AEGIS ETL pipeline - transforms sample data for Elasticsearch'
    )
    parser.add_argument(
        "--input_path",
        required=True,
        help="GCS path to input JSONL files (e.g., gs://bucket/*.jsonl)"
    )
    parser.add_argument(
        "--output_path",
        required=True,
        help="GCS output path (currently unused but required)"
    )
    parser.add_argument(
        '--project_name',
        required=True,
        help='Project name (should be "aegis")'
    )
    parser.add_argument(
        "--annotation_path",
        required=False,
        default=None,
        help=(
            "Optional GCS path to the annotation JSONL file "
            "(e.g. gs://bucket/annotations/PRJEB80366.jsonl). "
            "If omitted, the pipeline runs exactly as before, "
            "with no annotation enrichment."
        )
    )
    parser.add_argument(
        "--skip_alias_update",
        action="store_true",
        help=(
            "If set, the pipeline creates and fills the date-stamped index "
            "as usual but does NOT repoint the live 'data_portal' alias. "
            "Use this for test runs so AEGIS users keep seeing the current "
            "index."
        )
    )

    pipeline_args, other_args = parser.parse_known_args(argv)

    # create and run the pipeline
    # pipeline = aegis_pipeline.aegis_etl(
    #     input_path=pipeline_args.input_path,
    #     output_path=pipeline_args.output_path,
    #     project_name=pipeline_args.project_name,
    #     pipeline_options_args=other_args
    # )

    pipeline = aegis_pipeline.aegis_etl(
        input_path=pipeline_args.input_path,
        output_path=pipeline_args.output_path,
        project_name='aegis',
        pipeline_options_args=other_args,
        annotation_path=pipeline_args.annotation_path,
        skip_alias_update=pipeline_args.skip_alias_update
    )
    pipeline.run()