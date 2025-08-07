"""
Dispatcher that routes to the appropriate pipeline wrapper
based on a `--pipeline` argument.
"""
import argparse
from dependencies import (
    taxonomy_launcher,
    occurrences_launcher,
    cleaning_occs_launcher,
    spatial_annotation_launcher
)


def launch_pipeline(argv):
    parser = argparse.ArgumentParser()
    parser.add_argument("--pipeline", required=True, help="Name of the pipeline to run (e.g., taxonomy)")
    args, remaining_argv = parser.parse_known_args(argv)

    if args.pipeline == "taxonomy":
        taxonomy_launcher.run(remaining_argv)
    elif args.pipeline == "occurrences":
        occurrences_launcher.run(remaining_argv)
    elif args.pipeline == "cleaning_occs":
        cleaning_occs_launcher.run(remaining_argv)
    elif args.pipeline == "spatial_annotation":
        spatial_annotation_launcher.run(remaining_argv)
    else:
        raise ValueError(f"Unknown pipeline: {args.pipeline}")