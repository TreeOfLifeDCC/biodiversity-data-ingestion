"""
Top-level entry point that launches the pipeline.

This file provides the entrypoint that launches the workflow defined in the
package.

This entrypoint will be called when the Flex Template starts.

The dependencies package should be installed in the Flex Template image, and
in the runtime environment.
----
This will be referenced in the Dockerfile via the ENV variable FLEX_TEMPLATE_PYTHON_PY_FILE.

NOTE: This entry point calls launcher which dispatches different pipelines using --pipeline.



"""
import sys
import traceback

from dependencies import launcher

if __name__ == "__main__":
    try:
        launcher.launch_pipeline(sys.argv[1:])
    except Exception as e:
        print("Launcher failed with error:", str(e))
        traceback.print_exc()
        sys.exit(1)
