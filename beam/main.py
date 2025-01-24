"""
Top-level entry point that launches the pipeline.

This file provides the entrypoint that launches the workflow defined in the
package.

This entrypoint will be called when the Flex Template starts.

The dependencies package should be installed in the Flex Template image, and
in the runtime environment.
"""

import logging

from dependencies import launcher

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    launcher.run()
