import logging
import sys

from src.dependencies import launcher
from src.dependencies import aegis_launcher

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    is_aegis = any('project_name=aegis' in arg for arg in sys.argv)

    if is_aegis:
        aegis_launcher.run()
    else:
        launcher.run()