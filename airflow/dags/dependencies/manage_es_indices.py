"""Dynamic Elasticsearch alias rotation + generation pruning.

Replaces the hardcoded yesterday/two-days-ago alias swap and index deletion in
biodiversity_metadata_dag.py. For each (alias, physical index suffix) pair:
move the alias onto today's index (stripping it from ALL other current
holders), then delete every dated generation beyond the 2 newest.
"""
import logging
import re

from elasticsearch import Elasticsearch, NotFoundError

logger = logging.getLogger(__name__)

DATE_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})_(.+)$")


def get_client(host, password):
    return Elasticsearch([f"https://{host}"], http_auth=("elastic", password))


def dated_indices_for_suffix(es, suffix):
    """Indices named exactly '<YYYY-MM-DD>_<suffix>', newest date first.

    Indices without a parseable YYYY-MM-DD prefix, or whose suffix does not
    exactly equal `suffix`, are ignored — never returned, never deleted.
    """
    found = es.indices.get(index=f"*_{suffix}")
    dated = []
    for name in found:
        m = DATE_RE.match(name)
        if m and m.group(2) == suffix:
            dated.append((m.group(1), name))
    dated.sort(reverse=True)
    return [name for _, name in dated]
