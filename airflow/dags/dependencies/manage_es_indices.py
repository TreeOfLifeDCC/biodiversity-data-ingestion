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


def swap_alias_to_latest(es, alias, today_index):
    """Point `alias` at `today_index`, removing it from every other index that
    currently holds it — all in one atomic update_aliases call.
    """
    actions = [{"add": {"index": today_index, "alias": alias}}]
    try:
        current = es.indices.get_alias(name=alias)
    except NotFoundError:
        current = {}
    for existing_index in current:
        if existing_index != today_index:
            actions.append({"remove": {"index": existing_index, "alias": alias}})
    es.indices.update_aliases(actions=actions)
    logger.info(
        "Alias %s -> %s (removed from %d other index(es))",
        alias, today_index, len(actions) - 1,
    )


def prune_old_indices(es, suffix, keep=2):
    """Delete dated indices for `suffix` beyond the `keep` newest.

    Returns the list of deleted index names. No-op when count <= keep.
    """
    ordered = dated_indices_for_suffix(es, suffix)
    to_delete = ordered[keep:]
    for name in to_delete:
        es.indices.delete(index=name)
        logger.info("Deleted old index %s", name)
    return to_delete


def rotate(host, password, date_prefix, specs, keep=2):
    """Rotate aliases onto today's indices and prune old generations.

    specs: list of (alias_name, index_suffix). For each pair, move the alias
    onto '{date_prefix}_{index_suffix}', then prune that suffix to `keep`
    newest generations.
    """
    es = get_client(host, password)
    for alias, suffix in specs:
        today_index = f"{date_prefix}_{suffix}"
        swap_alias_to_latest(es, alias, today_index)
        prune_old_indices(es, suffix, keep=keep)
