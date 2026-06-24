import os
import sys
import fnmatch

import pytest
from elasticsearch import NotFoundError

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "airflow", "dags", "dependencies"),
)

import manage_es_indices as mei


class FakeIndicesClient:
    """In-memory stand-in for es.indices.*"""

    def __init__(self, indices=(), aliases=None):
        self.indices = set(indices)
        # alias name -> set of index names currently holding it
        self.aliases = {a: set(v) for a, v in (aliases or {}).items()}
        self.update_aliases_calls = []
        self.deleted = []

    def get(self, index):
        # `index` is a wildcard pattern like "*_data_portal"
        return {n: {} for n in self.indices if fnmatch.fnmatch(n, index)}

    def get_alias(self, name):
        holders = self.aliases.get(name)
        if not holders:
            raise NotFoundError("missing", meta=None, body=None)
        return {idx: {"aliases": {name: {}}} for idx in holders}

    def update_aliases(self, actions):
        self.update_aliases_calls.append(actions)
        for a in actions:
            if "add" in a:
                self.aliases.setdefault(a["add"]["alias"], set()).add(a["add"]["index"])
            if "remove" in a:
                self.aliases.get(a["remove"]["alias"], set()).discard(a["remove"]["index"])

    def delete(self, index):
        self.deleted.append(index)
        self.indices.discard(index)


class FakeES:
    def __init__(self, indices=(), aliases=None):
        self.indices = FakeIndicesClient(indices, aliases)


def test_dated_indices_orders_newest_first():
    es = FakeES(indices=[
        "2026-06-20_data_portal",
        "2026-06-24_data_portal",
        "2026-06-22_data_portal",
    ])
    assert mei.dated_indices_for_suffix(es, "data_portal") == [
        "2026-06-24_data_portal",
        "2026-06-22_data_portal",
        "2026-06-20_data_portal",
    ]


def test_dated_indices_ignores_non_dated_and_other_suffix():
    es = FakeES(indices=[
        "2026-06-24_data_portal",
        "legacy_data_portal",          # no date prefix -> ignored
        "2026-06-24_data_portal_v2",   # different exact suffix -> ignored
        "2026-06-24_tracking_status",  # different suffix -> ignored
    ])
    assert mei.dated_indices_for_suffix(es, "data_portal") == [
        "2026-06-24_data_portal",
    ]
