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


def test_swap_alias_adds_today_and_removes_stale_holder():
    es = FakeES(
        indices=["2026-06-24_data_portal", "2026-06-20_data_portal"],
        aliases={"data_portal": {"2026-06-20_data_portal"}},
    )
    mei.swap_alias_to_latest(es, "data_portal", "2026-06-24_data_portal")

    assert len(es.indices.update_aliases_calls) == 1  # atomic, single call
    actions = es.indices.update_aliases_calls[0]
    assert {"add": {"index": "2026-06-24_data_portal", "alias": "data_portal"}} in actions
    assert {"remove": {"index": "2026-06-20_data_portal", "alias": "data_portal"}} in actions
    # End state: alias points ONLY at today
    assert es.indices.aliases["data_portal"] == {"2026-06-24_data_portal"}


def test_swap_alias_strips_multiple_stale_holders():
    # The duplicate-data bug: alias somehow on two old generations at once.
    es = FakeES(
        indices=["2026-06-24_data_portal"],
        aliases={"data_portal": {"2026-06-20_data_portal", "2026-06-18_data_portal"}},
    )
    mei.swap_alias_to_latest(es, "data_portal", "2026-06-24_data_portal")
    assert es.indices.aliases["data_portal"] == {"2026-06-24_data_portal"}


def test_swap_alias_missing_alias_is_add_only():
    es = FakeES(indices=["2026-06-24_data_portal"], aliases={})
    mei.swap_alias_to_latest(es, "data_portal", "2026-06-24_data_portal")
    actions = es.indices.update_aliases_calls[0]
    assert actions == [
        {"add": {"index": "2026-06-24_data_portal", "alias": "data_portal"}}
    ]


def test_swap_alias_no_remove_when_already_on_today():
    es = FakeES(
        indices=["2026-06-24_data_portal"],
        aliases={"data_portal": {"2026-06-24_data_portal"}},
    )
    mei.swap_alias_to_latest(es, "data_portal", "2026-06-24_data_portal")
    actions = es.indices.update_aliases_calls[0]
    assert all("remove" not in a for a in actions)
