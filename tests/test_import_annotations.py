import os
import sys

sys.path.insert(
    0,
    os.path.join(os.path.dirname(__file__), "..", "airflow", "dags", "dependencies"),
)

import import_annotations as ia


def test_build_record_full_entry():
    entry = {
        "species": "Agelas oroides",
        "accession": "GCA_949130485.1",
        "annotation_gtf": "https://ftp/gtf.gz",
        "annotation_gff3": "https://ftp/gff3.gz",
        "proteins": "https://ftp/pep.fa.gz",
        "transcripts": "https://ftp/cdna.fa.gz",
        "softmasked_genome": "https://ftp/softmasked.fa.gz",
        "repeat_library": "https://ftp/repeat.fa",
        "ftp_dumps": "https://ftp/dir/",
        "beta_link": "https://beta.ensembl.org/species/abc",
        "annotation_method": "Ensembl Genebuild",
        "busco_score": "C:94.5%[S:87.7%,D:6.8%],F:3.0%,M:2.5%,n:2805",
        "busco_lineage": "eudicotyledons_odb12",
    }
    rec = ia.build_record(entry, "72715")
    assert rec == {
        "species": "Agelas oroides",
        "accession": "GCA_949130485.1",
        "tax_id": "72715",
        "annotation": {"GTF": "https://ftp/gtf.gz", "GFF3": "https://ftp/gff3.gz"},
        "proteins": {"FASTA": "https://ftp/pep.fa.gz"},
        "transcripts": {"FASTA": "https://ftp/cdna.fa.gz"},
        "softmasked_genome": {"FASTA": "https://ftp/softmasked.fa.gz"},
        "repeat_library": {"FASTA": "https://ftp/repeat.fa"},
        "other_data": {"ftp_dumps": "https://ftp/dir/"},
        "view_in_browser": "https://beta.ensembl.org/species/abc",
        "annotation_method": "Ensembl Genebuild",
        "busco_score": "C:94.5%[S:87.7%,D:6.8%],F:3.0%,M:2.5%,n:2805",
        "busco_lineage": "eudicotyledons_odb12",
    }


def test_build_record_missing_optionals_and_coming_soon():
    entry = {
        "species": "Cassiopea sp. PORT0000214",
        "accession": "GCA_964204825.1",
        "ftp_dumps": "https://ftp/prerelease/",
        "beta_link": "Coming soon!",
    }
    rec = ia.build_record(entry, "3146530")
    assert rec["annotation"] == {"GTF": None, "GFF3": None}
    assert rec["proteins"] == {"FASTA": None}
    assert rec["transcripts"] == {"FASTA": None}
    assert rec["softmasked_genome"] == {"FASTA": None}
    assert rec["repeat_library"] is None
    assert rec["view_in_browser"] == "Coming soon!"
    assert rec["annotation_method"] is None
    assert rec["busco_score"] is None
    assert rec["busco_lineage"] is None


def test_project_sources_groupings():
    assert ia.PROJECT_SOURCES["dtol"] == ["darwin_tree_of_life"]
    assert ia.PROJECT_SOURCES["erga"] == [
        "darwin_tree_of_life",
        "erga_bge",
        "erga_pilot",
    ]
    assert ia.PROJECT_SOURCES["asg"] == ["asg"]
    assert ia.PROJECT_SOURCES["aegis"] == ["aegis"]
    assert ia.PROJECT_SOURCES["gbdp"] == [
        "darwin_tree_of_life",
        "erga_bge",
        "erga_pilot",
        "asg",
        "aegis",
        "vgp",
        "canadian_biogenome",
    ]


class _FakeResp:
    def __init__(self, content=b"", status=200):
        self.content = content
        self._status = status

    def raise_for_status(self):
        if self._status >= 400:
            raise RuntimeError(f"HTTP {self._status}")


_ENA_XML = (
    b"<ASSEMBLY_SET><ASSEMBLY accession='GCA_x'>"
    b"<TAXON><TAXON_ID>117779</TAXON_ID></TAXON>"
    b"</ASSEMBLY></ASSEMBLY_SET>"
)


def test_resolve_tax_id_success():
    cache = {}
    got = ia.resolve_tax_id(
        "GCA_x", cache, _get=lambda *a, **k: _FakeResp(_ENA_XML), sleep_s=0
    )
    assert got == "117779"
    assert cache["GCA_x"] == "117779"


def test_resolve_tax_id_uses_cache():
    calls = []

    def _get(*a, **k):
        calls.append(1)
        return _FakeResp(_ENA_XML)

    cache = {}
    ia.resolve_tax_id("GCA_x", cache, _get=_get, sleep_s=0)
    ia.resolve_tax_id("GCA_x", cache, _get=_get, sleep_s=0)
    assert len(calls) == 1


def test_resolve_tax_id_failure_returns_none():
    def _get(*a, **k):
        raise RuntimeError("boom")

    cache = {}
    got = ia.resolve_tax_id("GCA_bad", cache, _get=_get, retries=2, sleep_s=0)
    assert got is None
    assert cache["GCA_bad"] is None


_YAML_DOC = b"""
- species: Acropora austera
  accession: GCA_964273435.1
  annotation_gtf: https://ftp/gtf.gz
  ftp_dumps: https://ftp/dir/
  beta_link: https://beta.ensembl.org/species/abc
"""


def test_fetch_project_yaml_parses_list_and_sends_token():
    seen = {}

    def _get(url, headers=None, timeout=None):
        seen["url"] = url
        seen["headers"] = headers
        return _FakeResp(_YAML_DOC)

    out = ia.fetch_project_yaml("asg", "tok123", _get=_get)
    assert isinstance(out, list)
    assert out[0]["accession"] == "GCA_964273435.1"
    assert "_data/asg/species.yaml" in seen["url"]
    assert seen["headers"]["Authorization"] == "Bearer tok123"


def test_fetch_project_yaml_rejects_non_list():
    def _get(*a, **k):
        return _FakeResp(b"message: Not Found")

    try:
        ia.fetch_project_yaml("asg", "tok", _get=_get)
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_build_project_dedups_accession_and_groups_by_tax():
    # Two source dirs (erga uses 3; here gbdp-style overlap is simulated with asg
    # by monkeypatching PROJECT_SOURCES via a fetch that returns per-dir data).
    per_dir = {
        "darwin_tree_of_life": [
            {"species": "A", "accession": "GCA_1.1", "ftp_dumps": "u"},
            {"species": "B", "accession": "GCA_2.1", "ftp_dumps": "u"},
        ],
        "erga_bge": [
            {"species": "A", "accession": "GCA_1.1", "ftp_dumps": "u"},  # dup
            {"species": "C", "accession": "GCA_3.1", "ftp_dumps": "u"},
        ],
        "erga_pilot": [],
    }
    tax_map = {"GCA_1.1": "10", "GCA_2.1": "10", "GCA_3.1": "30"}

    def _fetch(project, token):
        return per_dir[project]

    def _resolve(acc, cache):
        return tax_map[acc]

    by_tax = ia.build_project("erga", "tok", {}, _fetch=_fetch, _resolve=_resolve)
    # GCA_1.1 and GCA_2.1 share tax 10; GCA_1.1 appears once despite the dup.
    assert sorted(by_tax.keys()) == ["10", "30"]
    accs_10 = sorted(r["accession"] for r in by_tax["10"])
    assert accs_10 == ["GCA_1.1", "GCA_2.1"]
    assert len(by_tax["30"]) == 1


def test_build_project_drops_unresolved_but_keeps_resolved():
    def _fetch(project, token):
        return [
            {"species": "X", "accession": "GCA_9.1", "ftp_dumps": "u"},
            {"species": "Y", "accession": "GCA_8.1", "ftp_dumps": "u"},
        ]

    def _resolve(acc, cache):
        return {"GCA_9.1": None, "GCA_8.1": "80"}[acc]

    by_tax = ia.build_project("dtol", "tok", {}, _fetch=_fetch, _resolve=_resolve)
    assert list(by_tax.keys()) == ["80"]
    assert len(by_tax["80"]) == 1


def test_build_project_raises_when_all_unresolved():
    def _fetch(project, token):
        return [{"species": "X", "accession": "GCA_9.1", "ftp_dumps": "u"}]

    def _resolve(acc, cache):
        return None

    try:
        ia.build_project("dtol", "tok", {}, _fetch=_fetch, _resolve=_resolve)
        assert False, "expected RuntimeError"
    except RuntimeError:
        pass


def test_main_builds_all_projects_and_writes(monkeypatch):
    written = {}

    def fake_build_project(name, token, cache, **kw):
        return {"10": [{"accession": "GCA_1.1", "tax_id": "10"}]}

    def fake_write(name, by_tax):
        written[name] = by_tax

    monkeypatch.setattr(ia, "build_project", fake_build_project)
    monkeypatch.setattr(ia, "write_jsonl", fake_write)

    ia.main("tok")
    assert set(written.keys()) == {"dtol", "erga", "asg", "aegis", "gbdp"}


def test_main_projects_filter_builds_subset(monkeypatch):
    written = {}

    monkeypatch.setattr(
        ia, "build_project", lambda name, token, cache, **kw: {"10": []}
    )
    monkeypatch.setattr(ia, "write_jsonl", lambda name, by_tax: written.__setitem__(name, by_tax))

    ia.main("tok", projects=["aegis"])
    assert set(written.keys()) == {"aegis"}


def test_write_jsonl_serializes_lines(tmp_path, monkeypatch):
    out = tmp_path / "asg.jsonl"

    class _Path:
        def __init__(self, p):
            self.p = p

        def __truediv__(self, name):
            return _Path(out)

        def mkdir(self, exist_ok=False):
            pass

        def open(self, mode):
            return open(self.p, mode)

    import types

    fake_mod = types.SimpleNamespace(ObjectStoragePath=lambda *a, **k: _Path(out))
    monkeypatch.setitem(
        sys.modules, "airflow.io.path", fake_mod
    )
    monkeypatch.setitem(sys.modules, "airflow.io", types.ModuleType("airflow.io"))
    monkeypatch.setitem(sys.modules, "airflow", types.ModuleType("airflow"))

    ia.write_jsonl("asg", {"10": [{"accession": "GCA_1.1", "tax_id": "10"}]})

    import json as _json

    lines = out.read_text().splitlines()
    assert len(lines) == 1
    rec = _json.loads(lines[0])
    assert rec == {"annotations": [{"accession": "GCA_1.1", "tax_id": "10"}], "tax_id": "10"}
