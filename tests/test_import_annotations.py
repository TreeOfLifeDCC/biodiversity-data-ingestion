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


def test_project_sources_groupings():
    assert ia.PROJECT_SOURCES["dtol"] == ["darwin_tree_of_life"]
    assert ia.PROJECT_SOURCES["erga"] == [
        "darwin_tree_of_life",
        "erga_bge",
        "erga_pilot",
    ]
    assert ia.PROJECT_SOURCES["asg"] == ["asg"]
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
