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
