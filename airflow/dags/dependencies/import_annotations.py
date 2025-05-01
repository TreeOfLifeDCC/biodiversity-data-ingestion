#!/usr/bin/env python3
# import_tol_qc.py

"""
Download annotations files and put them in GCS
"""
import os
import yaml
import json
import requests
from collections import defaultdict
from time import sleep
from lxml import etree
from airflow.io.path import ObjectStoragePath


def main(github_token: str):
    for project_name in [
        "asg",
        "canadian_biogenome",
        "darwin_tree_of_life",
        "erga_bge",
        "erga_pilot",
        "vgp",
    ]:
        os.system(
            f'curl -H "Accept: application/octet-stream" '
            f'-H "Authorization: Bearer {github_token}" -o {project_name}.yaml '
            f"https://raw.githubusercontent.com/Ensembl/projects.ensembl.org/refs/"
            f"heads/main/_data/{project_name}/species.yaml"
        )
    # parsing annotations for ERGA
    parse_annotation_yaml(
        ["darwin_tree_of_life.yaml", "erga_bge.yaml", "erga_pilot.yaml"],
        "erga",
    )
    # parsing annotations for DToL
    parse_annotation_yaml(["darwin_tree_of_life.yaml"], "dtol")
    # parsing annotations for ASG
    parse_annotation_yaml(["asg.yaml"], "asg")
    # parsing annotations for GBDP
    parse_annotation_yaml(
        [
            "darwin_tree_of_life.yaml",
            "erga_bge.yaml",
            "erga_pilot.yaml",
            "asg.yaml",
            "canadian_biogenome.yaml",
            "vgp.yaml",
        ],
        "gbdp",
    )


def parse_annotation_yaml(yaml_filenames_list: list, project_name: str) -> None:
    annotations = defaultdict(list)
    for filename in yaml_filenames_list:
        with open(filename, "r") as yaml_file:
            yaml_data = yaml.safe_load(yaml_file)
            for record in yaml_data:
                annotation = dict()
                annotation["species"] = record["species"]
                annotation["accession"] = record["accession"]
                acc_response = requests.get(
                    f"https://www.ebi.ac.uk/ena/browser/api/xml/{annotation['accession']}"
                )
                sleep(0.1)
                try:
                    root = etree.fromstring(acc_response.content)
                except etree.XMLSyntaxError:
                    print(f"XMLSyntaxError: {annotation['accession']}")
                    continue
                try:
                    tax_id = root.find("ASSEMBLY").find("TAXON").find("TAXON_ID").text
                except AttributeError:
                    if annotation["accession"] == "GCF_902459465.1":
                        tax_id = "7604"
                    elif annotation["accession"] == "GCF_902652985.1":
                        tax_id = "6579"
                annotation["tax_id"] = tax_id
                try:
                    annotation["annotation"] = {
                        "GTF": record["annotation_gtf"],
                        "GFF3": record["annotation_gff3"],
                    }
                except KeyError:
                    annotation["annotation"] = {"GTF": None, "GFF3": None}
                try:
                    annotation["proteins"] = {"FASTA": record["proteins"]}
                except KeyError:
                    annotation["proteins"] = {"FASTA": None}
                try:
                    annotation["transcripts"] = {"FASTA": record["transcripts"]}
                except KeyError:
                    annotation["transcripts"] = {"FASTA": None}
                try:
                    annotation["softmasked_genome"] = {
                        "FASTA": record["softmasked_genome"]
                    }
                except KeyError:
                    annotation["softmasked_genome"] = {"FASTA": None}
                try:
                    annotation["repeat_library"] = {"FASTA": record["repeat_library"]}
                except KeyError:
                    annotation["repeat_library"] = None
                annotation["other_data"] = {"ftp_dumps": record["ftp_dumps"]}
                try:
                    annotation["view_in_browser"] = record["beta_link"]
                except KeyError:
                    try:
                        annotation["view_in_browser"] = record["ensembl_link"]
                    except KeyError:
                        annotation["view_in_browser"] = None
                annotations[annotation["tax_id"]].append(annotation)

    base = ObjectStoragePath(
        "gcs://prj-ext-prod-biodiv-data-in-annotations", conn_id="google_cloud_default"
    )
    base.mkdir(exist_ok=True)
    path = base / f"{project_name}.jsonl"
    with path.open("w") as file:
        for tax_id, annotations in annotations.items():
            body = dict()
            body["annotations"] = annotations
            body["tax_id"] = tax_id
            file.write(f"{json.dumps(body)}\n")
