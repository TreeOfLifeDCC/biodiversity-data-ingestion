from __future__ import annotations

import csv
import logging
import pathlib

import requests

from dependencies.aegis_transforms import fetch_taxonomy

logger = logging.getLogger(__name__)

# Synthetic taxId range for genera that don't resolve at ENA (and the
# "Unassigned" bucket).
_SYNTH_BASE = 90_000_000


def _read_tsv(path: str) -> list[dict]:
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def build_edna_docs(
    tsv_path: str,
    *,
    resolve: bool = True,
    project_name: str = "AEGIS",
    countries: list[str] | None = None,
) -> list[dict]:
    countries = countries or ["Iceland"]
    rows = _read_tsv(tsv_path)

    # Per-layer age and total reads (denominator for relative abundance).
    sample_age: dict[str, int] = {}
    sample_total: dict[str, int] = {}
    for r in rows:
        sid = r["id"]
        try:
            reads = int(r["n_reads"])
        except (ValueError, KeyError):
            continue
        try:
            sample_age[sid] = int(round(float(r["median_CE"])))
        except (ValueError, KeyError):
            pass
        sample_total[sid] = sample_total.get(sid, 0) + reads

    # All layers in age order — the shared time axis for every taxon's series.
    layers = sorted(sample_age, key=lambda s: sample_age[s])

    # Per genus: reads by layer.
    by_genus: dict[str, dict[str, int]] = {}
    for r in rows:
        genus = (r.get("genus") or "").strip() or "NA"
        try:
            reads = int(r["n_reads"])
        except (ValueError, KeyError):
            continue
        by_genus.setdefault(genus, {})[r["id"]] = reads

    genus_taxid: dict[str, int] = {}
    for r in rows:
        g = (r.get("genus") or "").strip() or "NA"
        raw = (r.get("taxid") or r.get("taxId") or "").strip()
        if g in genus_taxid or not raw:
            continue
        try:
            genus_taxid[g] = int(raw)
        except ValueError:
            pass
    docs: list[dict] = []
    synth = _SYNTH_BASE

    for genus, per_layer in sorted(by_genus.items()):
        is_unassigned = genus in ("NA", "", "unassigned", "Unassigned")

        # Abundance series across the full timeline
        abundance = []
        present_ages = []
        read_total = 0
        for sid in layers:
            reads = per_layer.get(sid, 0)
            total = sample_total.get(sid, 0)
            prop = round(100 * reads / total, 3) if total else 0.0
            abundance.append({"age": sample_age[sid], "reads": reads, "prop": prop})
            read_total += reads
            if reads > 0:
                present_ages.append(sample_age[sid])

        # Taxonomy / taxId.
        tax_id = None
        phylogeny = {r_: "Other" for r_ in
                     ("kingdom", "phylum", "class", "order", "family", "genus")}
        common_name = None
        scientific_name = "Unassigned" if is_unassigned else genus
        if not is_unassigned:
            tax_id = genus_taxid.get(genus)
            if tax_id is not None and resolve:
                tax = fetch_taxonomy(tax_id)
                phylogeny = tax["phylogeny"]
                common_name = tax["commonName"]
                if phylogeny.get("genus", "Other") == "Other":
                    phylogeny["genus"] = genus
        if not is_unassigned and phylogeny.get("genus", "Other") == "Other":
            phylogeny["genus"] = genus
        if tax_id is None:
            tax_id = synth
            synth += 1

        docs.append({
            "taxId": tax_id,
            "scientificName": scientific_name,
            "commonName": common_name,
            "phylogeny": phylogeny,
            "dataType": "environmental_dna",
            "currentStatus": "Raw Data - Submitted",
            "currentStatusOrder": 2,
            "bioSamplesStatus": "Done",
            "rawDataStatus": "Done",
            "assembliesStatus": "Not applicable",
            "annotationStatus": "Not applicable",
            # Per-taxon assignment quality: real ENA taxId vs synthetic/Unassigned.
            "resolvedStatus": "Done" if tax_id < _SYNTH_BASE else "No",
            "unassignedStatus": "Done" if tax_id >= _SYNTH_BASE else "No",
            "sampleCount": len(present_ages),
            "readTotal": read_total,
            "ageOldest": min(present_ages) if present_ages else None,
            "ageYoungest": max(present_ages) if present_ages else None,
            "countries": countries,
            # Empty so these records satisfy the data_portal API model, which
            # requires rawData/assemblies; environmental-DNA taxa have neither
            # per-genus (the reads live on the samples, the abundance below).
            "rawData": [],
            "assemblies": [],
            "abundance": abundance,
        })

    logger.info(
        "Built %d eDNA genus docs from %s (%d layers, resolve=%s)",
        len(docs), tsv_path, len(layers), resolve,
    )
    return docs
