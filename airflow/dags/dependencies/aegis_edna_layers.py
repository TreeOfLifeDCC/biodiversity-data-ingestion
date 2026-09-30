from __future__ import annotations

import csv
import logging

logger = logging.getLogger(__name__)

_UNASSIGNED = {"NA", "", "unassigned", "Unassigned"}


def _read_tsv(path: str) -> list[dict]:
    with open(path, newline="") as fh:
        return list(csv.DictReader(fh, delimiter="\t"))


def build_layer_communities(
    tsv_path: str,
    *,
    genus_taxid: dict[str, int] | None = None,
) -> dict[str, dict]:
    genus_taxid = genus_taxid or {}
    rows = _read_tsv(tsv_path)

    age: dict[str, int] = {}
    total: dict[str, int] = {}
    per_layer: dict[str, list[tuple[str, int]]] = {}
    for r in rows:
        sid = (r.get("id") or "").strip()
        if not sid:
            continue
        try:
            reads = int(r["n_reads"])
        except (ValueError, KeyError):
            continue
        raw_age = (r.get("median_CE") or "").strip()
        if raw_age:
            try:
                age[sid] = int(round(float(raw_age)))
            except ValueError:
                pass
        total[sid] = total.get(sid, 0) + reads
        genus = (r.get("genus") or "").strip() or "NA"
        per_layer.setdefault(sid, []).append((genus, reads))

    out: dict[str, dict] = {}
    for sid, items in per_layer.items():
        layer_total = total.get(sid, 0)
        community = []
        taxa = 0
        for genus, reads in items:
            if reads <= 0 or genus in _UNASSIGNED:
                continue
            taxa += 1
            prop = round(100 * reads / layer_total, 3) if layer_total else 0.0
            entry = {"name": genus, "reads": reads, "prop": prop}
            tid = genus_taxid.get(genus)
            if tid is not None:
                entry["taxId"] = tid
            community.append(entry)
        # Most abundant first, so the FE can show a meaningful top-N.
        community.sort(key=lambda c: c["reads"], reverse=True)
        out[sid] = {
            "age": age.get(sid),
            "taxaCount": taxa,
            "readTotal": layer_total,
            "community": community,
        }

    logger.info(
        "Built per-layer communities for %d layers from %s", len(out), tsv_path
    )
    return out
