import re
from typing import Any


ACCESSION_RE = re.compile(r"^(GCA_\d+)(?:\.(\d+))?$")

EXPLICIT_MAIN_ASSEMBLY = "EXPLICIT_MAIN_ASSEMBLY"
HAPLOTYPE_OR_ALTERNATE_ASSEMBLY = "HAPLOTYPE_OR_ALTERNATE_ASSEMBLY"
UNMARKED_ASSEMBLY_CANDIDATE = "UNMARKED_ASSEMBLY_CANDIDATE"
NO_MATCHING_ASSEMBLY_METADATA = "NO_MATCHING_ASSEMBLY_METADATA"

SELECTED = "SELECTED"
REJECTED_NO_VALID_ANNOTATION_CANDIDATES = (
    "REJECTED_NO_VALID_ANNOTATION_CANDIDATES"
)
REJECTED_MISSING_GTF_URL = "REJECTED_MISSING_GTF_URL"
REJECTED_NO_SELECTABLE_ANNOTATION = "REJECTED_NO_SELECTABLE_ANNOTATION"


def parse_accession(accession: str | None) -> tuple[str | None, int | None]:
    if not accession:
        return None, None

    match = ACCESSION_RE.fullmatch(accession)
    if not match:
        return None, None

    root = match.group(1)
    version = int(match.group(2)) if match.group(2) is not None else None
    return root, version


def classify_assembly(
    assembly_name: str | None,
    description: str | None,
) -> str:
    text = " ".join(
        value.lower()
        for value in [assembly_name, description]
        if value
    )

    if not text:
        return NO_MATCHING_ASSEMBLY_METADATA

    if any(
        token in text
        for token in ["hap", "haplotype", ".alt", " alt", "alternate"]
    ):
        return HAPLOTYPE_OR_ALTERNATE_ASSEMBLY

    if any(
        token in text
        for token in [".pri", "_pri", " prim", "primary", "_prim"]
    ):
        return EXPLICIT_MAIN_ASSEMBLY

    return UNMARKED_ASSEMBLY_CANDIDATE


def group_candidates_by_root(
    candidates: list[dict[str, Any]],
) -> dict[str, list[dict[str, Any]]]:
    candidates_by_root: dict[str, list[dict[str, Any]]] = {}

    for candidate in candidates:
        candidates_by_root.setdefault(
            candidate["accession_root"],
            [],
        ).append(candidate)

    return candidates_by_root


def choose_highest_version_from_candidates(
    candidates: list[dict[str, Any]],
    selection_reason: str,
) -> dict[str, Any]:
    selected = max(
        candidates,
        key=lambda candidate: candidate["accession_version"],
    )

    return {
        "status": SELECTED,
        "selection_reason": selection_reason,
        **selected,
    }


def choose_by_root_highest_version(
    candidates: list[dict[str, Any]],
    highest_version_reason: str,
    tie_reason: str,
) -> dict[str, Any]:
    candidates_by_root = group_candidates_by_root(candidates)
    root_order = []

    for candidate in candidates:
        root = candidate["accession_root"]
        if root not in root_order:
            root_order.append(root)

    root_summaries = []
    for root, root_candidates in candidates_by_root.items():
        highest_candidate = max(
            root_candidates,
            key=lambda candidate: candidate["accession_version"],
        )
        root_summaries.append(
            {
                "root": root,
                "highest_version": highest_candidate["accession_version"],
                "highest_candidate": highest_candidate,
                "first_seen_order": root_order.index(root),
            }
        )

    max_version = max(summary["highest_version"] for summary in root_summaries)
    best_roots = [
        summary
        for summary in root_summaries
        if summary["highest_version"] == max_version
    ]

    if len(best_roots) == 1:
        selected = best_roots[0]["highest_candidate"]
        return {
            "status": SELECTED,
            "selection_reason": highest_version_reason,
            **selected,
        }

    selected_summary = min(
        best_roots,
        key=lambda summary: summary["first_seen_order"],
    )

    return {
        "status": SELECTED,
        "selection_reason": tie_reason,
        **selected_summary["highest_candidate"],
    }


def choose_hap1_or_fallback(
    candidates: list[dict[str, Any]],
) -> dict[str, Any]:
    hap1_candidates = [
        candidate
        for candidate in candidates
        if "hap1" in " ".join(
            str(value).lower()
            for value in [
                candidate.get("assembly_name"),
                candidate.get("assembly_description"),
            ]
            if value
        )
    ]

    if hap1_candidates:
        return choose_by_root_highest_version(
            hap1_candidates,
            highest_version_reason=(
                "selected_only_non_main_hap1_highest_version"
            ),
            tie_reason="selected_only_non_main_hap1_tie_first_annotation_order",
        )

    return choose_by_root_highest_version(
        candidates,
        highest_version_reason="selected_only_non_main_highest_version",
        tie_reason="selected_only_non_main_tie_first_annotation_order",
    )


def select_latest_main_annotation(source: dict[str, Any]) -> dict[str, Any]:
    annotations = source.get("annotation") or []
    assemblies = source.get("assemblies") or []

    assemblies_by_root = {}
    for assembly in assemblies:
        root, version_from_accession = parse_accession(assembly.get("accession"))
        if not root:
            continue

        assemblies_by_root[root] = {
            **assembly,
            "accession_root": root,
            "assembly_version": assembly.get("version") or version_from_accession,
            "classification": classify_assembly(
                assembly.get("assembly_name"),
                assembly.get("description"),
            ),
        }

    candidates = []

    for annotation in annotations:
        accession = annotation.get("accession")
        accession_root, accession_version = parse_accession(accession)
        annotation_files = annotation.get("annotation") or {}
        gtf_url = annotation_files.get("GTF")

        if not accession_root or accession_version is None:
            continue

        assembly = assemblies_by_root.get(accession_root)

        if assembly:
            classification = assembly["classification"]
            assembly_name = assembly.get("assembly_name")
            assembly_description = assembly.get("description")
        else:
            classification = NO_MATCHING_ASSEMBLY_METADATA
            assembly_name = None
            assembly_description = None

        candidates.append(
            {
                "tax_id": source.get("tax_id"),
                "species": (
                    annotation.get("species")
                    or source.get("organism")
                    or source.get("scientific_name")
                ),
                "accession": accession,
                "accession_root": accession_root,
                "accession_version": accession_version,
                "gtf_url": gtf_url,
                "assembly_name": assembly_name,
                "assembly_description": assembly_description,
                "assembly_classification": classification,
                "view_in_browser": annotation.get("view_in_browser"),
                "annotation_method": annotation.get("annotation_method"),
                "busco_score": annotation.get("busco_score"),
            }
        )

    if not candidates:
        return {
            "status": REJECTED_NO_VALID_ANNOTATION_CANDIDATES,
            "tax_id": source.get("tax_id"),
            "organism": source.get("organism") or source.get("scientific_name"),
        }

    candidates_with_gtf = [
        candidate
        for candidate in candidates
        if candidate.get("gtf_url")
    ]

    if not candidates_with_gtf:
        return {
            "status": REJECTED_MISSING_GTF_URL,
            "tax_id": source.get("tax_id"),
            "candidate_accessions": [
                candidate["accession"]
                for candidate in candidates
            ],
        }

    explicit_main = [
        candidate
        for candidate in candidates_with_gtf
        if candidate["assembly_classification"] == EXPLICIT_MAIN_ASSEMBLY
    ]

    if explicit_main:
        return choose_highest_version_from_candidates(
            explicit_main,
            selection_reason=(
                "selected_explicit_main_highest_accession_version"
            ),
        )

    unmarked = [
        candidate
        for candidate in candidates_with_gtf
        if candidate["assembly_classification"] == UNMARKED_ASSEMBLY_CANDIDATE
    ]

    if unmarked:
        return choose_by_root_highest_version(
            unmarked,
            highest_version_reason="selected_unmarked_assembly_highest_version",
            tie_reason="selected_unmarked_assembly_tie_first_annotation_order",
        )

    no_assembly_metadata = [
        candidate
        for candidate in candidates_with_gtf
        if candidate["assembly_classification"] == NO_MATCHING_ASSEMBLY_METADATA
    ]

    if no_assembly_metadata:
        return choose_by_root_highest_version(
            no_assembly_metadata,
            highest_version_reason=(
                "selected_no_assembly_metadata_highest_version"
            ),
            tie_reason=(
                "selected_no_assembly_metadata_tie_first_annotation_order"
            ),
        )

    non_main = [
        candidate
        for candidate in candidates_with_gtf
        if (
            candidate["assembly_classification"]
            == HAPLOTYPE_OR_ALTERNATE_ASSEMBLY
        )
    ]

    if non_main:
        if len(non_main) == 1:
            return {
                "status": SELECTED,
                "selection_reason": "selected_only_non_main_single_candidate",
                **non_main[0],
            }

        return choose_hap1_or_fallback(non_main)

    return {
        "status": REJECTED_NO_SELECTABLE_ANNOTATION,
        "tax_id": source.get("tax_id"),
        "candidate_accessions": [
            candidate["accession"]
            for candidate in candidates_with_gtf
        ],
    }
