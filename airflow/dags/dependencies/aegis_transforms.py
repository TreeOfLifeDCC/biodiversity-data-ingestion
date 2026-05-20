"""
Transformation functions for the AEGIS ingestion pipeline.

Builds Elasticsearch documents for the `samples` and `data_portal` indices
from raw BioSamples + ENA metadata records.
"""

import logging
from collections import defaultdict
from datetime import datetime
from time import sleep

import requests
from lxml import etree

logger = logging.getLogger(__name__)

STATUS_RANKS = {
    "Submitted to BioSamples": 1,
    "Raw Data - Submitted": 2,
    "Assemblies - Submitted": 3,
    "Annotation Complete": 4,
}
RANK_TO_STATUS = {v: k for k, v in STATUS_RANKS.items()}

# All known BioSamples characteristic names from ERC000053 checklist + standard fields.
# Anything NOT in this set will be collected as custom_fields.
_ERC000053_FIELDS = {
    "organism",
    "ENA-CHECKLIST",
    "organism part",
    "lifestage",
    "sex",
    "tolid",
    "collected_by",
    "collection date",
    "geographic location (latitude)",
    "geographic location (longitude)",
    "geographic location (region and locality)",
    "geographic location (country and/or sea)",
    "geographic location (elevation)",
    "habitat",
    "elevation",
    "collecting institution",
    "sample derived from",
    "project name",
    "relationship",
    "sample symbiont of",
    "symbiont",
    "sample collection method",
    "sample coordinator affiliation",
    "sample same as",
    "barcoding center",
    "project name",
    "identified_by",
    "identifier_affiliation",
    "original collection date",
    "original geographic location",
    "original geographic location (latitude)",
    "original geographic location (longitude)",
    "sample coordinator",
    "GAL",
    "specimen_id",
    "GAL_sample_id",
    "proxy voucher",
    "proxy biomaterial",
    "bio_material",
    "specimen_voucher",
    "culture_or_strain_id",
    "depth",
    "latitude start",
    "longitude start",
    "latitude end",
    "longitude end",
    # BioSamples standard fields (not checklist, but not custom either)
    "common name",
    "INSDC center name",
    "INSDC first public",
    "INSDC last update",
    "INSDC status",
    "SRA accession",
    "external references",
}

# Fields allowed by the ES strict mapping for rawData and assemblies
_RAW_DATA_FIELDS = {
    "study_accession", "sample_accession", "experiment_accession",
    "run_accession", "fastq_ftp", "instrument_platform", "instrument_model",
    "library_layout", "library_strategy", "library_source",
    "library_selection", "read_count", "base_count",
    "first_public", "last_updated",
}
_ASSEMBLY_FIELDS = {
    "accession", "assembly_name", "description", "study_accession",
    "sample_accession", "last_updated", "version",
}


def extract_char(characteristics: dict, field_name: str) -> str | None:
    """Extract the first text value from a BioSamples characteristics entry."""
    try:
        return characteristics[field_name][0]["text"]
    except (KeyError, IndexError, TypeError):
        return None


def extract_chars_all(characteristics: dict, field_name: str) -> list[str]:
    """Extract all text values from a BioSamples characteristics entry."""
    entries = characteristics.get(field_name)
    if not entries:
        return []
    out: list[str] = []
    for entry in entries:
        if isinstance(entry, dict):
            text = entry.get("text")
            if text:
                out.append(text)
    return out


def compute_tracking_system(sample: dict) -> str:
    """Compute tracking status from available data on a sample record."""
    if sample.get("assemblies"):
        return "Assemblies - Submitted"
    if sample.get("experiments"):
        return "Raw Data - Submitted"
    return "Submitted to BioSamples"


# ERC000053 allows these "missing"/"not collected" tokens for date fields.
_MISSING_DATE_TOKENS = {
    "not applicable",
    "not collected",
    "not provided",
    "restricted access",
    "missing",
    "missing: control sample",
    "missing: sample group",
    "missing: synthetic construct",
    "missing: lab stock",
    "missing: third party data",
    "missing: data agreement established pre-2023",
    "missing: endangered species",
    "missing: human-identifiable",
}


def parse_iso_date_lenient(value: str | None) -> tuple[str | None, str | None]:
    """
    Parse an ERC000053 date string. Returns (iso_date, text_token):

    - (YYYY-MM-DD, None) for parseable dates. Partial dates and timestamps
      are normalized to a full date (month/day default to 01).
    - (None, raw_value) for permitted "missing"/"not collected" tokens, so
      callers can preserve the original text in a sibling keyword field.
    - (None, None) for empty or unparseable values.
    """
    if not value:
        return None, None
    v = value.strip()
    if v.lower() in _MISSING_DATE_TOKENS:
        return None, v
    # Full timestamp (with optional Z or offset).
    try:
        dt = datetime.fromisoformat(v.replace("Z", "+00:00"))
        return dt.strftime("%Y-%m-%d"), None
    except ValueError:
        pass
    # Partial forms allowed by the checklist regex.
    for fmt, normalize in (
        ("%Y-%m-%d", lambda d: d),
        ("%Y-%m", lambda d: f"{d}-01"),
        ("%Y", lambda d: f"{d}-01-01"),
    ):
        try:
            datetime.strptime(v, fmt)
            return normalize(v), None
        except ValueError:
            continue
    return None, None


def _parse_float(value: str | None) -> float | None:
    if not value:
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def _dedup(items: list[dict], key: str) -> list[dict]:
    """Deduplicate a list of dicts by a key field, preserving order."""
    seen = {}
    for item in items:
        k = item.get(key)
        if k and k not in seen:
            seen[k] = item
    return list(seen.values())


def _get_common_name(scientific_name: str, cache: dict) -> str | None:
    """Look up common name from ENA taxonomy REST API, with caching."""
    if scientific_name in cache:
        return cache[scientific_name]
    common_name = None
    try:
        resp = requests.get(
            f"https://www.ebi.ac.uk/ena/taxonomy/rest/scientific-name/"
            f"{scientific_name}",
            timeout=10,
        )
        sleep(0.1)
        if resp.ok:
            data = resp.json()
            if data and "commonName" in data[0]:
                common_name = data[0]["commonName"]
    except Exception:
        logger.warning("Failed to fetch common name for %s", scientific_name)
    cache[scientific_name] = common_name
    return common_name


# ERC000053 mandatory characteristics (BioSamples keys).
_MANDATORY_FIELDS = (
    "organism part",
    "lifestage",
    "project name",
    "collected_by",
    "collection date",
    "geographic location (region and locality)",
    "habitat",
    "sex",
    "geographic location (country and/or sea)",
    "collecting institution",
)

# Controlled vocabularies short enough to embed. Country and GAL lists are
# long — skip strict vocab enforcement for those; presence is checked instead.
_LIFESTAGE_VALUES = {
    "adult", "egg", "embryo", "gametophyte", "juvenile", "larva",
    "pupa", "spore-bearing structure", "sporophyte", "vegetative cell",
    "vegetative structure", "zygote",
    "not applicable", "not collected", "not provided",
    "missing: control sample",
    "missing: data agreement established pre-2023",
    "missing: endangered species", "missing: human-identifiable",
    "missing: lab stock", "missing: sample group",
    "missing: synthetic construct", "missing: third party data",
}
_SYMBIONT_VALUES = {"Y", "N"}


def validate_sample(sample_id: str, sample: dict) -> list[dict]:
    """
    Check a single ERC000053 sample for missing mandatory fields and
    out-of-vocabulary values on enumerated fields.

    Returns a list of issue dicts; empty list means no issues found.
    Validation is non-rejecting — callers may still index the sample.
    """
    chars = sample.get("characteristics", {})
    issues: list[dict] = []
    accession = sample.get("accession", sample_id)

    for field in _MANDATORY_FIELDS:
        if not extract_char(chars, field):
            issues.append({
                "accession": accession,
                "type": "missing_mandatory",
                "field": field,
            })

    lifestage = extract_char(chars, "lifestage")
    if lifestage and lifestage not in _LIFESTAGE_VALUES:
        issues.append({
            "accession": accession,
            "type": "unknown_vocab_value",
            "field": "lifestage",
            "value": lifestage,
        })

    symbiont = extract_char(chars, "symbiont")
    if symbiont and symbiont not in _SYMBIONT_VALUES:
        issues.append({
            "accession": accession,
            "type": "unknown_vocab_value",
            "field": "symbiont",
            "value": symbiont,
        })

    return issues


def filter_by_checklist(
    metadata: dict[str, dict],
) -> tuple[dict[str, dict], list[dict]]:
    """
    Split metadata into samples following ERC000053 and those that don't.

    Returns (valid_samples, wrong_checklist_samples).
    """
    valid = {}
    wrong = []
    for sample_id, sample in metadata.items():
        checklist = extract_char(
            sample.get("characteristics", {}), "ENA-CHECKLIST"
        )
        if checklist == "ERC000053":
            valid[sample_id] = sample
        else:
            wrong.append({
                "accession": sample.get("accession", sample_id),
                "checklist": checklist,
                "organism": extract_char(
                    sample.get("characteristics", {}), "organism"
                ),
            })
    logger.info(
        "Checklist filter: %d valid (ERC000053), %d wrong",
        len(valid), len(wrong),
    )
    return valid, wrong


def fetch_taxonomy(tax_id) -> dict:
    """
    Fetch taxonomy from ENA XML API for a given taxId.

    Returns dict with keys: scientificName, commonName, phylogeny.
    phylogeny is a dict keyed by standard Linnaean rank names, each with
    sub-dict {scientificName, commonName}.
    """
    linnaean_ranks = ("kingdom", "phylum", "class", "order", "family", "genus")
    # Always return all six ranks so the UI never has to handle a null
    # phylogeny — unclassified/environmental taxa (e.g. taxId 32644
    # "unidentified") have no Linnaean lineage at ENA.
    result = {
        "scientificName": None,
        "commonName": None,
        "phylogeny": {rank: "Other" for rank in linnaean_ranks},
    }
    try:
        response = requests.get(
            f"https://www.ebi.ac.uk/ena/browser/api/xml/{tax_id}",
            timeout=30,
        )
        response.raise_for_status()
        root = etree.fromstring(response.content)
        taxon_el = root.find("taxon")
        if taxon_el is None:
            return result

        result["scientificName"] = taxon_el.get("scientificName")
        result["commonName"] = taxon_el.get("commonName")

        lineage = taxon_el.find("lineage")
        if lineage is not None:
            for ancestor in lineage.findall("taxon"):
                rank = ancestor.get("rank")
                if rank in linnaean_ranks:
                    result["phylogeny"][rank] = (
                        ancestor.get("scientificName") or "Other"
                    )
    except Exception:
        logger.exception("Failed to fetch taxonomy for taxId=%s", tax_id)

    return result


def build_sample_doc(
    sample_id: str,
    sample: dict,
    common_name_cache: dict,
    annotated_biosample_ids: set[str] | None = None,
) -> dict:
    """
    Transform a single BioSamples record into a flat document for the
    `samples` ES index.  Extracts all ERC000053 checklist fields and
    collects any extra characteristics as custom_fields.

    `annotated_biosample_ids`, when supplied, is the set of accessions that
    appear as `biosample_id` on an annotation record. A sample in that set
    is the reference specimen for an Ensembl build and gets its
    `trackingSystem` promoted to "Annotation Complete".
    """
    chars = sample.get("characteristics", {})
    raw_tax_id = sample.get("taxId")
    accession = sample.get("accession", sample_id)
    is_annotated = bool(
        annotated_biosample_ids and accession in annotated_biosample_ids
    )

    collection_date, collection_date_text = parse_iso_date_lenient(
        extract_char(chars, "collection date")
    )
    original_collection_date, original_collection_date_text = parse_iso_date_lenient(
        extract_char(chars, "original collection date")
    )

    doc = {
        "accession": accession,
        "taxId": int(raw_tax_id) if raw_tax_id is not None else None,
        "scientificName": extract_char(chars, "organism"),
        "trackingSystem": (
            "Annotation Complete" if is_annotated else compute_tracking_system(sample)
        ),
        # `projectTag` is the BioSamples filter tag (e.g. "AEGIS"); `projectName`
        # is the checklist's mandatory multi-valued `project name` characteristic.
        "projectTag": sample.get("project_tag") or sample.get("project_name"),
        "projectName": extract_chars_all(chars, "project name"),
        # Mandatory fields
        "organismPart": extract_char(chars, "organism part"),
        "lifestage": extract_char(chars, "lifestage"),
        "sex": extract_char(chars, "sex"),
        "collectedBy": extract_char(chars, "collected_by"),
        "collectionDate": collection_date,
        "locality": extract_char(chars, "geographic location (region and locality)"),
        "country": extract_char(chars, "geographic location (country and/or sea)"),
        "habitat": extract_char(chars, "habitat"),
        "collectingInstitution": extract_char(chars, "collecting institution"),
        # Recommended fields
        "tolid": extract_char(chars, "tolid"),
        "specimenVoucher": extract_chars_all(chars, "specimen_voucher"),
        # Optional fields — relationships
        "derivedFrom": extract_char(chars, "sample derived from"),
        "sampleSymbiontOf": extract_char(chars, "sample symbiont of"),
        "symbiont": extract_char(chars, "symbiont"),
        "relationship": extract_char(chars, "relationship"),
        "sampleSameAs": extract_char(chars, "sample same as"),
        # Optional fields — collection metadata
        "sampleCollectionMethod": extract_chars_all(chars, "sample collection method"),
        "identifiedBy": extract_char(chars, "identified_by"),
        "identifierAffiliation": extract_char(chars, "identifier_affiliation"),
        "sampleCoordinator": extract_char(chars, "sample coordinator"),
        "sampleCoordinatorAffiliation": extract_char(chars, "sample coordinator affiliation"),
        "barcodingCenter": extract_char(chars, "barcoding center"),
        "gal": extract_char(chars, "GAL"),
        "specimenId": extract_char(chars, "specimen_id"),
        "galSampleId": extract_char(chars, "GAL_sample_id"),
        "proxyVoucher": extract_chars_all(chars, "proxy voucher"),
        "proxyBiomaterial": extract_chars_all(chars, "proxy biomaterial"),
        "bioMaterial": extract_chars_all(chars, "bio_material"),
        "cultureOrStrainId": extract_char(chars, "culture_or_strain_id"),
        # Optional fields — original location (for relocated specimens)
        "originalCollectionDate": original_collection_date,
        "originalGeographicLocation": extract_char(chars, "original geographic location"),
        # BioSamples-injected provenance (not part of the checklist proper)
        "sraAccession": extract_char(chars, "SRA accession"),
        "insdcCenterName": extract_char(chars, "INSDC center name"),
        "insdcFirstPublic": extract_char(chars, "INSDC first public"),
        "insdcLastUpdate": extract_char(chars, "INSDC last update"),
        "insdcStatus": extract_char(chars, "INSDC status"),
        # `externalReferences` is a top-level field on the BioSamples record
        # (list of {url, duo}), not a characteristic.
        "externalReferences": [
            ref["url"]
            for ref in (sample.get("externalReferences") or [])
            if isinstance(ref, dict) and ref.get("url")
        ],
    }
    # Preserve "missing"/"not collected" tokens so they're not silently lost
    # when the ES `date` mapping would reject them.
    if collection_date_text:
        doc["collectionDateText"] = collection_date_text
    if original_collection_date_text:
        doc["originalCollectionDateText"] = original_collection_date_text

    # commonName: try sample characteristics first, fall back to cached ENA lookup
    common_name = extract_char(chars, "common name")
    if not common_name and doc["scientificName"]:
        common_name = _get_common_name(doc["scientificName"], common_name_cache)
    doc["commonName"] = common_name

    # geo_point — omit key entirely if either coordinate is missing
    lat = _parse_float(extract_char(chars, "geographic location (latitude)"))
    lon = _parse_float(extract_char(chars, "geographic location (longitude)"))
    if lat is not None and lon is not None:
        doc["location"] = {"lat": lat, "lon": lon}

    # Float fields — only include if parseable
    for char_name, es_field in [
        ("elevation", "elevation"),
        ("depth", "depth"),
        ("original geographic location (latitude)", "originalLatitude"),
        ("original geographic location (longitude)", "originalLongitude"),
        ("latitude start", "latitudeStart"),
        ("longitude start", "longitudeStart"),
        ("latitude end", "latitudeEnd"),
        ("longitude end", "longitudeEnd"),
    ]:
        val = _parse_float(extract_char(chars, char_name))
        if val is not None:
            doc[es_field] = val

    # Custom fields — any characteristic not in the ERC000053 known set
    custom_fields = []
    for field_name in chars:
        if field_name not in _ERC000053_FIELDS:
            value = extract_char(chars, field_name)
            if value:
                custom_fields.append({"key": field_name, "value": value})
    if custom_fields:
        doc["customFields"] = custom_fields

    return doc


def build_data_portal_docs(
    samples: dict[str, dict],
    annotations_by_tax: dict[int, list[dict]] | None = None,
) -> list[dict]:
    """
    Group samples by taxId, fetch taxonomy for each species, and build
    species-level documents for the `data_portal` ES index.

    If `annotations_by_tax` is provided (keyed by int taxId), matching
    annotation records are nested under `doc["annotations"]`.
    """
    annotations_by_tax = annotations_by_tax or {}
    # Group samples by taxId
    by_tax_id: defaultdict[str, list[dict]] = defaultdict(list)
    for sample_id, sample in samples.items():
        tax_id = sample.get("taxId")
        if tax_id is None:
            logger.warning("Sample %s has no taxId, skipping", sample_id)
            continue
        by_tax_id[str(tax_id)].append(sample)

    docs = []
    for tax_id, group in by_tax_id.items():
        taxonomy = fetch_taxonomy(tax_id)
        sleep(0.1)

        # Aggregate experiments and assemblies across all samples
        all_experiments = []
        all_assemblies = []
        locations = []
        countries = set()
        highest_rank = 0

        for sample in group:
            chars = sample.get("characteristics", {})

            # Experiments / assemblies — strip to mapped fields only
            for exp in sample.get("experiments", []):
                all_experiments.append({k: v for k, v in exp.items() if k in _RAW_DATA_FIELDS})
            for asm in sample.get("assemblies", []):
                all_assemblies.append({k: v for k, v in asm.items() if k in _ASSEMBLY_FIELDS})

            # Tracking status — find highest across group
            status = compute_tracking_system(sample)
            rank = STATUS_RANKS.get(status, 0)
            if rank > highest_rank:
                highest_rank = rank

            # Geo
            lat = _parse_float(
                extract_char(chars, "geographic location (latitude)")
            )
            lon = _parse_float(
                extract_char(chars, "geographic location (longitude)")
            )
            if lat is not None and lon is not None:
                locations.append({"lat": lat, "lon": lon})

            # Country
            country = extract_char(
                chars, "geographic location (country and/or sea)"
            )
            if country:
                countries.add(country)

        annotations = annotations_by_tax.get(int(tax_id))
        if annotations:
            highest_rank = max(highest_rank, STATUS_RANKS["Annotation Complete"])
        current_status = RANK_TO_STATUS.get(
            highest_rank, "Submitted to BioSamples"
        )

        # Determine sub-statuses
        has_experiments = len(all_experiments) > 0
        has_assemblies = len(all_assemblies) > 0
        has_annotations = bool(annotations)

        # Deduplicate
        deduped_experiments = _dedup(all_experiments, "run_accession")
        deduped_assemblies = _dedup(all_assemblies, "accession")

        # Use taxonomy commonName, fall back to first sample's common name
        common_name = taxonomy["commonName"]
        if not common_name:
            for s in group:
                cn = extract_char(
                    s.get("characteristics", {}), "common name"
                )
                if cn:
                    common_name = cn
                    break

        doc = {
            "taxId": int(tax_id),
            "scientificName": taxonomy["scientificName"],
            "commonName": common_name,
            "phylogeny": taxonomy["phylogeny"],
            "currentStatus": current_status,
            "currentStatusOrder": highest_rank,
            "bioSamplesStatus": "Done",
            "rawDataStatus": "Done" if has_experiments else "Waiting",
            "assembliesStatus": "Done" if has_assemblies else "Waiting",
            "annotationStatus": "Done" if has_annotations else "Waiting",
            "rawData": deduped_experiments,
            "assemblies": deduped_assemblies,
            "sampleCount": len(group),
            "countries": sorted(countries) if countries else [],
        }

        # locations — omit if empty (geo_point array can't be empty list)
        if locations:
            doc["locations"] = locations

        if annotations:
            doc["annotations"] = annotations

        docs.append(doc)

    return docs
