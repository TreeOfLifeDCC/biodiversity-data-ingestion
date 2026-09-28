from __future__ import annotations

import logging

from dependencies.aegis_transforms import (
    extract_char,
    extract_chars_all,
    parse_iso_date_lenient,
    compute_tracking_system,
    _parse_float,
    _valid_lat_lon,
)

logger = logging.getLogger(__name__)

ANCIENT_CHECKLIST = "ERC000059"

# NCBI taxon for the negative controls
_BLANK_TAX_ID = "2582415"

_KNOWN_ANCIENT_FIELDS = {
    # provenance / identity
    "ENA-CHECKLIST", "ENA first public", "INSDC center name", "INSDC status",
    "SRA accession", "Submitter Id", "external references", "title",
    "organism", "scientific_name", "common name", "project name",
    # shared collection fields
    "collected_by", "collecting institution", "collection date",
    "geographic location (country and/or sea)",
    "geographic location (latitude)", "geographic location (longitude)",
    "geographic location (region and locality)",
    "depth", "elevation", "description",
    "sample coordinator", "sample coordinator affiliation",
    # ancient-specific first-class fields
    "environmental medium",
    "broad-scale environmental context", "local environmental context",
    "past broad-scale environmental context",
    "past local environmental context",
    "geological epoch", "sample age inference method",
    "sample age range oldest limit", "sample age range youngest limit",
    "damage treatment", "master core sample id",
    # control markers
    "negative control status", "negative control type",
}


def normalize_checklist(value: str | None) -> str | None:
    return value.split(":")[0] if value else None


def is_ancient(sample: dict) -> bool:
    chars = sample.get("characteristics", {})
    return normalize_checklist(extract_char(chars, "ENA-CHECKLIST")) == ANCIENT_CHECKLIST


def is_blank_control(sample: dict) -> bool:
    chars = sample.get("characteristics", {})
    if (extract_char(chars, "negative control status") or "").strip().upper() == "TRUE":
        return True
    if str(sample.get("taxId")) == _BLANK_TAX_ID:
        return True
    return (extract_char(chars, "organism") or "").strip().lower() == "blank sample"


def filter_ancient(
    metadata: dict[str, dict],
) -> tuple[dict[str, dict], list[str], list[dict]]:
    valid: dict[str, dict] = {}
    blanks: list[str] = []
    other: list[dict] = []
    for sample_id, sample in metadata.items():
        accession = sample.get("accession", sample_id)
        if not is_ancient(sample):
            other.append({
                "accession": accession,
                "checklist": extract_char(sample.get("characteristics", {}), "ENA-CHECKLIST"),
            })
            continue
        if is_blank_control(sample):
            blanks.append(accession)
            continue
        valid[sample_id] = sample
    logger.info(
        "Ancient filter: %d valid, %d blank controls excluded, %d non-ERC000059",
        len(valid), len(blanks), len(other),
    )
    return valid, blanks, other


def build_ancient_sample_doc(sample_id: str, sample: dict) -> dict:
    chars = sample.get("characteristics", {})
    raw_tax_id = sample.get("taxId")
    accession = sample.get("accession", sample_id)

    collection_date, collection_date_text = parse_iso_date_lenient(
        extract_char(chars, "collection date")
    )

    doc: dict = {
        "accession": accession,
        "taxId": int(raw_tax_id) if raw_tax_id is not None else None,
        "scientificName": extract_char(chars, "organism"),
        "trackingSystem": compute_tracking_system(sample),
        "projectTag": sample.get("project_tag") or sample.get("project_name"),
        "projectName": extract_chars_all(chars, "project name"),
        # Shared collection fields
        "collectedBy": extract_char(chars, "collected_by"),
        "collectionDate": collection_date,
        "locality": extract_char(chars, "geographic location (region and locality)"),
        "country": extract_char(chars, "geographic location (country and/or sea)"),
        "collectingInstitution": extract_char(chars, "collecting institution"),
        "sampleCoordinator": extract_char(chars, "sample coordinator"),
        "sampleCoordinatorAffiliation": extract_char(chars, "sample coordinator affiliation"),
        # Provenance / identity
        "sraAccession": extract_char(chars, "SRA accession"),
        "insdcCenterName": extract_char(chars, "INSDC center name"),
        "insdcStatus": extract_char(chars, "INSDC status"),
        "submitterId": extract_char(chars, "Submitter Id"),
        "description": extract_char(chars, "description"),
        "externalReferences": [
            ref["url"]
            for ref in (sample.get("externalReferences") or [])
            if isinstance(ref, dict) and ref.get("url")
        ],
        # Ancient-specific first-class fields
        "environmentalMedium": extract_char(chars, "environmental medium"),
        "broadScaleEnvironmentalContext": extract_char(chars, "broad-scale environmental context"),
        "localEnvironmentalContext": extract_char(chars, "local environmental context"),
        "pastBroadScaleEnvironmentalContext": extract_char(chars, "past broad-scale environmental context"),
        "pastLocalEnvironmentalContext": extract_char(chars, "past local environmental context"),
        "geologicalEpoch": extract_char(chars, "geological epoch"),
        "sampleAgeInferenceMethod": extract_char(chars, "sample age inference method"),
        "damageTreatment": extract_char(chars, "damage treatment"),
        "masterCoreSampleId": extract_char(chars, "master core sample id"),
    }
    if collection_date_text:
        doc["collectionDateText"] = collection_date_text

    # geo_point
    lat = _parse_float(extract_char(chars, "geographic location (latitude)"))
    lon = _parse_float(extract_char(chars, "geographic location (longitude)"))
    if _valid_lat_lon(lat, lon):
        doc["location"] = {"lat": lat, "lon": lon}

    # Numeric fields
    for char_name, es_field in (
        ("depth", "depth"),
        ("elevation", "elevation"),
        ("sample age range oldest limit", "sampleAgeRangeOldestLimit"),
        ("sample age range youngest limit", "sampleAgeRangeYoungestLimit"),
    ):
        value = _parse_float(extract_char(chars, char_name))
        if value is not None:
            doc[es_field] = value

    doc = {k: v for k, v in doc.items() if v is not None and v != []}

    custom = []
    for name in chars:
        if name not in _KNOWN_ANCIENT_FIELDS:
            value = extract_char(chars, name)
            if value:
                custom.append({"key": name, "value": value})
    if custom:
        doc["customFields"] = custom

    return doc
