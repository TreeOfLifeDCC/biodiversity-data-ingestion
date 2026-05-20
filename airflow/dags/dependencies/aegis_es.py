"""
Elasticsearch index management for the AEGIS ingestion pipeline.

Handles index creation, mapping, bulk indexing, alias swapping, and cleanup.
"""

import logging

from elasticsearch import Elasticsearch, NotFoundError
from elasticsearch.helpers import bulk

logger = logging.getLogger(__name__)

INDEX_SETTINGS = {
    "number_of_shards": 1,
    "number_of_replicas": 1,
    "max_result_window": 100000,
    "analysis": {
        "filter": {
            "autocomplete_filter": {
                "type": "edge_ngram",
                "min_gram": 1,
                "max_gram": 20,
                "token_chars": ["letter", "digit", "whitespace"],
            }
        },
        "normalizer": {
            "lower_case_normalizer": {
                "type": "custom",
                "filter": ["lowercase"],
            }
        },
        "analyzer": {
            "autocomplete": {
                "type": "custom",
                "tokenizer": "standard",
                "filter": ["lowercase", "autocomplete_filter"],
            }
        },
    },
}

_TEXT_KW = {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}}
_TEXT_KW_SHORT = {"type": "text", "fields": {"keyword": {"type": "keyword"}}}

SAMPLES_MAPPING = {
    "dynamic": "strict",
    "date_detection": False,
    "properties": {
        # Core identifiers
        "accession":             {"type": "keyword"},
        "taxId":                 {"type": "long"},
        "scientificName":        _TEXT_KW_SHORT,
        "commonName":            _TEXT_KW_SHORT,
        "trackingSystem":        {"type": "keyword"},
        "projectTag":            {"type": "keyword"},
        "projectName":           {"type": "keyword"},
        # Mandatory ERC000053 fields
        "organismPart":          {"type": "keyword"},
        "lifestage":             {"type": "keyword"},
        "sex":                   {"type": "keyword"},
        "collectedBy":           _TEXT_KW_SHORT,
        "collectionDate":        {"type": "date"},
        "collectionDateText":    {"type": "keyword"},
        "locality":              _TEXT_KW_SHORT,
        "country":               {"type": "keyword"},
        "habitat":               _TEXT_KW_SHORT,
        "collectingInstitution": {"type": "keyword"},
        # Geo
        "location":              {"type": "geo_point"},
        "elevation":             {"type": "float"},
        # Recommended fields
        "tolid":                 {"type": "keyword"},
        "specimenVoucher":       {"type": "keyword"},
        # Relationships
        "derivedFrom":           {"type": "keyword"},
        "sampleSymbiontOf":      {"type": "keyword"},
        "symbiont":              {"type": "keyword"},
        "relationship":          {"type": "keyword"},
        "sampleSameAs":          {"type": "keyword"},
        # Collection metadata
        "sampleCollectionMethod":        {"type": "keyword"},
        "identifiedBy":                  _TEXT_KW_SHORT,
        "identifierAffiliation":         {"type": "keyword"},
        "sampleCoordinator":             _TEXT_KW_SHORT,
        "sampleCoordinatorAffiliation":  {"type": "keyword"},
        "barcodingCenter":               {"type": "keyword"},
        "gal":                           {"type": "keyword"},
        "specimenId":                    {"type": "keyword"},
        "galSampleId":                   {"type": "keyword"},
        "proxyVoucher":                  {"type": "keyword"},
        "proxyBiomaterial":              {"type": "keyword"},
        "bioMaterial":                   {"type": "keyword"},
        "cultureOrStrainId":             {"type": "keyword"},
        # Original location (relocated specimens)
        "originalCollectionDate":        {"type": "date"},
        "originalCollectionDateText":    {"type": "keyword"},
        "originalGeographicLocation":    _TEXT_KW_SHORT,
        "originalLatitude":              {"type": "float"},
        "originalLongitude":             {"type": "float"},
        # Transect coordinates
        "latitudeStart":         {"type": "float"},
        "longitudeStart":        {"type": "float"},
        "latitudeEnd":           {"type": "float"},
        "longitudeEnd":          {"type": "float"},
        # Depth
        "depth":                 {"type": "float"},
        # BioSamples-injected provenance
        "sraAccession":          {"type": "keyword"},
        "insdcCenterName":       {"type": "keyword"},
        "insdcFirstPublic":      {"type": "date"},
        "insdcLastUpdate":       {"type": "date"},
        "insdcStatus":           {"type": "keyword"},
        "externalReferences":    {"type": "keyword"},
        # Custom fields (non-checklist characteristics)
        "customFields": {
            "type": "nested",
            "properties": {
                "key":   {"type": "keyword"},
                "value": _TEXT_KW_SHORT,
            },
        },
    },
}

DATA_PORTAL_MAPPING = {
    "dynamic": "strict",
    "date_detection": False,
    "properties": {
        "taxId":              {"type": "long"},
        "scientificName":     _TEXT_KW,
        "commonName":         _TEXT_KW,
        "phylogeny": {
            "properties": {
                "kingdom":  _TEXT_KW,
                "phylum":   _TEXT_KW,
                "class":    _TEXT_KW,
                "order":    _TEXT_KW,
                "family":   _TEXT_KW,
                "genus":    _TEXT_KW,
                "species":  _TEXT_KW,
            }
        },
        "currentStatus":      _TEXT_KW,
        "currentStatusOrder": {"type": "long"},
        "bioSamplesStatus":   {"type": "keyword"},
        "rawDataStatus":      {"type": "keyword"},
        "assembliesStatus":   {"type": "keyword"},
        "annotationStatus":   {"type": "keyword"},
        "rawData": {
            "properties": {
                "study_accession":      _TEXT_KW,
                "sample_accession":     _TEXT_KW,
                "experiment_accession": _TEXT_KW,
                "run_accession":        _TEXT_KW,
                "fastq_ftp":            _TEXT_KW,
                "instrument_platform":  _TEXT_KW,
                "instrument_model":     _TEXT_KW,
                "library_layout":       _TEXT_KW,
                "library_strategy":     _TEXT_KW,
                "library_source":       _TEXT_KW,
                "library_selection":    _TEXT_KW,
                "read_count":           _TEXT_KW,
                "base_count":           _TEXT_KW,
                "first_public":         {"type": "date"},
                "last_updated":         {"type": "date"},
            }
        },
        "assemblies": {
            "properties": {
                "accession":        _TEXT_KW,
                "assembly_name":    _TEXT_KW,
                "description":      _TEXT_KW,
                "study_accession":  _TEXT_KW,
                "sample_accession": _TEXT_KW,
                "last_updated":     {"type": "date"},
                "version":          _TEXT_KW,
            }
        },
        "sampleCount":        {"type": "integer"},
        "locations":          {"type": "geo_point"},
        "countries":          {"type": "keyword"},
        "annotations": {
            "type": "nested",
            "properties": {
                "assemblyAccession": {"type": "keyword"},
                "assemblyName":      {"type": "keyword"},
                "assemblyLevel":     {"type": "keyword"},
                "biosampleId":       {"type": "keyword"},
                "strain":            {"type": "keyword"},
                "strainType":        {"type": "keyword"},
                "speciesKey":        {"type": "keyword"},
                "scientificName":    _TEXT_KW,
                "commonName":        _TEXT_KW,
                "provider":          {"type": "keyword"},
                "release":           {"type": "keyword"},
                "annotationFiles": {
                    "properties": {
                        "category": {"type": "keyword"},
                        "name":     {"type": "keyword"},
                        "path":     {"type": "keyword"},
                    },
                },
                "homologyFiles": {
                    "properties": {
                        "category": {"type": "keyword"},
                        "name":     {"type": "keyword"},
                        "path":     {"type": "keyword"},
                    },
                },
                "assemblyFiles": {
                    "properties": {
                        "category": {"type": "keyword"},
                        "name":     {"type": "keyword"},
                        "path":     {"type": "keyword"},
                    },
                },
            },
        },
    },
}


def get_es_client(host: str, password: str) -> Elasticsearch:
    # Accept the Airflow Variable with or without an explicit scheme so we
    # don't end up with `https://https://...` (which a proxy will 400).
    if not host.startswith(("http://", "https://")):
        host = f"https://{host}"
    logger.info("Connecting to Elasticsearch at %s", host)
    return Elasticsearch(
        host,
        basic_auth=("elastic", password),
    )


def create_index_with_mapping(
    es: Elasticsearch, index_name: str, mapping: dict
) -> None:
    """Create an index with settings and mapping. No-op if it already exists."""
    if es.indices.exists(index=index_name):
        return
    es.indices.create(
        index=index_name,
        settings=INDEX_SETTINGS,
        mappings=mapping,
    )
    logger.info("Created index %s", index_name)


def bulk_index_documents(
    es: Elasticsearch, index_name: str, docs: list[dict], id_field: str
) -> None:
    """Bulk-index documents. Uses id_field value as the document _id."""
    actions = []
    skipped = 0
    for doc in docs:
        doc_id = doc.get(id_field)
        if doc_id is None:
            skipped += 1
            continue
        actions.append({
            "_index": index_name,
            "_id": str(doc_id),
            "_source": doc,
        })
    if skipped:
        logger.warning("Skipped %d documents missing '%s' field", skipped, id_field)

    success, errors = bulk(es, actions, raise_on_error=False)
    if errors:
        for err in errors:
            logger.error("Bulk index error: %s", err)
        raise RuntimeError(f"Bulk indexing had {len(errors)} errors")
    logger.info("Indexed %d documents into %s", success, index_name)


def swap_aliases(
    es: Elasticsearch,
    alias_configs: list[dict],
) -> None:
    """
    Atomically swap aliases to point to new indices.

    alias_configs is a list of dicts with keys: alias, new_index.
    Removes the alias from any indices currently holding it, then adds
    it to new_index — all in a single atomic update_aliases call.
    """
    actions = []
    for cfg in alias_configs:
        alias_name = cfg["alias"]
        # Find all indices currently holding this alias and remove them
        try:
            current = es.indices.get_alias(name=alias_name)
            for existing_index in current:
                if existing_index != cfg["new_index"]:
                    actions.append(
                        {"remove": {"index": existing_index, "alias": alias_name}}
                    )
        except NotFoundError:
            pass  # Alias doesn't exist yet (first run)
        actions.append({"add": {"index": cfg["new_index"], "alias": alias_name}})

    es.indices.update_aliases(actions=actions)
    logger.info("Alias swap complete: %s", [c["alias"] for c in alias_configs])


def delete_index_if_exists(es: Elasticsearch, index_name: str) -> None:
    """Delete an index only if it exists."""
    try:
        if es.indices.exists(index=index_name):
            es.indices.delete(index=index_name)
            logger.info("Deleted index %s", index_name)
    except NotFoundError:
        pass
