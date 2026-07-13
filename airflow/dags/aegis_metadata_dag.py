"""
AEGIS metadata ingestion DAG.

Fetches sample metadata from BioSamples + ENA for project PRJEB80366,
builds Elasticsearch documents for the `samples` and `data_portal` indices,
and indexes them with alias rotation.
"""

import logging

import pendulum

from airflow.decorators import dag, task

logger = logging.getLogger(__name__)

from dependencies.aegis_projects import aegis_projects

STUDY_ID = "PRJEB80366"
PROJECT_NAME = aegis_projects[STUDY_ID]["project_name"]


@task(multiple_outputs=False)
def fetch_metadata() -> dict:
    """Fetch BioSamples + ENA metadata for AEGIS project PRJEB80366.

    `multiple_outputs=False` keeps the whole metadata dict in one XCom row;
    without it TaskFlow infers multiple_outputs from `-> dict` and writes
    one XCom per BioSamples accession.
    """
    from dependencies import collect_metadata_experiments_assemblies

    return collect_metadata_experiments_assemblies.main(
        STUDY_ID, "AEGIS", PROJECT_NAME
    )


@task
def build_samples_docs(metadata: dict, annotations: dict) -> list[dict]:
    """Filter by ERC000053 checklist, build per-sample ES documents."""
    import json

    from dependencies.aegis_transforms import (
        build_sample_doc,
        filter_by_checklist,
        validate_sample,
    )

    # Reference specimens flagged as `biosample_id` on annotation records get
    # `trackingSystem = "Annotation Complete"`.
    annotated_biosample_ids = {
        rec["biosampleId"]
        for records in (annotations or {}).values()
        for rec in records
        if rec.get("biosampleId")
    }

    valid_metadata, wrong_checklist = filter_by_checklist(metadata)

    if wrong_checklist:
        with open("wrong_checklist.json", "w") as f:
            json.dump(wrong_checklist, f, indent=2)
        logger.info(
            "Wrote %d non-ERC000053 samples to wrong_checklist.json",
            len(wrong_checklist),
        )

    # Non-rejecting validation: collect issues but still index every sample.
    all_issues: list[dict] = []
    for sample_id, record in valid_metadata.items():
        all_issues.extend(validate_sample(sample_id, record))
    if all_issues:
        with open("validation_issues.json", "w") as f:
            json.dump(all_issues, f, indent=2)
        logger.warning(
            "Found %d validation issues across %d ERC000053 samples; "
            "see validation_issues.json",
            len(all_issues), len(valid_metadata),
        )

    common_name_cache: dict[str, str | None] = {}
    return [
        build_sample_doc(
            sample_id, record, common_name_cache, annotated_biosample_ids
        )
        for sample_id, record in valid_metadata.items()
    ]


@task(multiple_outputs=False)
def fetch_annotations() -> dict:
    """
    Load the Ensembl annotation manifest from the path stored in the Airflow
    Variable `aegis_annotations_path` (supports local path, https://, or
    gs:// URIs). Returns {taxId: [record, ...]}; empty dict when the
    Variable is unset, so the species-doc build degrades gracefully.

    `multiple_outputs=False` is required: TaskFlow would otherwise infer it
    from the `-> dict` annotation and reject integer taxId keys.
    """
    from airflow.models import Variable

    from dependencies.aegis_annotations import (
        build_annotation_records,
        load_annotations,
    )

    path = Variable.get("aegis_annotations_path", default_var="")
    if not path:
        logger.warning(
            "Airflow Variable 'aegis_annotations_path' is unset; "
            "data_portal docs will be built without annotations."
        )
        return {}
    return build_annotation_records(load_annotations(path))


@task
def build_data_portal_docs(metadata: dict, annotations: dict) -> list[dict]:
    """Build per-species Elasticsearch documents (only ERC000053 samples)."""
    from dependencies.aegis_transforms import (
        build_data_portal_docs as _build,
        filter_by_checklist,
    )

    valid_metadata, _ = filter_by_checklist(metadata)
    # XCom serializes dict keys as strings; convert back to int taxIds.
    annotations_by_tax = {int(k): v for k, v in (annotations or {}).items()}
    return _build(valid_metadata, annotations_by_tax)


@task
def index_to_es(
    samples_docs: list[dict],
    data_portal_docs: list[dict],
) -> None:
    """Create indices, bulk index documents, swap aliases, clean up old indices."""
    from datetime import datetime, timedelta

    from airflow.models import Variable

    from dependencies.aegis_es import (
        get_es_client,
        create_index_with_mapping,
        bulk_index_documents,
        # swap_aliases,         # TODO: uncomment for production
        # delete_index_if_exists,
        SAMPLES_MAPPING,
        DATA_PORTAL_MAPPING,
    )

    host = Variable.get("aegis_elasticsearch_host")
    password = Variable.get("aegis_elasticsearch_password")

    now = datetime.utcnow()
    today = now.strftime("%Y-%m-%d")
    two_days_ago = (now - timedelta(days=2)).strftime("%Y-%m-%d")

    samples_index = f"{today}_samples"
    data_portal_index = f"{today}_data_portal"

    es = get_es_client(host, password)

    # Create today's indices with mappings
    create_index_with_mapping(es, samples_index, SAMPLES_MAPPING)
    create_index_with_mapping(es, data_portal_index, DATA_PORTAL_MAPPING)

    # Bulk index documents
    bulk_index_documents(es, samples_index, samples_docs, id_field="accession")
    bulk_index_documents(es, data_portal_index, data_portal_docs, id_field="taxId")

    # TODO: uncomment for production (Composer) — skipped during local testing
    #       to avoid touching existing MVP aliases
    # swap_aliases(es, [
    #     {"alias": "samples", "new_index": samples_index},
    #     {"alias": "data_portal", "new_index": data_portal_index},
    # ])
    #
    # delete_index_if_exists(es, f"{two_days_ago}_samples")
    # delete_index_if_exists(es, f"{two_days_ago}_data_portal")


@dag(
    # schedule="0 11 * * *",
    # start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    catchup=False,
    tags=["aegis_metadata_ingestion"],
)
def aegis_metadata_ingestion():
    """
    This DAG pulls data from ENA (PRJEB80366) and BioSamples and builds
    Elasticsearch indices for the AEGIS data portal.
    """
    metadata = fetch_metadata()
    annotations = fetch_annotations()
    samples = build_samples_docs(metadata, annotations)
    data_portal = build_data_portal_docs(metadata, annotations)
    index_to_es(samples, data_portal)


aegis_metadata_ingestion()
