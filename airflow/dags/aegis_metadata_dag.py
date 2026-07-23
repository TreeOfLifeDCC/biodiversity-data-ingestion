"""
AEGIS metadata ingestion DAG.

Fetches sample metadata from BioSamples + ENA for project PRJEB80366,
builds Elasticsearch documents for the `samples` and `data_portal` indices,
and indexes them with alias rotation.

Ensembl annotations are produced by `import_annotations.py` from the
`_data/aegis/species.yaml` file in the Ensembl/projects.ensembl.org GitHub
repo (same producer + format as the dtol/erga/asg/gbdp manifests) and joined
onto species docs by tax_id.
"""

import logging

import pendulum

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

from dependencies import import_annotations, manage_es_indices
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
def build_samples_docs(metadata: dict) -> list[dict]:
    """Filter by ERC000053 checklist, build per-sample ES documents."""
    import json

    from dependencies.aegis_transforms import (
        build_sample_doc,
        filter_by_checklist,
        validate_sample,
    )

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
        build_sample_doc(sample_id, record, common_name_cache)
        for sample_id, record in valid_metadata.items()
    ]


@task(multiple_outputs=False)
def fetch_annotations() -> dict:
    """
    Load the `aegis.jsonl` annotation manifest (produced upstream by
    `import_annotations_task`) from GCS. Returns {taxId: [record, ...]};
    empty dict on failure so the species-doc build degrades gracefully.

    `multiple_outputs=False` is required: TaskFlow would otherwise infer it
    from the `-> dict` annotation and reject integer taxId keys.
    """
    from dependencies.aegis_annotations import load_annotation_manifest

    try:
        return load_annotation_manifest()
    except Exception as exc:  # missing manifest, GCS/permission error, etc.
        logger.warning(
            "Could not load aegis annotation manifest (%s); data_portal docs "
            "will be built without annotations.",
            exc,
        )
        return {}


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
    """Create indices, bulk index documents, rotate aliases, prune old indices."""
    from datetime import datetime

    from airflow.models import Variable

    from dependencies.aegis_es import (
        get_es_client,
        create_index_with_mapping,
        bulk_index_documents,
        SAMPLES_MAPPING,
        DATA_PORTAL_MAPPING,
    )
    from dependencies import manage_es_indices

    host = Variable.get("aegis_elasticsearch_host")
    password = Variable.get("aegis_elasticsearch_password")

    today = datetime.utcnow().strftime("%Y-%m-%d")
    samples_index = f"{today}_samples"
    data_portal_index = f"{today}_data_portal"

    es = get_es_client(host, password)

    # Create today's indices with mappings
    create_index_with_mapping(es, samples_index, SAMPLES_MAPPING)
    create_index_with_mapping(es, data_portal_index, DATA_PORTAL_MAPPING)

    # Bulk index documents
    bulk_index_documents(es, samples_index, samples_docs, id_field="accession")
    bulk_index_documents(es, data_portal_index, data_portal_docs, id_field="taxId")

    # Rotate aliases onto today's indices and prune to the 2 newest
    # generations — same strategy as biodiversity_metadata_dag. `rotate`
    # builds its own client and hard-prefixes https://, so pass a bare host.
    rotate_host = host.replace("https://", "").replace("http://", "")
    manage_es_indices.rotate(
        host=rotate_host,
        password=password,
        date_prefix=today,
        specs=[("samples", "samples"), ("data_portal", "data_portal")],
        keep=2,
    )


@dag(
    schedule="0 7 * * *",
    #schedule_interval=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="Europe/London"),

    catchup=False,
    tags=["aegis_metadata_ingestion"],
)
def aegis_metadata_ingestion():
    """
    This DAG pulls data from ENA (PRJEB80366) and BioSamples and builds
    Elasticsearch indices for the AEGIS data portal.
    """
    github_token = Variable.get("github_token")
    import_annotations_task = PythonOperator(
        task_id="import_annotations_task",
        python_callable=import_annotations.main,
        op_kwargs={"github_token": github_token, "projects": ["aegis"]},
    )

    metadata = fetch_metadata()
    annotations = fetch_annotations()
    import_annotations_task >> annotations

    samples = build_samples_docs(metadata)
    data_portal = build_data_portal_docs(metadata, annotations)
    index_to_es(samples, data_portal)


aegis_metadata_ingestion()
