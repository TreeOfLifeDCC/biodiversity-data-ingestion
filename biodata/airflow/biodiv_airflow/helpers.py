import requests
import time

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from biodiv_airflow.config import BiodivConfig
from google.auth.transport.requests import Request
from google.oauth2 import id_token


def split_gcs_uri(gcs_uri: str) -> tuple[str, str]:
    # gcs_uri like "gs://bucket/path/to/object_or_prefix"
    if not gcs_uri.startswith("gs://"):
        raise ValueError(f"Not a GCS URI: {gcs_uri}")
    no_scheme = gcs_uri[5:]
    bucket, _, path = no_scheme.partition("/")
    return bucket, path


def write_gcs_marker(gcs_marker_uri: str, gcp_conn_id: str = "google_cloud_default", **_):
    bucket, object_name = split_gcs_uri(gcs_marker_uri)

    hook = GCSHook(gcp_conn_id=gcp_conn_id)
    # Upload an empty marker file. This is idempotent: re-uploading overwrites.
    hook.upload(
        bucket_name=bucket,
        object_name=object_name,
        data=b"",
        mime_type="text/plain",
    )


def call_delete_service(
    delete_url: str,
    *,
    timeout_s: int = 30,
    max_attempts: int = 5,
    base_sleep_s: float = 2.0,
) -> str:
    """
    Call a private Cloud Run delete-service using an OIDC ID token.
    Idempotent handling:
      - 2xx: success
      - 404: treat as already deleted (success)
      - 409: treat as already deleting / conflict (success)
    Retries on transient errors (5xx, 429, network).
    Fails loudly on 401/403.
    """

    _IDEMPOTENT_HTTP_STATUSES = {200, 202, 204, 404, 409}

    auth_req = Request()

    last_err: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            token = id_token.fetch_id_token(auth_req, delete_url)

            resp = requests.post(
                delete_url,
                headers={"Authorization": f"Bearer {token}"},
                timeout=timeout_s,
            )

            # Fast-fail on authn/authz problems — retries won't fix IAM.
            if resp.status_code in (401, 403):
                raise PermissionError(
                    f"Delete service auth failed ({resp.status_code}). "
                    f"Caller likely missing roles/run.invoker. Body: {resp.text[:500]}"
                )

            # Idempotent success statuses
            if resp.status_code in _IDEMPOTENT_HTTP_STATUSES:
                return f"delete_service_ok status={resp.status_code} body={resp.text[:1000]}"

            # Retryable statuses
            if resp.status_code in (429, 500, 502, 503, 504):
                raise RuntimeError(
                    f"Transient delete-service error status={resp.status_code} body={resp.text[:500]}"
                )

            # Non-retryable unexpected statuses
            resp.raise_for_status()
            return f"delete_service_ok status={resp.status_code} body={resp.text[:1000]}"

        except (requests.Timeout, requests.ConnectionError, RuntimeError) as e:
            last_err = e
            if attempt == max_attempts:
                break

            sleep_s = base_sleep_s * (2 ** (attempt - 1))
            time.sleep(sleep_s)

    raise RuntimeError(f"Delete service failed after {max_attempts} attempts: {last_err}")


def validate_config(cfg: BiodivConfig, *, require_delete_service: bool = False) -> str:
    required_vars = {
        "biodiv_gcp_project": cfg.gcp_project,
        "biodiv_gcp_region": cfg.gcp_region,
        "biodiv_bucket": cfg.gcs_bucket,
        "biodiv_image_tag": cfg.image_tag,
        "biodiv_dataflow_worker_sa_email": cfg.dataflow_service_account,
        "elasticsearch_host": cfg.elastic_host,
        "elasticsearch_user": cfg.elastic_user,
        "elasticsearch_password": cfg.elastic_password,
        "biodiv_bq_dataset": cfg.bq_dataset,
    }

    if require_delete_service:
        required_vars["dev_delete_service_url"] = cfg.delete_service

    missing = []
    for k, v in required_vars.items():
        sv = str(v).strip()
        if not sv or sv.lower() == "none":
            missing.append(k)

    if missing:
        raise ValueError(f"Missing Airflow Variables: {', '.join(missing)}")

    try:
        int(cfg.min_new_species_threshold)
        int(cfg.elastic_pages)
        int(cfg.elastic_size)
    except (TypeError, ValueError) as e:
        raise ValueError(
            "min_new_species_threshold, elasticsearch_pages, and elasticsearch_size must be integers"
        ) from e

    return "config_ok"


# -------------------------------
# Helpers for Airflow gate
# -------------------------------
def _fetch_es_annotated_tax_ids(
    *,
    host: str,
    user: str,
    password: str,
    index: str,
    page_size: int,
    max_pages: int,
) -> set[str]:
    from elasticsearch import Elasticsearch

    es = Elasticsearch(
        hosts=host,
        basic_auth=(user, password),
        request_timeout=30,
        retry_on_timeout=True,
        max_retries=3,
    )

    tax_ids: set[str] = set()
    after = None
    page_i = 0

    max_pages = max_pages if max_pages is not None else 0  # 0 => fetch all

    def should_continue(i: int) -> bool:
        return (max_pages <= 0) or (i < max_pages)

    while should_continue(page_i):
        search_kwargs = {
            "index": index,
            "size": page_size,
            "sort": [{"tax_id": "asc"}],
            "query": {"term": {"annotation_complete": "Done"}},
            "_source": ["tax_id"],
        }

        if after is not None:
            search_kwargs["search_after"] = after

        response = es.search(**search_kwargs)
        hits = response.get("hits", {}).get("hits", [])

        if not hits:
            break

        for hit in hits:
            src = hit.get("_source", {}) or {}
            tax_id = src.get("tax_id")
            if tax_id is not None:
                tax_ids.add(str(tax_id))

        last_sort = hits[-1].get("sort")
        if last_sort is None:
            raise RuntimeError(
                "Elasticsearch response missing 'sort' values for search_after pagination."
            )

        after = last_sort
        page_i += 1

    return tax_ids


def _fetch_bq_logged_tax_ids(
    *,
    project_id: str,
    bq_dataset: str,
    gate_table: str = "bp_log_taxonomy",
) -> set[str]:
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)

    query = f"""
        SELECT DISTINCT tax_id
        FROM `{project_id}.{bq_dataset}.{gate_table}`
        WHERE tax_id IS NOT NULL
    """

    rows = client.query(query).result()
    return {str(row["tax_id"]) for row in rows if row["tax_id"] is not None}


def evaluate_new_species_gate(
    cfg: BiodivConfig,
    *,
    elastic_index: str = "data_portal",
    gate_table: str = "bp_log_taxonomy",
) -> dict:
    threshold = int(cfg.min_new_species_threshold)

    es_tax_ids = _fetch_es_annotated_tax_ids(
        host=cfg.elastic_host,
        user=cfg.elastic_user,
        password=cfg.elastic_password,
        index=elastic_index,
        page_size=int(cfg.elastic_size),
        max_pages=int(cfg.elastic_pages),
    )

    bq_tax_ids = _fetch_bq_logged_tax_ids(
        project_id=cfg.gcp_project,
        bq_dataset=cfg.bq_dataset,
        gate_table=gate_table,
    )

    new_tax_ids = sorted(es_tax_ids - bq_tax_ids)

    result = {
        "should_run": len(new_tax_ids) >= threshold,
        "threshold": threshold,
        "annotated_tax_ids_count": len(es_tax_ids),
        "existing_tax_ids_count": len(bq_tax_ids),
        "new_tax_ids_count": len(new_tax_ids),
        "sample_new_tax_ids": new_tax_ids[:20],
    }

    print(f"[GATE] threshold={result['threshold']}")
    print(f"[GATE] annotated_tax_ids_count={result['annotated_tax_ids_count']}")
    print(f"[GATE] existing_tax_ids_count={result['existing_tax_ids_count']}")
    print(f"[GATE] new_tax_ids_count={result['new_tax_ids_count']}")
    print(f"[GATE] should_run={result['should_run']}")
    print(f"[GATE] sample_new_tax_ids={result['sample_new_tax_ids']}")

    return result


def choose_branch(
    cfg: BiodivConfig,
    *,
    elastic_index: str = "data_portal",
    gate_table: str = "bp_log_taxonomy",
    run_task_id: str = "run_taxonomy",
    skip_task_id: str = "mark_pipelines_skip_success",
    **context,
) -> str:
    result = evaluate_new_species_gate(
        cfg,
        elastic_index=elastic_index,
        gate_table=gate_table,
    )

    ti = context.get("ti")
    if ti:
        ti.xcom_push(key="gate_result", value=result)

    if result["should_run"]:
        print(f"[GATE] Threshold met. Continuing pipeline. gate_result={result}")
        return run_task_id
    else:
        print(f"[GATE] Threshold not met. Skipping pipeline. gate_result={result}")
        return skip_task_id

