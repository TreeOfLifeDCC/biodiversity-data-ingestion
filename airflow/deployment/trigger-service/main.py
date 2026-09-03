import os

import functions_framework
import google.auth
from google.api_core.exceptions import NotFound
from google.auth.transport.requests import AuthorizedSession
from google.cloud.orchestration.airflow import service_v1


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"Missing required env var: {name}")
    return value


def _allowed_dag_ids() -> set[str]:
    raw_value = _require_env("ALLOWED_DAG_IDS")
    dag_ids = {item.strip() for item in raw_value.split(",") if item.strip()}
    if not dag_ids:
        raise ValueError("ALLOWED_DAG_IDS must contain at least one DAG ID")
    return dag_ids


@functions_framework.http
def trigger_composer_dag(request):
    if request.method != "POST":
        return ("Use POST\n", 405)

    project_id = _require_env("PROJECT_ID")
    region = _require_env("REGION")
    env_name = _require_env("COMPOSER_ENV_NAME")
    allowed_dag_ids = _allowed_dag_ids()

    payload = request.get_json(silent=True) or {}

    dag_id = str(payload.get("dag_id") or "").strip()
    conf = payload.get("conf", {})
    if not isinstance(conf, dict):
        return ("conf must be a JSON object\n", 400)

    if not dag_id:
        return ("Missing dag_id\n", 400)

    if dag_id not in allowed_dag_ids:
        return ("dag_id not allowed\n", 403)

    env_resource = f"projects/{project_id}/locations/{region}/environments/{env_name}"

    client = service_v1.EnvironmentsClient()

    try:
        env = client.get_environment(name=env_resource)
    except NotFound:
        return (f"environment not found: {env_resource}\n", 404)

    web_server_url = (env.config.airflow_uri or "").rstrip("/")
    if not web_server_url:
        return (f"environment has no Airflow webserver URI: {env_resource}\n", 503)

    credentials, _ = google.auth.default(scopes=["https://www.googleapis.com/auth/cloud-platform"])
    authed_session = AuthorizedSession(credentials)

    response = authed_session.post(
        f"{web_server_url}/api/v1/dags/{dag_id}/dagRuns",
        json={"conf": conf},
        timeout=90,
    )

    if response.status_code in (200, 201):
        return (
            f"dag trigger requested\n"
            f"environment: {env_resource}\n"
            f"dag_id: {dag_id}\n"
            f"response: {response.text}\n",
            200,
        )

    return (
        f"dag trigger failed\n"
        f"status: {response.status_code}\n"
        f"body: {response.text}\n",
        response.status_code,
    )
