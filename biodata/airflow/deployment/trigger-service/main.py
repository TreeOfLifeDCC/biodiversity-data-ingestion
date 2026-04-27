import os

import functions_framework
import google.auth
from google.auth.transport.requests import AuthorizedSession
from google.cloud.orchestration.airflow import service_v1


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"Missing required env var: {name}")
    return value


@functions_framework.http
def trigger_composer_dag(request):
    if request.method != "POST":
        return ("Use POST\n", 405)

    project_id = _require_env("PROJECT_ID")
    region = _require_env("REGION")
    env_name = _require_env("COMPOSER_ENV_NAME")
    default_dag_id = _require_env("DAG_ID")

    payload = request.get_json(silent=True) or {}

    dag_id = str(payload.get("dag_id") or default_dag_id).strip()
    conf = payload.get("conf", {})

    if dag_id != default_dag_id:
        return ("dag_id not allowed\n", 403)

    env_resource = f"projects/{project_id}/locations/{region}/environments/{env_name}"

    client = service_v1.EnvironmentsClient()
    env = client.get_environment(name=env_resource)
    web_server_url = env.config.airflow_uri.rstrip("/")

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