import os
import functions_framework

from google.cloud.orchestration.airflow import service_v1
from google.cloud.orchestration.airflow.service_v1.types import LoadSnapshotRequest


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise ValueError(f"Missing required env var: {name}")
    return value


@functions_framework.http
def load_composer_snapshot(request):
    if request.method != "POST":
        return ("Use POST\n", 405)

    project_id = _require_env("PROJECT_ID")
    region = _require_env("REGION")
    env_name = _require_env("COMPOSER_ENV_NAME")
    allowed_snapshot_prefix = _require_env("ALLOWED_SNAPSHOT_PREFIX")

    payload = request.get_json(silent=True) or {}
    snapshot_path = str(payload.get("snapshot_path", "")).strip()

    if not snapshot_path:
        return "Missing snapshot_path\n", 400

    if not snapshot_path.startswith(allowed_snapshot_prefix):
        return "snapshot_path not allowed\n", 403

    environment = (
        f"projects/{project_id}/locations/{region}/environments/{env_name}"
    )

    client = service_v1.EnvironmentsClient()
    operation = client.load_snapshot(
        request=LoadSnapshotRequest(
            environment=environment,
            snapshot_path=snapshot_path,
        )
    )

    return (
        f"load requested\n"
        f"environment: {environment}\n"
        f"snapshot_path: {snapshot_path}\n"
        f"operation: {operation.operation.name}\n",
        200,
    )