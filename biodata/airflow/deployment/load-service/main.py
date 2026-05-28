import os
import functions_framework

from google.api_core.exceptions import FailedPrecondition, NotFound, PermissionDenied
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

    try:
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

    except NotFound as e:
        return (f"environment or snapshot not found: {environment}\n{e}\n", 404)

    except PermissionDenied as e:
        return (f"permission denied loading snapshot: {environment}\n{e}\n", 403)

    except FailedPrecondition as e:
        return (
            f"load not applied due to state (FailedPrecondition): {environment}\n{e}\n",
            200,
        )