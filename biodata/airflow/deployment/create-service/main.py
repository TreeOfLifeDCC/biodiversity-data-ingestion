import os
import functions_framework

from google.api_core.exceptions import AlreadyExists, FailedPrecondition
from google.cloud.orchestration.airflow import service_v1
from google.cloud.orchestration.airflow.service_v1.types import (
    CreateEnvironmentRequest,
    Environment,
    EnvironmentConfig,
    NodeConfig,
    SoftwareConfig,
StorageConfig,
    WorkloadsConfig,
)


def _require_env(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        raise ValueError(f"Missing required env var: {name}")
    return v


def _float_env(name: str, default: float) -> float:
    v = os.environ.get(name, "").strip()
    return float(v) if v else default


def _int_env(name: str, default: int) -> int:
    v = os.environ.get(name, "").strip()
    return int(v) if v else default


@functions_framework.http
def start_composer_environment(http_request):
    """
    Creates a Composer environment (Composer 3) using prod-matching settings.
    Configuration comes from env vars so dev/prod remain aligned without code changes.

    Required env vars:
      PROJECT_ID
      REGION
      COMPOSER_ENV_NAME
      NODE_SERVICE_ACCOUNT
      IMAGE_VERSION          (e.g. composer-3-airflow-2.10.2-build.11)

    Optional overrides (defaults match your prod code):
      SCHEDULER_CPU, SCHEDULER_MEM_GB, SCHEDULER_STORAGE_GB, SCHEDULER_COUNT
      WEBSERVER_CPU, WEBSERVER_MEM_GB, WEBSERVER_STORAGE_GB
      WORKER_CPU, WORKER_MEM_GB, WORKER_STORAGE_GB, WORKER_MIN_COUNT, WORKER_MAX_COUNT
      TRIGGERER_CPU, TRIGGERER_MEM_GB, TRIGGERER_COUNT
    """

    project_id = _require_env("PROJECT_ID")
    region = _require_env("REGION")
    env_name = _require_env("COMPOSER_ENV_NAME")
    node_sa = _require_env("NODE_SERVICE_ACCOUNT")
    image_version = _require_env("IMAGE_VERSION")
    composer_bucket = _require_env("COMPOSER_BUCKET")

    parent = f"projects/{project_id}/locations/{region}"
    env_resource = f"{parent}/environments/{env_name}"

    # --- WorkloadsConfig ---
    scheduler = WorkloadsConfig.SchedulerResource(
        cpu=_float_env("SCHEDULER_CPU", 1.0),
        memory_gb=_float_env("SCHEDULER_MEM_GB", 4.0),
        storage_gb=_float_env("SCHEDULER_STORAGE_GB", 1.0),
        count=_int_env("SCHEDULER_COUNT", 1),
    )

    web_server = WorkloadsConfig.WebServerResource(
        cpu=_float_env("WEBSERVER_CPU", 1.0),
        memory_gb=_float_env("WEBSERVER_MEM_GB", 4.0),
        storage_gb=_float_env("WEBSERVER_STORAGE_GB", 1.0),
    )

    worker = WorkloadsConfig.WorkerResource(
        cpu=_float_env("WORKER_CPU", 1.0),
        memory_gb=_float_env("WORKER_MEM_GB", 4.0),
        storage_gb=_float_env("WORKER_STORAGE_GB", 10.0),
        min_count=_int_env("WORKER_MIN_COUNT", 1),
        max_count=_int_env("WORKER_MAX_COUNT", 10),
    )

    triggerer = WorkloadsConfig.TriggererResource(
        cpu=_float_env("TRIGGERER_CPU", 1.0),
        memory_gb=_float_env("TRIGGERER_MEM_GB", 2.0),
        count=_int_env("TRIGGERER_COUNT", 1),
    )

    workloads = WorkloadsConfig(
        scheduler=scheduler,
        web_server=web_server,
        worker=worker,
        triggerer=triggerer,
    )

    env_config = EnvironmentConfig(
        workloads_config=WorkloadsConfig(
            scheduler=scheduler,
            web_server=web_server,
            worker=worker,
            triggerer=triggerer,
        ),
        software_config=SoftwareConfig(
            image_version=image_version,
        ),
        node_config=NodeConfig(service_account=node_sa),
    )

    environment = Environment(
        name=env_resource,
        config=env_config,
        storage_config=StorageConfig(
            bucket=composer_bucket,
        ),
    )

    client = service_v1.EnvironmentsClient()

    try:
        create_req = CreateEnvironmentRequest(
            parent=parent,
            environment=environment,
        )
        op = client.create_environment(request=create_req)
        return (
            f"create requested: {env_resource}\noperation: {op.operation.name}\n",
            200,
        )

    except AlreadyExists:
        # Idempotency: if env exists, treat as success
        return (f"already exists: {env_resource}\n", 200)

    except FailedPrecondition as e:
        # Often indicates env is being created/deleted or project not ready for action.
        return (f"create not applied due to state (FailedPrecondition): {env_resource}\n{e}\n", 200)