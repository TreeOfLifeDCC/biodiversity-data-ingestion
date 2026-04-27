import os
import functions_framework

from google.api_core.exceptions import NotFound, FailedPrecondition
from google.cloud.orchestration.airflow import service_v1
from google.cloud.orchestration.airflow.service_v1.types import DeleteEnvironmentRequest


@functions_framework.http
def delete_composer_environment(http_request):
    """
    Deletes a Composer environment. Intended for authenticated Cloud Run invocation.
    Uses env vars set on the Cloud Run service:
      PROJECT_ID, REGION, COMPOSER_ENV_NAME
    """

    project_id = os.environ["PROJECT_ID"]
    region = os.environ["REGION"]
    env_name = os.environ["COMPOSER_ENV_NAME"]

    env_resource = f"projects/{project_id}/locations/{region}/environments/{env_name}"

    client = service_v1.EnvironmentsClient()

    try:
        req = DeleteEnvironmentRequest(name=env_resource)
        op = client.delete_environment(request=req)
        # Return operation name for traceability
        return (f"delete requested: {env_resource}\noperation: {op.operation.name}\n", 200)

    except NotFound:
        # Idempotency: if it's already gone, treat as success
        return (f"already deleted (NotFound): {env_resource}\n", 200)

    except FailedPrecondition as e:
        return (f"delete not applied due to state (FailedPrecondition): {env_resource}\n{e}\n", 200)