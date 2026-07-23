import logging
import os

import functions_framework
from flask import jsonify
from google.api_core import exceptions
from google.cloud import storage
from google.cloud.orchestration.airflow import service_v1


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()

    if not value:
        raise ValueError(f"Missing required environment variable: {name}")

    return value


def _optional_env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _bool_env(name: str, default: bool = False) -> bool:
    raw_value = os.environ.get(name)

    if raw_value is None:
        return default

    normalized = raw_value.strip().lower()

    if normalized in {"true", "1", "yes", "y"}:
        return True

    if normalized in {"false", "0", "no", "n"}:
        return False

    raise ValueError(
        f"Environment variable {name} must be true or false; "
        f"received: {raw_value!r}"
    )


def _int_env(name: str, default: int) -> int:
    raw_value = os.environ.get(name, "").strip()

    if not raw_value:
        return default

    value = int(raw_value)

    if value < 1:
        raise ValueError(f"{name} must be greater than zero")

    return value


@functions_framework.http
def cleanup_composer_bucket(http_request):
    """
    Deletes objects from the configured custom Composer bucket.

    The service refuses to delete bucket content until the configured Composer
    environment no longer exists.

    Required environment variables:
      PROJECT_ID
      REGION
      COMPOSER_ENV_NAME
      COMPOSER_BUCKET

    Optional environment variables:
      COMPOSER_BUCKET_PREFIX
      ALLOW_FULL_BUCKET_CLEANUP
      MAX_OBJECTS_PER_REQUEST

    Request body:
      {"dry_run": true}

    The bucket and prefix cannot be supplied in the request body. This prevents
    callers from using the service to target arbitrary buckets.
    """

    if http_request.method != "POST":
        return (
            jsonify(
                {
                    "status": "error",
                    "message": "Only POST requests are supported.",
                }
            ),
            405,
        )

    try:
        project_id = _require_env("PROJECT_ID")
        region = _require_env("REGION")
        environment_name = _require_env("COMPOSER_ENV_NAME")
        bucket_name = _require_env("COMPOSER_BUCKET")

        bucket_prefix = _optional_env("COMPOSER_BUCKET_PREFIX")
        allow_full_cleanup = _bool_env(
            "ALLOW_FULL_BUCKET_CLEANUP",
            default=False,
        )
        max_objects = _int_env(
            "MAX_OBJECTS_PER_REQUEST",
            default=5000,
        )

    except (ValueError, TypeError) as error:
        logger.exception("Invalid service configuration")

        return (
            jsonify(
                {
                    "status": "configuration_error",
                    "message": str(error),
                }
            ),
            500,
        )

    # Reject gs:// because the Storage client expects only the bucket name.
    if bucket_name.startswith("gs://"):
        return (
            jsonify(
                {
                    "status": "configuration_error",
                    "message": (
                        "COMPOSER_BUCKET must contain only the bucket name, "
                        "without gs://."
                    ),
                }
            ),
            500,
        )

    # Safety control: an empty prefix means every object in the bucket.
    if not bucket_prefix and not allow_full_cleanup:
        return (
            jsonify(
                {
                    "status": "cleanup_refused",
                    "message": (
                        "COMPOSER_BUCKET_PREFIX is empty and "
                        "ALLOW_FULL_BUCKET_CLEANUP is not true."
                    ),
                    "bucket": bucket_name,
                }
            ),
            400,
        )

    request_json = http_request.get_json(silent=True) or {}
    dry_run = request_json.get("dry_run") is True

    environment_resource = (
        f"projects/{project_id}/locations/{region}/"
        f"environments/{environment_name}"
    )

    # ---------------------------------------------------------
    # Safety check: Composer environment must no longer exist.
    # ---------------------------------------------------------

    composer_client = service_v1.EnvironmentsClient()

    try:
        composer_client.get_environment(
            request={"name": environment_resource}
        )

        logger.warning(
            "Cleanup refused because Composer environment still exists: %s",
            environment_resource,
        )

        return (
            jsonify(
                {
                    "status": "cleanup_refused",
                    "message": "Composer environment still exists.",
                    "environment": environment_resource,
                    "bucket": bucket_name,
                    "prefix": bucket_prefix,
                }
            ),
            409,
        )

    except exceptions.NotFound:
        # Expected state: environment deletion has completed.
        logger.info(
            "Composer environment no longer exists; cleanup is permitted: %s",
            environment_resource,
        )

    except exceptions.GoogleAPICallError as error:
        logger.exception("Unable to verify Composer environment state")

        return (
            jsonify(
                {
                    "status": "verification_failed",
                    "message": str(error),
                    "environment": environment_resource,
                }
            ),
            502,
        )

    # ---------------------------------------------------------
    # Delete bucket objects.
    # ---------------------------------------------------------

    storage_client = storage.Client(project=project_id)

    deleted_count = 0
    already_missing_count = 0
    precondition_failed_count = 0
    inspected_count = 0
    limit_reached = False

    try:
        blobs = storage_client.list_blobs(
            bucket_name,
            prefix=bucket_prefix or None,
            versions=True,
        )

        for blob in blobs:
            if inspected_count >= max_objects:
                limit_reached = True
                break

            inspected_count += 1

            logger.info(
                "%s object: gs://%s/%s generation=%s",
                "Would delete" if dry_run else "Deleting",
                bucket_name,
                blob.name,
                blob.generation,
            )

            if dry_run:
                continue

            try:
                # Delete exactly the generation returned by list_blobs.
                # This prevents deleting a replacement generation that might
                # have been created between listing and deletion.
                blob.delete(if_generation_match=blob.generation)
                deleted_count += 1

            except exceptions.NotFound:
                # Idempotency: another invocation may already have deleted it.
                already_missing_count += 1

            except exceptions.PreconditionFailed:
                # The object changed after it was listed. Leave it for a
                # subsequent cleanup invocation instead of deleting blindly.
                precondition_failed_count += 1

    except exceptions.NotFound:
        # Treat an already-deleted bucket as a successful idempotent outcome.
        return (
            jsonify(
                {
                    "status": "already_clean",
                    "message": "Bucket does not exist.",
                    "bucket": bucket_name,
                    "prefix": bucket_prefix,
                }
            ),
            200,
        )

    except exceptions.Forbidden as error:
        logger.exception("Cleanup service lacks Cloud Storage permissions")

        return (
            jsonify(
                {
                    "status": "permission_denied",
                    "message": str(error),
                    "bucket": bucket_name,
                }
            ),
            403,
        )

    except exceptions.GoogleAPICallError as error:
        logger.exception("Cloud Storage cleanup failed")

        return (
            jsonify(
                {
                    "status": "cleanup_failed",
                    "message": str(error),
                    "bucket": bucket_name,
                    "prefix": bucket_prefix,
                    "deleted_count": deleted_count,
                }
            ),
            502,
        )

    target = (
        f"gs://{bucket_name}/{bucket_prefix}"
        if bucket_prefix
        else f"gs://{bucket_name}"
    )

    if dry_run:
        return (
            jsonify(
                {
                    "status": "dry_run_complete",
                    "target": target,
                    "objects_inspected": inspected_count,
                    "limit_reached": limit_reached,
                    "max_objects_per_request": max_objects,
                }
            ),
            200,
        )

    response_status = (
        "cleanup_incomplete" if limit_reached else "cleanup_complete"
    )
    response_code = 202 if limit_reached else 200

    return (
        jsonify(
            {
                "status": response_status,
                "target": target,
                "objects_inspected": inspected_count,
                "objects_deleted": deleted_count,
                "objects_already_missing": already_missing_count,
                "precondition_failures": precondition_failed_count,
                "limit_reached": limit_reached,
                "message": (
                    "Invoke the service again to continue cleanup."
                    if limit_reached or precondition_failed_count
                    else "Cleanup completed."
                ),
            }
        ),
        response_code,
    )
