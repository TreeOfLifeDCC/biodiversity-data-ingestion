from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from datetime import date, datetime
from typing import Any, Iterable, Iterator, Mapping

logger = logging.getLogger(__name__)


# --- Configuration ------------------------------------------------------------

DEFAULT_MYSQL_HOST = "mysql-ens-genebuild-prod-1"
DEFAULT_MYSQL_PORT = 4527
DEFAULT_MYSQL_USER = "ensro"  # public read-only account, no password
DEFAULT_MYSQL_DB = "gb_assembly_metadata"

DEFAULT_GCS_PREFIX = "annotations"

# Upstream genebuild registry truncates some metric_name values at an older
# column-length limit. We normalize the known truncated names to their full
# canonical form so downstream ES sees one field per metric instead of two.
# If new truncated names show up, add them here AND raise upstream.
METRIC_NAME_ALIASES: dict[str, str] = {
    "genebuild.stats.average_coding_exons_per_coding_tr":
        "genebuild.stats.average_coding_exons_per_coding_transcript",
    "genebuild.stats.average_coding_exons_per_transcrip":
        "genebuild.stats.average_coding_exons_per_transcript",
}

# Status fields carried verbatim into each JSONL record alongside `metrics`.
STATUS_FIELDS: tuple[str, ...] = (
    "gb_status",
    "annotation_method",
    "annotation_source",
    "date_started",
    "date_status_update",
    "last_genebuild_update",
    "release_date",
    "release_type",
)

# Query runs on the EBI genebuild registry. Ordering is required by the
# row-grouping logic in `rows_to_records`.
EXPORT_QUERY = """
SELECT
    gs.gca_accession,
    gs.genebuild_status_id,
    gs.gb_status,
    gs.annotation_method,
    gs.annotation_source,
    gs.date_started,
    gs.date_status_update,
    gs.last_genebuild_update,
    gs.release_date,
    gs.release_type,
    am.metrics_name,
    am.metrics_value
FROM assembly a
JOIN bioproject b ON a.assembly_id = b.assembly_id
JOIN genebuild_status gs
    ON a.assembly_id = gs.assembly_id
   AND gs.last_attempt = 1
LEFT JOIN annotation_metrics am
    ON a.assembly_id = am.assembly_id
   AND gs.genebuild_status_id = am.genebuild_status_id
WHERE b.bioproject_id = %(bioproject)s
ORDER BY gs.gca_accession, gs.genebuild_status_id, am.metrics_name
"""


# --- Core transform (pure, easy to test) --------------------------------------

def _normalize_metric_name(name: str) -> str:
    return METRIC_NAME_ALIASES.get(name, name)


def _coerce_status_value(value: Any) -> Any:
    """Make a status column JSON-friendly. Dates -> ISO strings, others pass through."""
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def rows_to_records(rows: Iterable[Mapping[str, Any]]) -> Iterator[dict]:
    """Group flat MySQL rows into one record per assembly.

    Assumes rows are ordered by `gca_accession`, `genebuild_status_id`,
    `metrics_name` (the export query enforces that). The `last_attempt = 1`
    filter should mean one status row per assembly; if not, the later
    `genebuild_status_id` wins and a warning is logged.
    """
    current_acc: str | None = None
    current_status_id: int | None = None
    current_record: dict | None = None

    for row in rows:
        acc = row["gca_accession"]
        status_id = row["genebuild_status_id"]

        if acc != current_acc:
            if current_record is not None:
                yield current_record
            current_acc = acc
            current_status_id = status_id
            current_record = {
                "gca_accession": acc,
                "genebuild_status_id": status_id,
                **{f: _coerce_status_value(row[f]) for f in STATUS_FIELDS},
                "metrics": {},
            }
        elif status_id != current_status_id:
            logger.warning(
                "Multiple last_attempt=1 status rows for %s "
                "(genebuild_status_id %s then %s); keeping the latter.",
                acc, current_status_id, status_id,
            )
            current_status_id = status_id
            current_record.update(
                {f: _coerce_status_value(row[f]) for f in STATUS_FIELDS}
            )
            current_record["genebuild_status_id"] = status_id
            current_record["metrics"] = {}

        name = row.get("metrics_name")
        if name is not None:
            canonical = _normalize_metric_name(name)
            raw = row.get("metrics_value")
            value_str = "" if raw is None else str(raw)
            current_record["metrics"].setdefault(canonical, []).append(value_str)

    if current_record is not None:
        yield current_record


def serialize_jsonl(records: Iterable[Mapping[str, Any]]) -> Iterator[str]:
    """Render each record as a JSONL line with stable key ordering."""
    for r in records:
        yield json.dumps(r, sort_keys=True, ensure_ascii=False) + "\n"


# --- MySQL fetch --------------------------------------------------------------

def fetch_rows(
    *,
    bioproject: str,
    host: str = DEFAULT_MYSQL_HOST,
    port: int = DEFAULT_MYSQL_PORT,
    user: str = DEFAULT_MYSQL_USER,
    password: str | None = None,
    database: str = DEFAULT_MYSQL_DB,
) -> list[dict]:
    """Run the export query and return all rows as dicts."""
    import pymysql  # local import: keeps import-time light for Airflow scheduling

    conn = pymysql.connect(
        host=host,
        port=port,
        user=user,
        password=password or "",
        database=database,
        cursorclass=pymysql.cursors.DictCursor,
        charset="utf8mb4",
    )
    try:
        with conn.cursor() as cur:
            cur.execute(EXPORT_QUERY, {"bioproject": bioproject})
            return cur.fetchall()
    finally:
        conn.close()


# --- GCS upload ---------------------------------------------------------------

def gcs_object_name(bioproject: str, prefix: str = DEFAULT_GCS_PREFIX) -> str:
    return f"{prefix}/{bioproject}.jsonl"


def upload_jsonl_to_gcs(
    *,
    lines: Iterable[str],
    bucket: str,
    object_name: str,
) -> str:
    """Upload JSONL content to gs://{bucket}/{object_name} and return the URI."""
    from google.cloud import storage

    body = "".join(lines).encode("utf-8")
    client = storage.Client()
    blob = client.bucket(bucket).blob(object_name)
    blob.upload_from_string(body, content_type="application/x-ndjson")
    uri = f"gs://{bucket}/{object_name}"
    logger.info("Uploaded %d bytes (%s) to %s", len(body), object_name, uri)
    return uri


# --- Orchestration ------------------------------------------------------------

def run_import(
    *,
    bioproject: str,
    bucket: str | None = None,
    mysql_host: str = DEFAULT_MYSQL_HOST,
    mysql_port: int = DEFAULT_MYSQL_PORT,
    mysql_user: str = DEFAULT_MYSQL_USER,
    mysql_password: str | None = None,
    mysql_database: str = DEFAULT_MYSQL_DB,
    gcs_prefix: str = DEFAULT_GCS_PREFIX,
    output_path: str | None = None,
    dry_run: bool = False,
) -> dict:
    """End-to-end: fetch -> group -> write (GCS, local file, or nowhere).

    Exactly one of `bucket`, `output_path`, or `dry_run=True` must be effective.
    Returns a small summary dict (useful as Airflow XCom).
    """
    if not bioproject:
        raise ValueError("bioproject is required")
    if not dry_run and not bucket and not output_path:
        raise ValueError("must provide one of: bucket, output_path, or dry_run=True")

    rows = fetch_rows(
        bioproject=bioproject,
        host=mysql_host,
        port=mysql_port,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database,
    )
    logger.info("Fetched %d rows for bioproject %s", len(rows), bioproject)

    records = list(rows_to_records(rows))
    lines = list(serialize_jsonl(records))
    logger.info("Produced %d JSONL records", len(records))
    if not records:
        logger.warning("No records produced for bioproject %s (empty output).", bioproject)

    summary: dict[str, Any] = {
        "bioproject": bioproject,
        "row_count": len(rows),
        "record_count": len(records),
        "uri": None,
    }

    if dry_run:
        logger.info("Dry-run: skipping write.")
        return summary

    if output_path:
        with open(output_path, "w", encoding="utf-8") as fh:
            fh.writelines(lines)
        logger.info("Wrote %d records to %s", len(records), output_path)
        summary["uri"] = output_path
        return summary

    assert bucket is not None  # checked above
    object_name = gcs_object_name(bioproject, prefix=gcs_prefix)
    summary["uri"] = upload_jsonl_to_gcs(lines=lines, bucket=bucket, object_name=object_name)
    return summary


# --- CLI ----------------------------------------------------------------------

def _build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="import_aegis_annotations",
        description=(
            "Fetch genebuild annotation status + metrics for a bioproject and "
            "write JSONL to GCS (or to a local file for inspection)."
        ),
    )
    p.add_argument("--bioproject", required=True, help="e.g. PRJEB80366")
    p.add_argument("--bucket", help="GCS bucket (required unless --output or --dry-run)")
    p.add_argument("--gcs-prefix", default=DEFAULT_GCS_PREFIX,
                   help=f"GCS key prefix (default: {DEFAULT_GCS_PREFIX})")
    p.add_argument("--mysql-host", default=DEFAULT_MYSQL_HOST)
    p.add_argument("--mysql-port", type=int, default=DEFAULT_MYSQL_PORT)
    p.add_argument("--mysql-user", default=DEFAULT_MYSQL_USER)
    p.add_argument("--mysql-password", default=os.environ.get("MYSQL_PASSWORD"),
                   help="defaults to $MYSQL_PASSWORD; ensro normally has none")
    p.add_argument("--mysql-database", default=DEFAULT_MYSQL_DB)
    p.add_argument("--output", help="Write JSONL to a local path instead of GCS.")
    p.add_argument("--dry-run", action="store_true",
                   help="Fetch and transform, but write nothing.")
    p.add_argument("--log-level", default="INFO")
    return p


def main(argv: list[str] | None = None) -> int:
    args = _build_arg_parser().parse_args(argv)
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    if not args.dry_run and not args.output and not args.bucket:
        print("error: must provide --bucket, --output, or --dry-run", file=sys.stderr)
        return 2

    summary = run_import(
        bioproject=args.bioproject,
        bucket=args.bucket,
        mysql_host=args.mysql_host,
        mysql_port=args.mysql_port,
        mysql_user=args.mysql_user,
        mysql_password=args.mysql_password,
        mysql_database=args.mysql_database,
        gcs_prefix=args.gcs_prefix,
        output_path=args.output,
        dry_run=args.dry_run,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
