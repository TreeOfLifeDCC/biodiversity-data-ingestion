import json
import os
import re
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp.internal.clients import bigquery as bq
from datetime import datetime, timezone

def sanitize_species_name(species: str) -> str:
    """
    Extracts the genus and species epithet (first two words) from a species name
    and returns a sanitized string that can safely be used in file paths.
    """
    parts = species.strip().split()
    if not parts:
        return ''
    genus_species = '_'.join(parts[:2])
    safe = re.sub(r'[^A-Za-z0-9_]', '_', genus_species)
    safe = re.sub(r'_+', '_', safe).strip('_')
    return safe


def extract_species_name(file_path: str) -> str:
    """
    Extracts the species name from a file path like 'occ_Panthera_leo.jsonl'
    and converts it to a space-separated name like 'Panthera leo'.
    """
    match = re.search(r'occ_(.+?)\.jsonl$', file_path)
    return match.group(1).replace('_', ' ') if match else "Unknown species"


def write_species_file(kv, output_dir):
    """
    Writes JSONL records for a single species to a file in the output directory.
    `kv`: tuple (species_name, iterable of json strings)
    """
    species_name, records = kv
    safe_name = re.sub(r'[^A-Za-z0-9_]', '_', species_name.replace(' ', '_'))
    path = os.path.join(output_dir, f'occ_{safe_name}.jsonl')
    with FileSystems.create(path) as f:
        for line in records:
            f.write((json.dumps(line) + "\n").encode("utf-8"))


def merge_annotations(inputs):
    """
    Merges climate and biogeo annotations keyed by occurrenceID into a single pcollection.
    :param inputs: mapped pcollection using occurrenceID
    :return:
    """
    merged = {}
    for src in ["climate", "biogeo"]:
        recs = inputs.get(src, [])
        if recs:
            merged.update(recs[0])
    return merged


def convert_dict_to_table_schema(schema_dict_list):
    """
    Converts a list of schema dicts (from JSON) into a Beam-compatible TableSchema.
    Recursively parse nested fields (Type: RECORD).
    """
    def _convert_field(field_dict):
        field = bq.TableFieldSchema()
        field.name = field_dict["name"]
        field.type = field_dict["type"]
        field.mode = field_dict.get("mode", "NULLABLE")

        if field.type == "RECORD" and "fields" in field_dict:
            field.fields.extend([_convert_field(f) for f in field_dict["fields"]])

        return field

    schema = bq.TableSchema()
    schema.fields.extend([_convert_field(f) for f in schema_dict_list])
    return schema


def fetch_spatial_file_to_local(shapefile_path: str, local_dir: str) -> str:
    """
    Copy a shapefile and its required sidecar files from Beam FileSystems
    (local filesystem or gs://) into a local directory, and return the
    local path to the .shp file.

    Expected input:
        /path/to/ne_10m_land.shp
        gs://bucket/path/ne_10m_land/ne_10m_land.shp
    """
    if not shapefile_path.lower().endswith(".shp"):
        raise ValueError(
            f"Expected a .shp path, got: {shapefile_path}"
        )

    os.makedirs(local_dir, exist_ok=True)

    base_dir = os.path.dirname(shapefile_path.rstrip("/"))
    shp_name = os.path.basename(shapefile_path)
    stem, _ = os.path.splitext(shp_name)

    # Shapefile components commonly needed by geopandas/pyogrio
    allowed_exts = {".shp", ".shx", ".dbf", ".prj", ".cpg", ".qix", ".fix"}

    match_result = FileSystems.match([f"{base_dir}/*"])[0]
    metadata_list = match_result.metadata_list

    copied = []

    for metadata in metadata_list:
        src_path = metadata.path

        # Skip directory-like placeholders, which may appear in gs:// listings
        if src_path.endswith("/"):
            continue

        file_name = os.path.basename(src_path)
        if not file_name:
            continue

        src_stem, src_ext = os.path.splitext(file_name)

        # Copy only files belonging to the requested shapefile
        if src_stem != stem:
            continue
        if src_ext.lower() not in allowed_exts:
            continue

        dest_path = os.path.join(local_dir, file_name)

        with FileSystems.open(src_path) as file_source:
            with open(dest_path, "wb") as file_dest:
                file_dest.write(file_source.read())

        copied.append(dest_path)

    local_shp_path = os.path.join(local_dir, shp_name)

    required = [
        os.path.join(local_dir, f"{stem}.shp"),
        os.path.join(local_dir, f"{stem}.shx"),
        os.path.join(local_dir, f"{stem}.dbf"),
    ]
    missing = [p for p in required if not os.path.exists(p)]

    if missing:
        raise FileNotFoundError(
            f"Missing shapefile components after copy: {missing}. "
            f"Source path: {shapefile_path}. "
            f"Copied files: {copied}"
        )

    return local_shp_path


# -----------------------------------
# Helpers for data provenance
# -----------------------------------
def to_provenance_request(rec: dict) -> dict:
    """
    Minimal taxonomy-driven provenance request.
    Keeps only fields required to fetch/compose provenance output.
    """
    return {
        "tax_id": rec.get("tax_id"),
        "accession": rec.get("accession"),
        "gbif_usageKey": rec.get("gbif_usageKey"),
    }

# -----------------------------------
# Helpers for the Bigquery gate table
# -----------------------------------
def to_kv_tax_id(rec) -> tuple:
    """
        Convert an ES taxonomy record into a key-value pair keyed by tax_id.

        This is used prior to CoGroupByKey to join Elasticsearch records
        against the BigQuery gate table.

        Parameters
        ----------
        rec : dict
            Record emitted from FetchESFn. Expected to contain at least:
            - tax_id (str)
            - accession (str)
            - species (str)

        Returns
        -------
        tuple[str, dict]
            (tax_id, original_record)

        Notes
        -----
        tax_id is explicitly cast to string to ensure key type consistency
        with the BigQuery gate table.
        """
    return (str(rec["tax_id"]), rec)


def to_kv_existing_tax_id(row) -> tuple:
    """
        Convert a BigQuery gate table row into a keyed marker for join.

        This transform prepares existing tax_ids for a CoGroupByKey join
        against ES-derived records in order to filter already-ingested taxa.

        Parameters
        ----------
        row : dict
            Row returned from ReadFromBigQuery containing:
            - tax_id (str)

        Returns
        -------
        tuple[str, bool]
            (tax_id, True)

        Notes
        -----
        The boolean value is a presence marker only; its content is irrelevant.
        tax_id is cast to string to ensure deterministic join behavior.
        """
    # row is a dict like {"tax_id": 123}
    return (str(row["tax_id"]), True)


def keep_new_tax_ids(kv):
    """
        Filter ES records to retain only tax_ids not present in the gate table.

        This function operates on the output of CoGroupByKey where:
          - key = tax_id
          - value = {'es': [...], 'bq': [...]}

        Parameters
        ----------
        kv : tuple[str, dict]
            (tax_id, grouped_records) where grouped_records contains:
                - 'es': list of ES records
                - 'bq': list of matching gate records (empty if unseen)

        Yields
        ------
        dict
            ES record(s) whose tax_id does not exist in the gate table.

        Notes
        -----
        If any entry exists in the 'bq' group, the tax_id is considered
        already processed and is filtered out.

        This implements a set-difference operation:
            ES_tax_ids − Gate_tax_ids
        """
    tax_id, groups = kv
    es_recs = groups.get("es", [])
    seen = groups.get("bq", [])
    if seen:
        return
    for r in es_recs:
        yield r


def to_gate_row(rec: dict, status: str) -> dict:
    """
        Transform a taxonomy validation record into a row suitable
        for insertion into the BigQuery gate table.

        Parameters
        ----------
        rec : dict
            Record produced after ValidateNamesFn. Expected keys include:
                - tax_id
                - accession
                - species
                - gbif_usageKey
                - gbif_matchType
                - gbif_rank
                - gbif_scientificName
                - gbif_status
                - gbif_confidence

        status : str
            Processing status label. Typically:
                - 'validated'
                - 'to_check'

        Returns
        -------
        dict
            Row matching the schema of bq_taxonomy_gate:
                - tax_id (STRING)
                - accession (STRING)
                - species (STRING)
                - gbif_usageKey (INTEGER)
                - matchtype (STRING)
                - gbif_rank (STRING)
                - gbif_scientificName (STRING)
                - gbif_status (STRING)
                - gbif_confidence (INTEGER)
                - date_seen (TIMESTAMP, UTC)
                - status (STRING)

        Notes
        -----
        All fields are explicitly cast to their BigQuery types to ensure
        deterministic schema compliance during FILE_LOADS.

        date_seen is generated at transformation time using timezone-aware UTC.
        """
    return {
        "tax_id": str(rec.get("tax_id")) if rec.get("tax_id") is not None else None,
        "accession": str(rec.get("accession")) if rec.get("accession") is not None else None,
        "species": str(rec.get("species")) if rec.get("species") is not None else None,
        "gbif_usageKey": int(rec["gbif_usageKey"]) if rec.get("gbif_usageKey") is not None else None,
        "gbif_matchType": str(rec.get("gbif_matchType")) if rec.get("gbif_matchType") is not None else None,
        "gbif_rank": str(rec.get("gbif_rank")) if rec.get("gbif_rank") is not None else None,
        "gbif_scientificName": str(rec.get("gbif_scientificName")) if rec.get("gbif_scientificName") is not None else None,
        "gbif_status": str(rec.get("gbif_status")) if rec.get("gbif_status") is not None else None,
        "gbif_confidence": int(rec["gbif_confidence"]) if rec.get("gbif_confidence") is not None else None,
        "date_seen": datetime.now(timezone.utc),
        "status": str(status),
    }