import json
import os
import re
import requests

from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp.internal.clients import bigquery as bq
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

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


def merge_summary_annotations(grouped):
    """
    Merge climate and biogeographic summary annotations for one accession.

    CoGroupByKey returns lists per source. If one source has no match, the
    corresponding value is an empty list, so we default to an empty dict.
    """
    climate = (grouped.get("climate") or [{}])[0]
    biogeo = (grouped.get("biogeo") or [{}])[0]
    return {**climate, **biogeo}


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


def parse_annotations(sample):
    sample_to_return = dict()
    sample_to_return["record_type"] = sample["record_type"]
    sample_to_return["accession"] = sample["accession"]
    info = sample["info"].split(";")
    for item in info:
        if item:
            try:
                k, v = item.split()
                if k in ["gene_id", "gene_version", "gene_source", "gene_biotype", "transcript_id",
                         "transcript_version", "transcript_source", "transcript_biotype", "tag", "exon_number",
                         "exon_id", "exon_version", "protein_id", "protein_version", "gene_name"]:
                    sample_to_return[k] = v.replace('"', '')
            except ValueError:
                pass
    return sample_to_return


def filter_new_accessions(element):
    accession, groups = element

    # If accession NOT in existing → process it
    if not groups["existing"]:
        for record in groups["candidates"]:
            yield record


# -----------------------------------
# Helpers for ENA and Ensembl pipelines
# -----------------------------------

ENSEMBL_REQUEST_TIMEOUT_SECONDS = 30
ENSEMBL_MAX_REQUEST_ATTEMPTS = 5
ENSEMBL_DEFAULT_RETRY_AFTER_SECONDS = 10
ENSEMBL_RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}

ENA_REQUEST_TIMEOUT_SECONDS = 30
ENA_MAX_REQUEST_ATTEMPTS = 5
ENA_DEFAULT_RETRY_AFTER_SECONDS = 10
ENA_RETRYABLE_STATUS_CODES = {408, 429, 500, 502, 503, 504}

ENA_TAG_TO_KEY = {
    'ungapped-length': 'ungapped_length',
    'n50': 'scaffold_n50',
    'scaffold-count': 'scaffold_count',
    'count-contig': 'contig_count',
    'contig-n50': 'contig_n50',
    'contig-L50': 'contig_l50',
    'contig-n75': 'contig_n75',
    'contig-n90': 'contig_n90',
    'scaf-L50': 'scaffold_l50',
    'scaf-n75': 'scaffold_n75',
    'scaf-n90': 'scaffold_n90',
    'spanned-gaps': 'spanned_gaps',
    'unspanned-gaps': 'unspanned_gaps',
    'replicon-count': 'replicon_count',
    'count-non-chromosome-replicon': 'non_chromosome_replicon_count',
}

ENA_INTEGER_METRIC_KEYS = {
    'ungapped_length',
    'scaffold_n50',
    'scaffold_count',
    'contig_n50',
    'contig_count',
    'spanned_gaps',
    'unspanned_gaps',
    'contig_l50',
    'scaffold_l50',
    'contig_n75',
    'contig_n90',
    'scaffold_n75',
    'scaffold_n90',
    'replicon_count',
    'non_chromosome_replicon_count',
}

ENA_ASSEMBLY_METRIC_KEYS = [
    'assembly_level',
    'ungapped_length',
    'scaffold_n50',
    'scaffold_count',
    'contig_n50',
    'contig_count',
    'coverage',
    'spanned_gaps',
    'unspanned_gaps',
    'contig_l50',
    'scaffold_l50',
    'contig_n75',
    'contig_n90',
    'scaffold_n75',
    'scaffold_n90',
    'replicon_count',
    'non_chromosome_replicon_count',
]


class EnsemblApiError(RuntimeError):
    """Raised when the Ensembl API returns an unexpected response."""


class EnaApiError(RuntimeError):
    """Raised when the ENA API returns an unexpected response."""


def _request_json(
    method: str,
    url: str,
    *,
    service_name: str,
    error_cls: type[RuntimeError],
    timeout_seconds: int,
    max_request_attempts: int,
    default_retry_after_seconds: int,
    retryable_status_codes: set[int],
    **kwargs: Any,
) -> dict[str, Any]:
    def retry_delay_seconds(
        attempt: int,
        response: requests.Response | None = None,
    ) -> float:
        if response is not None:
            retry_after = response.headers.get('Retry-After')
            if retry_after:
                return parse_retry_after(retry_after)

        return min(default_retry_after_seconds, attempt * 2)

    def parse_retry_after(retry_after: str) -> float:
        try:
            return max(float(retry_after), 0)
        except ValueError:
            try:
                parsed_date = parsedate_to_datetime(retry_after)
            except (TypeError, ValueError):
                return default_retry_after_seconds

            return max((parsed_date.timestamp() - time.time()), 0)

    last_response: requests.Response | None = None

    for attempt in range(1, max_request_attempts + 1):
        try:
            response = requests.request(
                method,
                url,
                timeout=timeout_seconds,
                **kwargs,
            )
        except requests.RequestException as exc:
            if attempt == max_request_attempts:
                raise error_cls(f'{service_name} request failed: {exc}') from exc

            time.sleep(retry_delay_seconds(attempt=attempt))
            continue

        last_response = response

        if response.status_code in retryable_status_codes:
            if attempt == max_request_attempts:
                break

            time.sleep(retry_delay_seconds(attempt=attempt, response=response))
            continue

        try:
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            raise error_cls(f'{service_name} request failed: {exc}') from exc
        except ValueError as exc:
            raise error_cls(f'{service_name} response was not valid JSON.') from exc

        if not isinstance(payload, dict):
            raise error_cls(f'{service_name} response JSON was not an object.')

        return payload

    raise error_cls(
        f'{service_name} request failed after {max_request_attempts} attempts with '
        f'HTTP {last_response.status_code}: {last_response.text}'
    )


def _request_ensembl_json(method: str, url: str, **kwargs: Any) -> dict[str, Any]:
    return _request_json(
        method,
        url,
        service_name='Ensembl',
        error_cls=EnsemblApiError,
        timeout_seconds=ENSEMBL_REQUEST_TIMEOUT_SECONDS,
        max_request_attempts=ENSEMBL_MAX_REQUEST_ATTEMPTS,
        default_retry_after_seconds=ENSEMBL_DEFAULT_RETRY_AFTER_SECONDS,
        retryable_status_codes=ENSEMBL_RETRYABLE_STATUS_CODES,
        **kwargs,
    )


def retrieve_genome_id(genome_accession: str) -> str:
    genome_id_graphql_query = f'''query{{
      genomes(
        by_keyword: {{
          assembly_accession_id:{json.dumps(genome_accession)}
        }}) 
      {{
        genome_id
      }}
    }}'''

    payload = _request_ensembl_json(
        'POST',
        'https://beta.ensembl.org/data/graphql',
        json={'query': genome_id_graphql_query},
    )

    if payload.get('errors'):
        raise EnsemblApiError(f'Ensembl GraphQL errors: {payload["errors"]}')

    try:
        data = payload['data']
        genomes = data['genomes']
    except (KeyError, TypeError) as exc:
        raise EnsemblApiError('Ensembl GraphQL response did not include data.genomes.') from exc

    if not isinstance(genomes, list):
        raise EnsemblApiError('Ensembl GraphQL data.genomes was not a list.')

    if not genomes:
        raise EnsemblApiError(f'No genome found for accession {genome_accession}.')

    if len(genomes) > 1:
        raise EnsemblApiError(
            f'Expected one uuid for accession {genome_accession}, found {len(genomes)}.'
        )

    genome = genomes[0]
    if not isinstance(genome, dict):
        raise EnsemblApiError(f'Genome response was not an object: {genome}')

    genome_id = genome.get('genome_id')
    if not genome_id:
        raise EnsemblApiError(f'Genome response did not include genome_id: {genome}')

    return genome_id


def retrieve_genome_stats(genome_accession: str) -> dict[str, Any]:
    genome_id = retrieve_genome_id(genome_accession)

    return retrieve_genome_stats_by_id(genome_id)


def retrieve_genome_stats_by_id(genome_id: str) -> dict[str, Any]:
    ensembl_stats_url = f'https://beta.ensembl.org/api/metadata/genome/{genome_id}/stats'

    return _request_ensembl_json('GET', ensembl_stats_url)


def _request_ena_json(method: str, url: str, **kwargs: Any) -> dict[str, Any]:
    return _request_json(
        method,
        url,
        service_name='ENA',
        error_cls=EnaApiError,
        timeout_seconds=ENA_REQUEST_TIMEOUT_SECONDS,
        max_request_attempts=ENA_MAX_REQUEST_ATTEMPTS,
        default_retry_after_seconds=ENA_DEFAULT_RETRY_AFTER_SECONDS,
        retryable_status_codes=ENA_RETRYABLE_STATUS_CODES,
        **kwargs,
    )


def fetch_ena_assembly(accession: str) -> dict[str, Any]:
    """Fetch a single ENA assembly summary record for an accession."""
    payload = _request_ena_json('GET', f'https://www.ebi.ac.uk/ena/browser/api/summary/{accession}')

    summaries = payload.get('summaries')
    if not isinstance(summaries, list):
        raise EnaApiError('ENA summary response did not include summaries list.')

    if not summaries:
        raise EnaApiError(f'No ENA summaries returned for accession {accession}.')

    record = summaries[0]
    if not isinstance(record, dict):
        raise EnaApiError(f'ENA summary record was not an object: {record}')

    accession_root = accession.split('.')[0]
    returned_accession = record.get('accession')
    if returned_accession not in (None, accession, accession_root):
        raise EnaApiError(
            f'ENA returned accession {returned_accession} for requested {accession}.'
        )

    return record


def retrieve_ena_assembly_stats(accession: str) -> dict[str, Any]:
    """Fetch ENA assembly metrics as a flat BigQuery-ready record."""
    def _coerce_to_integer(key: str, value: Any) -> int | str | None:
        if key not in ENA_INTEGER_METRIC_KEYS:
            return value

        if value in (None, ''):
            return None

        try:
            return int(value)
        except (TypeError, ValueError) as exc:
            raise EnaApiError(
                f'Could not parse ENA metric {key}={value!r} as integer.'
            ) from exc

    def _coerce_to_float(key: str, value: Any) -> float | None:
        if value in (None, ''):
            return None

        try:
            return float(value)
        except (TypeError, ValueError) as exc:
            raise EnaApiError(
                f'Could not parse ENA metric {key}={value!r} as float.'
            ) from exc

    record = fetch_ena_assembly(accession)
    metrics = {key: None for key in ENA_ASSEMBLY_METRIC_KEYS}

    assembly_level = record.get('assemblyLevel')
    if isinstance(assembly_level, str):
        metrics['assembly_level'] = assembly_level.strip().lower()
    elif assembly_level is not None:
        metrics['assembly_level'] = assembly_level

    coverage = record.get('assemblyCoverage')
    metrics['coverage'] = _coerce_to_float('coverage', coverage)

    attributes = record.get('attributes') or []
    if not isinstance(attributes, list):
        raise EnaApiError('ENA summary attributes was not a list.')

    for attribute in attributes:
        if not isinstance(attribute, dict):
            raise EnaApiError(f'ENA summary attribute was not an object: {attribute}')

        tag = attribute.get('tag')
        key = ENA_TAG_TO_KEY.get(tag)
        if key is None:
            continue

        metrics[key] = _coerce_to_integer(key, attribute.get('value'))

    return {
        'accession': accession,
        **metrics,
    }