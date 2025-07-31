import json
import os
import re
from apache_beam.io.filesystems import FileSystems
from apache_beam.io.gcp.internal.clients import bigquery as bq

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
    Merges climate and biogeo annorations keyed by occurrenceID into a single pcollection.
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
    Downloads all files associated with a shapefile (e.g. .shp, .shx, .dbf) from GCS or local FS into a temp directory.
    Returns the local path to the .shp file.
    """
    base_dir = shapefile_path.rsplit("/", 1)[0]
    shp_name = shapefile_path.split("/")[-1]

    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    match_result = FileSystems.match([f"{base_dir}/*"])[0]
    for metadata in match_result.metadata_list:
        fname = os.path.basename(metadata.path)
        dest_path = os.path.join(local_dir, fname)
        with FileSystems.open(metadata.path) as fsrc, open(dest_path, "wb") as fdst:
            fdst.write(fsrc.read())

    return os.path.join(local_dir, shp_name)


def merge_gbif_url(kv):
    accession, groups = kv
    provenance_records = groups.get("provenance", [])
    taxonomy_records = groups.get("taxonomy", [])

    if not provenance_records:
        return

    gbif_key = None
    if taxonomy_records:
        gbif_key = taxonomy_records[0].get("gbif_usageKey")

    gbif_url = f"https://www.gbif.org/species/{gbif_key}" if gbif_key else None

    for record in provenance_records:
        record["gbif_url"] = gbif_url
        yield record