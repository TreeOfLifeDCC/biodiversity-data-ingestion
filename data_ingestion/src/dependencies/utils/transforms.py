import tempfile

import geopandas as gpd
import json
import os
import numpy as np
import pyproj
import sys
import rasterio
import time

from elasticsearch import Elasticsearch
from apache_beam import DoFn, pvalue
from apache_beam.metrics import Metrics
from apache_beam.io.filesystems import FileSystems
from collections import defaultdict
from pygbif import species as gbif_spp, occurrences as gbif_occ
from shapely import wkt
from shapely.geometry import Point, MultiPoint
from shapely.ops import transform
from rasterio.mask import mask
from dependencies.utils.helpers import sanitize_species_name, fetch_spatial_file_to_local, extract_species_name


class FetchESFn(DoFn):
    def __init__(self, host, user, password, index, page_size, max_pages, *unused_args, **unused_kwargs):
        super().__init__(*unused_args, **unused_kwargs)
        self.host = host
        self.user = user
        self.password = password
        self.index = index
        self.page_size = page_size
        self.max_pages = max_pages

    def setup(self):
        self.es = Elasticsearch(
            hosts=self.host,
            basic_auth=(self.user, self.password)
        )

    def process(self, element):
        after = None
        for _ in range(self.max_pages):
            query = {
                'size': self.page_size,
                'sort': {'tax_id': 'asc'},
                'query': {'match': {'annotation_complete': 'Done'}}
            }
            if after:
                query['search_after'] = after

            response = self.es.search(index=self.index, body=query)
            hits = response.get('hits', {}).get('hits', [])
            if not hits:
                break

            for hit in hits:
                ann = hit['_source']['annotation'][-1]
                yield {
                    'accession': ann['accession'],
                    'species': ann['species'],
                    'tax_id': hit['_source']['tax_id']
                }

            after = hits[-1].get('sort')


class ENATaxonomyFn(DoFn):
    """
    Fetches taxonomy info from ENA by tax_id.
    Includes retry with backoff and optional sleep for throttling.
    """

    def __init__(self, include_lineage=True, sleep_seconds=0.25, max_retries=3):
        self.include_lineage = include_lineage
        self.sleep_seconds = sleep_seconds
        self.timeout = 10
        self.max_retries = max_retries

    def setup(self):
        import requests
        from lxml import etree
        self.requests = requests
        self.etree = etree

    def get_with_retries(self, url):
        for i in range(self.max_retries):
            try:
                resp = self.requests.get(url, timeout=self.timeout)
                resp.raise_for_status()
                return resp
            except Exception as e:
                if i < self.max_retries - 1:
                    time.sleep(1.5 ** i)
                else:
                    raise e

    def process(self, record):
        tax_id = record.get("tax_id")
        if not tax_id:
            record["ena_error"] = "Missing tax_id"
            yield record
            return

        if self.sleep_seconds:
            time.sleep(self.sleep_seconds)

        url = f"https://www.ebi.ac.uk/ena/browser/api/xml/{tax_id}"

        try:
            resp = self.get_with_retries(url)
            root = self.etree.fromstring(resp.content)
        except Exception as e:
            record["ena_error"] = f"Retry failed: {str(e)}"
            yield record
            return

        try:
            record["scientificName"] = root.find("taxon").get("scientificName")  # TODO: Use this as the new source for species name.
        except Exception:
            record["ena_error"] = "Missing scientificName"
            yield record
            return

        if self.include_lineage:
            lineage_fields = ["kingdom", "phylum", "class", "order", "family", "genus"]
            for f in lineage_fields:
                record[f] = None
            try:
                for taxon in root.find("taxon").find("lineage").findall("taxon"):
                    rank = taxon.get("rank")
                    if rank in lineage_fields:
                        record[rank] = taxon.get("scientificName")
            except Exception:
                record["ena_warning"] = "Lineage parsing failed"

        yield record


class ValidateNamesFn(DoFn):
    VALIDATED = 'validated'
    TO_CHECK = 'to_check'

    def process(self, record):
        name = record.get('scientificName') # Name from ENA
        if not name:
            yield pvalue.TaggedOutput(self.TO_CHECK, record)
            return

        gb = gbif_spp.name_backbone(name=name, rank='species', strict=False, verbose=False)
        record.update({
            'gbif_matchType': gb.get('matchType'),
            'gbif_confidence': gb.get('confidence'),
            'gbif_scientificName': gb.get('scientificName'),
            'gbif_usageKey': gb.get('usageKey'),
            'gbif_status': gb.get('status'),
            'gbif_rank': gb.get('rank')
        })

        mt = gb.get('matchType')
        confidence = gb.get('confidence', 0)
        if mt == 'NONE' or (mt == 'FUZZY' and confidence < 95) or mt == 'HIGHERRANK':
            if gb.get('acceptedUsageKey'):
                record['gbif_acceptedUsageKey'] = gb.get('acceptedUsageKey')
            if gb.get('alternatives'):
                record['gbif_alternatives'] = gb.get('alternatives')
            yield pvalue.TaggedOutput(self.TO_CHECK, record)
        else:
            yield record


class WriteSpeciesOccurrencesFn(DoFn):
    """
    For each validated species record, fetches GBIF occurrences and writes to one file per species.
    Tracks success, skipped, and failed records using Beam metrics.
    """

    def __init__(self, output_dir, max_records=150):
        self.output_dir = output_dir
        self.max_records = max_records

    def setup(self):
        self.gbif_client = gbif_occ

    def process(self, record):
        species = record.get('scientificName')  # Name from ENA
        usage_key = record.get('gbif_usageKey')
        safe_name = sanitize_species_name(species)
        filename = f"occ_{safe_name}.jsonl"
        out_path = f"{self.output_dir}/{filename}"
        tmp_path = out_path + ".tmp"

        try:
            resp = self.gbif_client.search(
                taxonKey=usage_key,
                basisOfRecord=['PRESERVED_SPECIMEN', 'MATERIAL_SAMPLE'],
                occurrenceStatus='PRESENT',
                hasCoordinate=True,
                hasGeospatialIssue=False,
                limit=self.max_records
            )

            occurrences = resp.get('results', [])
            lines = []

            for occ in occurrences:
                occ_out = {
                    'accession': record.get('accession'),
                    'tax_id': record.get('tax_id'),
                    'species': record.get('scientificName'),  # Name from ENA
                    'gbif_usageKey': occ.get('taxonKey'),
                    'gbif_species': occ.get('species'),  # Name in GBIF.
                    'decimalLatitude': occ.get('decimalLatitude'),
                    'decimalLongitude': occ.get('decimalLongitude'),
                    'coordinateUncertaintyInMeters': occ.get('coordinateUncertaintyInMeters'),
                    'geodeticDatum': occ.get('geodeticDatum'),
                    'elevation': occ.get('elevation'),
                    'eventDate': occ.get('eventDate'),
                    'countryCode': occ.get('countryCode'),
                    'gadm': occ.get('gadm'),
                    'basisOfRecord': occ.get('basisOfRecord'),
                    'occurrenceID': occ.get('occurrenceID'),
                    'gbifID': occ.get('gbifID'),
                    'institutionCode': occ.get('institutionCode'),
                    'collectionCode': occ.get('collectionCode'),
                    'catalogNumber': occ.get('catalogNumber'),
                    'iucnRedListCategory': occ.get('iucnRedListCategory')
                }
                lines.append(json.dumps(occ_out))

            with FileSystems.create(tmp_path) as f:
                for line in lines:
                    f.write((line + '\n').encode('utf-8'))
            FileSystems.rename([tmp_path], [out_path])

        except Exception as e:
            yield pvalue.TaggedOutput('dead', {
                'species': species,
                'error': str(e)
            })


class GenerateUncertaintyAreaFn(DoFn):
    def process(self, record):
        lat = record.get("decimalLatitude")
        lon = record.get("decimalLongitude")
        radius = record.get("coordinateUncertaintyInMeters")

        if lat is None or lon is None or radius in (None, "", "NaN"):
            print(f"[WARN] Skipping record with missing coordinates or uncertainty: {record}", file=sys.stderr)
            return

        try:
            lat = float(lat)
            lon = float(lon)
            radius = max(float(radius), 100.0)  # Clamp to minimum 100 to avoid point geometries and empty masks.
        except (TypeError, ValueError):
            print(f"[WARN] Invalid numeric values in record: {record}", file=sys.stderr)
            return

        # Define AEQD projection centered on the point
        aeqd_proj = pyproj.Proj(proj='aeqd', lat_0=lat, lon_0=lon, datum='WGS84')
        wgs84_proj = pyproj.Proj(proj='latlong', datum='WGS84')

        project_to_aeqd = pyproj.Transformer.from_proj(wgs84_proj, aeqd_proj, always_xy=True).transform
        project_to_wgs84 = pyproj.Transformer.from_proj(aeqd_proj, wgs84_proj, always_xy=True).transform

        point_wgs84 = Point(lon, lat)
        point_aeqd = transform(project_to_aeqd, point_wgs84)
        buffer_aeqd = point_aeqd.buffer(radius)
        buffer_wgs84 = transform(project_to_wgs84, buffer_aeqd)

        updated = record.copy()
        updated["uncertainty_geom_wkt"] = buffer_wgs84.wkt
        yield updated


class AnnotateWithCHELSAFn(DoFn):
    def __init__(self, climate_dir, output_key="clim_dataset"):
        self.output_key = output_key
        self.gcs_climate_dir = climate_dir

        self.temp_vars = {
            'bio1', 'bio5', 'bio6', 'bio8', 'bio9', 'bio10', 'bio11'
        }
        self.precip_vars = {
            'bio12', 'bio13', 'bio14', 'bio16', 'bio17', 'bio18', 'bio19'
        }
        self.raw_vars = {
            'bio2', 'bio3', 'bio4', 'bio7', 'bio15'
        }

    def setup(self):
        # Create a temporary directory on the worker
        self.temp_dir = tempfile.mkdtemp()
        self.climate_dir = self.temp_dir

        # Match all climate files in GCS
        gcs_pattern = os.path.join(self.gcs_climate_dir.rstrip('/'), '*.tif')
        match_result = FileSystems.match([gcs_pattern])[0]
        metadata_list = match_result.metadata_list

        # Download each file to the temp directory
        for metadata in metadata_list:
            gcs_file_path = metadata.path
            filename = os.path.basename(gcs_file_path)

            local_path = os.path.join(self.temp_dir, filename)

            with FileSystems.open(metadata.path) as gcs_file:
                with open(local_path, 'wb') as local_file:
                    local_file.write(gcs_file.read())

        # Checking CRS
        self.layers = {}
        for file in os.listdir(self.climate_dir):
            if file.endswith(".tif"):
                var_name = file.split("_")[1]
                path = os.path.join(self.climate_dir, file)
                dataset = rasterio.open(path)
                if dataset.crs.to_string() != "EPSG:4326":
                    raise ValueError(f"{file} must use EPSG:4326 CRS")
                self.layers[var_name] = dataset

    def process(self, record):
        if "uncertainty_geom_wkt" not in record:
            return

        try:
            polygon = wkt.loads(record["uncertainty_geom_wkt"])
            geojson_geom = [json.loads(json.dumps(polygon.__geo_interface__))]
        except Exception:
            return

        climate_values = {}

        for var, dataset in self.layers.items():
            try:
                clipped, _ = mask(dataset, geojson_geom, crop=True, filled=True)
                arr = clipped[0]

                if dataset.nodata is not None:
                    arr = arr[arr != dataset.nodata]

                arr = arr[~np.isin(arr, [65535, 0, -32768])]
                arr = arr[~np.isnan(arr)]

                if arr.size > 0:
                    mean_val = np.mean(arr)

                    if var in self.temp_vars:
                        climate_values[var] = round(mean_val * 0.1 - 273.15, 2)
                    elif var in self.precip_vars:
                        climate_values[var] = round(mean_val * 0.1)
                    elif var in self.raw_vars:
                        climate_values[var] = round(float(mean_val), 2)
                    else:
                        climate_values[var] = round(float(mean_val), 2)
                else:
                    climate_values[var] = None

            except Exception:
                climate_values[var] = None

        output = {
            "accession": record.get("accession"),
            "tax_id": record.get("tax_id"),
            "species": record.get("species"),
            "decimalLatitude": record.get("decimalLatitude"),
            "decimalLongitude": record.get("decimalLongitude"),
            self.output_key: climate_values,
            "occurrenceID": record.get("occurrenceID")
        }

        yield output


class ClimateSummaryFn(DoFn):
    def __init__(self, input_key="clim_CHELSA"):
        self.input_key = input_key

    def process(self, element):
        accession, records = element
        values = defaultdict(list)

        # Extracting metadata to include in summary
        metadata = records[0]
        species = metadata.get("species")
        tax_id = metadata.get("tax_id")

        for r in records:
            clim = r.get(self.input_key, {})
            for var, val in clim.items():
                if isinstance(val, (int, float)):
                    values[var].append(val)

        summary = {
            "accession": accession,
            "species": species,
            "tax_id": tax_id,
            self.input_key: {}
        }

        for var, vals in values.items():
            arr = np.array(vals)
            summary[self.input_key][var] = {
                "mean": round(np.mean(arr), 2),
                "sd": round(np.std(arr), 2),
                "median": round(np.median(arr), 2),
                "p5": round(np.percentile(arr, 5), 2),
                "p95": round(np.percentile(arr, 95), 2),
                "min": round(np.min(arr), 2),
                "max": round(np.max(arr), 2)
            }

        def convert_numpy(obj):
            if isinstance(obj, dict):
                return {k: convert_numpy(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_numpy(v) for v in obj]
            elif isinstance(obj, (np.generic,)):
                return obj.item()
            else:
                return obj

        output = (accession, convert_numpy(summary))

        yield output


class AnnotateWithBiogeoFn(DoFn):
    def __init__(self, vector_path, keep_fields, output_key="biogeo_area"):
        """
        Args:
            vector_path (str): Path to vector data file (e.g., Ecoregions shapefile or .zip)
            keep_fields (dict): Mapping of output keys to field names in the vector layer.
                                Example: {"realm": "REALM", "biome": "BIOME_NAME"}
            output_key (str): Output dictionary name in the result record.
        """
        self.vector_path = vector_path
        self.keep_fields = keep_fields
        self.output_key = output_key

    def setup(self):
        local_path = fetch_spatial_file_to_local(self.vector_path, "/tmp/biogeo_vector/")
        self.gdf = gpd.read_file(local_path)

        if self.gdf.crs.to_string() != "EPSG:4326":
            raise ValueError(f"Vector file must use EPSG:4326 CRS, got {self.gdf.crs}")

        fields = list(self.keep_fields.values()) + ["geometry"]
        self.gdf = self.gdf[fields]
        self.sindex = self.gdf.sindex

    def process(self, record):
        if "uncertainty_geom_wkt" not in record:
            return

        try:
            geom = wkt.loads(record["uncertainty_geom_wkt"])
        except Exception:
            return

        candidates = list(self.sindex.query(geom, predicate="intersects"))
        matches = self.gdf.iloc[candidates]
        matches = matches[matches.intersects(geom)]

        if matches.empty:
            return

        output = {
            "accession": record.get("accession"),
            "tax_id": record.get("tax_id"),
            "species": record.get("species"),
            "decimalLatitude": record.get("decimalLatitude"),
            "decimalLongitude": record.get("decimalLongitude"),
            self.output_key: {},
            "occurrenceID": record.get("occurrenceID")
        }

        for out_key, vector_col in self.keep_fields.items():
            unique_vals = sorted(set(matches[vector_col].dropna().astype(str)))
            output[self.output_key][out_key] = unique_vals

        yield output


class BiogeoSummaryNestedFn(DoFn):
    def __init__(self, output_key="biogeo_Ecoregion"):
        self.output_key = output_key

    def process(self, element):
        # species, records = element
        accession, records = element
        region_sets = defaultdict(set)

        # Extracting metadata to include in summary
        metadata = records[0]
        species = metadata.get("species")
        tax_id = metadata.get("tax_id")

        for r in records:
            region_data = r.get(self.output_key, {})
            for field, values in region_data.items():
                if isinstance(values, list):
                    region_sets[field].update(values)

        nested = {
            field: {
                "count": len(vals),
                "values": sorted(list(vals))
            }
            for field, vals in region_sets.items()
        }

        summary = {
            "accession": accession,
            "species": species,
            "tax_id": tax_id,
            "biogeo_Ecoregion": nested
        }

        output = (accession, summary)

        yield output


class EstimateRangeFn(DoFn):
    def process(self, element):
        file_path = element.metadata.path
        species = extract_species_name(file_path)

        with FileSystems.open(file_path) as f:
            content = f.read().decode("utf-8")
        lines = content.splitlines()

        accession = None
        coords = []
        for line in lines:
            try:
                record = json.loads(line)

                if accession is None:  # Read accession only for first record.
                    accession = record["accession"]

                lat = record.get("decimalLatitude")
                lon = record.get("decimalLongitude")

                if lat is not None and lon is not None:
                    coords.append(Point(lon, lat))
            except Exception:
                continue

        if len(coords) < 3:
            return [{
                "accession": accession,
                "species": species,
                "range_km2": None,
                "note": "Insufficient points for convex hull"
            }]

        multipoint = MultiPoint(coords)
        hull = multipoint.convex_hull

        project = pyproj.Transformer.from_crs("EPSG:4326", "EPSG:6933", always_xy=True).transform
        hull_proj = transform(project, hull)
        area_km2 = hull_proj.area / 1e6

        return [{
            "accession": accession,
            "species": species,
            "range_km2": round(area_km2, 2)
        }]


class FetchProvenanceMetadataFn(DoFn):
    """
    Apache Beam DoFn to extract data provenance metadata from ElasticSearch.
    Emits a record for each annotation including:
      - accession
      - Biodiversity portal URL
      - GTF URL (if available)
      - Ensembl browser URL
    """

    def __init__(self, host, user, password, index, page_size=100, max_pages=10):
        self.host = host
        self.user = user
        self.password = password
        self.index = index
        self.page_size = page_size
        self.max_pages = max_pages

    def setup(self):
        self.es = Elasticsearch(
            hosts=self.host,
            basic_auth=(self.user, self.password)
        )
        self.seen_accessions = set()  # Deduplication tracker (per worker)

    def process(self, unused_element):
        after = None
        for _ in range(self.max_pages):
            query = {
                'size': self.page_size,
                'sort': {'tax_id': 'asc'},
                'query': {'match': {'annotation_complete': 'Done'}}
            }

            if after:
                query['search_after'] = after

            try:
                response = self.es.search(index=self.index, body=query)
            except Exception as e:
                yield {'error': f'Failed to query Elasticsearch: {str(e)}'}
                break

            hits = response.get('hits', {}).get('hits', [])
            if not hits:
                break

            for record in hits:
                tax_id = record['_source'].get('tax_id')
                annotations = record['_source'].get('annotation', [])

                for annotation in annotations:
                    accession = annotation.get("accession")
                    if accession in self.seen_accessions:
                        continue
                    self.seen_accessions.add(accession)

                    yield {
                        "accession": annotation.get("accession"),
                        "Biodiversity_portal": f"https://www.ebi.ac.uk/biodiversity/data_portal/{tax_id}",
                        "GTF": annotation.get("annotation", {}).get("GTF", "no_gtf"),
                        "Ensembl_browser": annotation.get("view_in_browser")
                    }

            after = hits[-1].get('sort')
