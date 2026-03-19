import logging
import tempfile
import geopandas as gpd
import json
import os
import numpy as np
import pyproj
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
from typing import Any, Dict, Iterable, List, Optional
from dependencies.utils.helpers import sanitize_species_name, fetch_spatial_file_to_local, extract_species_name

logger = logging.getLogger(__name__)


class FetchESFn(DoFn):
    def __init__(self, host, user, password, index, page_size, max_pages, *unused_args, **unused_kwargs):
        super().__init__(*unused_args, **unused_kwargs)
        self.host = host
        self.user = user
        self.password = password
        self.index = index
        self.page_size = page_size
        self.max_pages = max_pages
        self.emitted = Metrics.counter(self.__class__, "es_records_emitted")
        self.pages_fetched = Metrics.counter(self.__class__, "es_pages_fetched")

    def setup(self):
        self.es = Elasticsearch(
            hosts=self.host,
            basic_auth=(self.user, self.password),
            request_timeout=30,
            retry_on_timeout=True,
            max_retries=3,
        )

    def process(self, element):
        after = None
        page_i = 0

        max_pages = self.max_pages if self.max_pages is not None else 0  # None => fetch all

        def should_continue(i: int) -> bool:
            return (max_pages <= 0) or (i < max_pages)

        while should_continue(page_i):
            search_kwargs = {
                "index": self.index,
                "size": self.page_size,
                "sort": [{"tax_id": "asc"}],
                "query": {"term": {"annotation_complete": "Done"}},
                "_source": ["tax_id", "annotation.accession", "annotation.species"],
            }

            if after is not None:
                search_kwargs["search_after"] = after

            response = self.es.search(**search_kwargs)

            hits = response.get("hits", {}).get("hits", [])
            if not hits:
                break

            self.pages_fetched.inc()

            for hit in hits:
                src = hit.get("_source", {})
                ann_list = src.get("annotation") or []

                if not ann_list:
                    raise ValueError(
                        f"Elasticsearch document with tax_id={src.get('tax_id')} "
                        "has empty or missing 'annotation' array while "
                        "annotation_complete='Done'. This violates index invariants."
                    )

                # Retrieving the latest annotation.
                ann = ann_list[-1]
                accession = ann.get("accession")
                species = ann.get("species")
                tax_id = src.get("tax_id")

                self.emitted.inc()
                yield {"accession": accession, "species": species, "tax_id": tax_id}

            last_sort = hits[-1].get("sort")

            if last_sort is None:
                raise RuntimeError(
                    "Elasticsearch response missing 'sort' values for search_after pagination. "
                    "Check the query 'sort' clause and ES mapping."
                )

            after = last_sort
            page_i += 1


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
    """
    Generate a buffer polygon around each occurrence point based on
    coordinate uncertainty, and store it as WKT in `uncertainty_geom_wkt`.
    Uses local AEQD projection to compute accurate metric buffers around points.

    Records with missing or invalid coordinates / uncertainty are skipped.
    """

    def __init__(self, min_radius_m=100.0):
        self.min_radius_m = min_radius_m
        self.missing_fields = Metrics.counter(self.__class__, "missing_fields")
        self.invalid_numeric = Metrics.counter(self.__class__, "invalid_numeric")
        self.generated_buffers = Metrics.counter(self.__class__, "generated_buffers")

    def process(self, record):
        lat = record.get("decimalLatitude")
        lon = record.get("decimalLongitude")
        radius = record.get("coordinateUncertaintyInMeters")

        record_id = record.get("gbifID") or record.get("occurrenceID") or record.get("accession")

        if lat is None or lon is None or radius in (None, "", "NaN"):
            self.missing_fields.inc()
            logger.warning(
                "Skipping record with missing coordinates or uncertainty. record_id=%s",
                record_id,
            )
            return

        try:
            lat = float(lat)
            lon = float(lon)
            radius = max(float(radius), self.min_radius_m)
        except (TypeError, ValueError):
            self.invalid_numeric.inc()
            logger.warning(
                "Skipping record with invalid numeric coordinate/uncertainty values. "
                "record_id=%s lat=%r lon=%r radius=%r",
                record_id,
                lat,
                lon,
                radius,
            )
            return

        # ---------------------------------------------------------------------
        # Projection strategy:
        #
        # We cannot buffer directly in WGS84 (lat/lon) because:
        # - units are in degrees, not meters
        # - distances are not uniform across the globe
        #
        # Instead:
        # 1. Reproject the point to a local Azimuthal Equidistant projection (AEQD)
        #    centered on the point itself.
        #    → This preserves distances from the center point.
        #
        # 2. Perform the buffer in meters in this projected space.
        #
        # 3. Reproject the buffered geometry back to WGS84.
        # ---------------------------------------------------------------------

        # Define local Azimuthal Equidistant projection centered at the point
        aeqd_proj = pyproj.Proj(proj="aeqd", lat_0=lat, lon_0=lon, datum="WGS84")

        # Define WGS84 geographic CRS
        wgs84_proj = pyproj.Proj(proj="latlong", datum="WGS84")

        # Transformer: WGS84 → AEQD (for metric buffering)
        project_to_aeqd = pyproj.Transformer.from_proj(
            wgs84_proj, aeqd_proj, always_xy=True
        ).transform

        # Transformer: AEQD → WGS84 (back to geographic coordinates)
        project_to_wgs84 = pyproj.Transformer.from_proj(
            aeqd_proj, wgs84_proj, always_xy=True
        ).transform

        # Create point in WGS84 (lon, lat order!)
        point_wgs84 = Point(lon, lat)

        # Project point to AEQD (meters)
        point_aeqd = transform(project_to_aeqd, point_wgs84)

        # Project point to AEQD (meters)
        buffer_aeqd = point_aeqd.buffer(radius)

        # Reproject buffer back to WGS84
        buffer_wgs84 = transform(project_to_wgs84, buffer_aeqd)

        updated = record.copy()
        updated["uncertainty_geom_wkt"] = buffer_wgs84.wkt

        self.generated_buffers.inc()
        yield updated


class AnnotateWithCHELSAFn(DoFn):
    """
    Annotate occurrence records with CHELSA climate variables derived from the
    uncertainty area around each occurrence.

    For each record, this transform:
      1. Reads the uncertainty polygon from `uncertainty_geom_wkt`
      2. Masks each CHELSA raster to that polygon
      3. Computes the mean raster value within the polygon
      4. Applies CHELSA unit conversions where needed

    Notes
    -----
    - Climate summaries are computed over the area of uncertainty, not at a
      single point location.
    - The input climate directory can be local or gs:// because files are
      discovered and copied with Beam FileSystems.
    """

    def __init__(self, climate_dir, output_key="clim_dataset"):
        self.output_key = output_key
        self.gcs_climate_dir = climate_dir
        self.layers = {}

        self.temp_vars = {
            "bio1", "bio5", "bio6", "bio8", "bio9", "bio10", "bio11"
        }
        self.precip_vars = {
            "bio12", "bio13", "bio14", "bio16", "bio17", "bio18", "bio19"
        }
        self.raw_vars = {
            "bio2", "bio3", "bio4", "bio7", "bio15"
        }

        self.records_missing_geom = Metrics.counter(
            self.__class__, "records_missing_uncertainty_geom"
        )
        self.records_invalid_geom = Metrics.counter(
            self.__class__, "records_invalid_uncertainty_geom"
        )
        self.raster_mask_failures = Metrics.counter(
            self.__class__, "raster_mask_failures"
        )
        self.records_annotated = Metrics.counter(
            self.__class__, "records_annotated"
        )

    def setup(self):
        # Create a temporary local directory on the worker and copy raster files
        # there once. This avoids reopening files from remote storage per record.
        self.temp_dir = tempfile.mkdtemp()
        self.climate_dir = self.temp_dir

        # Match all climate rasters from the provided directory (local or gs://).
        pattern = os.path.join(self.gcs_climate_dir.rstrip("/"), "*.tif")
        match_result = FileSystems.match([pattern])[0]
        metadata_list = match_result.metadata_list

        # Copy each raster to the worker-local temp directory.
        for metadata in metadata_list:
            src_path = metadata.path
            filename = os.path.basename(src_path)
            local_path = os.path.join(self.temp_dir, filename)

            with FileSystems.open(src_path) as src_file:
                with open(local_path, "wb") as dst_file:
                    dst_file.write(src_file.read())

        # Open raster datasets once per worker.
        # These stay available during processing and are released in teardown().
        for file_name in os.listdir(self.climate_dir):
            if not file_name.endswith(".tif"):
                continue

            var_name = file_name.split("_")[1]
            path = os.path.join(self.climate_dir, file_name)
            dataset = rasterio.open(path)

            if dataset.crs.to_string() != "EPSG:4326":
                dataset.close()
                raise ValueError(f"{file_name} must use EPSG:4326 CRS")

            self.layers[var_name] = dataset

    def process(self, record):
        record_id = (
                record.get("gbifID")
                or record.get("occurrenceID")
                or record.get("accession")
        )

        if "uncertainty_geom_wkt" not in record:
            self.records_missing_geom.inc()
            logger.warning(
                "Skipping climate annotation: missing uncertainty_geom_wkt. record_id=%s",
                record_id,
            )
            return

        try:
            polygon = wkt.loads(record["uncertainty_geom_wkt"])
            geojson_geom = [polygon.__geo_interface__]
        except Exception as exc:
            self.records_invalid_geom.inc()
            logger.warning(
                "Skipping climate annotation: invalid uncertainty geometry. "
                "record_id=%s error=%s",
                record_id,
                exc,
            )
            return

        climate_values = {}

        for var, dataset in self.layers.items():
            try:
                # Mask the raster to the uncertainty polygon and crop to the
                # intersecting window. Use filled=False so rasterio returns a
                # masked array instead of inserting fill values into excluded cells.
                # This avoids incorrectly dropping valid CHELSA values such as 0.
                clipped, _ = mask(dataset, geojson_geom, crop=True, filled=False)
                arr = clipped[0]

                # Keep only unmasked cells from the clipped raster.
                arr = arr.compressed()

                # Drop NaNs if any remain after compression.
                arr = arr[~np.isnan(arr)]

                if arr.size == 0:
                    climate_values[var] = None
                    continue

                mean_val = np.mean(arr)

                # CHELSA variables use different encodings by variable type.
                # Apply the appropriate conversion back to interpretable units.
                if var in self.temp_vars:
                    climate_values[var] = round(mean_val * 0.1 - 273.15, 2)
                elif var in self.precip_vars:
                    climate_values[var] = round(mean_val * 0.1)
                elif var in self.raw_vars:
                    climate_values[var] = round(float(mean_val), 2)
                else:
                    climate_values[var] = round(float(mean_val), 2)

            except Exception as exc:
                self.raster_mask_failures.inc()
                logger.warning(
                    "Failed climate masking for variable=%s record_id=%s error=%s",
                    var,
                    record_id,
                    exc,
                )
                climate_values[var] = None

        output = {
            "accession": record.get("accession"),
            "tax_id": record.get("tax_id"),
            "species": record.get("species"),
            "decimalLatitude": record.get("decimalLatitude"),
            "decimalLongitude": record.get("decimalLongitude"),
            self.output_key: climate_values,
            "occurrenceID": record.get("occurrenceID"),
        }

        self.records_annotated.inc()
        yield output

    def teardown(self):
        # Close open raster datasets explicitly to release GDAL/file handles.
        for var, dataset in self.layers.items():
            try:
                dataset.close()
            except Exception as exc:
                logger.warning(
                    "Failed to close raster dataset for variable=%s error=%s",
                    var,
                    exc,
                )

        self.layers.clear()


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


class FetchProvenanceByTaxIdBatchFn(DoFn):
    """
    Taxonomy-driven provenance fetch from ES.

    Input: List[{"tax_id", "accession", "gbif_usageKey"}]
    Output: dict rows with:
      tax_id, accession, Biodiversity_portal, GTF, Ensembl_browser, gbif_url
    Behavior:
      - Query ES using terms query on tax_id (batched).
      - Use latest annotation: annotation[-1]
      - Drop tax_ids not returned by ES (emit only for hits).
    """

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        index: str,
    ):
        self.host = host
        self.user = user
        self.password = password
        self.index = index

        self.es_batches = Metrics.counter(self.__class__, "es_batches")
        self.es_hits = Metrics.counter(self.__class__, "es_hits")
        self.es_empty_hits = Metrics.counter(self.__class__, "es_empty_hits")
        self.bad_tax_id = Metrics.counter(self.__class__, "bad_tax_id")
        self.missing_annotation = Metrics.counter(self.__class__, "missing_annotation")

    @staticmethod
    def _normalize_tax_id_str(tax_id: Any) -> Optional[str]:
        if tax_id is None:
            return None
        return str(tax_id)

    def setup(self):
        self.es = Elasticsearch(
            hosts=self.host,
            basic_auth=(self.user, self.password),
            request_timeout=30,
            retry_on_timeout=True,
            max_retries=3,
        )

    def process(self, batch: List[Dict[str, Any]]) -> Iterable[Dict[str, Any]]:
        if not batch:
            return

        # Map taxonomy request by normalized tax_id for merge
        req_by_tax_id: Dict[str, Dict[str, Any]] = {}
        tax_ids: List[Any] = []

        for req in batch:
            tax_id_raw = req.get("tax_id")
            tax_id_norm = self._normalize_tax_id_str(tax_id_raw)
            if not tax_id_norm:
                self.bad_tax_id.inc()
                continue
            req_by_tax_id[tax_id_norm] = req
            tax_ids.append(tax_id_raw)

        if not tax_ids:
            return

        es_query = {"terms": {"tax_id": tax_ids}}
        source_fields = [
            "tax_id",
            "annotation.accession",
            "annotation.annotation.GTF",
            "annotation.view_in_browser",
        ]

        self.es_batches.inc()
        resp = self.es.search(
            index=self.index,
            query=es_query,
            size=len(tax_ids),
            source=source_fields,
        )

        hits = resp.get("hits", {}).get("hits", []) or []
        if not hits:
            self.es_empty_hits.inc()
            return

        for hit in hits:
            self.es_hits.inc()
            src = hit.get("_source", {}) or {}
            tax_id_norm = self._normalize_tax_id_str(src.get("tax_id"))
            if not tax_id_norm:
                self.bad_tax_id.inc()
                continue

            req = req_by_tax_id.get(tax_id_norm)
            if req is None:
                # Defensive: should not happen if ES indexed correctly
                continue

            ann_list = src.get("annotation") or []
            if not ann_list:
                self.missing_annotation.inc()
                continue

            latest = ann_list[-1] or {}
            latest_ann = latest.get("annotation") or {}

            accession = req.get("accession") or latest.get("accession")
            gbif_key = req.get("gbif_usageKey")

            yield {
                "tax_id": str(req.get("tax_id")),
                "accession": accession,
                "Biodiversity_portal": f"https://www.ebi.ac.uk/biodiversity/data_portal/{tax_id_norm}",
                "GTF": latest_ann.get("GTF"),
                "Ensembl_browser": latest.get("view_in_browser"),
                "gbif_url": f"https://www.gbif.org/species/{gbif_key}" if gbif_key else None,
            }
