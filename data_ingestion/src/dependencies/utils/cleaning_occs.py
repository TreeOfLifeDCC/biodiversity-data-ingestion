import math

import geopandas as gpd
from shapely.geometry import Point

from dependencies.utils.helpers import fetch_spatial_file_to_local


def filter_zero_coords(record):
    """
    Filter out records with both latitude and longitude equal to zero (0,0).

    Args:
        record (dict): A single GBIF occurrence record with keys 'decimalLatitude' and 'decimalLongitude'.

    Returns:
        dict or None: The original record if it does not have both coordinates at zero,
                      otherwise None to indicate filtering out.
    """
    if record is None:
        return None
    try:
        lat = float(record.get('decimalLatitude', None))
        lon = float(record.get('decimalLongitude', None))
    except (TypeError, ValueError):
        return None
    if lat == 0.0 and lon == 0.0:
        return None
    return record


def filter_invalid_coords(record):
    """
    Filter out records with invalid latitude or longitude values.

    Args:
        record (dict): A single GBIF occurrence record with keys 'decimalLatitude' and 'decimalLongitude'.

    Returns:
        dict or None: The original record if latitude is between -90 and 90 and longitude is between -180 and 180;
                      otherwise None to indicate filtering out.
    """
    if record is None:
        return None
    try:
        lat = float(record.get('decimalLatitude', None))
        lon = float(record.get('decimalLongitude', None))
    except (TypeError, ValueError):
        return None
    if not (-90.0 <= lat <= 90.0) or not (-180.0 <= lon <= 180.0):
        return None
    return record


def filter_high_uncertainty(record, max_uncertainty=5000, min_uncertainty=1000):
    """
    Filter out records with coordinate uncertainty outside [min_resolution, max_uncertainty].

    Args:
        record (dict): Occurrence record.
        max_uncertainty (float): Maximum allowed uncertainty in meters.
        min_uncertainty (float): Minimum resolution threshold (should match raster resolution).

    Returns:
        dict or None
    """
    if record is None:
        return None
    raw_uncert = record.get('coordinateUncertaintyInMeters')
    try:
        uncert = float(raw_uncert) if raw_uncert not in (None, "") else None
    except (TypeError, ValueError):
        return None

    if uncert is None or uncert < min_uncertainty or uncert > max_uncertainty:
        return None

    return record


def filter_sea(record, land_gdf):
    """
    Drop records that fall outside all land polygons (i.e., over sea).

    Args:
        record (dict): Occurrence with 'decimalLatitude' and 'decimalLongitude'.
        land_gdf (GeoDataFrame): Pre-loaded Natural Earth land polygons with spatial index.

    Returns:
        dict or None: record if on land, else None.
    """
    if record is None:
        return None
    try:
        lat = float(record['decimalLatitude'])
        lon = float(record['decimalLongitude'])
    except (KeyError, TypeError, ValueError):
        return None
    pt = Point(lon, lat)
    possible = list(land_gdf.sindex.query(pt, predicate="intersects"))
    for idx in possible:
        if pt.within(land_gdf.geometry.iloc[idx]):
            return record
    return None


def haversine_dist(lat1, lon1, lat2, lon2):
    """
    Calculate the great-circle distance between two points on a sphere using the haversine formula.

    Args:
        lat1, lon1: Latitude and longitude of point 1 in decimal degrees.
        lat2, lon2: Latitude and longitude of point 2 in decimal degrees.

    Returns:
        Distance in meters.

    Reference:
        https://community.esri.com/t5/coordinate-reference-systems-blog/distance-on-a-sphere-the-haversine-formula/ba-p/902128#:~:text=For%20example%2C%20haversine(θ),longitude%20of%20the%20two%20points.
    """
    R = 6371000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(delta_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def filter_centroid(record, centroids, max_dist=5000):
    """
    Drop records within max_dist (in meters) of any administrative centroid.

    Args:
        record (dict): Occurrence with 'decimalLatitude', 'decimalLongitude'.
        centroids (list of (lat, lon) tuples): Pre-loaded centroid coordinates.
        max_dist (float): Buffer distance in meters (default 5000m).

    Returns:
        dict or None: The record if no centroid is within max_dist, else None.
    """
    if record is None:
        return None
    try:
        lat = float(record['decimalLatitude'])
        lon = float(record['decimalLongitude'])
    except (KeyError, TypeError, ValueError):
        return None
    for cen_lat, cen_lon in centroids:
        if haversine_dist(lat, lon, cen_lat, cen_lon) <= max_dist:
            return None
    return record


def load_land_gdf(shapefile_path):
    """
    Load the continental land shapefile into a GeoDataFrame object, using GCS-aware local copying.
    :param path:
    :return: GeoDataFrame with sindex
    """
    local_path = fetch_spatial_file_to_local(shapefile_path, "/tmp/land_shapefile")
    gdf = gpd.read_file(local_path)
    gdf.sindex
    return gdf


def load_centroid_list(shapefile_path):
    """
    Loads the centroid shapefile,using GCS-aware local copying.
    :param path:
    :return: a list of (lat, lon) tuples with centroid coordinates.
    """
    local_path = fetch_spatial_file_to_local(shapefile_path, "/tmp/centroid_shapefile")
    gdf = gpd.read_file(local_path)
    return list(zip(gdf.geometry.y, gdf.geometry.x))


def select_best_record(records):
    """
    Given a list of occurrence records (same lat/lon), return the one with
    the lowest coordinateUncertaintyInMeters.
    """
    return min(
        records,
        key=lambda r: float(r['coordinateUncertaintyInMeters'])
    )