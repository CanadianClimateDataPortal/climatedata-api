import datetime
import io
import math
from typing import Tuple

import xarray as xr
from clisops.core.subset import subset_bbox
from flask import current_app as app
import geopandas as gpd
from scipy.spatial import KDTree
import pickle
import numpy as np
import zipfile

from werkzeug.exceptions import BadRequest


def open_dataset(dataset_name, filetype, var, freq, period=None, partition=None):
    """
    Open and return a xarray dataset. Try all path formats, since provided datasets had inconsistent naming convention
    :return: Dataset object
    """
    if partition:
        filename_formats = app.config['FILENAME_FORMATS'][dataset_name]['partitions'][filetype]
    else:
        filename_formats = app.config['FILENAME_FORMATS'][dataset_name][filetype]

    # try all filename formats and return first found
    for filename_format in filename_formats:
        if partition:
            dataset_path = app.config['DATASETS_ROOT'] / dataset_name / "partitions" / partition / var / freq \
                           / filename_format.format(var=var, freq=freq, period=period)
        else:
            dataset_path = app.config['DATASETS_ROOT'] / dataset_name / filetype / var / freq \
                           / filename_format.format(var=var, freq=freq, period=period)
        print (f"trying {dataset_path}")
        if not dataset_path.exists():
            continue
        dataset = xr.open_dataset(dataset_path, decode_times=False)
        dataset['time'] = xr.decode_cf(dataset).time
        return dataset

    raise FileNotFoundError(f"Dataset not found for {dataset_name}, {filetype}, {var}, {freq}, {period}, {partition}")


def open_dataset_by_path(path):
    """
    Open and return a xarray dataset
    :param path: path of the dataset
    :return: Dataset object
    """
    try:
        dataset = xr.open_dataset(path, decode_times=False)
        dataset['time'] = xr.decode_cf(dataset).time
        return dataset
    except FileNotFoundError:
        raise FileNotFoundError(f"Dataset file not found: {path}")


def convert_time_series_dataset_to_list(dataset, decimals):
    """
    Converts xarray dataset to a list.
    We assume that the coordinates are timestamps, which are converted to milliseconds since 1970-01-01 (integer)
    """

    # we cast to int if decimals is 0
    def _convert(float_list):
        return float_list if decimals > 0 else list(map(int, float_list))

    return [[int(a[0].timestamp() * 1000)] + _convert(a[1:]) for a in
            dataset.to_dataframe().astype('float64').round(decimals).reset_index().values.tolist()]


def convert_time_series_dataset_to_dict(dataset, decimals):
    """
    Converts xarray dataset to a dict.
    We assume that the coordinates are timestamps, which are converted to milliseconds since 1970-01-01 (integer)
    """

    # we cast to int if decimals is 0
    def _convert(float_list):
        return float_list if decimals > 0 else list(map(int, float_list))

    return {int(a[0].timestamp() * 1000): _convert(a[1:]) for a in
            dataset.to_dataframe().astype('float64').round(decimals).reset_index().values.tolist()}


SAFE_CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789_-"  # Safe for URL
SAFE_CHARACTER_LUT = {c: idx for idx, c in enumerate(SAFE_CHARACTERS)}
MULTIPLIER = 100
DIVIDER = 32


def format_metadata(ds) -> str:
    """
    For an xarray dataset, return its formatted metadata.
    Source: https://github.com/bird-house/finch/blob/master/finch/processes/utils.py
    Source is copied here because Finch has way too many dependencies we don't need
    """

    def _fmt_attrs(obj, name="", comment="# ", tab=" "):
        """Return string of an object's attribute."""
        lines = ["", name]
        for key, val in obj.attrs.items():
            lines.append(
                tab + key + ":: " + str(val).replace("\n", "\n" + comment + tab + "  ")
            )

        out = ("\n" + comment + tab).join(lines)
        return out

    objs = [
        ({"": ds}, "Global attributes"),
        (ds.coords, "Coordinates"),
        (ds.data_vars, "Data variables"),
    ]

    out = ""
    for obj, name in objs:
        out += "# " + name
        tab = "" if name == "Global attributes" else "  "
        for key, val in obj.items():
            out += _fmt_attrs(val, key, tab=tab)
        out += "\n#\n"
    return out


def decode_compressed_points(compressed_points):
    """
    Decode compressed list of points (lat,lon) and returns it as a list of tuples
    See: https://github.com/ljanecek/point-compression/blob/master/index.js
    """
    lat_lon = []
    points_array = []
    point = []
    last_lat = 0
    last_lon = 0

    for c in compressed_points:
        num = SAFE_CHARACTER_LUT[c]

        if num < DIVIDER:
            point.append(num)
            points_array.append(point)
            point = []
        else:
            num = num - DIVIDER
            point.append(num)

    for point in points_array:
        result = 0
        point.reverse()
        for p in point:
            if result == 0:
                result = p
            else:
                result = result * DIVIDER + p

        d_iag = int((math.sqrt(8 * result + 5) - 1) / 2)

        lat_y = result - (d_iag * (d_iag + 1)) / 2
        lon_x = d_iag - lat_y

        if lat_y % 2 == 1:
            lat_y = (lat_y + 1) * -1
        if lon_x % 2 == 1:
            lon_x = (lon_x + 1) * -1

        lat_y = lat_y / 2
        lon_x = lon_x / 2
        lat = lat_y + last_lat
        lon = lon_x + last_lon

        last_lat = lat
        last_lon = lon
        lat = lat / MULTIPLIER
        lon = lon / MULTIPLIER

        lat_lon.append((lat, lon))

    return lat_lon


def generate_kdtrees():
    """
        Pre-generate a pickled kdtree since this operation takes too long for web usage
    """
    app.config['CACHE_FOLDER'].mkdir(parents=True, exist_ok=True)
    for gridname, gridpath in app.config['GRIDS'].items():
        print(f"Generating kd-tree for shapefile {gridname}. This will take a few minutes.")
        outfile = app.config['CACHE_FOLDER'] / f"kdtree-{gridname}.pickle"
        tmpfile = outfile.with_suffix('.tmp')
        geodf = gpd.GeoDataFrame.from_file(app.config['DATASETS_ROOT'] / gridpath)

        # validate if gid matches index in dataframe
        for idx, row in geodf.iterrows():
            assert row.gid == idx

        centroids = geodf.centroid
        tree = KDTree(np.array(list(zip(centroids.y, centroids.x))))
        with tmpfile.open('wb') as f:
            pickle.dump(tree, f)
        tmpfile.rename(outfile)


def make_zip(content):
    """
    Create an in-memory buffer of a zip file
    @param content: Array of (filename, data) tuples
    @return: a in-memory buffer of the Zipfile
    """
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, 'w') as zip_file:
        for filename, data in content:
            zip_file.writestr(filename, data, zipfile.ZIP_DEFLATED)
    buffer.seek(0)
    return buffer

def load_s2d_datasets_by_periods(var: str,
                                 freq: str,
                                 period_dates: list[datetime.datetime],
                                 ref_period: datetime.datetime
                                 ) -> tuple[xr.Dataset, xr.Dataset, xr.Dataset]:
    """
    Load S2D datasets (forecast, climatology, skill) for the given periods.

    :param var: variable name associated with the datasets to load
    :param freq: frequency associated with the datasets to load
    :param period_dates: list of datetime objects representing the periods to load
    :param ref_period: datetime object representing the reference period for the skill dataset
    :return: tuple with the selected forecast, climatology and skill datasets
    """
    # Load forecast data
    forecast_dataset = open_dataset_by_path(app.config['NETCDF_S2D_FORECAST_FILENAME_FORMATS'].format(
        root=app.config['DATASETS_ROOT'],
        var=var,
        freq=freq
    ))
    # Check that all period dates are available in the dataset before selecting them
    for period_date in period_dates:
        try:
            forecast_dataset.sel(time=period_date)
        except:
            raise ValueError(f"Bad request: period {period_date} not available in forecast dataset")
    forecast_slice = forecast_dataset.sel(time=period_dates)

    # Load climatology data
    climatology_dataset = open_dataset_by_path(app.config['NETCDF_S2D_CLIMATOLOGY_FILENAME_FORMATS'].format(
        root=app.config['DATASETS_ROOT'],
        var=var,
        freq=freq
    ))
    climatology_period_dates = []
    for period_date in period_dates:
        climatology_period_dates.append(period_date.replace(year=1991))
    climatology_slice = climatology_dataset.sel(time=climatology_period_dates)

    # Load skill data
    skill_dataset = open_dataset_by_path(app.config['NETCDF_S2D_SKILL_FILENAME_FORMATS'].format(
        root=app.config['DATASETS_ROOT'],
        var=var,
        freq=freq,
        ref_period=ref_period.month
    ))
    for period_date in period_dates:
        if period_date.month not in skill_dataset['time'].dt.month.values:
            raise ValueError(f"Bad request: period {period_date} not available in skill dataset")
    skill_slice = skill_dataset.sel(time=skill_dataset['time'].dt.month.isin([d.month for d in period_dates]))

    return forecast_slice, climatology_slice, skill_slice

def get_subset_by_bbox(dataset: xr.Dataset, bbox: Tuple[float, float, float, float]) -> xr.Dataset:
    """
    Subsets a dataset by filtering its lat and lon coordinates to be within the given bounding box.

    :param dataset: xarray dataset to subset
    :param bbox: bounding box [lat_min, lon_min, lat_max, lon_max]
    :return: subsetted xarray dataset
    """
    # make sure bbox is in the correct order
    lat_min, lat_max = sorted([bbox[0], bbox[2]])
    lon_min, lon_max = sorted([bbox[1], bbox[3]])
    return subset_bbox(dataset, lat_bnds=[lat_min, lat_max], lon_bnds=[lon_min, lon_max])


def get_subset_by_points(dataset: xr.Dataset, points: list[Tuple[float, float]]) -> xr.Dataset:
    """
    Subsets a dataset by filtering its lat and lon coordinates to be the nearest values to the given points.

    Note that in the returned dataset, any lat/lon combinations that don't correspond to any requested point
    will be assigned nan values.

    :param dataset: xarray dataset to subset
    :param points: list of (lat, lon) tuples
    :return: subsetted xarray dataset
    """
    # Filter to the nearest concerned lat and lon values
    nearest_points = [
        (dataset.lat.sel(lat=lat, method="nearest").item(),
         dataset.lon.sel(lon=lon, method="nearest").item())
        for lat, lon in points
    ]
    used_lats = sorted(set(lat for lat, _ in nearest_points))
    used_lons = sorted(set(lon for _, lon in nearest_points))

    subset = dataset.sel(lat=used_lats, lon=used_lons)

    # Filter the subset to only keep the values that correspond exactly to the requested lat/lon combinations
    # nan values are assigned to any lat/lon combinations that don't correspond to any requested point
    mask = np.full((len(used_lats), len(used_lons)), False)

    for lat, lon in nearest_points:
        i = used_lats.index(lat)
        j = used_lons.index(lon)
        mask[i, j] = True

    filtered_ds = subset.where(mask)
    return filtered_ds


def retrieve_s2d_release_date(var, freq):
    """
    Return the release date of the forecast data associated with a given variable and frequency.

    The returned string is in the YYYY-MM-DD format.
    The input data should normally only use the first day of months, so the returned day is always "01".
    """
    if (var not in app.config['S2D_VARIABLES']) or (freq not in app.config['S2D_FREQUENCIES']):
        raise ValueError(f"Invalid variable or frequency: {var} {freq}")

    dataset = open_dataset_by_path(app.config['NETCDF_S2D_FORECAST_FILENAME_FORMATS'].format(
        root=app.config['DATASETS_ROOT'],
        var=var,
        freq=freq
    ))

    latest_datetime = dataset['time'].min().values
    return str(latest_datetime.astype('datetime64[D]'))
