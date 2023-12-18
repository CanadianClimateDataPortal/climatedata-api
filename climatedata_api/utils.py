import io
import math
import xarray as xr
from flask import current_app as app
import geopandas as gpd
from scipy.spatial import KDTree
import pickle
import numpy as np
import zipfile

def open_dataset(dataset_name, filetype, var, freq, period=None, partition=None):
    """
    Open and return a xarray dataset. Try all path formats, since provided datasets had inconsistent naming convention
    :return: Dataset object
    """
    if partition:
        filename_format = app.config['FILENAME_FORMATS'][dataset_name]['partitions'][filetype]
        dataset_path = app.config['DATASETS_ROOT'] / dataset_name / "partitions" / partition / var / freq \
            / filename_format.format(var=var, freq=freq, period=period)
    else:
        filename_format = app.config['FILENAME_FORMATS'][dataset_name][filetype]
        dataset_path = app.config['DATASETS_ROOT'] / dataset_name / filetype / var / freq \
            / filename_format.format(var=var, freq=freq, period=period)
    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset not found {dataset_path}")
    dataset = xr.open_dataset(dataset_path, decode_times=False)
    dataset['time'] = xr.decode_cf(dataset).time
    return dataset


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
