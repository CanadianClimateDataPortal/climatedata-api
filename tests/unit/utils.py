import numpy as np
import xarray as xr

from default_settings import S2D_FORECAST_DATA_VAR_NAMES, S2D_CLIMATO_DATA_VAR_NAMES


def generate_s2d_test_datasets(lat_min: float,
                               lat_max: float,
                               lon_min: float,
                               lon_max: float,
                               forecast_times: list[str],
                               skill_times: list[str],
                               add_test_metadata: bool = False) -> tuple[xr.Dataset, xr.Dataset, xr.Dataset]:
    """
    Generate synthetic xarray Datasets for S2D forecast, climatology, and skill data.
    :param lat_min: Minimum latitude of the grid to generate.
    :param lat_max: Maximum latitude of the grid to generate.
    :param lon_min: Minimum longitude of the grid to generate.
    :param lon_max: Maximum longitude of the grid to generate.
    :param forecast_times: List of forecast times (e.g., ["2025-01-01", "2025-02-01", ...]), where forecast data is generated.
    :param skill_times: List of skill times (e.g., ["1991-01-01", "1991-02-01", ...]), where skill data is generated.
    :param add_test_metadata: If true, adds test metadata to datasets and variables.
    :return: Tuple of (forecast_ds, climato_ds, skill_ds)
    """

    dims = ("time", "lat", "lon")
    forecast_lats, grid_lats = generate_coord_range(lat_min, lat_max)
    forecast_lons, grid_lons = generate_coord_range(lon_min, lon_max)

    forecast_shape = (len(forecast_times), len(forecast_lats), len(forecast_lons))
    forecast_data = {
        var: (dims, np.random.uniform(0, 100, size=forecast_shape).astype(np.float32))
        for var in S2D_FORECAST_DATA_VAR_NAMES
    }
    forecast_ds = xr.Dataset(
        data_vars=forecast_data,
        coords={
            "time": np.array(forecast_times, dtype="datetime64[ns]"),
            "lat": np.array(forecast_lats, dtype=np.float64),
            "lon": np.array(forecast_lons, dtype=np.float64),
        },
    )

    climato_times = [f"1991-{month:02d}-01" for month in range(1, 13)]
    climato_shape = (len(climato_times), len(grid_lats), len(grid_lons))
    climato_data = {
        var: (dims, np.random.uniform(10, 25, size=climato_shape).astype(np.float64))
        for var in S2D_CLIMATO_DATA_VAR_NAMES
    }
    climato_ds = xr.Dataset(
        data_vars=climato_data,
        coords={
            "time": np.array(climato_times, dtype="datetime64[ns]"),
            "lat": np.array(grid_lats, dtype=np.float64),
            "lon": np.array(grid_lons, dtype=np.float64),
        },
    )

    skill_shape = (len(skill_times), len(grid_lats), len(grid_lons))
    skill_ds = xr.Dataset(
        data_vars={
            "skill_CRPSS": (
            dims, np.random.uniform(-1, 1, size=skill_shape).astype(np.float32)),
            "skill_level": (dims, np.random.randint(0, 4, size=skill_shape)),
        },
        coords={
            "time": np.array(skill_times, dtype="datetime64[ns]"),
            "lat": np.array(grid_lats, dtype=np.float64),
            "lon": np.array(grid_lons, dtype=np.float64),
        },
    )

    datasets = (forecast_ds, climato_ds, skill_ds)

    if add_test_metadata:
        forecast_ds.attrs = {"dataset": "forecast"}
        climato_ds.attrs = {"dataset": "climatology"}
        skill_ds.attrs = {"dataset": "skill"}

        for ds in datasets:
            for var in ds.data_vars:
                ds[var].attrs["var_name"] = var

        # Metadata for coordinates, only add to forecast dataset
        for coord in forecast_ds.coords:
            forecast_ds[coord].attrs["coord_name"] = coord

    return datasets

def generate_coord_range(min_val: float, max_val: float) -> tuple[list[float], list[float]]:
    """
    Generate two aligned sequences of values between min_val and max_val:
      - forecast_range: values aligned on .5, spaced by 1.0, representing the forecast coordinate range
      - high_res_range: values aligned on the same range, spaced by 1/12, representing the skill/climatology range
    :param min_val: Minimum value of the range (inclusive)
    :param max_val: Maximum value of the range (inclusive)
    :return: Tuple of (forecast_range, high_res_range)
    """

    step = 1 / 12

    # Find the first and last numbers aligned on .5 values, inside the input range
    start = np.ceil(min_val - 0.5) + 0.5
    end = np.floor(max_val - 0.5) + 0.5

    forecast_range = np.arange(start, end + 0.0001, 1.0)

    # Find extended bounds aligned on the 1/12 grid
    pad_min_val = min_val - 1e-9
    pad_max_val = max_val + 1e-9

    first_aligned = start
    while first_aligned - step >= pad_min_val:
        first_aligned -= step

    last_aligned = end
    while last_aligned + step <= pad_max_val:
        last_aligned += step

    high_res_range = np.arange(first_aligned, last_aligned + step / 2, step)
    high_res_range = np.round(high_res_range, 6)

    return forecast_range.tolist(), high_res_range.tolist()
