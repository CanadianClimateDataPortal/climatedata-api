import numpy as np
import xarray as xr


FORECAST_DATA_VAR_NAMES = [
    "prob_unusually_low",
    "prob_below_normal",
    "prob_near_normal",
    "prob_above_normal",
    "prob_unusually_high"
]

CLIMATO_DATA_VAR_NAMES = [
    "cutoff_unusually_low_p20",
    "cutoff_below_normal_p33",
    "historical_median_p50",
    "cutoff_above_normal_p66",
    "cutoff_unusually_high_p80"
]


def generate_s2d_test_datasets(lats, lons, forecast_times, skill_times, add_test_metadata=False):
    dims = ("time", "lat", "lon")

    forecast_shape = (len(forecast_times), len(lats), len(lons))
    forecast_data = {
        var: (dims, np.random.uniform(0, 100, size=np.prod(forecast_shape)).astype(np.float32).reshape(forecast_shape))
        for var in FORECAST_DATA_VAR_NAMES
    }
    forecast_ds = xr.Dataset(
        data_vars=forecast_data,
        coords={
            "time": np.array(forecast_times, dtype="datetime64[ns]"),
            "lat": np.array(lats, dtype=np.float64),
            "lon": np.array(lons, dtype=np.float64),
        },
    )

    climato_times = [f"1991-{month:02d}-01" for month in range(1, 13)]
    climato_shape = (len(climato_times), len(lats), len(lons))
    climato_data = {
        var: (dims, np.random.uniform(10, 25, size=np.prod(climato_shape)).astype(np.float32).reshape(climato_shape))
        for var in CLIMATO_DATA_VAR_NAMES
    }
    climato_ds = xr.Dataset(
        data_vars=climato_data,
        coords={
            "time": np.array(climato_times, dtype="datetime64[ns]"),
            "lat": np.array(lats, dtype=np.float64),
            "lon": np.array(lons, dtype=np.float64),
        },
    )

    skill_shape = (len(skill_times), len(lats), len(lons))
    skill_ds = xr.Dataset(
        data_vars={
            "skill_CRPSS": (
            dims, np.random.uniform(-1, 1, size=np.prod(skill_shape)).astype(np.float32).reshape(skill_shape)),
            "skill_level": (dims, np.random.randint(0, 4, size=np.prod(skill_shape)).reshape(skill_shape)),
        },
        coords={
            "time": np.array(skill_times, dtype="datetime64[ns]"),
            "lat": np.array(lats, dtype=np.float64),
            "lon": np.array(lons, dtype=np.float64),
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

            # Metadata for coordinates
            for coord in ds.coords:
                ds[coord].attrs["coord_name"] = coord

    return datasets
