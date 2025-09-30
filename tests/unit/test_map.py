import numpy as np
import xarray as xr
from unittest.mock import patch

from climatedata_api.map import get_s2d_release_date, get_s2d_gridded_values

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


def generate_test_datasets(lats, lons, forecast_times, skill_times):
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

    return forecast_ds, climato_ds, skill_ds


class TestGetS2DReleaseDate:
    def test_valid_xarray(self, test_app):
        times = np.array(['2025-09-01T00:00:00.000000000', '2025-10-01T00:00:00.000000000',
                          '2025-08-01T00:00:00.000000000', '2026-02-01T00:00:00.000000000'], dtype='datetime64[ns]')
        test_dataset = xr.Dataset(coords={"time": times})

        # Patch open_dataset_by_path to return the test data dataset
        with patch('climatedata_api.map.open_dataset_by_path', return_value=test_dataset):
            result = get_s2d_release_date('air_temp', 'seasonal')

        assert result == '2025-08-01'


    def test_bad_param(self, test_app):
        """Should return 400 if a param is not allowed."""

        response, status = get_s2d_release_date('wrong_var', 'seasonal')
        assert status == 400
        assert response == "Bad request"

        response, status = get_s2d_release_date('air_temp', 'wrong_freq')
        assert status == 400
        assert response == "Bad request"

class TestGetS2DGriddedValues:
    @patch("climatedata_api.map.get_s2d_release_date", return_value="2025-01-01")
    @patch("climatedata_api.map.open_dataset_by_path")
    def test_valid_data(self, mock_open_dataset, mock_release, test_app):
        lats = [43.0, 61.04]
        lons = [-140.0, -61.11]
        forecast_times = [f"2025-{month:02d}-01" for month in range(1, 13)]
        skill_times = [f"1991-{month:02d}-01" for month in range(1, 13)]
        forecast_ds, climato_ds, skill_ds = generate_test_datasets(lats, lons, forecast_times, skill_times)

        mock_open_dataset.side_effect = [forecast_ds, climato_ds, skill_ds]

        result = get_s2d_gridded_values(lats[0], lons[0], 'air_temp', 'seasonal', period='2025-08')

        assert result == {
            **{
                var: forecast_ds[var].sel(time="2025-08-01", lat=lats[0], lon=lons[0]).item()
                for var in FORECAST_DATA_VAR_NAMES
            },
            **{
                var: climato_ds[var].sel(time="1991-08-01", lat=lats[0], lon=lons[0]).item()
                for var in CLIMATO_DATA_VAR_NAMES
            },
            "skill_level": skill_ds["skill_level"].sel(time="1991-08-01", lat=lats[0], lon=lons[0]).item(),
            "skill_CRPSS": skill_ds["skill_CRPSS"].sel(time="1991-08-01", lat=lats[0], lon=lons[0]).item(),
        }

    def test_bad_params(self, test_app):
        """Should return 400 if a param is not allowed."""

        response, status = get_s2d_gridded_values(
            lat="61.04", lon="-61.11",
            var="wrong_var", freq="seasonal",
            period="2025-07"
        )
        assert status == 400
        assert response == "Bad request"

        response, status = get_s2d_gridded_values(
            lat="61.04", lon="-61.11",
            var="air_temp", freq="wrong_freq",
            period="2025-07"
        )
        assert status == 400
        assert response == "Bad request"

        response, status = get_s2d_gridded_values(
            lat="61.04", lon="-61.11",
            var="air_temp", freq="seasonal",
            period="2025-07-01"  # wrong format
        )
        assert status == 400
        assert response == "Bad request"

    @patch("climatedata_api.map.get_s2d_release_date", return_value="2025-01-01")
    @patch("climatedata_api.map.open_dataset_by_path")
    def test_missing_time(self, mock_open_dataset, mock_release, test_app):
        lats = [43.0, 61.04]
        lons = [-140.0, -61.11]
        forecast_times = [f"2025-{month:02d}-01" for month in range(3, 13)]
        skill_times = [f"1991-{month:02d}-01" for month in range(1, 11)]
        forecast_ds, climato_ds, skill_ds = generate_test_datasets(lats, lons, forecast_times, skill_times)

        mock_open_dataset.side_effect = [forecast_ds, climato_ds, skill_ds]
        test_period = "2025-01"
        response, status = get_s2d_gridded_values(lats[0], lons[0], 'air_temp', 'seasonal', period=test_period)

        assert status == 400
        assert "not available in forecast dataset" in response

        mock_open_dataset.side_effect = [forecast_ds, climato_ds, skill_ds]
        test_period = "2025-12"
        response, status = get_s2d_gridded_values(lats[0], lons[0], 'air_temp', 'seasonal', period=test_period)
        assert status == 400
        assert "not available in skill dataset" in response
