import numpy as np
import xarray as xr
from unittest.mock import patch

from climatedata_api.map import get_s2d_release_date, get_s2d_gridded_values
from default_settings import (
    S2D_CLIMATO_DATA_VAR_NAMES,
    S2D_FORECAST_DATA_VAR_NAMES,
    S2D_FREQUENCY_SEASONAL,
    S2D_VARIABLE_AIR_TEMP,
)
from tests.unit.utils import generate_s2d_test_datasets


class TestGetS2DReleaseDate:
    def test_valid_xarray(self, test_app):
        # Only testing on datetime values that use the first day of the month and the hour 0, since the input data
        # should have been both pre-processed to this format and validated by the Airflow pipeline preemptively.
        times = np.array(['2025-09-01T00:00:00.000000000', '2025-10-01T00:00:00.000000000',
                          '2025-08-01T00:00:00.000000000', '2026-02-01T00:00:00.000000000'], dtype='datetime64[ns]')
        test_dataset = xr.Dataset(coords={"time": times})

        # Patch open_dataset_by_path to return the test data dataset
        with patch('climatedata_api.map.open_dataset_by_path', return_value=test_dataset):
            result = get_s2d_release_date(S2D_VARIABLE_AIR_TEMP, S2D_FREQUENCY_SEASONAL)

        assert result == '2025-08-01'


    def test_bad_param(self, test_app):
        """Should return 400 if a param is not allowed."""

        response, status = get_s2d_release_date('wrong_var', S2D_FREQUENCY_SEASONAL)
        assert status == 400
        assert response == "Bad request"

        response, status = get_s2d_release_date(S2D_VARIABLE_AIR_TEMP, 'wrong_freq')
        assert status == 400
        assert response == "Bad request"

class TestGetS2DGriddedValues:
    @patch("climatedata_api.map.get_s2d_release_date", return_value="2025-01-01")
    @patch("climatedata_api.utils.open_dataset_by_path")
    def test_valid_data(self, mock_open_dataset, mock_release, test_app):
        forecast_times = [f"2025-{month:02d}-01" for month in range(1, 13)]
        skill_times = [f"1991-{month:02d}-01" for month in range(1, 13)]
        forecast_ds, climato_ds, skill_ds = generate_s2d_test_datasets(
            lat_min=43.0,
            lat_max=45.0,
            lon_min=-140.0,
            lon_max=-138.0,
            forecast_times=forecast_times,
            skill_times=skill_times
        )

        # Add fake extra datavars to the datasets to ensure they are ignored and not returned by the function
        forecast_ds["extra_forecast_var"] = (("time", "lat", "lon"), np.random.rand(len(forecast_ds.time), len(forecast_ds.lat), len(forecast_ds.lon)))
        climato_ds["extra_climato_var"] = (("time", "lat", "lon"), np.random.rand(len(climato_ds.time), len(climato_ds.lat), len(climato_ds.lon)))
        skill_ds["extra_skill_var"] = (("time", "lat", "lon"), np.random.rand(len(skill_ds.time), len(skill_ds.lat), len(skill_ds.lon)))

        mock_open_dataset.side_effect = [forecast_ds, climato_ds, skill_ds]

        requested_lat = 43.77
        requested_lon = -139.07
        forecast_lat = 43.5
        forecast_lon = -139.5
        grid_lat = 43.75
        grid_lon = -139.083333

        result = get_s2d_gridded_values(requested_lat, requested_lon, S2D_VARIABLE_AIR_TEMP, S2D_FREQUENCY_SEASONAL, period='2025-08')

        assert result == {
            **{
                var: forecast_ds[var].sel(time="2025-08-01", lat=forecast_lat, lon=forecast_lon).item()
                for var in S2D_FORECAST_DATA_VAR_NAMES
            },
            **{
                var: climato_ds[var].sel(time="1991-08-01", lat=grid_lat, lon=grid_lon).item()
                for var in S2D_CLIMATO_DATA_VAR_NAMES
            },
            "skill_level": skill_ds["skill_level"].sel(time="1991-08-01", lat=grid_lat, lon=grid_lon).item(),
            "skill_CRPSS": skill_ds["skill_CRPSS"].sel(time="1991-08-01", lat=grid_lat, lon=grid_lon).item(),
        }

    def test_bad_params(self, test_app):
        """Should return 400 if a param is not allowed."""

        response, status = get_s2d_gridded_values(
            lat="61.04", lon="-61.11",
            var="wrong_var", freq=S2D_FREQUENCY_SEASONAL,
            period="2025-07"
        )
        assert status == 400
        assert response == "Bad request"

        response, status = get_s2d_gridded_values(
            lat="61.04", lon="-61.11",
            var=S2D_VARIABLE_AIR_TEMP, freq="wrong_freq",
            period="2025-07"
        )
        assert status == 400
        assert response == "Bad request"

        response, status = get_s2d_gridded_values(
            lat="61.04", lon="-61.11",
            var=S2D_VARIABLE_AIR_TEMP, freq=S2D_FREQUENCY_SEASONAL,
            period="2025-07-01"  # wrong format
        )
        assert status == 400
        assert response == "Bad request"

    @patch("climatedata_api.map.get_s2d_release_date", return_value="2025-01-01")
    @patch("climatedata_api.utils.open_dataset_by_path")
    def test_missing_time(self, mock_open_dataset, mock_release, test_app):
        forecast_times = [f"2025-{month:02d}-01" for month in range(3, 13)]
        skill_times = [f"1991-{month:02d}-01" for month in range(1, 11)]
        forecast_ds, climato_ds, skill_ds = generate_s2d_test_datasets(
            lat_min=43.0,
            lat_max=45.0,
            lon_min=-140.0,
            lon_max=-138.0,
            forecast_times=forecast_times,
            skill_times=skill_times
        )

        requested_lat = 43.77
        requested_lon = -139.07

        mock_open_dataset.side_effect = [forecast_ds, climato_ds, skill_ds]
        test_period = "2025-01"
        response, status = get_s2d_gridded_values(requested_lat, requested_lon, S2D_VARIABLE_AIR_TEMP, S2D_FREQUENCY_SEASONAL, period=test_period)

        assert status == 400
        assert isinstance(response, ValueError) and "not available in forecast dataset" in str(response)

        mock_open_dataset.side_effect = [forecast_ds, climato_ds, skill_ds]
        test_period = "2025-12"
        response, status = get_s2d_gridded_values(requested_lat, requested_lon, S2D_VARIABLE_AIR_TEMP, S2D_FREQUENCY_SEASONAL, period=test_period)
        assert status == 400
        assert isinstance(response, ValueError) and "not available in skill dataset" in str(response)
