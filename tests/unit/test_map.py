import numpy as np
import xarray as xr
from unittest.mock import patch

from climatedata_api.map import get_s2d_release_date, get_s2d_gridded_values
from default_settings import S2D_VARIABLE_AIR_TEMP, S2D_FREQUENCY_SEASONAL
from tests.unit.utils import generate_s2d_test_datasets, FORECAST_DATA_VAR_NAMES, CLIMATO_DATA_VAR_NAMES


class TestGetS2DReleaseDate:
    def test_valid_xarray(self, test_app):
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
    @patch("climatedata_api.map.open_dataset_by_path")
    def test_valid_data(self, mock_open_dataset, mock_release, test_app):
        lats = [43.0, 61.04]
        lons = [-140.0, -61.11]
        forecast_times = [f"2025-{month:02d}-01" for month in range(1, 13)]
        skill_times = [f"1991-{month:02d}-01" for month in range(1, 13)]
        forecast_ds, climato_ds, skill_ds = generate_s2d_test_datasets(lats, lons, forecast_times, skill_times)

        mock_open_dataset.side_effect = [forecast_ds, climato_ds, skill_ds]

        result = get_s2d_gridded_values(lats[0], lons[0], S2D_VARIABLE_AIR_TEMP, S2D_FREQUENCY_SEASONAL, period='2025-08')

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
    @patch("climatedata_api.map.open_dataset_by_path")
    def test_missing_time(self, mock_open_dataset, mock_release, test_app):
        lats = [43.0, 61.04]
        lons = [-140.0, -61.11]
        forecast_times = [f"2025-{month:02d}-01" for month in range(3, 13)]
        skill_times = [f"1991-{month:02d}-01" for month in range(1, 11)]
        forecast_ds, climato_ds, skill_ds = generate_s2d_test_datasets(lats, lons, forecast_times, skill_times)

        mock_open_dataset.side_effect = [forecast_ds, climato_ds, skill_ds]
        test_period = "2025-01"
        response, status = get_s2d_gridded_values(lats[0], lons[0], S2D_VARIABLE_AIR_TEMP, S2D_FREQUENCY_SEASONAL, period=test_period)

        assert status == 400
        assert "not available in forecast dataset" in response

        mock_open_dataset.side_effect = [forecast_ds, climato_ds, skill_ds]
        test_period = "2025-12"
        response, status = get_s2d_gridded_values(lats[0], lons[0], S2D_VARIABLE_AIR_TEMP, S2D_FREQUENCY_SEASONAL, period=test_period)
        assert status == 400
        assert "not available in skill dataset" in response
