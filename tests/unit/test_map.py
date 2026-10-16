import numpy as np
import pytest
import xarray as xr
from unittest.mock import patch
import json

from climatedata_api.map import get_s2d_release_date, get_s2d_gridded_values
from climatedata_api.utils import negotiated_response
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
        with patch('climatedata_api.utils.open_dataset_by_path', return_value=test_dataset):
            result = get_s2d_release_date(S2D_VARIABLE_AIR_TEMP, S2D_FREQUENCY_SEASONAL)

        assert result.status_code == 200
        assert result.content_type == 'application/json'
        response_data = json.loads(result.get_data(as_text=True))
        assert response_data == '2025-08-01'

    def test_bad_param(self, test_app):
        """Should return 400 if a param is not allowed."""

        response, status = get_s2d_release_date('wrong_var', S2D_FREQUENCY_SEASONAL)
        assert status == 400
        assert response == "Bad request"

        response, status = get_s2d_release_date(S2D_VARIABLE_AIR_TEMP, 'wrong_freq')
        assert status == 400
        assert response == "Bad request"


class TestGetS2DGriddedValues:
    @patch("climatedata_api.map.retrieve_s2d_release_date", return_value="2025-01-01")
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

        # Input lat/lon coordinates sent with the request
        requested_lat = 43.77
        requested_lon = -139.07

        # Nearest forecast grid lat/lon coordinates
        forecast_lat = 43.5
        forecast_lon = -139.5

        # Nearest climatology/skill grid lat/lon coordinates
        grid_lat = 43.75
        grid_lon = -139.083333

        with test_app.test_request_context(query_string={"period": "2025-08"}):
            result = get_s2d_gridded_values(requested_lat, requested_lon, S2D_VARIABLE_AIR_TEMP, S2D_FREQUENCY_SEASONAL)

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

    @pytest.mark.parametrize("accept, mimetype, decode", [
        ("application/json", "application/json", json.loads),
        ("text/plain", "text/plain", str),
    ])
    @patch("climatedata_api.map.retrieve_s2d_release_date", return_value="2025-01-01")
    @patch("climatedata_api.utils.open_dataset_by_path")
    def test_all_nan_location(self, mock_open_dataset, mock_release, accept, mimetype, decode, test_app):
        """Should return 404 if every value at the nearest grid cell is NaN (point outside the S2D grid)."""
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

        # Masked cells outside the S2D grid hold NaN.
        for ds in (forecast_ds, climato_ds, skill_ds):
            for var in ds.data_vars:
                ds[var] = xr.full_like(ds[var], np.nan, dtype=np.float64)

        mock_open_dataset.side_effect = [forecast_ds, climato_ds, skill_ds]

        requested_lat = 43.77
        requested_lon = -139.07

        with test_app.test_request_context(query_string={"period": "2025-08"}, headers={"Accept": accept}):
            response = get_s2d_gridded_values(requested_lat, requested_lon, S2D_VARIABLE_AIR_TEMP, S2D_FREQUENCY_SEASONAL)

        assert response.status_code == 404
        assert response.mimetype == mimetype
        assert decode(response.get_data(as_text=True)) == "No data available for the requested coordinates"

    @patch("climatedata_api.map.retrieve_s2d_release_date", return_value="2025-01-01")
    @patch("climatedata_api.utils.open_dataset_by_path")
    def test_partial_nan_location(self, mock_open_dataset, mock_release, test_app):
        """Should return 404 if only some values at the nearest grid cell are NaN."""
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

        # The forecast stays valid, only the skill dataset holds NaN.
        for var in skill_ds.data_vars:
            skill_ds[var] = xr.full_like(skill_ds[var], np.nan, dtype=np.float64)

        mock_open_dataset.side_effect = [forecast_ds, climato_ds, skill_ds]

        requested_lat = 43.77
        requested_lon = -139.07

        with test_app.test_request_context(query_string={"period": "2025-08"}, headers={"Accept": "application/json"}):
            response = get_s2d_gridded_values(requested_lat, requested_lon, S2D_VARIABLE_AIR_TEMP, S2D_FREQUENCY_SEASONAL)

        assert response.status_code == 404
        assert response.get_json() == "No data available for the requested coordinates"

    def test_bad_params(self, test_app):
        """Should return 400 if a param is not allowed."""

        with test_app.test_request_context(query_string={"period": "2025-07"}):
            response, status = get_s2d_gridded_values(
                lat="61.04", lon="-61.11",
                var="wrong_var", freq=S2D_FREQUENCY_SEASONAL,
            )
        assert status == 400
        assert response == "Bad request"

        with test_app.test_request_context(query_string={"period": "2025-07"}):
            response, status = get_s2d_gridded_values(
                lat="61.04", lon="-61.11",
                var=S2D_VARIABLE_AIR_TEMP, freq="wrong_freq",
            )
        assert status == 400
        assert response == "Bad request"

        with test_app.test_request_context(query_string={"period": "2025-07-01"}):  # wrong format
            response, status = get_s2d_gridded_values(
                lat="61.04", lon="-61.11",
                var=S2D_VARIABLE_AIR_TEMP, freq=S2D_FREQUENCY_SEASONAL,
            )
        assert status == 400
        assert response == "Bad request"

    @patch("climatedata_api.map.retrieve_s2d_release_date", return_value="2025-01-01")
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
        with test_app.test_request_context(query_string={"period": test_period}):
            response, status = get_s2d_gridded_values(requested_lat, requested_lon, S2D_VARIABLE_AIR_TEMP, S2D_FREQUENCY_SEASONAL)

        assert status == 400
        assert isinstance(response, ValueError) and "not available in forecast dataset" in str(response)

        mock_open_dataset.side_effect = [forecast_ds, climato_ds, skill_ds]
        test_period = "2025-12"
        with test_app.test_request_context(query_string={"period": test_period}):
            response, status = get_s2d_gridded_values(requested_lat, requested_lon, S2D_VARIABLE_AIR_TEMP, S2D_FREQUENCY_SEASONAL)
        assert status == 400
        assert isinstance(response, ValueError) and "not available in skill dataset" in str(response)


class TestNegotiatedResponse:
    @pytest.mark.parametrize("headers", [{"Accept": "*/*"}, {}, {"Accept": "application/json"}, {"Accept": "text/html"}])
    def test_json_by_default(self, headers, test_app):
        """Should return JSON for any Accept header other than text/plain, or none."""
        with test_app.test_request_context(headers=headers):
            response = negotiated_response("message", 404)

        assert response.status_code == 404
        assert response.mimetype == "application/json"
        assert response.get_json() == "message"
        assert "Accept" in response.vary

    def test_text_plain_with_dict(self, test_app):
        """Should return JSON when text/plain is preferred but the value is not a str."""
        with test_app.test_request_context(headers={"Accept": "text/plain"}):
            response = negotiated_response({"key": 1.5})

        assert response.status_code == 200
        assert response.mimetype == "application/json"
        assert response.get_json() == {"key": 1.5}
        assert "Accept" in response.vary

    @pytest.mark.parametrize("accept", ["text/plain", "text/*"])
    def test_text_plain_with_str(self, accept, test_app):
        """Should return text/plain when text/plain is preferred and the value is a str."""
        with test_app.test_request_context(headers={"Accept": accept}):
            response = negotiated_response("message", 404)

        assert response.status_code == 404
        assert response.mimetype == "text/plain"
        assert response.get_data(as_text=True) == "message"
        assert "Accept" in response.vary

    def test_error_with_non_str(self, test_app):
        """Should raise TypeError if an error status is given a non-str value."""
        with test_app.test_request_context():
            with pytest.raises(TypeError):
                negotiated_response({"key": 1.5}, 400)
