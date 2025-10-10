import datetime
import io
import tempfile
import zipfile
from unittest.mock import patch

import geopandas
import numpy
import pandas as pd
import pytest
import xarray

from climatedata_api.download import check_points_or_bbox
from default_settings import (
    DOWNLOAD_CSV_FORMAT,
    DOWNLOAD_JSON_FORMAT,
    DOWNLOAD_NETCDF_FORMAT,
    S2D_CLIMATO_DATA_VAR_NAMES,
    S2D_FILENAME_VALUES,
    S2D_FORECAST_DATA_VAR_NAMES,
    S2D_FORECAST_TYPE_EXPECTED,
    S2D_FORECAST_TYPE_UNUSUAL,
    S2D_FREQUENCY_SEASONAL,
    S2D_SKILL_LEVEL_STR,
    S2D_VARIABLE_AIR_TEMP,
)
from tests.unit.utils import generate_s2d_test_datasets


class TestDownloadS2D:
    @pytest.mark.parametrize("file_format", [DOWNLOAD_NETCDF_FORMAT, DOWNLOAD_CSV_FORMAT, DOWNLOAD_JSON_FORMAT])
    @pytest.mark.parametrize("subset_type", ["points", "bbox"])
    @pytest.mark.parametrize("forecast_type", [S2D_FORECAST_TYPE_EXPECTED, S2D_FORECAST_TYPE_UNUSUAL])
    @patch("climatedata_api.download.get_s2d_release_date", return_value="2025-01-01")
    @patch("climatedata_api.utils.open_dataset_by_path")
    def test_valid_payload(self, mock_open_dataset, mock_release, test_app, client, file_format, subset_type, forecast_type):
        """
        Tests the /download-s2d endpoint with various valid payload combinations (file format, points/bbox, and forecast type)
        and checks the contents of the returned zip file.
        """
        lats = [43.0, 51.7, 61.04, 68.75, 73.82]
        lons = [-141.0, -130, -120, -98.54, -61.11]
        forecast_times = [f"2025-{month:02d}-01" for month in range(1, 13)]
        skill_times = [f"1991-{month:02d}-01" for month in range(1, 13)]
        forecast_ds, climato_ds, skill_ds = generate_s2d_test_datasets(lats, lons, forecast_times, skill_times, add_test_metadata=True)

        mock_open_dataset.side_effect = [forecast_ds, climato_ds, skill_ds]

        if subset_type == "points":
            expected_points = [[51.7, -120], [51.7, -141], [68.75, -141], [73.82, -98.54]]
            expected_lats = sorted(set([pt[0] for pt in expected_points]))
            expected_lons = sorted(set([pt[1] for pt in expected_points]))
            subset_payload = {
                "points": expected_points,
            }

        else:  # bbox
            bbox = [50, -140, 70, -100]  # min_lat, min_lon, max_lat, max_lon
            subset_payload = {"bbox": bbox}
            expected_lats = [lat for lat in lats if bbox[0] <= lat <= bbox[2]]
            expected_lons = [lon for lon in lons if bbox[1] <= lon <= bbox[3]]
            expected_points = [[lat, lon] for lat in expected_lats for lon in expected_lons]

        # Send request to the endpoint to be tested
        payload = {
            "var": S2D_VARIABLE_AIR_TEMP,
            "format": file_format,
            "forecast_type": forecast_type,
            "frequency": S2D_FREQUENCY_SEASONAL,
            "periods": ["2025-06", "2025-12"],
            **subset_payload,
        }
        response = client.post("/download-s2d", json=payload)

        expected_zip_filename = f"MeanTemp_{S2D_FILENAME_VALUES[forecast_type]}_Seasonal_ReleaseJan2025.zip"
        assert response.status_code == 200
        assert response.mimetype == "application/zip"
        assert "attachment" in response.headers["Content-Disposition"]
        assert expected_zip_filename in response.headers["Content-Disposition"]

        def get_expected_value(lat, lon, var, month):
            if var in S2D_FORECAST_DATA_VAR_NAMES:
                expected_ds = forecast_ds
                month_sel = month
            elif var in S2D_CLIMATO_DATA_VAR_NAMES:
                expected_ds = climato_ds
                month_sel = month.replace(year=1991)
            else:
                expected_ds = skill_ds
                month_sel = month.replace(year=1991)
            return expected_ds.sel(lat=lat, lon=lon, time=month_sel, method="nearest")[var].values

        def get_related_dataset(var):
            if var in S2D_FORECAST_DATA_VAR_NAMES:
                return forecast_ds
            elif var in S2D_CLIMATO_DATA_VAR_NAMES:
                return climato_ds
            else:
                return skill_ds

        zip_bytes = io.BytesIO(response.data)
        with zipfile.ZipFile(zip_bytes) as z:
            expected_file_basenames_and_months = {
                f"MeanTemp_{S2D_FILENAME_VALUES[forecast_type]}_Dec-Feb_ReleaseJan2025": datetime.datetime(2025, 12, 1),
                f"MeanTemp_{S2D_FILENAME_VALUES[forecast_type]}_Jun-Aug_ReleaseJan2025": datetime.datetime(2025, 6, 1),
            }
            ext_map = {
                DOWNLOAD_NETCDF_FORMAT: ".nc",
                DOWNLOAD_CSV_FORMAT: ".csv",
                DOWNLOAD_JSON_FORMAT: ".json",
            }
            expected_filenames = [f"{name}{ext_map[file_format]}" for name in expected_file_basenames_and_months]
            if file_format in [DOWNLOAD_CSV_FORMAT, DOWNLOAD_JSON_FORMAT]:
                expected_filenames += [f"metadata_{name}.txt" for name in expected_file_basenames_and_months]

            file_list = z.namelist()

            # Check expected files
            assert set(file_list) == set(expected_filenames)

            expected_coords = ["lat", "lon"]
            if forecast_type == S2D_FORECAST_TYPE_EXPECTED:
                expected_data_vars = [
                    "prob_below_normal",
                    "prob_near_normal",
                    "prob_above_normal",
                    "cutoff_below_normal_p33",
                    "historical_median_p50",
                    "cutoff_above_normal_p66",
                    "skill_CRPSS",
                    "skill_level",
                ]
            else:  # unusual
                expected_data_vars = [
                    'prob_unusually_low',
                    'prob_unusually_high',
                    'cutoff_unusually_low_p20',
                    "historical_median_p50",
                    'cutoff_unusually_high_p80',
                    "skill_CRPSS",
                    "skill_level",
                ]

            # Check contents of each file
            for filename in file_list:
                with z.open(filename) as f:
                    month = next(
                        (v for k, v in expected_file_basenames_and_months.items() if k in filename),
                        None)
                    assert month is not None

                    if filename.endswith(ext_map[DOWNLOAD_NETCDF_FORMAT]):
                        with tempfile.NamedTemporaryFile(suffix=".nc") as tmp:
                            tmp.write(f.read())
                            tmp.flush()
                            ds = xarray.open_dataset(tmp.name)

                        assert set(ds.coords) == set(expected_coords)
                        assert set(ds.data_vars) == set(expected_data_vars)
                        assert set(ds["lat"].values) == set(expected_lats)
                        assert set(ds["lon"].values) == set(expected_lons)
                        assert ds.dims == {"lat": len(expected_lats), "lon": len(expected_lons)}

                        for lat in ds["lat"].values:
                            for lon in ds["lon"].values:
                                for var in expected_data_vars:
                                    if [lat,lon] in expected_points:
                                        if var == "skill_level":
                                            assert ds.sel(lat=lat, lon=lon)[var].values == S2D_SKILL_LEVEL_STR[get_expected_value(lat, lon, var, month).item()]
                                        else:
                                            assert ds.sel(lat=lat, lon=lon)[var].values == get_expected_value(lat, lon, var, month)
                                    else:
                                        value = ds[var].sel(lat=lat, lon=lon).item()
                                        # Check for null values (for string or float data)
                                        assert value in ("", None) or (isinstance(value, float) and numpy.isnan(value))

                        # Check metadata
                        assert all([a in ds.attrs and forecast_ds.attrs[a] == ds.attrs[a] for a in forecast_ds.attrs])
                        assert 'time_period' in ds.attrs and ds.attrs['time_period'] == filename.split('_')[2]
                        for var in ds.data_vars:
                            related_dataset = get_related_dataset(var)
                            assert ds[var].attrs == related_dataset[var].attrs
                        for coord in ds.coords:
                            assert ds[coord].attrs == forecast_ds[coord].attrs

                    elif filename.endswith(ext_map[DOWNLOAD_CSV_FORMAT]):
                        df = pd.read_csv(f)
                        expected_columns = expected_coords + expected_data_vars
                        assert set(df.columns) == set(expected_columns)

                        for _, row in df.iterrows():
                            for var in expected_data_vars:
                                if var == "skill_level":
                                    assert row[var] == S2D_SKILL_LEVEL_STR[get_expected_value(row.lat, row.lon, var, month).item()]
                                else:
                                    assert numpy.isclose(row[var], get_expected_value(row.lat, row.lon, var, month).item())

                    elif filename.endswith(ext_map[DOWNLOAD_JSON_FORMAT]):
                        gdf = geopandas.read_file(io.BytesIO(f.read()))
                        expected_columns = expected_coords + expected_data_vars + ["geometry"]
                        assert set(gdf.columns) == set(expected_columns)

                        for _, row in gdf.iterrows():
                            for var in expected_data_vars:
                                if var == "skill_level":
                                    assert row[var] == S2D_SKILL_LEVEL_STR[get_expected_value(row.lat, row.lon, var, month).item()]
                                else:
                                    assert numpy.isclose(row[var], get_expected_value(row.lat, row.lon, var, month).item())
                            assert row["geometry"].geom_type == "Point"
                            assert row["geometry"].x == row["lon"]
                            assert row["geometry"].y == row["lat"]

                    elif filename.endswith(".txt"):
                        content = f.read().decode("utf-8")
                        expected_content = (
                            "=== Dataset global attributes ===\n"
                            "dataset: forecast\n"
                            f"time_period: {filename.split('_')[3]}\n\n"
                            "=== Coordinate: lat ===\n"
                            "coord_name: lat\n\n"
                            "=== Coordinate: lon ===\n"
                            "coord_name: lon\n"
                        )
                        for var in expected_data_vars:
                            expected_content += (
                                f"\n=== Variable: {var} ===\n"
                                f"var_name: {var}\n"
                            )
                        assert content == expected_content

    def test_bad_params(self, test_app, client):
        """Should return 400 if a param is not allowed."""

        initial_payload = {
            "var": S2D_VARIABLE_AIR_TEMP,
            "format": DOWNLOAD_NETCDF_FORMAT,
            "forecast_type": S2D_FORECAST_TYPE_EXPECTED,
            "frequency": S2D_FREQUENCY_SEASONAL,
            "periods": ["2025-06", "2025-12"],
            "points": [[51.7, -120]],
        }

        payload = initial_payload.copy()
        payload["var"] = "wrong_var"
        response = client.post("/download-s2d", json=payload)
        assert response.status_code == 400
        assert "Invalid variable" in response.text

        payload = initial_payload.copy()
        payload["format"] = "wrong_format"
        response = client.post("/download-s2d", json=payload)
        assert response.status_code == 400
        assert "Invalid format" in response.text

        payload = initial_payload.copy()
        payload["forecast_type"] = "wrong_forecast_type"
        response = client.post("/download-s2d", json=payload)
        assert response.status_code == 400
        assert "Invalid forecast type" in response.text

        payload = initial_payload.copy()
        payload["frequency"] = "wrong_freq"
        response = client.post("/download-s2d", json=payload)
        assert response.status_code == 400
        assert "Invalid frequency" in response.text

        payload = initial_payload.copy()
        for p in [["123-456"], ["2025-13"], ["2025/06"], ["2025-06-01"], ["June-2025"]]:
            payload["periods"] = p
            response = client.post("/download-s2d", json=payload)
            assert response.status_code == 400
            assert "Invalid periods" in response.text

class TestCheckPointsOrBbox:
    def test_points_and_bbox(self, test_app):
        with pytest.raises(ValueError) as excinfo:
            check_points_or_bbox([[1, 2]], [1, 2, 3, 4])
        assert "Can't request both points and bbox" in str(excinfo.value)

    def test_too_many_points(self, test_app):
        test_app.config["DOWNLOAD_POINTS_LIMIT"] = 10
        with pytest.raises(ValueError) as excinfo:
            check_points_or_bbox([[1, 2]] * 11, None)
        assert "Too many points requested" in str(excinfo.value)

    def test_invalid_point_length(self, test_app):
        with pytest.raises(ValueError) as excinfo:
            check_points_or_bbox([[1, 2, 3]], None)
        assert "Points must have exactly 2 coordinates" in str(excinfo.value)

    def test_bbox_invalid_length(self, test_app):
        with pytest.raises(ValueError) as excinfo:
            check_points_or_bbox(None, [1, 2, 3])
        assert "bbox must be an array of length 4" in str(excinfo.value)

    def test_neither_points_nor_bbox(self, test_app):
        with pytest.raises(ValueError) as excinfo:
            check_points_or_bbox(None, None)
        assert "Neither points or bbox requested" in str(excinfo.value)

    def test_empty_points(self, test_app):
        with pytest.raises(ValueError) as excinfo:
            check_points_or_bbox([], None)
        assert "Neither points or bbox requested" in str(excinfo.value)