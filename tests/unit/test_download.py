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

from default_settings import S2D_VARIABLE_AIR_TEMP, S2D_FORECAST_TYPE_EXPECTED, S2D_FREQUENCY_SEASONAL, \
    DOWNLOAD_NETCDF_FORMAT, DOWNLOAD_CSV_FORMAT, DOWNLOAD_JSON_FORMAT, S2D_FILENAME_VALUES, S2D_CLIMATO_DATA_VAR_NAMES, \
    S2D_FORECAST_DATA_VAR_NAMES
from tests.unit.utils import generate_s2d_test_datasets


class TestDownloadS2D:
    @pytest.mark.parametrize("file_format", [DOWNLOAD_NETCDF_FORMAT, DOWNLOAD_CSV_FORMAT, DOWNLOAD_JSON_FORMAT])
    @patch("climatedata_api.download.get_s2d_release_date", return_value="2025-01-01")
    @patch("climatedata_api.download.open_dataset_by_path")
    def test_valid_payload(self, mock_open_dataset, mock_release, test_app, client, file_format):
        lats = [43.0, 50.7, 61.04, 68.75, 73.82]
        lons = [-140.0, -130, -120, -98.54, -61.11]
        forecast_times = [f"2025-{month:02d}-01" for month in range(1, 13)]
        skill_times = [f"1991-{month:02d}-01" for month in range(1, 13)]
        forecast_ds, climato_ds, skill_ds = generate_s2d_test_datasets(lats, lons, forecast_times, skill_times, add_test_metadata=True)

        mock_open_dataset.side_effect = [forecast_ds, climato_ds, skill_ds]

        points = [[50.7, -120], [50.7, -140], [68.75, -140], [73.82, -98.54]]
        response = client.post("/download-s2d", json=
            { 'var' : S2D_VARIABLE_AIR_TEMP,
              'format' : file_format,
              'points': points,
              'forecast_type': S2D_FORECAST_TYPE_EXPECTED,
              'frequency': S2D_FREQUENCY_SEASONAL,
              'periods': ['2025-06', '2025-12']
            }
        )

        expected_zip_filename = "MeanTemp_ExpectedCond_Seasonal_ReleaseJan2025.zip"
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
            return expected_ds.sel(lat=lat, lon=lon, time=month_sel, method='nearest')[var].values

        def get_related_dataset(var):
            if var in S2D_FORECAST_DATA_VAR_NAMES:
                return forecast_ds
            elif var in S2D_CLIMATO_DATA_VAR_NAMES:
                return climato_ds
            else:
                return skill_ds

        zip_bytes = io.BytesIO(response.data)
        with (zipfile.ZipFile(zip_bytes) as z):
            expected_file_basenames_and_months = {
                "MeanTemp_ExpectedCond_Dec-Feb_ReleaseJan2025": datetime.datetime(2025, 12, 1),
                "MeanTemp_ExpectedCond_Jun-Aug_ReleaseJan2025": datetime.datetime(2025, 6, 1)
            }
            ext_map = {
                DOWNLOAD_NETCDF_FORMAT: ".nc",
                DOWNLOAD_CSV_FORMAT: ".csv",
                DOWNLOAD_JSON_FORMAT: ".json"
            }
            expected_filenames = [f"{name}{ext_map[file_format]}" for name in expected_file_basenames_and_months]
            if file_format in [DOWNLOAD_CSV_FORMAT, DOWNLOAD_JSON_FORMAT]:
                expected_filenames += [f"metadata_{name}.txt" for name in expected_file_basenames_and_months]

            file_list = z.namelist()

            # Check expected files
            assert set(file_list) == set(expected_filenames)

            expected_coords = ['lat', 'lon']
            expected_data_vars = ['prob_below_normal', 'prob_near_normal', 'prob_above_normal', 'cutoff_below_normal_p33', 'historical_median_p50', 'cutoff_above_normal_p66', 'skill_CRPSS', 'skill_level']
            expected_lats = [50.7,  68.75, 73.82]
            expected_lons = [-120.0, -140.0, -98.54]

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

                        assert all([k in ds.coords for k in expected_coords]) and len(ds.coords) == len(expected_coords)
                        assert all([k in ds for k in expected_data_vars]) and len(ds) == len(expected_data_vars)

                        assert set(ds["lat"].values) == set(expected_lats)
                        assert set(ds["lon"].values) == set(expected_lons)

                        assert ds.dims == {"lat": len(expected_lats), "lon": len(expected_lons)}

                        for lat in ds["lat"].values:
                            for lon in ds["lon"].values:
                                for var in expected_data_vars:
                                    if [lat, lon] in points:
                                        assert ds.sel(lat=lat, lon=lon)[var].values == get_expected_value(lat, lon, var, month)
                                    else:
                                        assert ds[var].sel(lat=lat, lon=lon).isnull().all()

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
                        assert all([k in df.columns for k in expected_columns]) and len(df.columns) == len(expected_columns)

                        for index, row in df.iterrows():
                            for var in expected_data_vars:
                                assert numpy.isclose(row[var], get_expected_value(row.lat, row.lon, var, month).item())

                    elif filename.endswith(ext_map[DOWNLOAD_JSON_FORMAT]):
                        gdf = geopandas.read_file(io.BytesIO(f.read()))

                        expected_columns = expected_coords + expected_data_vars + ["geometry"]
                        assert all([k in gdf.columns for k in expected_columns]) and len(gdf.columns) == len(expected_columns)

                        for index, row in gdf.iterrows():
                            for var in expected_data_vars:
                                assert numpy.isclose(row[var], get_expected_value(row.lat, row.lon, var, month).item())
                            assert row["geometry"].geom_type == "Point"
                            assert row["geometry"].x == row["lon"]
                            assert row["geometry"].y == row["lat"]
                    elif filename.endswith(".txt"):
                        content = f.read()
