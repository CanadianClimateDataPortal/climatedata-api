from unittest.mock import patch

from climatedata_api.download import download_s2d
from default_settings import S2D_VARIABLE_AIR_TEMP, S2D_FORECAST_TYPE_EXPECTED, S2D_FREQUENCY_SEASONAL, \
    DOWNLOAD_NETCDF_FORMAT, DOWNLOAD_CSV_FORMAT
from tests.unit.utils import generate_s2d_test_datasets


class TestDownloadS2D:
    @patch("climatedata_api.download.get_s2d_release_date", return_value="2025-01-01")
    @patch("climatedata_api.download.open_dataset_by_path")
    def test_valid_payload(self, mock_open_dataset, mock_release, test_app, client):
        lats = [43.0, 50.7, 61.04, 68.75, 73.82]
        lons = [-140.0, -130, -120, -98.54, -61.11]
        forecast_times = [f"2025-{month:02d}-01" for month in range(1, 13)]
        skill_times = [f"1991-{month:02d}-01" for month in range(1, 13)]
        forecast_ds, climato_ds, skill_ds = generate_s2d_test_datasets(lats, lons, forecast_times, skill_times, add_test_metadata=True)



        mock_open_dataset.side_effect = [forecast_ds, climato_ds, skill_ds]


        # response = client.post("/download-s2d", json=
        #     { 'var' : S2D_VARIABLE_AIR_TEMP,
        #       'format' : DOWNLOAD_NETCDF_FORMAT,
        #       'points': [[50.7, -120], [68.75, -140], [73.82, -98.54]],
        #       'forecast_type': S2D_FORECAST_TYPE_EXPECTED,
        #       'frequency': S2D_FREQUENCY_SEASONAL,
        #       'periods': ['2025-06', '2025-12']
        #     }
        # )
        # assert response.status_code == 200
        # assert response.get_json() == {"echo": "bar"}

        response = client.post("/download-s2d", json=
            { 'var' : S2D_VARIABLE_AIR_TEMP,
              'format' : DOWNLOAD_CSV_FORMAT,
              'points': [[50.7, -120], [68.75, -140], [73.82, -98.54]],
              'forecast_type': S2D_FORECAST_TYPE_EXPECTED,
              'frequency': S2D_FREQUENCY_SEASONAL,
              'periods': ['2025-06', '2025-12']
            }
        )
        assert response.status_code == 200
        assert response.get_json() == {"echo": "bar"}
