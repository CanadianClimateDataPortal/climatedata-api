import pytest
import requests

from tests.configs import TEST_HOST_URL


@pytest.mark.parametrize(
    "url,status_code",
    [
        pytest.param(
            f"{TEST_HOST_URL}/download-30y/60.31062731740045/-100.06347656250001/tx_max/ann?decimals=2",
            200,
            id="download_30y",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/download-30y/60.31062731740045/-100.06347656250001/tx_max/ann?decimals=2&dataset_name=CMIP6",
            200,
            id="download_30y_cmip6",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/download-regional-30y/census/1/tx_max/ann?decimals=3",
            200,
            id="download_regional_30y",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/download-regional-30y/census/1/tx_max/ann?decimals=3&dataset_name=CMIP6",
            200,
            id="download_regional_30y_cmip6",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/download-ahccd?format=csv&stations=3081680,8400413",
            200,
            id="download_ahccd",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/download-ahccd?format=csv&stations=3081680,8400413&variable_type_filter=P",
            200,
            id="download_ahccd_with_filter",
        ),
    ],
)
def test_api_download_get(url: str, status_code: int):
    response = requests.get(url)
    assert response.status_code == status_code


@pytest.mark.parametrize(
    "url,status_code,payload",
    [
        pytest.param(
            f"{TEST_HOST_URL}/download",
            200,
            {
                "var": "tx_max",
                "month": "jan",
                "format": "json",
                "points": [
                    [45.6323041086555, -73.81242277462837],
                    [45.62317816394269, -73.71014590931205],
                    [45.62317725541931, -73.61542460410394],
                    [45.71149235185937, -73.6250345109122],
                ],
            },
            id="download_dataset_json",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/download",
            200,
            {
                "var": "tx_max",
                "month": "jan",
                "format": "json",
                "zipped": True,
                "points": [
                    [45.6323041086555, -73.81242277462837],
                    [45.62317816394269, -73.71014590931205],
                    [45.62317725541931, -73.61542460410394],
                    [45.71149235185937, -73.6250345109122],
                ],
            },
            id="download_dataset_json_zipped",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/download",
            200,
            {
                "var": "tx_max",
                "month": "jan",
                "format": "csv",
                "points": [
                    [45.6323041086555, -73.81242277462837],
                    [45.62317816394269, -73.71014590931205],
                    [45.62317725541931, -73.61542460410394],
                    [45.71149235185937, -73.6250345109122],
                ],
            },
            id="download_dataset_csv",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/download",
            200,
            {
                "var": "tx_max",
                "month": "jan",
                "format": "csv",
                "dataset_name": "CMIP5",
                "points": [
                    [45.6323041086555, -73.81242277462837],
                    [45.62317816394269, -73.71014590931205],
                    [45.62317725541931, -73.61542460410394],
                    [45.71149235185937, -73.6250345109122],
                ],
            },
            id="download_dataset_cmip5_csv",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/download",
            200,
            {
                "var": "tx_max",
                "month": "jan",
                "format": "csv",
                "dataset_name": "CMIP6",
                "points": [
                    [45.6323041086555, -73.81242277462837],
                    [45.62317816394269, -73.71014590931205],
                    [45.62317725541931, -73.61542460410394],
                    [45.71149235185937, -73.6250345109122],
                ],
            },
            id="download_dataset_cmip6_csv",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/download",
            200,
            {
                "var": "tx_max",
                "month": "jan",
                "format": "csv",
                "zipped": True,
                "dataset_name": "CMIP6",
                "points": [
                    [45.6323041086555, -73.81242277462837],
                    [45.62317816394269, -73.71014590931205],
                    [45.62317725541931, -73.61542460410394],
                    [45.71149235185937, -73.6250345109122],
                ],
            },
            id="download_dataset_cmip6_csv_zipped",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/download",
            200,
            {
                "var": "tx_max",
                "month": "jan",
                "format": "netcdf",
                "dataset_name": "CMIP5",
                "points": [
                    [45.6323041086555, -73.81242277462837],
                    [45.62317816394269, -73.71014590931205],
                    [45.62317725541931, -73.61542460410394],
                    [45.71149235185937, -73.6250345109122],
                ],
            },
            id="download_dataset_cmip5_netcdf",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/download",
            200,
            {
                "var": "tx_max",
                "month": "jan",
                "format": "netcdf",
                "dataset_name": "CMIP6",
                "points": [
                    [45.6323041086555, -73.81242277462837],
                    [45.62317816394269, -73.71014590931205],
                    [45.62317725541931, -73.61542460410394],
                    [45.71149235185937, -73.6250345109122],
                ],
            },
            id="download_dataset_cmip6_netcdf",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/download",
            200,
            {
                "var": "slr",
                "month": "ann",
                "format": "csv",
                "points": [
                    [55.615479404227266, -80.75205994818893],
                    [55.615479404227266, -80.57633593798099],
                    [55.54715240897225, -80.63124969117096],
                    [55.54093496609795, -80.70812894563693],
                ],
            },
            id="download_dataset_slr_csv",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/download",
            200,
            {
                "var": "allowance",
                "month": "ann",
                "format": "csv",
                "dataset_name": "CMIP6",
                "points": [
                    [55.615479404227266, -80.75205994818893],
                    [55.615479404227266, -80.57633593798099],
                    [55.54715240897225, -80.63124969117096],
                    [55.54093496609795, -80.70812894563693],
                ]
            },
            id="download_dataset_allowance_csv",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/download",
            200,
            {
                "var": "spei_3m",
                "month": "dec",
                "format": "csv",
                "points": [
                    [49.97204543090215, -72.96164786407991],
                    [49.05163514254488, -74.11483668106955],
                    [48.23844206206425, -75.04837048529926],
                    [47.16613204524246, -76.00386979080497],
                ],
            },
            id="download_dataset_spei_3m_csv",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/download",
            200,
            {
                "var": "tx_max",
                "month": "ann",
                "format": "csv",
                "bbox": [45.704236999914066, -72.1259641636298, 45.86229102811587, -71.6173341617058],
            },
            id="download_dataset_bbox_csv",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/download",
            200,
            {
                "var": "tx_max",
                "month": "ann",
                "format": "netcdf",
                "bbox": [45.704236999914066, -72.1259641636298, 45.86229102811587, -71.6173341617058],
            },
            id="download_dataset_bbox_netcdf",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/download-ahccd",
            200,
            {"format": "csv", "stations": ["3081680", "8400413"]},
            id="download_ahccd_csv",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/download-ahccd",
            200,
            {"format": "csv", "stations": ["3034720"]},
            id="download_ahccd_station_precip_only_csv",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/download-ahccd",
            200,
            {"format": "csv", "stations": ["8402757"]},
            id="download_ahccd_station_temp_only_csv",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/download-ahccd",
            200,
            {"format": "csv", "stations": ["3034720", "8402757"]},
            id="download_ahccd_station_one_precip_one_temp_csv",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/download-ahccd",
            200,
            {"format": "csv", "zipped": True, "stations": ["3034720", "8402757"]},
            id="download_ahccd_station_one_precip_one_temp_zipped_csv",
        ),
    ],
)
def test_api_download_post(url: str, status_code: int, payload):
    response = requests.post(url, json=payload)
    assert response.status_code == status_code


@pytest.mark.parametrize(
    "url,expected_filename,payload",
    [
        pytest.param(
            f"{TEST_HOST_URL}/download",
            "random_csv_test_name.zip",
            {
                "var": "tx_max",
                "month": "ann",
                "format": "csv",
                "zipped": True,
                "custom_filename": "random_csv_test_name",
                "bbox": [45.704236999914066, -72.1259641636298, 45.86229102811587, -71.6173341617058],
            },
            id="download_zipped_custom_filename_csv",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/download",
            "random_json_test_name.zip",
            {
                "var": "tx_max",
                "month": "ann",
                "format": "json",
                "zipped": True,
                "custom_filename": "random_json_test_name",
                "bbox": [45.704236999914066, -72.1259641636298, 45.86229102811587, -71.6173341617058],
            },
            id="download_zipped_custom_filename_json",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/download",
            "random_netcdf_test_name.nc",
            {
                "var": "tx_max",
                "month": "ann",
                "format": "netcdf",
                "custom_filename": "random_netcdf_test_name",
                "bbox": [45.704236999914066, -72.1259641636298, 45.86229102811587, -71.6173341617058],
            },
            id="download_custom_filename_netcdf",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/download",
            "tx_max.nc",
            {
                "var": "tx_max",
                "month": "ann",
                "format": "netcdf",
                "bbox": [45.704236999914066, -72.1259641636298, 45.86229102811587, -71.6173341617058],
            },
            id="download_standard_filename_netcdf",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/download",
            "tx_max.zip",
            {
                "var": "tx_max",
                "month": "ann",
                "format": "json",
                "zipped": True,
                "bbox": [45.704236999914066, -72.1259641636298, 45.86229102811587, -71.6173341617058],
            },
            id="download_nozip_standard_filename_json",
        ),
    ],
)
def test_api_download_post_custom_filename(url, expected_filename, payload):
    response = requests.post(url, json=payload)
    filename = get_filename_from_content_disposition(response.headers["Content-Disposition"])
    assert filename == expected_filename


def get_filename_from_content_disposition(content_disposition):
    if not content_disposition:
        return None
    filename = [fn for fn in content_disposition.split(";") if "filename=" in fn]

    if filename and len(filename) == 1:
        return filename[0].split("=")[1]
    return None
