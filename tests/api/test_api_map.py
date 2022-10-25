import pytest
import requests

from tests.configs import TEST_HOST_URL


@pytest.mark.parametrize(
    "url,status_code",
    [
        pytest.param(
            f"{TEST_HOST_URL}/get-choro-values/census/tx_max/rcp85/ann/?period=1971",
            200,
            id="get_choro_values"
        ),
        pytest.param(
            f"{TEST_HOST_URL}/get-choro-values/census/tx_max/ssp585/ann/?period=2071&dataset_name=CMIP6&delta7100=true",
            200,
            id="get_choro_values_cmip6",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/get-delta-30y-gridded-values/60.31062731740045/-100.06347656250001/tx_max/ann?period=1951&decimals=2",
            200,
            id="get_delta_30y_gridded_values",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/get-delta-30y-gridded-values/60.31062731740045/-100.06347656250001/tx_max/ann?period=1951&decimals=2&delta7100=true",
            200,
            id="get_delta_30y_gridded_values_delta7100",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/get-delta-30y-gridded-values/60.31062731740045/-100.06347656250001/tx_max/ann?period=1951&decimals=2&dataset_name=CMIP6",
            200,
            id="get_delta_30y_gridded_values_cmip6",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/get-slr-gridded-values/61.04/-61.11?period=2050",
            200,
            id="get_slr_gridded_values"
        ),
        pytest.param(
            f"{TEST_HOST_URL}/get-delta-30y-regional-values/census/4510/tx_max/ann?period=1951",
            200,
            id="get_delta_30y_regional_values",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/get-delta-30y-regional-values/census/4510/tx_max/ann?period=1951&delta7100=true",
            200,
            id="get_delta_30y_regional_values_delta7100",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/get-delta-30y-regional-values/census/4510/tx_max/ann?period=1951&dataset_name=CMIP6",
            200,
            id="get_delta_30y_regional_values_cmip6",
        ),
    ],
)
def test_api_map(url: str, status_code: int):
    response = requests.get(url)
    assert response.status_code == status_code
