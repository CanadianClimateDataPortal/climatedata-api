import pytest
import requests

from tests.configs import TEST_HOST_URL


@pytest.mark.parametrize(
    "url,status_code",
    [
        pytest.param(
            f"{TEST_HOST_URL}/generate-charts/60.31062731740045/-100.06347656250001/tx_max/ann",
            200,
            id="generate_charts",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/generate-charts/60.31062731740045/-100.06347656250001/tx_max/ann?dataset_name=CMIP6",
            200,
            id="generate_charts_cmip6",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/generate-charts/60.31062731740045/-100.06347656250001/spei_12m/ann",
            200,
            id="generate_spei_charts",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/generate-charts/58.031372421776396/-61.12792968750001/slr/ann",
            200,
            id="generate_slr_charts",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/generate-charts/58.031372421776396/-61.12792968750001/allowance/ann?dataset_name=CMIP6",
            200,
            id="generate_allowance_charts",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/generate-regional-charts/census/4510/tx_max/ann",
            200,
            id="generate_regional_charts"
        ),
        pytest.param(
            f"{TEST_HOST_URL}/generate-regional-charts/census/4510/tx_max/ann?dataset_name=CMIP6",
            200,
            id="generate_regional_charts_cmip6",
        ),
    ],
)
def test_api_charts(url: str, status_code: int):
    response = requests.get(url)
    assert response.status_code == status_code
