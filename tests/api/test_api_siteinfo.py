import json

import pytest
import requests

from tests.configs import TEST_HOST_URL


@pytest.mark.parametrize(
    "url,status_code,expected",
    [
        pytest.param(
            f"{TEST_HOST_URL}/get-location-values/45.551538/-73.744616?dataset_name=cmip5",
            200,
            {
                "tg_mean": {
                    "1951": "6.3",
                    "1971": "6.6",
                    "2021": "8.9",
                    "2051": "10.9",
                    "2071": "12.5"
                },
                "prcptot": {
                    "1951": "966.6",
                    "1971": "1002.4",
                    "delta_2021_percent": "8",
                    "delta_2051_percent": "12",
                    "delta_2071_percent": "15"}
            },
            id="get_location_values"
        )
    ]
)
def test_api_siteinfo_json_loads(url: str, status_code: int, expected: dict):
    response = requests.get(url)
    content = json.loads(response.content)
    assert response.status_code == status_code
    assert content == expected
