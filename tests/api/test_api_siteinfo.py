import json

import pytest
import requests

from tests.configs import TEST_HOST_URL


@pytest.mark.parametrize(
    "url,status_code,validator",
    [
        pytest.param(
            f"{TEST_HOST_URL}/get-location-values/45.551538/-73.744616?dataset_name=cmip5",
            200,
            json.loads,
            id="get_location_values"
        )
    ]
)
def test_api_siteinfo(url: str, status_code: int, validator: callable):
    response = requests.get(url)
    assert response.status_code == status_code
    validator(response.content)
