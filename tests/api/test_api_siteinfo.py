import json

import pytest
import requests

from tests.configs import TEST_HOST_URL


def _check_json_validity(response_content):
    try:
        json.loads(response_content)
    except ValueError:
        return False
    return True


@pytest.mark.parametrize(
    "url,status_code",
    [
        pytest.param(
            f"{TEST_HOST_URL}/get-location-values/45.551538/-73.744616?dataset_name=cmip5",
            200,
            id="get_location_values"
        )
    ]
)
def test_api_siteinfo_json_loads(url: str, status_code: int):
    response = requests.get(url)
    is_json_valid = _check_json_validity(response.content)
    assert response.status_code == status_code
    assert is_json_valid
