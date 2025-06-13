import pytest
import requests

from climatedata_api.geomet import GEOMET_API_BASE_URL
from tests.configs import TEST_HOST_URL


@pytest.mark.parametrize(
    "url,status_code",
    [
        pytest.param(
            f"{TEST_HOST_URL}/get-geomet-collection-items-links/climate-normals?"
                    "CLIMATE_IDENTIFIER=2100800|7113534&sortby=MONTH&f=json&limit=150000&offset=0",
            200,
            id="get_geomet_collection_items_links",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/get-geomet-collection-items-links/climate-daily?"
                    "datetime=1967-03-01 00:00:00/2016-06-13 00:00:00&STN_ID=7341|29793|47427&sortby=PROVINCE_CODE,STN_ID,LOCAL_DATE&f=csv&limit=150000&offset=0",
            200,
            id="get_geomet_collection_items_links",
        ),
        pytest.param(
            f"{TEST_HOST_URL}/get-geomet-collection-items-links/invalid-collection",
            400,
            id="get_geomet_collection_items_links-invalid_collection",
        ),
    ],
)
def test_api_download_get(url: str, status_code: int):
    response = requests.get(url)
    assert response.status_code == status_code

    if response.status_code == 200:
        data = response.json()
        assert isinstance(data, list)
        for link in data:
            assert all(k in link for k in ["start_index", "end_index", "url"])
            assert isinstance(link["start_index"], int) and link["start_index"] >= 0
            assert isinstance(link["end_index"], int) and link["end_index"] >= link["start_index"]
            assert isinstance(link["url"], str) and link["url"].startswith(GEOMET_API_BASE_URL)
