import pytest
from climatedata_api.raster import calculate_hash


@pytest.mark.parametrize(
    "s,expected",
    [
        ("a", 97),
        ("12345678", -1861353340),
        ("12345678901", 745463030),
        ("https://climatedata.crim.ca/explore/variable/?coords=62.5325943454858,-98.48144531250001,4&delta=&dataset=cmip6&geo-select=&var=ice_days&var-group=other&mora=ann&rcp=ssp585&decade=1970s&sector=", -1241807304)
    ]
)
def test_calculate_hash(s, expected):
    assert calculate_hash(s) == expected
