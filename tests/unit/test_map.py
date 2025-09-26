import numpy as np
import xarray as xr
from unittest.mock import patch

from climatedata_api.map import get_s2d_release_date

class TestGetS2DReleaseDate:
    def test_valid_xarray(self, test_app):
        times = np.array(['2025-09-01T00:00:00.000000000', '2025-10-01T00:00:00.000000000',
                          '2025-08-01T00:00:00.000000000', '2026-02-01T00:00:00.000000000'], dtype='datetime64[ns]')
        test_dataset = xr.Dataset(coords={"time": times})

        # Patch open_dataset_by_path to return the test data dataset
        with patch('climatedata_api.map.open_dataset_by_path', return_value=test_dataset):
            result = get_s2d_release_date('air_temp', 'seasonal')

        assert result == '2025-08-01'


    def test_bad_param(self, test_app):
        """Should return 400 if a param is not allowed."""

        response, status = get_s2d_release_date('wrong_var', 'seasonal')
        assert status == 400
        assert response == "Bad request"

        response, status = get_s2d_release_date('air_temp', 'wrong_freq')
        assert status == 400
        assert response == "Bad request"
