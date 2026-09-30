import pytest
import json
from unittest.mock import patch, MagicMock
from climatedata_api.raster import calculate_hash, post_raster_route, get_raster
from climatedata_api.app import app


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


class TestPostRasterFunction:
    """Tests for the post_raster_route() function with parameter validation"""

    @pytest.fixture
    def app_context(self):
        """Setup Flask app context for testing"""
        with app.app_context():
            yield app

    def _setup_route_mocks(self, mock_get_raster, mock_decode):
        """Configure common mock values used by post_raster_route tests."""
        mock_decode.return_value = "https://climatedata.crim.ca/test"
        mock_get_raster.return_value = None

    def _invoke_post_raster_route(self, app_context, method, payload=None):
        """Invoke post_raster_route with mocking."""
        app_context.config['ALLOWED_DOMAINS'] = ['climatedata.crim.ca']
        # Mock functions related to file handling
        with patch('climatedata_api.raster.send_file'):
            with patch('climatedata_api.raster.os.unlink'):
                with patch('builtins.open'):
                    with app_context.test_request_context(
                        '/raster?url=test_encoded_url',
                        method=method,
                        json=payload,
                    ):
                        return post_raster_route()

    @patch('climatedata_api.raster.decode_and_validate_url')
    @patch('climatedata_api.raster.get_raster')
    def test_post_raster_route_valid_payload(self, mock_get_raster, mock_decode, app_context):
        """Tests post_raster_route with a valid POST payload"""

        self._setup_route_mocks(mock_get_raster, mock_decode)

        test_location_popup_html = ["<div>Test HTML</div>", "<div>Test 2 HTML</div>"]
        test_marker = [45.5, -120.3]
        self._invoke_post_raster_route(
            app_context,
            method='POST',
            payload={
                'locationPopupHtml': test_location_popup_html,
                'markerLatLon': test_marker,
            },
        )

        mock_get_raster.assert_called_once()
        call_args = mock_get_raster.call_args
        assert call_args[0][2] == test_location_popup_html  # location_popup_html argument
        assert call_args[0][3] == test_marker  # marker_lat_lon argument

    @patch('climatedata_api.raster.decode_and_validate_url')
    @patch('climatedata_api.raster.get_raster')
    def test_post_raster_route_with_invalid_location_popup_html(self, mock_get_raster, mock_decode, app_context):
        """Tests if post_raster_route validates locationPopupHtml parameter"""

        self._setup_route_mocks(mock_get_raster, mock_decode)
        self._invoke_post_raster_route(
            app_context,
            method='POST',
            payload={
                'locationPopupHtml': ["<div>1</div>", "<div>2</div>", "<div>3</div>"],  # Invalid: 3 items
                'markerLatLon': [45.5, -120.3],
            },
        )

        call_args = mock_get_raster.call_args
        assert call_args[0][2] is None  # location_popup_html is None (failed validation)
        assert call_args[0][3] == [45.5, -120.3]  # marker_lat_lon argument

    @patch('climatedata_api.raster.decode_and_validate_url')
    @patch('climatedata_api.raster.get_raster')
    def test_post_raster_route_with_invalid_marker(self, mock_get_raster, mock_decode, app_context):
        """Tests if post_raster_route validates markerLatLon parameter"""

        self._setup_route_mocks(mock_get_raster, mock_decode)
        self._invoke_post_raster_route(
            app_context,
            method='POST',
            payload={
                'locationPopupHtml': ["<div>Test</div>"],
                'markerLatLon': [45.5, -120.3, 100],  # Invalid: 3 values instead of 2
            },
        )

        call_args = mock_get_raster.call_args
        assert call_args[0][2] == ["<div>Test</div>"]  # location_popup_html argument
        assert call_args[0][3] is None  # marker_lat_lon is None (failed validation)

    @patch('climatedata_api.raster.decode_and_validate_url')
    @patch('climatedata_api.raster.get_raster')
    def test_post_raster_route_backward_compatibility_no_params(self, mock_get_raster, mock_decode, app_context):
        """Test backward compatibility - GET request without new parameters"""

        self._setup_route_mocks(mock_get_raster, mock_decode)
        self._invoke_post_raster_route(app_context, method='GET')

        # Verify get_raster was called with None values (no params provided)
        call_args = mock_get_raster.call_args
        assert call_args[0][2] is None  # location_popup_html argument
        assert call_args[0][3] is None  # marker_lat_lon argument


class TestGetRasterFunction:
    """Tests for the get_raster() function to verify JavaScript execution"""

    def _setup_mock_driver(self, mock_get_driver):
        """Create and register a mock Selenium driver."""
        mock_driver = MagicMock()
        mock_driver.find_element.return_value = MagicMock()
        mock_get_driver.return_value = mock_driver
        return mock_driver

    def _invoke_get_raster(self, *args):
        """Invoke get_raster with common wait/sleep patching."""
        with patch('climatedata_api.raster.WebDriverWait'):
            with patch('climatedata_api.raster.time.sleep'):
                get_raster(*args)

    @patch('climatedata_api.raster.get_selenium_driver')
    def test_get_raster_no_payload(self, mock_get_driver):
        """Test that prepare_raster is called without params when none provided"""

        mock_driver = self._setup_mock_driver(mock_get_driver)
        self._invoke_get_raster("http://test.com", "/tmp/test.png")

        # Verify execute_script was called with correct function
        calls = mock_driver.execute_script.call_args_list
        assert any("$.fn.prepare_raster();" in str(call) for call in calls)

    @patch('climatedata_api.raster.get_selenium_driver')
    def test_get_raster_with_payload(self, mock_get_driver):
        """Test that prepare_raster is called WITH params when provided"""

        mock_driver = self._setup_mock_driver(mock_get_driver)

        location_popup_html = ["<div>Test</div>"]
        marker = [45.5, -120.3]

        self._invoke_get_raster("http://test.com", "/tmp/test.png", location_popup_html, marker)

        calls = mock_driver.execute_script.call_args_list

        # Find the call with parameters
        param_call = None
        for call in calls:
            if call[0] and call[0][0] and "prepare_raster" in call[0][0]:
                param_call = call[0][0]
                break

        assert param_call is not None, "prepare_raster call not found"

        # Verify the JavaScript contains JSON-serialized parameters
        assert json.dumps(location_popup_html) in param_call
        assert json.dumps(marker) in param_call
        assert "$.fn.prepare_raster(" in param_call
