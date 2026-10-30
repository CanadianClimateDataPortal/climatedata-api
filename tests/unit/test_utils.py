import pytest

from climatedata_api.utils import negotiated_response

class TestNegotiatedResponse:
    @pytest.mark.parametrize("headers", [{"Accept": "*/*"}, {}, {"Accept": "application/json"}, {"Accept": "text/html"}])
    def test_json_by_default(self, headers, test_app):
        """Should return JSON for any Accept header other than text/plain, or none."""
        with test_app.test_request_context(headers=headers):
            response = negotiated_response("message", 404)

        assert response.status_code == 404
        assert response.mimetype == "application/json"
        assert response.get_json() == "message"
        assert "Accept" in response.vary

    def test_text_plain_with_dict(self, test_app):
        """Should return JSON when text/plain is preferred but the value is not a str."""
        with test_app.test_request_context(headers={"Accept": "text/plain"}):
            response = negotiated_response({"key": 1.5})

        assert response.status_code == 200
        assert response.mimetype == "application/json"
        assert response.get_json() == {"key": 1.5}
        assert "Accept" in response.vary

    @pytest.mark.parametrize("accept", ["text/plain", "text/*"])
    def test_text_plain_with_str(self, accept, test_app):
        """Should return text/plain when text/plain is preferred and the value is a str."""
        with test_app.test_request_context(headers={"Accept": accept}):
            response = negotiated_response("message", 404)

        assert response.status_code == 404
        assert response.mimetype == "text/plain"
        assert response.get_data(as_text=True) == "message"
        assert "Accept" in response.vary

    def test_error_with_non_str(self, test_app):
        """Should raise TypeError if an error status is given a non-str value."""
        with test_app.test_request_context():
            with pytest.raises(TypeError):
                negotiated_response({"key": 1.5}, 404)

