import pytest
from flask import Flask

from climatedata_api.download import download_s2d


@pytest.fixture
def test_app():
    """
    A Flask app fixture for testing, with default settings loaded.
    """
    app = Flask(__name__)
    app.config.from_object("default_settings")

    # register endpoints to test
    app.add_url_rule("/download-s2d", view_func=download_s2d, methods=["POST"])

    with app.app_context():
        yield app

@pytest.fixture
def client(test_app):
    test_app.testing = True
    with test_app.test_client() as client:
        yield client
