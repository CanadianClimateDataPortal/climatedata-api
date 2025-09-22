import pytest
from flask import Flask

@pytest.fixture
def test_app():
    """
    A Flask app fixture for testing, with default settings loaded.
    """
    app = Flask(__name__)
    app.config.from_object("default_settings")
    with app.app_context():
        yield app
