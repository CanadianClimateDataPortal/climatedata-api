"""CORS policy tests for browser clients."""

from climatedata_api.app import app


ALLOWED_ORIGINS = (
    "https://climatedata.ca",
    "https://donneesclimatiques.ca",
    "https://dev-en.climatedata.ca",
    "https://dev-fr.climatedata.ca",
)


def test_allowed_origins_receive_cors_header():
    client = app.test_client()

    for origin in ALLOWED_ORIGINS:
        response = client.get("/", headers={"Origin": origin})
        assert response.status_code == 200
        assert response.headers["Access-Control-Allow-Origin"] == origin
        assert response.headers["Vary"] == "Origin"


def test_unlisted_origin_does_not_receive_cors_header():
    response = app.test_client().get(
        "/", headers={"Origin": "https://example.com"}
    )

    assert response.status_code == 200
    assert "Access-Control-Allow-Origin" not in response.headers


def test_json_post_preflight_is_allowed_for_dev_frontend():
    response = app.test_client().options(
        "/raster",
        headers={
            "Origin": "https://dev-en.climatedata.ca",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "Content-Type",
        },
    )

    assert response.status_code == 200
    assert response.headers["Access-Control-Allow-Origin"] == "https://dev-en.climatedata.ca"
    assert "POST" in response.headers["Access-Control-Allow-Methods"]
    assert response.headers["Access-Control-Allow-Headers"] == "Content-Type"
