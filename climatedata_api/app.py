import pandas as pd
import requests
import sentry_sdk
from werkzeug.exceptions import BadRequest
import xarray as xr
from flask import Flask
from sentry_sdk.integrations.flask import FlaskIntegration

from climatedata_api.charts import generate_charts, generate_regional_charts
from climatedata_api.download import (download, download_30y, download_ahccd,
                                      download_regional_30y)
from climatedata_api.geomet import get_geomet_collection_download_links
from climatedata_api.map import (get_allowance_gridded_values,
                                 get_choro_values,
                                 get_delta_30y_gridded_values,
                                 get_delta_30y_regional_values,
                                 get_slr_gridded_values,
                                 get_id_list_from_points)
from climatedata_api.siteinfo import (get_location_values,
                                      get_location_values_allyears)
from climatedata_api.raster import get_raster_route
from climatedata_api.utils import generate_kdtrees

pd.set_option('display.max_rows', 10000)
xr.set_options(keep_attrs=True)

app = Flask(__name__)
app.config.from_object('default_settings')
app.config.from_envvar('CLIMATEDATA_FLASK_SETTINGS', silent=True)

if 'SENTRY_DSN' in app.config:
    sentry_sdk.init(
        app.config['SENTRY_DSN'],
        environment=app.config['SENTRY_ENV'],
        integrations=[FlaskIntegration()]
    )

# charts routes
app.add_url_rule('/generate-charts/<lat>/<lon>/<var>/<month>', view_func=generate_charts)
app.add_url_rule('/generate-charts/<lat>/<lon>/<var>', view_func=generate_charts)
app.add_url_rule('/generate-regional-charts/<partition>/<index>/<var>/<month>', view_func=generate_regional_charts)
app.add_url_rule('/generate-regional-charts/<partition>/<index>/<var>', view_func=generate_regional_charts)

# map routes
app.add_url_rule('/get-choro-values/<partition>/<var>/<scenario>/<month>/', view_func=get_choro_values)
app.add_url_rule('/get-choro-values/<partition>/<var>/<scenario>', view_func=get_choro_values)
app.add_url_rule('/get-delta-30y-gridded-values/<lat>/<lon>/<var>/<month>', view_func=get_delta_30y_gridded_values)
app.add_url_rule('/get-slr-gridded-values/<lat>/<lon>', view_func=get_slr_gridded_values)
app.add_url_rule('/get-allowance-gridded-values/<lat>/<lon>', view_func=get_allowance_gridded_values)
app.add_url_rule('/get-delta-30y-regional-values/<partition>/<index>/<var>/<month>',
                 view_func=get_delta_30y_regional_values)
app.add_url_rule('/get-gids/<compressed_points>', view_func=get_id_list_from_points)

# download routes
app.add_url_rule('/download', view_func=download, methods=['POST'])
app.add_url_rule('/download-ahccd', view_func=download_ahccd, methods=['GET', 'POST'])
app.add_url_rule('/download-30y/<lat>/<lon>/<var>/<month>', view_func=download_30y)
app.add_url_rule('/download-regional-30y/<partition>/<index>/<var>/<month>', view_func=download_regional_30y)

# various site information
app.add_url_rule('/get_location_values_allyears.php', view_func=get_location_values_allyears)
app.add_url_rule('/get-location-values/<lat>/<lon>', view_func=get_location_values)

# raster routes
app.add_url_rule('/raster', view_func=get_raster_route)

# geomet routes
app.add_url_rule('/get-geomet-collection-items-links/<collectionId>', view_func=get_geomet_collection_download_links)

@app.errorhandler(BadRequest)
def handle_bad_request(e):
    return f"Bad request: {e.description}", 400


@app.route('/status')
def check_status():
    """
        Check server status (geoserver + this app)
        curl 'http://localhost:5000/status'
        Return 200 OK if everything is fine
        Return 500 if one component is broken
    """
    for check in app.config['SYSTEM_CHECKS']:
        try:
            r = requests.get(check['URL'].format(app.config['SYSTEM_CHECKS_HOST']), timeout=5)
            if r.status_code != 200:
                raise Exception('Status not 200')
            check['validator'](r.content)
        except Exception as ex:
            if app.config['DEBUG']:
                raise ex
            else:
                return f"Test {check['name']} failed.<br> Message: {ex}", 500
    return "All checks successful", 200


@app.cli.command("generate-kdtrees")
def cli_generate_kdtrees():
    generate_kdtrees()
