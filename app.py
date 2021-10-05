from flask import Flask
import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration
import requests
from charts import generate_charts, generate_regional_charts
from map import get_choro_values
from download import download,download_ahccd
from siteinfo import get_location_values_allyears

import pandas as pd

pd.set_option('display.max_rows', 10000)

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
app.add_url_rule('/get-choro-values/<partition>/<var>/<model>/<month>/', view_func=get_choro_values)
app.add_url_rule('/get-choro-values/<partition>/<var>/<model>', view_func=get_choro_values)

# download routes
app.add_url_rule('/download', view_func=download, methods=['POST'])
app.add_url_rule('/download-ahccd', view_func=download_ahccd, methods=['GET', 'POST'])

# various site information
app.add_url_rule('/get_location_values_allyears.php', view_func=get_location_values_allyears)


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
                return "Test {} failed.<br> Message: {}".format(check['name'], ex), 500
    return "All checks successful", 200
