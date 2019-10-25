from flask import Flask, request, jsonify
from werkzeug.exceptions import BadRequestKeyError
import sentry_sdk
import xarray as xr
import pandas as pd
import sys
import glob
import sentry_sdk
from sentry_sdk.integrations.flask import FlaskIntegration


app = Flask(__name__)
app.config.from_object('default_settings')
app.config.from_envvar('CLIMATEDATA_FLASK_SETTINGS', silent=True)

if 'SENTRY_DSN' in app.config:
    sentry_sdk.init(
        app.config['SENTRY_DSN'],
        integrations=[FlaskIntegration()]
    )

pd.set_option('display.max_rows', 10000)




def open_dataset(var, msys, month):
    for filename in app.config['NETCDF_FILENAME_FORMATS']:
        try:
            dataseturl = filename.format(root=app.config['NETCDF_ROOT_FOLDER'],
                                   var=var,
                                   msys=msys,
                                   month=month)
            return xr.open_dataset(dataseturl, decode_times=False)
        except FileNotFoundError:
            pass
    raise FileNotFoundError("Dataset not found")


def get_dataset_values(args, json_format, download=False):
    try:
        lati = float(args['lat'])
        loni = float(args['lon'])
        var = args['var']
        month = request.args.get('month','ann')
        monthpath,msys = app.config['MONTH_LUT'][month]
        if var not in app.config['VARIABLES']:
            raise ValueError
    except (ValueError, BadRequestKeyError, KeyError):
        return "Bad request", 400

    dataset = open_dataset(var, msys, monthpath)
    dataset['time'] = xr.decode_cf(dataset).time
    location_slice = dataset.sel(lon=loni, lat=lati, method='nearest')
    data_frame = location_slice.to_dataframe()
    dropped_data_frame = data_frame.drop(columns=['lon', 'lat'], axis=1)
    json_frame = dropped_data_frame.to_json(**json_format)
    if download:
        if msys == 'YS':
            return """[{{"variable": "{var}",
"calculated": "by year",
"latitude": {lat},
"longitude": {lon}}},
{{"data":
""".format(var=var,lat=lati, lon=loni) + json_frame + '}]'
        else:
            return """[{{"variable": "{var}",
"calculated": "by month",
"month": "{month}",
"latitude": {lat},
"longitude": {lon}}},
{{"data":
""".format(var=var, lat=lati, lon=loni, month=app.config['MONTH_OUTPUT_LUT'][month]) + json_frame + '}]'
    else:
        return json_frame

@app.route('/get_values.php')
def get_values():
    return get_dataset_values(request.args, {'orient': 'values', 'date_format': 'epoch', 'date_unit': 's'})

@app.route('/download_csv.php')
def download_csv():
    return get_dataset_values(request.args, {'orient': 'index', 'date_format': 'iso', 'date_unit': 's'}, download=True)
