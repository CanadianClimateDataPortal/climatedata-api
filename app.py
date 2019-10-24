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

@app.route('/get_values.php')
def get_values():
    try:
        lati = float(request.args['lat'])
        loni = float(request.args['lon'])
        var = request.args['var']
        month = request.args['month']
        month,msys = app.config['MONTH_LUT'][month]
        if var not in app.config['VARIABLES']:
            raise ValueError
    except (ValueError, BadRequestKeyError, KeyError):
        return "Bad request", 400

    dataset = open_dataset(var, msys, month)
    dataset['time'] = xr.decode_cf(dataset).time
    location_slice = dataset.sel(lon=loni, lat=lati, method='nearest')
    data_frame = location_slice.to_dataframe()
    dropped_data_frame = data_frame.drop(columns=['lon', 'lat'], axis=1)
    json_frame = dropped_data_frame.to_json(orient='values', date_format='epoch', date_unit='s')
    return(json_frame)

