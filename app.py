from flask import Flask, request, jsonify, Response
from werkzeug.exceptions import BadRequestKeyError
import sentry_sdk
import xarray as xr
import pandas as pd
import sys
import glob
import sentry_sdk
from textwrap import dedent
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
            return dedent("""\
                [{{"variable": "{var}",
                "calculated": "by year",
                "latitude": {lat},
                "longitude": {lon}}},
                {{"data":
                """.format(var=var,lat=lati, lon=loni)) + json_frame + '}]'
        else:
            return dedent("""\
                [{{"variable": "{var}",
                "calculated": "by month",
                "month": "{month}",
                "latitude": {lat},
                "longitude": {lon}}},
                {{"data":
                """.format(var=var, lat=lati, lon=loni, month=app.config['MONTH_OUTPUT_LUT'][month])) + json_frame + '}]'
    else:
        return json_frame

@app.route('/get_values.php')
def get_values():
    return Response(get_dataset_values(request.args, {'orient': 'values', 'date_format': 'epoch', 'date_unit': 's'}),
                    mimetype='application/json')

@app.route('/download_csv.php')
def download_csv():
    return Response(get_dataset_values(request.args, {'orient': 'index', 'date_format': 'iso', 'date_unit': 's'}, download=True),
                    mimetype='application/json')

@app.route('/get_location_values_allyears.php')
def get_location_values_allyears():
    try:
        lati = float(request.args['lat'])
        loni = float(request.args['lon'])
    except (ValueError, BadRequestKeyError, KeyError):
        return "Bad request", 400

    anusplin_dataset = xr.open_dataset(app.config['NETCDF_ROOT_FOLDER'] + "/locations/SearchLocation_30yAvg_anusplin_nrcan_canada_tg_mean_prcptot_YS.nc")

    anusplin_1950_location_slice = anusplin_dataset.sel(time='1950-01-01', lat=lati, lon=loni, method='nearest')
    anusplin_1980_location_slice = anusplin_dataset.sel(time='1980-01-01', lat=lati, lon=loni, method='nearest')
    anusplin_2005_location_slice = anusplin_dataset.sel(time='2005-01-01', lat=lati, lon=loni, method='nearest')

    anusplin_1950_temp = float(anusplin_1950_location_slice.tg_mean.values)
    anusplin_1950_temp = anusplin_1950_temp - 273.15
    anusplin_1950_temp = round(anusplin_1950_temp, 1)

    anusplin_1980_precip = float(anusplin_1980_location_slice.prcptot.values)
    anusplin_1980_precip = round(anusplin_1980_precip)

    anusplin_2005_temp = float(anusplin_2005_location_slice.tg_mean.values)
    anusplin_2005_temp = anusplin_2005_temp - 273.15
    anusplin_2005_temp = round(anusplin_2005_temp, 1)

    bcc_dataset = xr.open_dataset(app.config['NETCDF_ROOT_FOLDER'] + "/locations/SearchLocation_30yAvg_wDeltas_BCCAQv2+ANUSPLIN300_rcp85_tg_mean_prcptot_YS.nc")

    bcc_2020_location_slice = bcc_dataset.sel(time='2020-01-01', lat=lati, lon=loni, method='nearest')
    bcc_2050_location_slice = bcc_dataset.sel(time='2050-01-01', lat=lati, lon=loni, method='nearest')
    bcc_2090_location_slice = bcc_dataset.sel(time='2090-01-01', lat=lati, lon=loni, method='nearest')

    bcc_2020_temp = float(bcc_2020_location_slice.tg_mean_p50.values)
    bcc_2020_temp = bcc_2020_temp - 273.15
    bcc_2020_temp = round(bcc_2020_temp, 1)
    bcc_2020_precip = float(bcc_2020_location_slice.delta_prcptot_p50_vs_7100.values)
    bcc_2020_precip = round(bcc_2020_precip)

    bcc_2050_temp = float(bcc_2050_location_slice.tg_mean_p50.values)
    bcc_2050_temp = bcc_2050_temp - 273.15
    bcc_2050_temp = round(bcc_2050_temp, 1)
    bcc_2050_precip = float(bcc_2050_location_slice.delta_prcptot_p50_vs_7100.values)
    bcc_2050_precip = round(bcc_2050_precip)

    bcc_2090_temp = float(bcc_2090_location_slice.tg_mean_p50.values)
    bcc_2090_temp = bcc_2090_temp - 273.15
    bcc_2090_temp = round(bcc_2090_temp, 1)
    bcc_2090_precip = float(bcc_2090_location_slice.delta_prcptot_p50_vs_7100.values)
    bcc_2090_precip = round(bcc_2090_precip)

    return {
        "anusplin_1950_temp": anusplin_1950_temp,
        "anusplin_2005_temp": anusplin_2005_temp,
        "anusplin_1980_precip": anusplin_1980_precip,
        "bcc_2020_temp": bcc_2020_temp,
        "bcc_2050_temp": bcc_2050_temp,
        "bcc_2090_temp": bcc_2090_temp,
        "bcc_2020_precip": bcc_2020_precip,
        "bcc_2050_precip": bcc_2050_precip,
        "bcc_2090_precip": bcc_2090_precip
    }