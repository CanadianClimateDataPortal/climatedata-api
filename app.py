from flask import Flask, request, jsonify, Response
import json
from werkzeug.exceptions import BadRequestKeyError
import sentry_sdk
import xarray as xr
import pandas as pd
import numpy as np
import sys
import glob
import sentry_sdk
from textwrap import dedent
from sentry_sdk.integrations.flask import FlaskIntegration
import requests
from itertools import starmap


app = Flask(__name__)
app.config.from_object('default_settings')
app.config.from_envvar('CLIMATEDATA_FLASK_SETTINGS', silent=True)

if 'SENTRY_DSN' in app.config:
    sentry_sdk.init(
        app.config['SENTRY_DSN'],
        environment=app.config['SENTRY_ENV'],
        integrations=[FlaskIntegration()]
    )

pd.set_option('display.max_rows', 10000)




def open_dataset(var, msys, month, formats, root):
    for filename in formats:
        try:
            dataseturl = filename.format(root=root,
                                   var=var,
                                   msys=msys,
                                   month=month)
            dataset=  xr.open_dataset(dataseturl, decode_times=False)
            dataset['time'] = xr.decode_cf(dataset).time
            return dataset
        except FileNotFoundError:
            pass
    raise FileNotFoundError("Dataset not found")

def open_dataset_by_path(path):
    try:
        dataset=  xr.open_dataset(path, decode_times=False)
        dataset['time'] = xr.decode_cf(dataset).time
        return dataset
    except FileNotFoundError:
        raise FileNotFoundError("Dataset not found")

"""
Converts xarray dataset to a list. 
We assume that the coordinates are timestamps, which are converted to miliseconds since 1970-01-01 (integer)
"""
def convert_dataset_to_list(dataset):
    return [[int(a[0].timestamp()*1000)] +a[1:] for a in dataset.to_dataframe().astype('float64').round(2).reset_index().values.tolist()]



"""
    Rewrite of get_values, generating a JSON ready for highcharts 
"""
@app.route('/generate-charts/<lat>/<lon>/<var>/<month>')
@app.route('/generate-charts/<lat>/<lon>/<var>')
def generate_charts(var, lat, lon, month='ann'):
    try:
        lati = float(lat)
        loni = float(lon)
        monthpath,msys = app.config['MONTH_LUT'][month]
        if var not in app.config['VARIABLES']:
            raise ValueError
    except (ValueError, BadRequestKeyError, KeyError):
        return "Bad request", 400

    # SPEI treatement is very different
    if var in app.config['SPEI_VARIABLES']:
        return generate_spei_charts(var, lati, loni, month)

    # so does sea level rise
    if var == 'slr':
        return generate_slr_charts(lati, loni)

    monthnumber = app.config['MONTH_NUMBER_LUT'][month]
    anusplin_dataset = open_dataset(var, msys, monthpath,
                                    app.config['NETCDF_ANUSPLINV1_FILENAME_FORMATS'],
                                    app.config['NETCDF_ANUSPLINV1_YEARLY_FOLDER'])
    anusplin_location_slice = anusplin_dataset.sel(lon=loni, lat=lati, method='nearest').drop(['lat','lon']).dropna('time')
    anusplin_location_slice = anusplin_location_slice.sel(time=(anusplin_location_slice.time.dt.month == monthnumber))

    if anusplin_location_slice[var].attrs.get('units') == 'K':
        anusplin_location_slice = anusplin_location_slice + app.config['KELVIN_TO_C']

    bccaq_dataset = open_dataset(var, msys, monthpath,
                                 app.config['NETCDF_BCCAQV2_FILENAME_FORMATS'],
                                 app.config['NETCDF_BCCAQV2_YEARLY_FOLDER'])
    # TODO-to-validate: even if bccaq and anusplin has the same grid, because of rounding issues
    # there are some edge cases where the bccaq slice may not geographically match the anusplin slice
    bccaq_location_slice = bccaq_dataset.sel(lon=loni, lat=lati, method='nearest').drop(['lat','lon']).dropna('time')
    if bccaq_location_slice['rcp26_{}_p50'.format(var)].attrs.get('units') == 'K':
        bccaq_location_slice = bccaq_location_slice + app.config['KELVIN_TO_C']

    return_values = {'observations': convert_dataset_to_list(anusplin_location_slice)}

    # we return the historical values for a single model before HISTORICAL_DATE_LIMIT
    bccaq_location_slice_historical = bccaq_location_slice.where(bccaq_location_slice.time <= np.datetime64(app.config['HISTORICAL_DATE_LIMIT_BEFORE']), drop=True)
    return_values['modeled_historical_median'] = convert_dataset_to_list(bccaq_location_slice_historical['rcp26_{}_p50'.format(var)])
    return_values['modeled_historical_range'] = convert_dataset_to_list(xr.merge([bccaq_location_slice_historical['rcp26_{}_p10'.format(var)],
                                                                                  bccaq_location_slice_historical['rcp26_{}_p90'.format(var)]]))
    # we return values in historical for all models after HISTORICAL_DATE_LIMIT
    bccaq_location_slice = bccaq_location_slice.where(bccaq_location_slice.time >= np.datetime64(app.config['HISTORICAL_DATE_LIMIT_AFTER']), drop=True)

    for model in app.config['MODELS']:
        return_values[model + '_median'] = convert_dataset_to_list(bccaq_location_slice['{}_{}_p50'.format(model, var)])
        return_values[model + '_range'] = convert_dataset_to_list(xr.merge([bccaq_location_slice['{}_{}_p10'.format(model, var)],
                                                          bccaq_location_slice['{}_{}_p90'.format(model, var)]]))
    return  return_values

"""
    Copied from generate-charts, but handles the exceptions for SPEI (i.e. single file)
"""
def generate_spei_charts(var, lati, loni, month):

    # very specific exception for location page, return december values (annual doesn't exists anyway for SPEI)
    if month == 'ann':
        month = 'dec'

    monthnumber = app.config['MONTH_NUMBER_LUT'][month]
    dataset = open_dataset_by_path(app.config['NETCDF_SPEI_FILENAME_FORMATS'].format(root=app.config['NETCDF_SPEI_FOLDER'],var=var))
    observed_dataset = open_dataset_by_path(app.config['NETCDF_SPEI_OBSERVED_FILENAME_FORMATS'].format(root=app.config['NETCDF_SPEI_FOLDER'],var=var))

    location_slice = dataset.sel(lon=loni, lat=lati, method='nearest').drop(['lat','lon','scale']).dropna('time')
    location_slice = location_slice.sel(time=(location_slice.time.dt.month == monthnumber))

    observed_location_slice = observed_dataset.sel(lon=loni, lat=lati, method='nearest').drop(['lat','lon','scale']).dropna('time')
    observed_location_slice = observed_location_slice.sel(time=(observed_location_slice.time.dt.month == monthnumber))
    observed_location_slice = observed_location_slice.where(observed_location_slice.time >= np.datetime64(app.config['SPEI_DATE_LIMIT']), drop=True)
    return_values = {}
    return_values['observations']= convert_dataset_to_list(observed_location_slice)

    # we return the historical values for a single model before HISTORICAL_DATE_LIMIT
    location_slice_historical = location_slice.where(location_slice.time <= np.datetime64(app.config['HISTORICAL_DATE_LIMIT_BEFORE']), drop=True)
    location_slice_historical = location_slice_historical.where(
        location_slice.time >= np.datetime64(app.config['SPEI_DATE_LIMIT']), drop=True)
    return_values['modeled_historical_median'] = convert_dataset_to_list(location_slice_historical['rcp26_spei_p50'])
    return_values['modeled_historical_range'] = convert_dataset_to_list(xr.merge([location_slice_historical['rcp26_spei_p10'],
                                                                                  location_slice_historical['rcp26_spei_p90']]))
    # we return values in historical for all models after HISTORICAL_DATE_LIMIT
    location_slice = location_slice.where(location_slice.time >= np.datetime64(app.config['HISTORICAL_DATE_LIMIT_AFTER']), drop=True)

    for model in app.config['MODELS']:
        return_values[model + '_median'] = convert_dataset_to_list(location_slice['{}_spei_p50'.format(model)])
        return_values[model + '_range'] = convert_dataset_to_list(xr.merge([location_slice['{}_spei_p10'.format(model)],
                                                          location_slice['{}_spei_p90'.format(model)]]))
    return  return_values

"""
    Copied from generate_spei_charts: no historical, no observations, single file
"""
def generate_slr_charts(lati, loni):
    dataset = open_dataset_by_path(app.config['NETCDF_SLR_PATH'])
    location_slice = dataset.sel(lon=loni, lat=lati, method='nearest').drop(['lat','lon']).dropna('time')
    return_values = {}
    return_values['observations']= []
    return_values['modeled_historical_median']=[]
    return_values['modeled_historical_range']=[]

    for model in app.config['MODELS']:
        return_values[model + '_median'] = convert_dataset_to_list(location_slice['{}_slr_p50'.format(model)])
        return_values[model + '_range'] = convert_dataset_to_list(xr.merge([location_slice['{}_slr_p05'.format(model)],
                                                          location_slice['{}_slr_p95'.format(model)]]))
    return  return_values


"""
    Get data for specific region
"""
@app.route('/generate-regional-charts/<partition>/<index>/<var>/<month>')
@app.route('/generate-regional-charts/<partition>/<index>/<var>')
def generate_regional_charts(partition, index, var, month='ann'):
    try:
        indexi = int(index)
        msys = "YS" if month == "ann" else "MS"
        monthnumber = app.config['MONTH_NUMBER_LUT'][month]
        if var not in app.config['VARIABLES']:
            raise ValueError
        bccaq_path = app.config['PARTITIONS_PATH_FORMATS'][partition]['allyears'].format(
            root=app.config['PARTITIONS_FOLDER'][partition]['BCCAQ'],
            var=var,
            msys=msys)
        anusplin_path = app.config['PARTITIONS_PATH_FORMATS'][partition]['ANUSPLIN'].format(
            root=app.config['PARTITIONS_FOLDER'][partition]['ANUSPLIN'],
            var=var,
            msys=msys)
    except (ValueError, BadRequestKeyError, KeyError):
        return "Bad request", 400



    anusplin_dataset = open_dataset_by_path(anusplin_path)
    anusplin_location_slice = anusplin_dataset.sel(region=indexi).drop([i for i in anusplin_dataset.coords if i != 'time'])
    if anusplin_location_slice[var].attrs.get('units') == 'K':
        anusplin_location_slice = anusplin_location_slice + app.config['KELVIN_TO_C']

    bccaq_dataset = open_dataset_by_path(bccaq_path)
    bccaq_location_slice = bccaq_dataset.sel(region= indexi).drop([i for i in anusplin_dataset.coords if i != 'time'])

    # we filter the appropriate month from the MS-allyears file
    if msys == "MS":
        bccaq_location_slice= bccaq_location_slice.sel(time=(bccaq_location_slice.time.dt.month == monthnumber))
        anusplin_location_slice = anusplin_location_slice.sel(time=(anusplin_location_slice.time.dt.month == monthnumber))

    if bccaq_location_slice['rcp26_{}_p50'.format(var)].attrs.get('units') == 'K':
        bccaq_location_slice = bccaq_location_slice + app.config['KELVIN_TO_C']
    return_values = {'observations': convert_dataset_to_list(anusplin_location_slice)}



    # we return the historical values for a single model before HISTORICAL_DATE_LIMIT
    bccaq_location_slice_historical = bccaq_location_slice.where(bccaq_location_slice.time <= np.datetime64(app.config['HISTORICAL_DATE_LIMIT_BEFORE']), drop=True)
    return_values['modeled_historical_median'] = convert_dataset_to_list(bccaq_location_slice_historical['rcp26_{}_p50'.format(var)])
    return_values['modeled_historical_range'] = convert_dataset_to_list(xr.merge([bccaq_location_slice_historical['rcp26_{}_p10'.format(var)],
                                                                                  bccaq_location_slice_historical['rcp26_{}_p90'.format(var)]]))
    # we return values in historical for all models after HISTORICAL_DATE_LIMIT
    bccaq_location_slice = bccaq_location_slice.where(bccaq_location_slice.time >= np.datetime64(app.config['HISTORICAL_DATE_LIMIT_AFTER']), drop=True)

    for model in app.config['MODELS']:
        return_values[model + '_median'] = convert_dataset_to_list(bccaq_location_slice['{model}_{var}_p50'.format(var=var, model=model)])
        return_values[model + '_range'] = convert_dataset_to_list(xr.merge([bccaq_location_slice['{model}_{var}_p10'.format(var=var, model=model)],
                                                          bccaq_location_slice['{model}_{var}_p90'.format(var=var, model=model)]]))
    return  return_values

"""
    Get regional data for all regions, single date
"""
@app.route('/get-choro-values/<partition>/<var>/<model>/<month>/')
@app.route('/get-choro-values/<partition>/<var>/<model>')
def get_choro_values(partition, var, model, month='ann'):
    try:
        msys = "YS" if month == "ann" else "MS"
        monthnumber = app.config['MONTH_NUMBER_LUT'][month]
        if model not in app.config['MODELS']:
            raise ValueError
        if var not in app.config['VARIABLES']:
            raise ValueError
        period = int(request.args['period'])
        dataset_path = app.config['PARTITIONS_PATH_FORMATS'][partition]['means'].format(
            root=app.config['PARTITIONS_FOLDER'][partition]['BCCAQ'],
            var=var,
            msys=msys)
    except (TypeError, ValueError, BadRequestKeyError, KeyError):
        return "Bad request", 400

    bccaq_dataset = open_dataset_by_path(dataset_path)
    bccaq_time_slice = bccaq_dataset.sel(time="{}-{}-01".format(period, monthnumber))

    return Response(json.dumps(bccaq_time_slice["{model}_{var}_p50".format(var=var,model=model)]
                        .drop([i for i in bccaq_time_slice.coords if i != 'region']).to_dataframe().astype('float64')
                        .round(2).fillna(0).transpose().values.tolist()[0]),
                    mimetype='application/json')

@app.route('/get_location_values_allyears.php')
def get_location_values_allyears():
    try:
        lati = float(request.args['lat'])
        loni = float(request.args['lon'])
    except (ValueError, BadRequestKeyError, KeyError):
        return "Bad request", 400

    anusplin_dataset = xr.open_dataset(app.config['NETCDF_LOCATIONS_FOLDER'] + "/SearchLocation_30yAvg_anusplin_nrcan_canada_tg_mean_prcptot_YS.nc")
    anusplin_location_slice  = anusplin_dataset.sel(lat=lati, lon=loni, method='nearest')

    anusplin_1950_location_slice = anusplin_location_slice.sel(time='1951-01-01')
    anusplin_1980_location_slice = anusplin_location_slice.sel(time='1981-01-01')

    anusplin_1950_temp = round(anusplin_1950_location_slice.tg_mean.item() + app.config['KELVIN_TO_C'], 1)
    anusplin_1980_temp = round(anusplin_1980_location_slice.tg_mean.item() + app.config['KELVIN_TO_C'], 1)
    anusplin_1950_precip = round(anusplin_1950_location_slice.prcptot.item())


    bcc_dataset = xr.open_dataset(app.config['NETCDF_LOCATIONS_FOLDER'] + "/SearchLocation_30yAvg_wDeltas_BCCAQv2+ANUSPLIN300_rcp85_tg_mean_prcptot_YS.nc")
    bcc_location_slice = bcc_dataset.sel(lat=lati, lon=loni, method='nearest')
    bcc_2020_location_slice = bcc_location_slice.sel(time='2021-01-01')
    bcc_2050_location_slice = bcc_location_slice.sel(time='2051-01-01')
    bcc_2070_location_slice = bcc_location_slice.sel(time='2071-01-01')

    bcc_2020_temp = round(bcc_2020_location_slice.tg_mean_p50.values.item() + app.config['KELVIN_TO_C'],  1)
    bcc_2020_precip = round(bcc_2020_location_slice.delta_prcptot_p50_vs_7100.values.item())

    bcc_2050_temp = round(bcc_2050_location_slice.tg_mean_p50.values.item() + app.config['KELVIN_TO_C'], 1)
    bcc_2050_precip = round(bcc_2050_location_slice.delta_prcptot_p50_vs_7100.values.item())

    bcc_2070_temp = round(bcc_2070_location_slice.tg_mean_p50.values.item() + app.config['KELVIN_TO_C'], 1)
    bcc_2070_precip = round(bcc_2070_location_slice.delta_prcptot_p50_vs_7100.values.item())

    return json.dumps({
        "anusplin_1950_temp": anusplin_1950_temp,
        "anusplin_2005_temp": anusplin_1980_temp, # useless value kept for frontend transition
        "anusplin_1980_temp": anusplin_1980_temp,
        "anusplin_1980_precip": anusplin_1950_precip, # useless value kept for frontend transition
        "anusplin_1950_precip": anusplin_1950_precip,
        "bcc_2020_temp": bcc_2020_temp,
        "bcc_2050_temp": bcc_2050_temp,
        "bcc_2070_temp": bcc_2070_temp,
        "bcc_2090_temp": bcc_2070_temp, # useless value kept for frontend transition
        "bcc_2020_precip": bcc_2020_precip,
        "bcc_2050_precip": bcc_2050_precip,
        "bcc_2070_precip": bcc_2070_precip,
        "bcc_2090_precip": bcc_2070_precip # useless value kept for frontend transition
    })
"""
    Returns a new function that returns a dataframe for a specific coordinate (lat,lon)
    Parameters:
      dataset: the xarray dataset to convert
      adjust: constant to add to the whole dataset after the select but before conversion (ex: for Kelvin to °C) 
"""
def  getframe(dataset,adjust):
    return lambda lat,lon: (dataset.sel(lat=lat,lon=lon, method='nearest').dropna('time') + adjust).to_dataframe()

"""
    Outputs a dataframe to JSON for use with the portal
    Parameters:
      df: the dataframe
      var: name of the variable to export
      freq: the frequency sampling (MS|YS)
      period: the period if the frequency sampling requires one
"""
def outputJSON(df, var, freq, period=''):
    if freq == 'YS':
        calculated = 'by year'
        monthstr = ''

    if freq == 'MS':
        calculated = 'by month'
        monthstr = f""""month": "{period}","""

    if freq == 'QS-DEC':
        calculated = 'by season'
        monthstr = f""""season": "{period}","""

    if freq == '2QS-APR':
        calculated = 'from April to September'
        monthstr = ''


    return dedent(f"""\
    [{{"variable": "{var}",
       "calculated": "{calculated}",
       {monthstr}
       "latitude": {df.lat[0]:.2f},
       "longitude": {df.lon[0]:.2f} }},
       {{"data": {df.drop(columns=['lon', 'lat'], axis=1).to_json(orient='index', date_format='iso', date_unit='s', double_precision=2)} }}]
    """)


"""
    Performs a download of annual/monthly dataset.
    example POST data:
    { 'var' : 'tx_max',
      'month' : 'jan',
      'format' : 'json',
      'points': [[45.6323041086555,-73.81242277462837], [45.62317816394269,-73.71014590931205], [45.62317725541931,-73.61542460410394], [45.71149235185937,-73.6250345109122]]
    }
    
    CURL format:
    curl  -s http://localhost:5000/download -H "Content-Type: application/json" -X POST -d '{ "var" : "tx_max",
      "month" : "jan",
      "format" : "json",
      "points": [[45.6323041086555,-73.81242277462837], [45.62317816394269,-73.71014590931205], [45.62317725541931,-73.61542460410394], [45.71149235185937,-73.6250345109122]]
    }'
    
    slr example:
    curl -s http://localhost:5000/download -H "Content-Type: application/json" -X POST -d '{ "var" : "slr",
      "month" : "ann",
      "format" : "csv",
      "points": [[55.615479404227266,-80.75205994818893], [55.615479404227266,-80.57633593798099], [55.54715240897225,-80.63124969117096], [55.54093496609795,-80.70812894563693]]
    }'
    
    spei example:
    curl -s http://localhost:5000/download -H "Content-Type: application/json" -X POST -d '{ "var" : "spei_3m",
      "month" : "dec",
      "format" : "csv",
      "points": [[49.97204543090215,-72.96164786407991], [49.05163514254488,-74.11483668106955], [48.23844206206425,-75.04837048529926], [47.16613204524246,-76.00386979080497]]
    }'
          
    var: variable to fetch
    month: frequency-sampling to fetch
    format: csv or json
    points: array of [lat,lon] coordinates
"""
@app.route('/download', methods= ['POST'])
def download():
    args = request.get_json()
    try:
        var = args['var']
        month = args['month']
        format = args['format']
        points = args['points']
        monthpath, freq = app.config['MONTH_LUT'][month]
        if var not in app.config['VARIABLES']:
            raise ValueError
        if len(points) ==0:
            raise ValueError
        for p in points:
            if len(p) != 2:
                raise ValueError
    except (ValueError, BadRequestKeyError, KeyError, TypeError):
        return "Bad request", 400

    if var == 'slr':
        dataset = open_dataset_by_path(app.config['NETCDF_SLR_PATH'])
    elif var in app.config['SPEI_VARIABLES']:
        monthnumber = app.config['MONTH_NUMBER_LUT'][month]
        dataset = open_dataset_by_path(
            app.config['NETCDF_SPEI_FILENAME_FORMATS'].format(root=app.config['NETCDF_SPEI_FOLDER'], var=var))
        dataset = dataset.sel(time=(dataset.time.dt.month == monthnumber))
    else:
        dataset = open_dataset(var, freq, monthpath,app.config['NETCDF_BCCAQV2_FILENAME_FORMATS'],
                                app.config['NETCDF_BCCAQV2_YEARLY_FOLDER'])
    if var not in app.config['SPEI_VARIABLES'] and dataset['rcp26_{}_p50'.format(var)].attrs.get('units') == 'K':
        adjust = app.config['KELVIN_TO_C']
    else:
        adjust = 0
    dfs = starmap(getframe(dataset,adjust), points)
    dfs = filter(lambda df: not df.empty, dfs)

    if format == 'csv':
        return Response(pd.concat(dfs).to_csv(float_format='%.2f'), mimetype='text/csv')
    if format == 'json':
        return Response("[" + ",".join(map(lambda df: outputJSON(df, var, freq, month), dfs)) + "]", mimetype='application/json')
    return "Bad request", 400



"""
    Check server status (geoserver + this app)
    Return 200 OK if everything is fine
    Return 500 if one component is broken
"""
@app.route('/status')
def check_status():
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
    return "All checks successful",200