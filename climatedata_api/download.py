import calendar
import itertools
import os
import shutil
import tempfile
import zipfile
from datetime import datetime
from textwrap import dedent

import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype

import xarray as xr
from clisops.core.subset import subset_bbox
from flask import Response, after_this_request
from flask import current_app as app
from flask import request, send_file
from werkzeug.exceptions import BadRequestKeyError

from climatedata_api.map import get_s2d_release_date
from climatedata_api.utils import format_metadata, make_zip, open_dataset, open_dataset_by_path
from default_settings import DOWNLOAD_CSV_FORMAT, DOWNLOAD_NETCDF_FORMAT, DOWNLOAD_JSON_FORMAT, S2D_FREQUENCY_MONTHLY, \
    S2D_FREQUENCY_SEASONAL, S2D_FORECAST_TYPE_EXPECTED, S2D_FILENAME_VALUES


def float_format_dataframe(df, decimals):
    for v in df:
        if v not in ['time', 'lat', 'lon'] and is_numeric_dtype(df[v]):
            df[v] = df[v].map(lambda x: f"{x:.{decimals}f}" if not pd.isna(x) else '')


def get_subset(dataset, point, adjust, limit=None):
    """
        Returns a sub-dataset for a specific coordinate (lat,lon)
        :param dataset: the xarray dataset to select from
        :param point:   (1x2 vector): coordinates of a single point to select [lat,lon]
                        (1x4 vector): bounding box to select
        :param adjust: constant to add to the whole dataset after the select but before conversion (ex: for Kelvin to °C)
        :param limit: lower time limit of the data to keep
        :return: a slice from the dataset
    """
    if len(point) == 2:
        ds = dataset.sel(lat=point[0], lon=point[1], method='nearest').dropna('time')
    elif len(point) == 4:
        ds = subset_bbox(dataset, lon_bnds=[point[1], point[3]], lat_bnds=[point[0], point[2]])
    else:
        raise ValueError("point argument must be of length 2 or 4")
    if adjust:
        for v in [v for v in ds.data_vars if 'delta' not in v]:
            ds[v] = ds[v] + adjust
    if limit:
        ds = ds.where(ds.time >= np.datetime64(limit), drop=True)
    return ds


def output_json(df, var, freq, decimals, period=''):
    """
        Outputs a dataframe to JSON for use with the portal
        :param df: the dataframe
        :param var: name of the variable to export
        :param freq: the frequency sampling (MS|YS)
        :param decimals: the number of decimals to output format
        :param period: the period if the frequency sampling requires one
        :return: the dataframe as a JSON string
    """
    if freq == 'YS':
        calculated = 'by year'
        monthstr = ''

    elif freq == 'MS':
        calculated = 'by month'
        monthstr = f""""month": "{period}","""

    elif freq == 'QS-DEC':
        calculated = 'by season'
        monthstr = f""""season": "{period}","""

    elif freq == '2QS-APR':
        calculated = 'from April to September'
        monthstr = ''

    else:
        raise Exception("Invalid frequency selected")

    return dedent(f"""\
    [{{"variable": "{var}",
       "calculated": "{calculated}",
       {monthstr}
       "latitude": {df.lat[0]:.2f},
       "longitude": {df.lon[0]:.2f} }},
       {{"data": {df.drop(columns=['lon', 'lat'], axis=1).to_json(orient='index', date_format='iso', date_unit='s', double_precision=decimals)} }}]
    """)


def output_netcdf(ds, encoding, format):
    """
    Export an in-memory dataset to a netcdf file, returns a file handle to an unlinked
    temporary file
    :param ds: a xarray dataset
    :param encoding: encoding dictionary used by to_netcdf
    :param format: netcdf format to use (NETCDF4, NETCDF4_CLASSIC)
    :return: A file handle to the exported netcdf
    """
    filename = os.path.join(app.config['TEMPDIR'], next(tempfile._get_candidate_names()))  # noqa
    ds.to_netcdf(filename, encoding=encoding, format=format)
    f = open(filename, "rb")
    os.unlink(filename)
    return f


def check_points_or_bbox(points, bbox, month=None):
    if points and bbox:
        raise ValueError("Can't request both points and bbox simultaneously")

    if points:
        if len(points) == 0:
            raise ValueError("Points parameter is empty")
        # Check if user abuses the API
        points_limit = app.config['DOWNLOAD_POINTS_LIMIT']
        if month == 'all':
            points_limit = points_limit / 12
        if len(points) > points_limit:
            raise ValueError("Too many points requested")
        for p in points:
            if len(p) != 2:
                raise ValueError("Points must have exactly 2 coordinates each")
    elif bbox:
        if len(bbox) != 4:
            raise ValueError("bbox must be an array of length 4")
        # TODO: calculate add API limit here... implementation may be way faster than points
    else:
        raise ValueError("Neither points or bbox requested")


def download():
    """
        Performs a download of annual/monthly dataset.
        example POST data:
        { 'var' : 'tx_max',
          'month' : 'jan',
          'format' : 'json',
          'decimals' : 3,
          'points': [[45.6323041086555,-73.81242277462837], [45.62317816394269,-73.71014590931205], [45.62317725541931,-73.61542460410394], [45.71149235185937,-73.6250345109122]]
        }

        JSON format:
        curl  -s http://localhost:5000/download -H "Content-Type: application/json" -X POST -d '{ "var" : "tx_max",
          "month" : "jan",
          "format" : "json",
          "points": [[45.6323041086555,-73.81242277462837], [45.62317816394269,-73.71014590931205], [45.62317725541931,-73.61542460410394], [45.71149235185937,-73.6250345109122]]
        }'

        JSON format (zipped):
        curl  -s http://localhost:5000/download -H "Content-Type: application/json" -X POST -d '{ "var" : "tx_max",
          "month" : "jan",
          "format" : "json",
          "zipped" : true,
          "points": [[45.6323041086555,-73.81242277462837], [45.62317816394269,-73.71014590931205], [45.62317725541931,-73.61542460410394], [45.71149235185937,-73.6250345109122]]
        }'

        CSV format:
        curl  -s http://localhost:5000/download -H "Content-Type: application/json" -X POST -d '{ "var" : "tx_max",
          "month" : "jan",
          "format" : "csv",
          "dataset_name": "CMIP5",
          "points": [[45.6323041086555,-73.81242277462837], [45.62317816394269,-73.71014590931205], [45.62317725541931,-73.61542460410394], [45.71149235185937,-73.6250345109122]]
        }'

        CSV format (zipped):
        curl  -s http://localhost:5000/download -H "Content-Type: application/json" -X POST -d '{ "var" : "tx_max",
          "month" : "jan",
          "format" : "csv",
          "zipped" : true,
          "dataset_name": "CMIP5",
          "points": [[45.6323041086555,-73.81242277462837], [45.62317816394269,-73.71014590931205], [45.62317725541931,-73.61542460410394], [45.71149235185937,-73.6250345109122]]
        }'


        CSV format (CMIP6):
        curl  -s http://localhost:5000/download -H "Content-Type: application/json" -X POST -d '{ "var" : "tx_max",
          "month" : "jan",
          "format" : "csv",
          "dataset_name": "CMIP6",
          "points": [[45.6323041086555,-73.81242277462837], [45.62317816394269,-73.71014590931205], [45.62317725541931,-73.61542460410394], [45.71149235185937,-73.6250345109122]]
        }'

        CSV format (CMIP6, 30ygraph):
        curl  -s http://localhost:5000/download -H "Content-Type: application/json" -X POST -d '{ "var" : "tx_max",
          "month" : "jan",
          "format" : "csv",
          "dataset_name": "CMIP6",
          "dataset_type": "30ygraph",
          "points": [[45.6323041086555,-73.81242277462837], [45.62317816394269,-73.71014590931205], [45.62317725541931,-73.61542460410394], [45.71149235185937,-73.6250345109122]]
        }'


        Netcdf Format
        curl  -s http://localhost:5000/download -H "Content-Type: application/json" -X POST -d '{ "var" : "tx_max",
          "month" : "jan",
          "format" : "netcdf",
          "points": [[45.6323041086555,-73.81242277462837], [45.62317816394269,-73.71014590931205], [45.62317725541931,-73.61542460410394], [45.71149235185937,-73.6250345109122]]
        }'

        Netcdf Format (CMIP6)
        curl  -s http://localhost:5000/download -H "Content-Type: application/json" -X POST -d '{ "var" : "tx_max",
          "month" : "jan",
          "format" : "netcdf",
          "dataset_name": "CMIP6",
          "points": [[45.6323041086555,-73.81242277462837], [45.62317816394269,-73.71014590931205], [45.62317725541931,-73.61542460410394], [45.71149235185937,-73.6250345109122]]
        }'

        Netcdf Format (CMIP6, 30ygraph)
        curl  -s http://localhost:5000/download -H "Content-Type: application/json" -X POST -d '{ "var" : "tx_max",
          "month" : "jan",
          "format" : "netcdf",
          "dataset_name": "CMIP6",
          "dataset_type": "30ygraph",
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

        bbox example (csv):
        curl -s http://localhost:5000/download -H "Content-Type: application/json" -X POST -d '{ "var" : "tx_max",
          "month" : "ann",
          "format" : "csv",
          "bbox": [45.704236999914066, -72.1259641636298, 45.86229102811587, -71.6173341617058]
        }'

        bbox example (netCDF):
        curl -s http://localhost:5000/download -H "Content-Type: application/json" -X POST -d '{ "var" : "tx_max",
          "month" : "all",
          "format" : "netcdf",
          "bbox": [45.704236999914066, -72.1259641636298, 45.86229102811587, -71.6173341617058]
        }'


        var: variable to fetch
        month: frequency-sampling to fetch
        format: csv, json or nc
        zipped: send the result as a zipfile  (valid for csv and json only, ignored for nc)
          points: array of [lat,lon] coordinates
        or
          bbox: bounding box coordinates: [min-lat, min-lon, max-lat, max-lon]
    """
    args = request.get_json()
    try:
        var = args['var']
        month = args['month']
        output_format = args['format']
        zipped = args.get('zipped', False)
        points = args.get('points', None)
        bbox = args.get('bbox', None)
        dataset_name = args.get('dataset_name', 'CMIP5').upper()
        dataset_type = args.get('dataset_type', 'allyears')
        custom_filename = args.get('custom_filename', None)

        decimals = int(args.get('decimals', 1))
        if decimals < 0:
            return "Bad request: invalid number of decimals", 400
        monthpath, freq = app.config['MONTH_LUT'][month]
        if var not in app.config['VARIABLES']:
            raise ValueError("Invalid variable requested")

        if dataset_name not in app.config['FILENAME_FORMATS']:
            raise KeyError("Invalid dataset requested")

        if dataset_type not in app.config['FILENAME_FORMATS'][dataset_name]:
            raise KeyError("Invalid dataset type requested")

        check_points_or_bbox(points, bbox, month)

    except ValueError as e:
        return f"Bad request: {str(e)}", 400
    except (BadRequestKeyError, KeyError, TypeError):
        return "Bad request", 400

    scenarios = app.config['SCENARIOS'][dataset_name]
    limit = None
    if var == 'slr':
        if dataset_name not in ['CMIP5', 'CMIP6']:
            return (f"Bad request: Invalid dataset `{dataset_name}` for slr."
                    " Only `CMIP5` and `CMIP6` are available for slr."), 400
        slr_path = app.config['NETCDF_SLR_CMIP5_PATH'] if dataset_name == "CMIP5" else app.config['NETCDF_SLR_CMIP6_PATH']
        datasets = [open_dataset_by_path(slr_path.format(root=app.config['DATASETS_ROOT']))]
    elif var == 'allowance':
        if dataset_name != 'CMIP6':
            return f"Bad request : `allowance` variable only uses the CMIP6 dataset, and has no {dataset_name} data available.\n", 400
        datasets = [open_dataset_by_path(app.config['NETCDF_ALLOWANCE_PATH'].format(root=app.config['DATASETS_ROOT']))]
    elif var in app.config['SPEI_VARIABLES']:
        datasets = [open_dataset_by_path(
            app.config['NETCDF_SPEI_FILENAME_FORMATS'].format(root=app.config['DATASETS_ROOT'], var=var))]
        if month != 'all':
            monthnumber = app.config['MONTH_NUMBER_LUT'][month]
            datasets[0] = datasets[0].sel(time=(datasets[0].time.dt.month == monthnumber))
        limit = app.config['SPEI_DATE_LIMIT']
    else:
        if month == 'all':
            datasets = [open_dataset(dataset_name, dataset_type, var, freq, m) for m in app.config['ALLMONTHS']]
        else:
            datasets = [open_dataset(dataset_name, dataset_type, var, freq, monthpath)]

    if var not in app.config['SPEI_VARIABLES'] and datasets[0][f'{scenarios[0]}_{var}_p50'].attrs.get('units') == 'K':
        adjust = app.config['KELVIN_TO_C']
    else:
        adjust = 0

    metadata = format_metadata(datasets[0])

    if points:
        subsetted_datasets = [[get_subset(dataset, p, adjust, limit) for dataset in datasets] for p in points]
    else:
        subsetted_datasets = [get_subset(dataset, bbox, adjust, limit) for dataset in datasets]

    filename = var

    if custom_filename:
        filename = custom_filename

    if output_format == DOWNLOAD_NETCDF_FORMAT:
        if points:
            combined_ds = xr.combine_nested(subsetted_datasets, ['region', 'time'], combine_attrs='override').dropna(
                'region', how='all')
        else:
            combined_ds = xr.merge(subsetted_datasets)

        encodings = {}
        for v in combined_ds.data_vars:
            if combined_ds[v].attrs.get('units') == 'K':
                combined_ds[v].attrs['units'] = 'degC'
            encodings[v] = {"zlib": True}
        f = output_netcdf(combined_ds, encodings, 'NETCDF4')
        return send_file(f, mimetype='application/x-netcdf4', download_name=f'{filename}.nc')

    if points:
        dfs = [[j.to_dataframe() for j in i] for i in subsetted_datasets]
        # remove empty dataframes
        dfs = [[df for df in dflist if not df.empty] for dflist in dfs]
        dfs = [dflist for dflist in dfs if not len(dflist) == 0]
        if len(dfs) == 0:
            return "No points found", 404
    else:
        dfs = [i.to_dataframe() for i in subsetted_datasets]

    if output_format == DOWNLOAD_CSV_FORMAT:
        if points:
            dfs = [j for sub in dfs for j in sub]  # flattens sublists
        concatenated_dfs = pd.concat(dfs)
        columns_order = [c for c in app.config['CSV_COLUMNS_ORDER'] if c in concatenated_dfs] + \
                        sorted([c for c in concatenated_dfs if c not in app.config['CSV_COLUMNS_ORDER']])
        concatenated_dfs = concatenated_dfs.sort_values(by=['lat', 'lon', 'time'])
        float_format_dataframe(concatenated_dfs, decimals)
        response_data = concatenated_dfs.to_csv(columns=columns_order)
        if zipped:
            zip_buffer = make_zip([
                ('metadata.txt', metadata),
                (f'{filename}.csv', response_data),
            ])
            return send_file(zip_buffer, mimetype='application/zip', as_attachment=True, download_name=f'{filename}.zip')
        else:
            return Response(response_data, mimetype='text/csv')

    if output_format == DOWNLOAD_JSON_FORMAT:
        if points:
            response_data = "[" + ",".join(
                map(lambda df: output_json(pd.concat(df).sort_values(by='time'), var, freq, decimals, month),
                    dfs)) + "]"
        else:
            df_groups = [g[1].reset_index().set_index('time') for g in
                         pd.concat(dfs).sort_values(by=['lat', 'lon', 'time']).groupby(by=['lat', 'lon'])]
            response_data = "[" + ",".join(map(lambda df: output_json(df, var, freq, decimals, month), df_groups)) + "]"

        if zipped:
            zip_buffer = make_zip([
                ('metadata.txt', metadata),
                (f'{filename}.json', response_data),
            ])
            return send_file(zip_buffer, mimetype='application/zip', as_attachment=True, download_name=f'{filename}.zip')
        else:
            return Response(response_data, mimetype='application/json')

    # invalid/non-supported output format requested
    return "Bad request", 400


def _format_30y_slice_to_csv(delta_30y_slice, var, decimals, dataset_name):
    """
        Returns a csv response object from a xarray slice
    """
    scenarios = app.config['SCENARIOS'][dataset_name]
    if delta_30y_slice[f'{scenarios[0]}_{var}_p50'].attrs.get('units') == 'K':
        for v in [v for v in delta_30y_slice.data_vars if 'delta' not in v]:
            delta_30y_slice[v] = delta_30y_slice[v] + app.config['KELVIN_TO_C']
    df = delta_30y_slice.to_dataframe()
    df = df.reindex(columns=["{1}_{var}_{0}p{2}".format(*a, var=var) for a in
                             itertools.product(['', f"{app.config['DELTA_NAMING'][dataset_name]}_"], scenarios, [10, 50, 90])])

    return df.to_csv(float_format=f"%.{decimals}f")


def download_30y(var, lat, lon, month):
    """
        Download the 30y and delta values for a single grid cell
        ex: curl 'http://localhost:5000/download-30y/60.31062731740045/-100.06347656250001/tx_max/ann?decimals=2'
            curl 'http://localhost:5000/download-30y/60.31062731740045/-100.06347656250001/tx_max/ann?decimals=2&dataset_name=CMIP6'
    :return: csv string
    """
    try:
        lati = float(lat)
        loni = float(lon)
        decimals = int(request.args.get('decimals', 1))
        dataset_name = request.args.get('dataset_name', 'CMIP5').upper()

        if decimals < 0:
            return "Bad request: invalid number of decimals", 400
        month_path, msys = app.config['MONTH_LUT'][month]
        if var not in app.config['VARIABLES']:
            raise ValueError
        if dataset_name not in app.config['FILENAME_FORMATS']:
            raise KeyError("Invalid dataset requested")
    except (ValueError, BadRequestKeyError, KeyError):
        return "Bad request", 400

    delta_30y_dataset = open_dataset(dataset_name, '30ygraph', var, msys, month_path)
    delta_30y_slice = delta_30y_dataset.sel(lon=loni, lat=lati, method='nearest').drop(['lat', 'lon']).dropna('time')
    csv_data = _format_30y_slice_to_csv(delta_30y_slice, var, decimals, dataset_name)
    delta_30y_dataset.close()
    return Response(csv_data, mimetype='text/csv', headers={"Content-disposition": f"attachment; filename={var}.csv"})


def download_regional_30y(partition, index, var, month):
    """
        Download the 30y and delta values for a single grid cell
        ex: curl 'http://localhost:5000/download-regional-30y/census/1/tx_max/ann?decimals=3'
            curl 'http://localhost:5000/download-regional-30y/census/1/tx_max/ann?decimals=3&dataset_name=CMIP6'
    :return: csv string
    """
    try:
        indexi = int(index)

        msys = app.config['MONTH_LUT'][month][1]
        monthnumber = app.config['MONTH_NUMBER_LUT'][month]
        decimals = int(request.args.get('decimals', 1))
        dataset_name = request.args.get('dataset_name', 'CMIP5').upper()

        if decimals < 0:
            return "Bad request: invalid number of decimals", 400
        if var not in app.config['VARIABLES']:
            raise ValueError
        if dataset_name not in app.config['FILENAME_FORMATS']:
            raise KeyError("Invalid dataset requested")
    except (ValueError, BadRequestKeyError, KeyError):
        return "Bad request", 400

    delta_30y_dataset = open_dataset(dataset_name, '30ygraph', var, msys, partition=partition)
    delta_30y_slice = delta_30y_dataset.sel(geom=indexi).drop(
        [i for i in delta_30y_dataset.coords if i != 'time']).dropna('time')

    # we filter the appropriate month/season from the MS or QS-DEC file
    if msys in ["MS", "QS-DEC"]:
        delta_30y_slice = delta_30y_slice.sel(
            time=(delta_30y_slice.time.dt.month == monthnumber))

    csv_data = _format_30y_slice_to_csv(delta_30y_slice, var, decimals, dataset_name)
    delta_30y_dataset.close()
    return Response(csv_data, mimetype='text/csv', headers={"Content-disposition": f"attachment; filename={var}.csv"})


def download_ahccd():
    """
        download one or multiple AHCCD station data
        Required parameters (POST/GET):
            format [string]: csv | netcdf
            stations [array of strings]: list of stations

        Optional parameters:
            variable_type_filter [string]: "T" for Temperature, "P" for precipitations


        curl examples:
        # case where two stations are in both temperatures and precipitations
        POST:
            curl -s http://localhost:5000/download-ahccd -H "Content-Type: application/json" -X POST -d '{ "format" : "csv", "stations": ["3081680","8400413"]}'
        GET:
            curl -s "http://localhost:5000/download-ahccd?format=csv&stations=3081680,8400413"
        with filter:
            curl -s "http://localhost:5000/download-ahccd?format=csv&stations=3081680,8400413&variable_type_filter=P"
        # case where the station is only in precipitations
        curl -s http://localhost:5000/download-ahccd -H "Content-Type: application/json" -X POST -d '{ "format" : "csv", "stations": ["3034720"]}'
        # case where the station is only in temperature
        curl -s http://localhost:5000/download-ahccd -H "Content-Type: application/json" -X POST -d '{ "format" : "csv", "stations": ["8402757"]}'
        # case where one stations is in temperatures and the other in precipitations
        curl -s http://localhost:5000/download-ahccd -H "Content-Type: application/json" -X POST -d '{ "format" : "csv", "stations": ["3034720","8402757"]}'
        # case with two stations (to check sorting)
        curl -s 'http://localhost:5000/download-ahccd?format=csv&stations=7042395,7047250'
        # as zip
        curl -s http://localhost:5000/download-ahccd -H "Content-Type: application/json" -X POST -d '{ "format" : "csv", "zipped" : "true", "stations": ["3034720","8402757"]}'
    """
    try:
        if request.method == 'POST':
            args = request.get_json()
            stations = args['stations']
        else:
            args = request.args
            stations = args['stations'].split(',')

        format = args['format']
        zipped = args.get('zipped', False)
        variable_type_filter = args.get('variable_type_filter', "").upper()
        if len(stations) == 0:
            raise ValueError
        if len(stations) > app.config['AHCCD_STATIONS_LIMIT']:
            return "Too many stations requested", 400
    except (ValueError, BadRequestKeyError, KeyError, TypeError):
        return "Bad request", 400

    # some local config variables
    commondrops = ['fromyear', 'frommonth', 'toyear', 'tomonth', 'stnid']
    variables = [
        {'name': 'tas',
         'type': 'T',
         'filename': 'ahccd_gen3_tas.nc',
         'drops': commondrops + ['no', 'pct_miss', 'joined', 'rcs']},
        {'name': 'tasmax',
         'type': 'T',
         'filename': 'ahccd_gen3_tasmax.nc',
         'drops': commondrops + ['no', 'pct_miss', 'joined', 'rcs']},
        {'name': 'tasmin',
         'type': 'T',
         'filename': 'ahccd_gen3_tasmin.nc',
         'drops': commondrops + ['no', 'pct_miss', 'joined', 'rcs']},
        {'name': 'pr',
         'type': 'P',
         'filename': 'ahccd_gen2_pr.nc',
         'drops': commondrops + ['stns_joined']},
        {'name': 'prlp',
         'type': 'P',
         'filename': 'ahccd_gen2_prlp.nc',
         'drops': commondrops + ['stns_joined']},
        {'name': 'prsn',
         'type': 'P',
         'filename': 'ahccd_gen2_prsn.nc',
         'drops': commondrops + ['stns_joined']}]

    if variable_type_filter:
        variables = [v for v in variables if v['type'] == variable_type_filter]

    if not variables:
        return "Invalid variable_type_filter", 400

    allds = []
    encoding = {}

    for var in variables:
        ds = xr.open_dataset(
            os.path.join(app.config['AHCCD_FOLDER'].format(root=app.config['DATASETS_ROOT']), var['filename']),
            mask_and_scale=False,
            decode_times=False)
        ds['time'] = xr.decode_cf(ds, drop_variables=ds.data_vars).time
        s = list(set(stations).intersection(set(ds['station'].values)))
        if s:
            ds = ds.sel(station=s)
            time_idx = slice(f'{int(ds.fromyear.min().values)}', f'{int(ds.toyear.max().values)}')
            ds = ds.sel(time=time_idx).drop(var['drops']).load()
            allds.append(ds)
            encoding[var['name']] = {"zlib": True}

    # we copy missing attributes of stations not present in the tas dataset
    if len(allds) == len(variables) and not variable_type_filter:
        for s in stations:
            if s not in allds[0]['station']:
                allds[0] = xr.merge([allds[0], allds[3].sel(station=[s]).drop(['pr', 'pr_flag'])],
                                    compat="no_conflicts")

    ds = xr.merge(allds, compat="override")
    if len(ds.data_vars) == 0:
        return "No station found or no requested stations matched variable_type_filter", 404

    # Remove blanks at the beginning and the end of the time range
    ds_range = ds.dropna('time', how='all', subset=[v['name'] for v in variables if v['name'] in ds])
    ds = ds.sel(time=slice(ds_range.time[0], ds_range.time[-1]))

    for v in ds.data_vars:
        ds[v].encoding["coordinates"] = None

    # we assign coordinates to make sure that Finch doesn't drop this data
    if variable_type_filter:
        ds = ds.assign_coords(lat=ds.lat, lon=ds.lon, station_name=ds.station_name, prov=ds.prov)

    if format == DOWNLOAD_NETCDF_FORMAT:
        f = output_netcdf(ds, encoding, 'NETCDF4_CLASSIC')
        return send_file(f, mimetype='application/x-netcdf4', as_attachment=True, download_name='ahccd.nc')

    if format == DOWNLOAD_CSV_FORMAT:
        df = ds.to_dataframe()
        response_data = ""
        first_station = True
        columns_order = [c for c in app.config['AHCCD_ORDER'] if c in df.columns]
        for _, station_df in df.groupby(["station"], as_index=False):
            temp_df = station_df.reset_index()
            temp_df = temp_df.drop([c for c in temp_df if c not in app.config['AHCCD_VALUES_COLUMNS']], axis=1)
            temp_df = temp_df.replace('', np.nan)
            tmin = int(np.min(temp_df.apply(pd.Series.first_valid_index)))
            tmax = int(np.max(temp_df.apply(pd.Series.last_valid_index)))
            station_df = station_df.iloc[tmin:tmax + 1]
            response_data += station_df.to_csv(chunksize=100000, columns=columns_order, header=first_station)
            first_station = False

        if zipped:
            zip_buffer = make_zip([
                ('metadata.txt', format_metadata(ds)),
                ('ahccd.csv', response_data)
            ])
            return send_file(zip_buffer, mimetype='application/zip', as_attachment=True, download_name=f'ahccd.zip')
        else:
            return Response(response_data,
                            mimetype='text/csv',
                            headers={"Content-disposition": "attachment; filename=ahccd.csv"})

    return "Bad request", 400


def download_s2d():
    """
        Performs a download of s2d data.
        example POST data:
        { 'var' : 'air_temp',
          'format' : 'json',
          'points': [[45.6323041086555,-73.81242277462837], [45.62317816394269,-73.71014590931205], [45.62317725541931,-73.61542460410394], [45.71149235185937,-73.6250345109122]],
          'forecast_type': 'expected',
          'frequency': 'monthly',
          'periods': ['2025-06', '2025-12']
        }

        JSON format:
        curl  -s http://localhost:5000/download-s2d -H "Content-Type: application/json" -X POST -d '{
          "var" : "air_temp",
          'format' : 'json',
          'points': [[45.6323041086555,-73.81242277462837], [45.62317816394269,-73.71014590931205], [45.62317725541931,-73.61542460410394], [45.71149235185937,-73.6250345109122]],
          'forecast_type': 'unusual',
          'frequency': 'monthly',
          'periods': ['2025-06', '2025-12']
        }'

        CSV format:
        curl  -s http://localhost:5000/download-s2d -H "Content-Type: application/json" -X POST -d '{
          "var" : "precip_accum",
          'format' : 'csv',
          'points': [[45.6323041086555,-73.81242277462837], [45.62317816394269,-73.71014590931205], [45.62317725541931,-73.61542460410394], [45.71149235185937,-73.6250345109122]],
          'forecast_type': 'expected',
          'frequency': 'seasonal',
          'periods': ['2025-06', '2025-12']
        }'

        Netcdf Format
        curl  -s http://localhost:5000/download-s2d -H "Content-Type: application/json" -X POST -d '{
          "var" : "precip_accum",
          'format' : 'netcdf',
          'points': [[45.6323041086555,-73.81242277462837], [45.62317816394269,-73.71014590931205], [45.62317725541931,-73.61542460410394], [45.71149235185937,-73.6250345109122]],
          'forecast_type': 'unusual',
          'frequency': 'seasonal',
          'periods': ['2025-06', '2025-12']
        }'

        bbox example (csv):
        curl -s http://localhost:5000/download-s2d -H "Content-Type: application/json" -X POST -d '{
          "var" : "air_temp",
          'format' : 'csv',
          "bbox": [45.704236999914066, -72.1259641636298, 45.86229102811587, -71.6173341617058],
          'forecast_type': 'expected',
          'frequency': 'monthly',
          'periods': ['2025-06', '2025-12']
        }'

        var: s2d variable to download
        format: csv, json or netcdf
          points: array of [lat,lon] coordinates
        or
          bbox: bounding box coordinates: [min-lat, min-lon, max-lat, max-lon]
        forecast_type: forecast type to download [expected, unusual],
        frequency: the selected frequency [monthly, seasonal, decadal]
        periods: the list of periods for which we want to download the data
    """
    args = request.get_json()
    try:
        var = args['var']
        output_format = args['format']
        points = args.get('points', None)
        bbox = args.get('bbox', None)
        forecast_type = args['forecast_type']
        freq = args['frequency']
        periods = args['periods']

        if var not in app.config['S2D_VARIABLES']:
            raise ValueError(f"Invalid variable `{var}`")
        if forecast_type not in app.config['S2D_FORECAST_TYPES']:
            raise ValueError(f"Invalid forecast_type `{forecast_type}`")
        if freq not in app.config['S2D_FREQUENCIES']:
            raise ValueError(f"Invalid frequency `{freq}`")

        try:
            period_dates = [datetime.strptime(s, "%Y-%m") for s in periods]
        except ValueError as e:
            raise ValueError(f"Invalid periods. They should follow the YYYY-MM format")


        if output_format not in [DOWNLOAD_JSON_FORMAT, DOWNLOAD_CSV_FORMAT, DOWNLOAD_NETCDF_FORMAT]:
            raise ValueError(f"Invalid format `{output_format}`")

        check_points_or_bbox(points, bbox)

    except ValueError as e:
        return f"Bad request: {str(e)}", 400
    except (BadRequestKeyError, KeyError, TypeError):
        return "Bad request", 400

    # TODO: refactor load functions?
    # Load forecast data
    forecast_dataset = open_dataset_by_path(app.config['NETCDF_S2D_FORECAST_FILENAME_FORMATS'].format(
        root=app.config['DATASETS_ROOT'],
        var=var,
        freq=freq
    ))
    for period_date in period_dates:
        if not ((forecast_dataset['time'].dt.year == period_date.year) &
                (forecast_dataset['time'].dt.month == period_date.month)).any():
            return f"Bad request: period {period_date} not available in forecast dataset", 400
    forecast_slice = forecast_dataset.sel(time=period_dates)

    # Load climatology data
    climatology_dataset = open_dataset_by_path(app.config['NETCDF_S2D_CLIMATOLOGY_FILENAME_FORMATS'].format(
        root=app.config['DATASETS_ROOT'],
        var=var,
        freq=freq
    ))
    climatology_period_dates = []
    for period_date in period_dates:
        climatology_period_dates.append(period_date.replace(year=1991))
    climatology_slice = climatology_dataset.sel(time=climatology_period_dates)

    # Load skill data
    release_date = get_s2d_release_date(var, freq)
    ref_period = datetime.strptime(release_date, "%Y-%m-%d")
    skill_dataset = open_dataset_by_path(app.config['NETCDF_S2D_SKILL_FILENAME_FORMATS'].format(
        root=app.config['DATASETS_ROOT'],
        var=var,
        freq=freq,
        ref_period=ref_period.month
    ))
    for period_date in period_dates:
        if period_date.month not in skill_dataset['time'].dt.month.values:
            return f"Bad request: period {period_date} not available in skill dataset", 400
    skill_slice = skill_dataset.sel(time=skill_dataset['time'].dt.month.isin([d.month for d in period_dates]))

    # TODO: review pour avoir format uniforme selon points ou bbox
    def filter_dataset(dataset, points, bbox):
        # Filter to points/bbox
        if points:
            subsetted_datasets = [[get_subset(dataset, p, adjust=False)] for p in points]
            # Combined datasets into a single xarray with a region dimension that represents each unique (lat, lon) points
            combined_ds = (xr.combine_nested(subsetted_datasets, ['region', 'time'], combine_attrs='override')
                .dropna('region', how='all'))
        else:
            combined_ds = get_subset(dataset, bbox, adjust=False)
        return combined_ds

    forecast_slice = filter_dataset(forecast_slice, points, bbox)
    climatology_slice = filter_dataset(climatology_slice, points, bbox)
    skill_slice = filter_dataset(skill_slice, points, bbox)

    # prepare filename placeholders values
    filename_var = S2D_FILENAME_VALUES[var]
    filename_forecast_type = S2D_FILENAME_VALUES[forecast_type]
    filename_freq = S2D_FILENAME_VALUES[freq]
    filename_release_date = calendar.month_abbr[ref_period.month] + str(ref_period.year)

    # merge datasets into a single xarray
    merged_slices = {}
    for period_date in period_dates:
        month = period_date.month

        # Add missing metadata
        if freq == S2D_FREQUENCY_MONTHLY:
            time_period_abbr = calendar.month_abbr[month]
        elif freq == S2D_FREQUENCY_SEASONAL:
            time_period_abbr = f"{calendar.month_abbr[month]}-{calendar.month_abbr[(month + 2) % 12]}"
        # elif freq == 'decadal':
            # pass
        else:
            raise ValueError(f"Invalid frequency `{freq}`")

        basename = f"{filename_var}_{filename_forecast_type}_{time_period_abbr}_Release{filename_release_date}"

        # Select only the times with this month in each dataset
        month_slices = [
            ds.sel(time=ds["time"].dt.month == month).drop_vars("time").squeeze("time", drop=True)
            for ds in [forecast_slice, climatology_slice, skill_slice]
        ]

        # Merge them for this month
        merged_slices[basename] = xr.merge(
            month_slices,
            combine_attrs='override'  # keeps attributes/metadata from the first dataset (forecast dataset in this case)
        )

        merged_slices[basename].attrs['time_period'] = time_period_abbr

        # remove unused data variables depending of forecast_type
        if forecast_type == S2D_FORECAST_TYPE_EXPECTED:
            merged_slices[basename] = merged_slices[basename].drop_vars([
                'prob_unusually_high',
                'prob_unusually_low',
                'cutoff_unusually_high_p80',
                'cutoff_unusually_low_p20'
            ])
        else:  # unusual
            merged_slices[basename] = merged_slices[basename].drop_vars([
                'prob_above_normal',
                'prob_near_normal',
                'prob_below_normal',
                'cutoff_above_normal_p66',
                'cutoff_below_normal_p33'
            ])

    zip_filename = f"{filename_var}_{filename_forecast_type}_{filename_freq}_Release{filename_release_date}.zip"
    tmpdir = tempfile.mkdtemp()
    zip_path = os.path.join(tmpdir, zip_filename)

    @after_this_request
    def cleanup(response):
        shutil.rmtree(tmpdir, ignore_errors=True)
        return response

    if output_format == DOWNLOAD_NETCDF_FORMAT:
        try:
            with zipfile.ZipFile(zip_path, "w") as zipf:
                for file_basename, ds in merged_slices.items():
                    encodings = {}
                    for v in ds.data_vars:
                        encodings[v] = {"zlib": True}

                    nc_filename = f"{file_basename}.nc"
                    nc_path = os.path.join(tmpdir, nc_filename)
                    ds.to_netcdf(nc_path, encoding=encodings, format='NETCDF4')
                    zipf.write(nc_path, arcname=nc_filename)
        except Exception:
            shutil.rmtree(tmpdir, ignore_errors=True)
            raise

        return send_file(
            zip_path,
            download_name=zip_filename,
            mimetype="application/zip"
        )

    if output_format == DOWNLOAD_CSV_FORMAT:
        try:
            with zipfile.ZipFile(zip_path, "w") as zipf:
                for file_basename, ds in merged_slices.items():
                    csv_filename = f"{file_basename}.csv"
                    csv_path = os.path.join(tmpdir, csv_filename)

                    df = ds.to_dataframe()
                    df = df.drop("region", axis=1, errors='ignore')

                    columns_order = [c for c in app.config['CSV_COLUMNS_ORDER'] if c in ds] + \
                                    [c for c in app.config['S2D_FORECAST_DATA_VAR_NAMES'] if c in ds] + \
                                    [c for c in app.config['S2D_CLIMATO_DATA_VAR_NAMES'] if c in ds] + \
                                    [c for c in app.config['S2D_SKILL_DATA_VAR_NAMES'] if c in ds]
                    df = df.sort_values(by=['lat', 'lon'])
                    df.to_csv(csv_path, columns=columns_order, index=False)

                    zipf.write(csv_path, arcname=csv_filename)
                # TODO: add metadata file
        except Exception:
            shutil.rmtree(tmpdir, ignore_errors=True)
            raise

        return send_file(zip_path, mimetype='application/zip', download_name=zip_filename)
    #
    # if output_format == DOWNLOAD_JSON_FORMAT:
    #     if points:
    #         response_data = "[" + ",".join(
    #             map(lambda df: output_json(pd.concat(df).sort_values(by='time'), var, freq, decimals, month),
    #                 dfs)) + "]"
    #     else:
    #         df_groups = [g[1].reset_index().set_index('time') for g in
    #                      pd.concat(dfs).sort_values(by=['lat', 'lon', 'time']).groupby(by=['lat', 'lon'])]
    #         response_data = "[" + ",".join(map(lambda df: output_json(df, var, freq, decimals, month), df_groups)) + "]"
    #
    #     if zipped:
    #         zip_buffer = make_zip([
    #             ('metadata.txt', metadata),
    #             (f'{filename}.json', response_data),
    #         ])
    #         return send_file(zip_buffer, mimetype='application/zip', as_attachment=True, download_name=f'{filename}.zip')
    #     else:
    #         return Response(response_data, mimetype='application/json')

    return 200
