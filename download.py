from flask import Flask, request, Response, send_file, current_app as app
from textwrap import dedent
import os
import tempfile
import numpy as np
import xarray as xr
import pandas as pd
from utils import open_dataset, open_dataset_by_path
from werkzeug.exceptions import BadRequestKeyError
import itertools


def get_frame(dataset, point, adjust, limit=None):
    """
        Returns a dataframe for a specific coordinate (lat,lon)
        :param dataset: the xarray dataset to convert
        :param point: (1x2 array): coordinates to select [lat,lon]
        :param adjust: constant to add to the whole dataset after the select but before conversion (ex: for Kelvin to °C)
        :param limit: lower time limit of the data to keep
        :return: a sliced from the dataset as a dataframe
    """
    ds = dataset.sel(lat=point[0], lon=point[1], method='nearest').dropna('time')
    if adjust:
        ds = ds + adjust
    if limit:
        ds = ds.where(ds.time >= np.datetime64(limit), drop=True)
    return ds.to_dataframe()


def output_json(df, var, freq, period=''):
    """
        Outputs a dataframe to JSON for use with the portal
        :param df: the dataframe
        :param var: name of the variable to export
        :param freq: the frequency sampling (MS|YS)
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
       {{"data": {df.drop(columns=['lon', 'lat'], axis=1).to_json(orient='index', date_format='iso', date_unit='s', double_precision=2)} }}]
    """)


def download():
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
    args = request.get_json()
    try:
        var = args['var']
        month = args['month']
        format = args['format']
        points = args['points']
        monthpath, freq = app.config['MONTH_LUT'][month]
        if var not in app.config['VARIABLES']:
            raise ValueError
        if len(points) == 0:
            raise ValueError

        # Check if user abuses the API
        points_limit = app.config['DOWNLOAD_POINTS_LIMIT']
        if month == 'all':
            points_limit = points_limit / 12
        if len(points) > points_limit:
            return "Bad request: too many points requested", 400
        for p in points:
            if len(p) != 2:
                raise ValueError
    except (ValueError, BadRequestKeyError, KeyError, TypeError):
        return "Bad request", 400

    limit = None
    if var == 'slr':
        datasets = [open_dataset_by_path(app.config['NETCDF_SLR_PATH'])]
    elif var in app.config['SPEI_VARIABLES']:
        datasets = [open_dataset_by_path(
            app.config['NETCDF_SPEI_FILENAME_FORMATS'].format(root=app.config['NETCDF_SPEI_FOLDER'], var=var))]
        if month != 'all':
            monthnumber = app.config['MONTH_NUMBER_LUT'][month]
            datasets[0] = datasets[0].sel(time=(datasets[0].time.dt.month == monthnumber))
        limit = app.config['SPEI_DATE_LIMIT']
    else:
        if month == 'all':
            datasets = [open_dataset(var, freq, m, app.config['NETCDF_BCCAQV2_FILENAME_FORMATS'],
                                     app.config['NETCDF_BCCAQV2_YEARLY_FOLDER']) for m in app.config['ALLMONTHS']]
        else:
            datasets = [open_dataset(var, freq, monthpath, app.config['NETCDF_BCCAQV2_FILENAME_FORMATS'],
                                     app.config['NETCDF_BCCAQV2_YEARLY_FOLDER'])]

    if var not in app.config['SPEI_VARIABLES'] and datasets[0]['rcp26_{}_p50'.format(var)].attrs.get('units') == 'K':
        adjust = app.config['KELVIN_TO_C']
    else:
        adjust = 0

    dfs = [[get_frame(dataset, p, adjust, limit) for dataset in datasets] for p in points]

    # remove empty dataframes
    dfs = [[df for df in dflist if not df.empty] for dflist in dfs]
    dfs = [dflist for dflist in dfs if not len(dflist) == 0]
    if len(dfs) == 0:
        return "No points found", 404

    if format == 'csv':
        dfs = [j for sub in dfs for j in sub]  # flattens sublists
        return Response(pd.concat(dfs).sort_values(by=['lat', 'lon', 'time']).to_csv(float_format='%.2f'),
                        mimetype='text/csv')
    if format == 'json':
        return Response("[" + ",".join(
            map(lambda df: output_json(pd.concat(df).sort_values(by='time'), var, freq, month), dfs)) + "]",
                        mimetype='application/json')
    return "Bad request", 400


def _format_30y_slice(delta_30y_slice, var, decimals):
    """
        Returns a csv response object from a xarray slice
    """
    if delta_30y_slice[f'rcp26_{var}_p50'].attrs.get('units') == 'K':
        for v in [v for v in delta_30y_slice.data_vars if 'delta' not in v]:
            delta_30y_slice[v] = delta_30y_slice[v] + app.config['KELVIN_TO_C']
    df = delta_30y_slice.to_dataframe()
    df = df.reindex(columns=["rcp{1}_{var}_{0}p{2}".format(*a, var=var) for a in
                             itertools.product(['', 'delta7100_'], [26, 45, 85], [10, 50, 90])])

    csv_data = df.to_csv(float_format=f"%.{decimals}f")

    return Response(csv_data, mimetype='text/csv')


def download_30y(var, lat, lon, month):
    """
        Download the 30y and delta values for a single grid cell
        ex: curl 'http://localhost:5000/download-30y/60.31062731740045/-100.06347656250001/tx_max/ann'
    :return: csv string
    """
    try:
        lati = float(lat)
        loni = float(lon)
        decimals = request.args.get('decimals', 2)
        month_path, msys = app.config['MONTH_LUT'][month]
        if var not in app.config['VARIABLES']:
            raise ValueError
    except (ValueError, BadRequestKeyError, KeyError):
        return "Bad request", 400

    delta_30y_dataset = open_dataset_by_path(
        app.config['NETCDF_BCCAQV2_30Y_FILENAME_FORMAT'].format(root=app.config['NETCDF_BCCAQV2_30Y_FOLDER'],
                                                                var=var,
                                                                msys=msys,
                                                                month=month_path))
    delta_30y_slice = delta_30y_dataset.sel(lon=loni, lat=lati, method='nearest').drop(['lat', 'lon']).dropna('time')
    response = _format_30y_slice(delta_30y_slice, var, decimals)
    delta_30y_dataset.close()
    return response


def download_regional_30y(partition, index, var, month):
    """
        Download the 30y and delta values for a single grid cell
        ex: curl 'http://localhost:5000/download-regional-30y/census/1/tx_max/ann'
    :return: csv string
    """
    try:
        indexi = int(index)
        decimals = request.args.get('decimals', 2)
        msys = app.config['MONTH_LUT'][month][1]
        monthnumber = app.config['MONTH_NUMBER_LUT'][month]
        if var not in app.config['VARIABLES']:
            raise ValueError
        delta30y_path = app.config['PARTITIONS_PATH_FORMATS'][partition]['30yGraph'].format(
            root=app.config['PARTITIONS_FOLDER'][partition]['30yGraph'],
            var=var,
            msys=msys)
    except (ValueError, BadRequestKeyError, KeyError):
        return "Bad request", 400

    delta_30y_dataset = open_dataset_by_path(delta30y_path)
    delta_30y_slice = delta_30y_dataset.sel(region=indexi).drop(
        [i for i in delta_30y_dataset.coords if i != 'time']).dropna('time')

    # we filter the appropriate month/season from the MS or QS-DEC file
    if msys in ["MS", "QS-DEC"]:
        delta_30y_slice = delta_30y_slice.sel(
            time=(delta_30y_slice.time.dt.month == monthnumber))

    response = _format_30y_slice(delta_30y_slice, var, decimals)
    delta_30y_dataset.close()
    return response

    return Response(csv_data, mimetype='text/csv')


def download_ahccd():
    """
        download one or multiple AHCCD station data

        curl examples:
        # case where two stations are in both temperatures and precipitations
        POST:
            curl -s http://localhost:5000/download-ahccd -H "Content-Type: application/json" -X POST -d '{ "format" : "csv", "stations": ["3081680","8400413"]}'
        GET:
            curl -s "http://localhost:5000/download-ahccd?format=csv&stations=3081680,8400413"
        # case where the station is only in precipitations
        curl -s http://localhost:5000/download-ahccd -H "Content-Type: application/json" -X POST -d '{ "format" : "csv", "stations": ["3034720"]}'
        # case where the station is only in temperature
        curl -s http://localhost:5000/download-ahccd -H "Content-Type: application/json" -X POST -d '{ "format" : "csv", "stations": ["8402757"]}'
        # case where one stations is in temperatures and the other in precipitations
        curl -s http://localhost:5000/download-ahccd -H "Content-Type: application/json" -X POST -d '{ "format" : "csv", "stations": ["3034720","8402757"]}'
    """
    try:
        if request.method == 'POST':
            args = request.get_json()
            stations = args['stations']
        else:
            args = request.args
            stations = args['stations'].split(',')

        format = args['format']
        if len(stations) == 0:
            raise ValueError
    except (ValueError, BadRequestKeyError, KeyError, TypeError):
        return "Bad request", 400

    # some local config variables
    commondrops = ['fromyear', 'frommonth', 'toyear', 'tomonth', 'stnid']
    variables = [
        {'name': 'tas',
         'filename': 'ahccd_gen3_tas.nc',
         'drops': commondrops + ['no', 'pct_miss', 'joined', 'rcs']},
        {'name': 'tasmax',
         'filename': 'ahccd_gen3_tasmax.nc',
         'drops': commondrops + ['no', 'pct_miss', 'joined', 'rcs']},
        {'name': 'tasmin',
         'filename': 'ahccd_gen3_tasmin.nc',
         'drops': commondrops + ['no', 'pct_miss', 'joined', 'rcs']},
        {'name': 'pr',
         'filename': 'ahccd_gen2_pr.nc',
         'drops': commondrops + ['stns_joined']},
        {'name': 'prlp',
         'filename': 'ahccd_gen2_prlp.nc',
         'drops': commondrops + ['stns_joined']},
        {'name': 'prsn',
         'filename': 'ahccd_gen2_prsn.nc',
         'drops': commondrops + ['stns_joined']}]

    allds = []
    encoding = {}

    for var in variables:
        ds = xr.open_dataset(os.path.join(app.config['AHCCD_FOLDER'], var['filename']), mask_and_scale=False,
                                decode_times=False)
        ds['time'] = xr.decode_cf(ds).time
        s = list(set(stations).intersection(set(ds['station'].values)))
        if s:
            ds = ds.sel(station=s)
            time_idx = slice(f'{int(ds.fromyear.min().values)}', f'{int(ds.toyear.max().values)}')
            ds = ds.sel(time=time_idx).drop(var['drops']).load()
            allds.append(ds)
            encoding[var['name']] = {"zlib": True}

    # we copy missing attributes of stations not present in the tas dataset
    if len(allds) == len(variables):
        for s in stations:
            if s not in allds[0]['station']:
                allds[0] = xr.merge([allds[0], allds[3].sel(station=[s]).drop(['pr', 'pr_flag'])],
                                    compat="no_conflicts")

    ds = xr.merge(allds, compat="override")
    if format == 'netcdf':
        filename = os.path.join(app.config['TEMPDIR'], next(tempfile._get_candidate_names()))
        ds.to_netcdf(filename, encoding=encoding, format='NETCDF4_CLASSIC')
        f = open(filename, "rb")
        os.unlink(filename)
        return send_file(f, mimetype='application/x-netcdf4', as_attachment=True, attachment_filename='ahccd.nc')

    if format == 'csv':
        df = ds.to_dataframe()
        df = df.reindex(columns=[c for c in app.config['AHCCD_ORDER'] if c in df.columns])
        return Response(df.to_csv(chunksize=100000),
                        mimetype='text/csv',
                        headers={"Content-disposition": "attachment; filename=ahccd.csv"})

    return "Bad request", 400
