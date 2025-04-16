import itertools
import os
import tempfile
from textwrap import dedent

import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype

import xarray as xr
from clisops.core.subset import subset_bbox
from flask import Response
from flask import current_app as app
from flask import request, send_file
from werkzeug.exceptions import BadRequestKeyError

from climatedata_api.utils import format_metadata, make_zip, open_dataset, open_dataset_by_path


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
            raise ValueError

        if dataset_name not in app.config['FILENAME_FORMATS']:
            raise KeyError("Invalid dataset requested")

        if dataset_type not in app.config['FILENAME_FORMATS'][dataset_name]:
            raise KeyError("Invalid dataset type requested")

        if points and bbox:
            return "Bad request: can't request both points and bbox simultaneously", 400

        if points:
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
        elif bbox:
            if len(bbox) != 4:
                return "Bad request: bbox must be an array of length 4", 400
            # TODO: calculate add API limit here... implementation may be way faster than points
        else:
            return "Bad request: neither points or bbox requested", 400

    except (ValueError, BadRequestKeyError, KeyError, TypeError):
        return "Bad request", 400

    scenarios = app.config['SCENARIOS'][dataset_name]
    limit = None
    if var == 'slr':
        if dataset_name not in ['CMIP5', 'CMIP6']:
            return (f"Bad request: Invalid dataset `{dataset_name}` for slr."
                    " Only `CMIP5` and `CMIP6` are available for slr."), 400
        slr_path = app.config['NETCDF_SLR_CMIP5_PATH'] if dataset_name == "CMIP5" else app.config['NETCDF_SLR_CMIP6_PATH']
        datasets = [open_dataset_by_path(slr_path.format(root=app.config['DATASETS_ROOT']))]
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

    if output_format == 'netcdf':
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

    if output_format == 'csv':
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

    if output_format == 'json':
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

    if format == 'netcdf':
        f = output_netcdf(ds, encoding, 'NETCDF4_CLASSIC')
        return send_file(f, mimetype='application/x-netcdf4', as_attachment=True, download_name='ahccd.nc')

    if format == 'csv':
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
