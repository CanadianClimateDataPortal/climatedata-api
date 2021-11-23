from flask import Flask, request, Response, send_file, current_app as app
from utils import open_dataset_by_path
import json
from werkzeug.exceptions import BadRequestKeyError


def get_choro_values(partition, var, model, month='ann'):
    """
        Get regional data for all regions, single date
        ex: curl 'http://localhost:5000/get-choro-values/census/tx_max/rcp85/ann/?period=1971'
    """
    try:
        msys = app.config['MONTH_LUT'][month][1]
        month_number = app.config['MONTH_NUMBER_LUT'][month]
        if model not in app.config['MODELS']:
            raise ValueError
        if var not in app.config['VARIABLES']:
            raise ValueError
        period = int(request.args['period'])
        delta7100 = request.args.get('delta7100', 'false')
        dataset_path = app.config['PARTITIONS_PATH_FORMATS'][partition]['means'].format(
            root=app.config['PARTITIONS_FOLDER'][partition]['BCCAQ'],
            var=var,
            msys=msys)
    except (TypeError, ValueError, BadRequestKeyError, KeyError):
        return "Bad request", 400

    delta = "_delta7100" if delta7100 == "true" else ""

    bccaq_dataset = open_dataset_by_path(dataset_path)
    bccaq_time_slice = bccaq_dataset.sel(time="{}-{}-01".format(period, month_number))

    return Response(json.dumps(bccaq_time_slice[f"{model}_{var}{delta}_p50"]
                               .drop([i for i in bccaq_time_slice.coords if i != 'region']).to_dataframe().astype(
        'float64')
                               .round(2).fillna(0).transpose().values.tolist()[0]),
                    mimetype='application/json')


def get_grid_hover_values(lat, lon, var, model, month):
    """
    Fetch specific data within the delta30y dataset
    ex:  curl 'http://localhost:5000/get-grid-hover-values/60.31062731740045/-100.06347656250001/tx_max/rcp85/ann?period=1951&decimals=2'
        curl 'http://localhost:5000/get-grid-hover-values/60.31062731740045/-100.06347656250001/tx_max/rcp85/ann?period=1951&decimals=2&delta7100=true'
    :param lat: latitude (float as string)
    :param lon: longitude (float as string)
    :param var: climate variable name
    :param model: [rcp26,rcp45,rcp85]
    :param month: [ann,jan,feb,...]
    :return: dictionary for p10, p50 and p90 of requested values
    """
    try:
        lati = float(lat)
        loni = float(lon)
        monthpath, msys = app.config['MONTH_LUT'][month]
        monthnumber = app.config['MONTH_NUMBER_LUT'][month]
        period = int(request.args['period'])
        delta7100 = request.args.get('delta7100', 'false')
        decimals = int(request.args.get('decimals', 2))
        if decimals < 0:
            return "Bad request: invalid number of decimals", 400

        if var not in app.config['VARIABLES']:
            raise ValueError
    except (ValueError, BadRequestKeyError, KeyError):
        return "Bad request", 400

    delta = "_delta7100" if delta7100 == "true" else ""

    delta_30y_dataset = open_dataset_by_path(
        app.config['NETCDF_BCCAQV2_30Y_FILENAME_FORMAT'].format(root=app.config['NETCDF_BCCAQV2_30Y_FOLDER'],
                                                                var=var,
                                                                msys=msys,
                                                                month=monthpath))
    delta_30y_slice = delta_30y_dataset.sel(lon=loni, lat=lati, method='nearest').sel(time=f"{period}-{monthnumber}-01")
    if delta_30y_slice[f'rcp26_{var}_p50'].attrs.get('units') == 'K' and not delta:
        delta_30y_slice = delta_30y_slice + app.config['KELVIN_TO_C']
    return {p: round(delta_30y_slice[f"{model}_{var}{delta}_{p}"].item(), decimals) for p in ['p10', 'p50', 'p90']}


def get_regional_hover_values(partition, index, var, model, month):
    """
    Fetch specific data within the delta30y partitionned dataset (health/census/...)
        ex: curl 'http://localhost:5000/get-regional-hover-values/census/4510/tx_max/rcp85/ann?period=1951'
            curl 'http://localhost:5000/get-regional-hover-values/census/4510/tx_max/rcp85/ann?period=1951&delta7100=true'
    :param partition: partition name
    :param index: ID of the region (int as string)
    :param var: climate variable name
    :param model: [rcp26,rcp45,rcp85]
    :param month: [ann,jan,feb,...]
    :return: dictionary for p10, p50 and p90 of requested values
    """
    try:
        indexi = int(index)
        msys = app.config['MONTH_LUT'][month][1]
        monthnumber = app.config['MONTH_NUMBER_LUT'][month]
        period = int(request.args['period'])
        delta7100 = request.args.get('delta7100', 'false')
        decimals = int(request.args.get('decimals', 2))
        if decimals < 0:
            return "Bad request: invalid number of decimals", 400

        if var not in app.config['VARIABLES']:
            raise ValueError

        delta30y_path = app.config['PARTITIONS_PATH_FORMATS'][partition]['30yGraph'].format(
            root=app.config['PARTITIONS_FOLDER'][partition]['30yGraph'],
            var=var,
            msys=msys)
    except (ValueError, BadRequestKeyError, KeyError):
        return "Bad request", 400

    delta = "_delta7100" if delta7100 == "true" else ""
    delta_30y_dataset = open_dataset_by_path(delta30y_path)
    delta_30y_slice = delta_30y_dataset.sel(region=indexi).sel(time=f"{period}-{monthnumber}-01")
    if delta_30y_slice[f'rcp26_{var}_p50'].attrs.get('units') == 'K' and not delta:
        delta_30y_slice = delta_30y_slice + app.config['KELVIN_TO_C']
    return {p: round(delta_30y_slice[f"{model}_{var}{delta}_{p}"].item(), decimals) for p in ['p10', 'p50', 'p90']}
