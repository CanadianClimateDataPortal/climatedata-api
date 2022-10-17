from flask import Flask, request, Response, send_file, current_app as app
from climatedata_api.utils import open_dataset, open_dataset_by_path
import json
from werkzeug.exceptions import BadRequestKeyError


def get_choro_values(partition, var, scenario, month='ann'):
    """
        Get regional data for all regions, single date
        ex: curl 'http://localhost:5000/get-choro-values/census/tx_max/rcp85/ann/?period=1971'
            curl 'http://localhost:5000/get-choro-values/census/tx_max/ssp585/ann/?period=2071&dataset_name=CMIP6&delta7100=true'
    """
    try:
        msys = app.config['MONTH_LUT'][month][1]
        month_number = app.config['MONTH_NUMBER_LUT'][month]
        dataset_name = request.args.get('dataset_name', 'CMIP5').upper()

        if dataset_name not in app.config['FILENAME_FORMATS']:
            raise KeyError("Invalid dataset requested")
        if scenario not in app.config['SCENARIOS'][dataset_name]:
            raise ValueError
        if var not in app.config['VARIABLES']:
            raise ValueError
        period = int(request.args['period'])
        delta7100 = request.args.get('delta7100', 'false')
    except (TypeError, ValueError, BadRequestKeyError, KeyError):
        return "Bad request", 400

    delta = f"_{app.config['DELTA_NAMING'][dataset_name]}" if delta7100 == "true" else ""

    bccaq_dataset = open_dataset(dataset_name, '30ymeans', var, msys, partition=partition)
    bccaq_time_slice = bccaq_dataset.sel(time=f"{period}-{month_number}-01")

    return Response(json.dumps(bccaq_time_slice[f"{scenario}_{var}{delta}_p50"]
                               .drop([i for i in bccaq_time_slice.coords if i != 'region']).to_dataframe().astype(
        'float64')
                               .round(2).fillna(0).transpose().values.tolist()[0]),
                    mimetype='application/json')


def _convert_delta30_values_to_dict(delta_30y_slice, var, delta, decimals, dataset_name, percentiles=['p10', 'p50', 'p90']):
    """

    :param delta_30y_slice: xarray dataset slice to convert
    :param var: climate variable name
    :param delta: "_delta7100" if delta is requested
    :param decimals: number of decimals
    :return: a dictionary of dictionaries containing all requested values
    """

    scenarios = app.config['SCENARIOS'][dataset_name]
    if delta_30y_slice[f'{scenarios[0]}_{var}_p50'].attrs.get('units') == 'K' and not delta:
        delta_30y_slice = delta_30y_slice + app.config['KELVIN_TO_C']

    values = {}
    for scenario in scenarios:
        values[scenario] = {p: round(delta_30y_slice[f"{scenario}_{var}{delta}_{p}"].item(), decimals) for p in
                            percentiles}
    return values


def get_delta_30y_gridded_values(lat, lon, var, month):
    """
    Fetch specific data within the delta30y dataset
    ex: curl 'http://localhost:5000/get-delta-30y-gridded-values/60.31062731740045/-100.06347656250001/tx_max/ann?period=1951&decimals=2'
        curl 'http://localhost:5000/get-delta-30y-gridded-values/60.31062731740045/-100.06347656250001/tx_max/ann?period=1951&decimals=2&delta7100=true'
        curl 'http://localhost:5000/get-delta-30y-gridded-values/60.31062731740045/-100.06347656250001/tx_max/ann?period=1951&decimals=2&dataset_name=CMIP6'
    :param lat: latitude (float as string)
    :param lon: longitude (float as string)
    :param var: climate variable name
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
        dataset_name = request.args.get('dataset_name', 'CMIP5').upper()

        if decimals < 0:
            return "Bad request: invalid number of decimals", 400
        if var not in app.config['VARIABLES']:
            raise ValueError
        if dataset_name not in app.config['FILENAME_FORMATS']:
            raise KeyError("Invalid dataset requested")
    except (ValueError, BadRequestKeyError, KeyError):
        return "Bad request", 400

    delta = f"_{app.config['DELTA_NAMING'][dataset_name]}" if delta7100 == "true" else ""

    delta_30y_dataset = open_dataset(dataset_name, '30ygraph', var, msys, period=monthpath)
    delta_30y_slice = delta_30y_dataset.sel(lon=loni, lat=lati, method='nearest').sel(time=f"{period}-{monthnumber}-01")
    return _convert_delta30_values_to_dict(delta_30y_slice, var, delta, decimals, dataset_name)


def get_slr_gridded_values(lat, lon):
    """
    Returns all values from sea-level change dataset for a requested location
    ex: curl http://localhost:5000/get-slr-gridded-values/61.04/-61.11?period=2050
    :param lat: latitude (float as string)
    :param lon: longitude (float as string)
    """
    try:
        lati = float(lat)
        loni = float(lon)
        period = int(request.args['period'])
    except (ValueError, BadRequestKeyError, KeyError):
        return "Bad request", 400

    dataset = open_dataset_by_path(app.config['NETCDF_SLR_PATH'].format(root=app.config['DATASETS_ROOT']))
    location_slice = dataset.sel(lon=loni, lat=lati, method='nearest').sel(time=f"{period}-01-01")

    dataset_enhanced = open_dataset_by_path(app.config['NETCDF_SLR_ENHANCED_PATH'].format(root=app.config['DATASETS_ROOT']))
    enhanced_location_slice = dataset_enhanced.sel(lon=loni, lat=lati, method='nearest')

    slr_values = _convert_delta30_values_to_dict(location_slice, 'slr', "", 0, 'CMIP5', percentiles=['p05', 'p50', 'p95'])
    slr_values['rcp85plus65'] = {'p50': round(enhanced_location_slice['enhanced_p50'].item(), 0)}
    return slr_values


def get_delta_30y_regional_values(partition, index, var, month):
    """
    Fetch specific data within the delta30y partitionned dataset (health/census/...)
        ex: curl 'http://localhost:5000/get-delta-30y-regional-values/census/4510/tx_max/ann?period=1951'
            curl 'http://localhost:5000/get-delta-30y-regional-values/census/4510/tx_max/ann?period=1951&dataset_name=CMIP6'
            curl 'http://localhost:5000/get-delta-30y-regional-values/census/4510/tx_max/ann?period=1951&delta7100=true'
    :param partition: partition name
    :param index: ID of the region (int as string)
    :param var: climate variable name
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
        dataset_name = request.args.get('dataset_name', 'CMIP5').upper()

        if decimals < 0:
            return "Bad request: invalid number of decimals", 400
        if var not in app.config['VARIABLES']:
            raise ValueError
        if dataset_name not in app.config['FILENAME_FORMATS']:
            raise KeyError("Invalid dataset requested")

    except (ValueError, BadRequestKeyError, KeyError):
        return "Bad request", 400

    delta = f"_{app.config['DELTA_NAMING'][dataset_name]}" if delta7100 == "true" else ""
    delta_30y_dataset = open_dataset(dataset_name, '30ygraph', var, msys, partition=partition)
    delta_30y_slice = delta_30y_dataset.sel(region=indexi).sel(time=f"{period}-{monthnumber}-01")

    return _convert_delta30_values_to_dict(delta_30y_slice, var, delta, decimals, dataset_name)
