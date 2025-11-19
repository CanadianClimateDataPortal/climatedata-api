from datetime import datetime
import json

from flask import Flask, Response
from flask import current_app as app
from flask import request, send_file
from werkzeug.exceptions import BadRequestKeyError

from climatedata_api.utils import (
    open_dataset,
    open_dataset_by_path,
    decode_compressed_points,
    load_s2d_datasets_by_periods,
    retrieve_s2d_release_date,
)
import pickle
import numpy as np
from sentry_sdk import capture_message

from default_settings import (
    S2D_CLIMATO_DATA_VAR_NAMES,
    S2D_FORECAST_DATA_VAR_NAMES,
    S2D_SKILL_DATA_VAR_NAMES,
)


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
        decimals = int(request.args.get('decimals', 2))
    except (TypeError, ValueError, BadRequestKeyError, KeyError):
        return "Bad request", 400

    delta = f"_{app.config['DELTA_NAMING'][dataset_name]}" if delta7100 == "true" else ""

    bccaq_dataset = open_dataset(dataset_name, '30ymeans', var, msys, partition=partition)
    bccaq_time_slice = bccaq_dataset.sel(time=f"{period}-{month_number}-01")

    return Response(json.dumps(bccaq_time_slice[f"{scenario}_{var}{delta}_p50"]
                               .drop([i for i in bccaq_time_slice.coords if i != 'region']).to_dataframe().astype(
        'float64')
        .round(decimals).fillna(0).transpose().values.tolist()[0]),
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
        # Skip the scenario if it's not available for this variable
        scenario_test_key = f"{scenario}_{var}{delta}_{percentiles[0]}"
        if scenario_test_key not in delta_30y_slice:
            continue
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
    ex: curl http://localhost:5000/get-slr-gridded-values/61.04/-61.11?period=2050&dataset_name=CMIP6
    :param lat: latitude (float as string)
    :param lon: longitude (float as string)
    """
    try:
        lati = float(lat)
        loni = float(lon)
        period = int(request.args['period'])
        dataset_name = request.args.get('dataset_name', 'CMIP5').upper()
        if dataset_name not in ['CMIP5', 'CMIP6']:
            raise KeyError("Invalid dataset requested")
    except (ValueError, BadRequestKeyError, KeyError):
        return "Bad request", 400

    slr_path = app.config['NETCDF_SLR_CMIP5_PATH'] if dataset_name == "CMIP5" else app.config['NETCDF_SLR_CMIP6_PATH']

    dataset = open_dataset_by_path(slr_path.format(root=app.config['DATASETS_ROOT']))
    location_slice = dataset.sel(lon=loni, lat=lati, method='nearest').sel(time=f"{period}-01-01")

    if dataset_name == "CMIP5":
        dataset_enhanced = open_dataset_by_path(
            app.config['NETCDF_SLR_ENHANCED_PATH'].format(root=app.config['DATASETS_ROOT']))
        enhanced_location_slice = dataset_enhanced.sel(lon=loni, lat=lati, method='nearest')

        slr_values = _convert_delta30_values_to_dict(
            location_slice, 'slr', '', 0, 'CMIP5', percentiles=['p05', 'p50', 'p95'])
        slr_values['rcp85plus65'] = {'p50': round(enhanced_location_slice['enhanced_p50'].item(), 0)}
    else:  # CMIP6
        slr_values = _convert_delta30_values_to_dict(
            location_slice, 'slr', '', 0, 'CMIP6', percentiles=['p17', 'p50', 'p83'])
        slr_values['ssp585lowConf'] = {'p83': round(location_slice['ssp585lowConf_slr_p83'].item(), 0)}
        slr_values['ssp585highEnd'] = {'p98': round(location_slice['ssp585highEnd_slr_p98'].item(), 0)}
        slr_values['uplift'] = {'uplift': round(location_slice['uplift'].item(), 0)}

    return slr_values


def get_allowance_gridded_values(lat, lon):
    """
    Returns all values from vertical allowance dataset for a requested location
    ex: curl http://localhost:5000/get-allowance-gridded-values/61.04/-61.11?period=2050&dataset_name=CMIP6
    :param lat: latitude (float as string)
    :param lon: longitude (float as string)
    """
    try:
        lati = float(lat)
        loni = float(lon)
        period = int(request.args['period'])
        dataset_name = request.args.get('dataset_name', 'CMIP6').upper()
        if dataset_name not in ['CMIP6']:
            raise KeyError("Invalid dataset requested")
    except (ValueError, BadRequestKeyError, KeyError):
        return "Bad request", 400

    dataset = open_dataset_by_path(app.config['NETCDF_ALLOWANCE_PATH'].format(root=app.config['DATASETS_ROOT']))
    location_slice = dataset.sel(lon=loni, lat=lati, method='nearest').sel(time=f"{period}-01-01")

    allowance_values = _convert_delta30_values_to_dict(
        location_slice, 'allowance', "", 0, dataset_name, percentiles=['p50'])

    return allowance_values


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
    delta_30y_slice = delta_30y_dataset.sel(geom=indexi).sel(time=f"{period}-{monthnumber}-01")

    return _convert_delta30_values_to_dict(delta_30y_slice, var, delta, decimals, dataset_name)


def get_id_list_from_points(compressed_points):
    """
    Return feature unique id (gid) that matches all points provided in request
    curl 'http://localhost:5000/get-gids/-stivPu3IqgC0-J91gBs_RxxxCq8Dl5I41gBiv6Bv3ctqH75E16yBztY0vNt3KprlB70D?gridname=canadagrid'

    """
    try:
        gridname = request.args.get('gridname')
        kdfile = app.config['CACHE_FOLDER'] / f"kdtree-{gridname}.pickle"
        points = decode_compressed_points(compressed_points)

    except KeyError:
        return "Bad request", 400
    points_to_search = np.array(points)

    with kdfile.open('rb') as f:
        tree = pickle.load(f)
    distances, gids = tree.query(points_to_search, distance_upper_bound=1.0)

    # verify that all points got a match. If a point doesn't have a match then there is a frontend issue, so we capture a message to Sentry
    for dist in distances:
        if dist == np.inf:
            print("Unmatched point found")
            capture_message(f"get_id_list_from_points: unmatched point found. Query is: {compressed_points}")
            break

    return gids.tolist()

def get_s2d_release_date(var, freq):
    """
    Return the release date of the forecast data associated with a given variable and frequency
    curl 'http://localhost:5000/get-s2d-release-date/air_temp/seasonal'

    The returned value is a JSON string in the YYYY-MM-DD format.
    """
    try:
        latest_datetime_str = retrieve_s2d_release_date(var, freq)
    except ValueError:
        return "Bad request", 400

    return Response(
        json.dumps(latest_datetime_str),
        mimetype='application/json'
    )

def get_s2d_gridded_values(lat, lon, var, freq, period):
    """
    Fetch specific data within the S2D dataset
    e.g. : curl 'http://localhost:5000/get-s2d-gridded-values/61.04/-61.11?/air_temp/seasonal?period=2025-07'
    """
    try:
        latitude = float(lat)
        longitude = float(lon)
        if var not in app.config['S2D_VARIABLES']:
            raise ValueError
        if freq not in app.config['S2D_FREQUENCIES']:
            raise ValueError
        period_date = datetime.strptime(period, "%Y-%m")
    except (ValueError, BadRequestKeyError):
        return "Bad request", 400

    ref_period = datetime.strptime(retrieve_s2d_release_date(var, freq), "%Y-%m-%d")

    try:
        forecast_slice, climatology_slice, skill_slice = load_s2d_datasets_by_periods(var, freq, [period_date], ref_period)
    except ValueError as e:
        return e, 400

    forecast_slice = forecast_slice.sel(lon=longitude, lat=latitude, method='nearest')
    climatology_slice = climatology_slice.sel(lon=longitude, lat=latitude, method='nearest')
    skill_slice = skill_slice.sel(lon=longitude, lat=latitude, method='nearest')

    values = {}
    for var_list, dataset in [
            (S2D_FORECAST_DATA_VAR_NAMES, forecast_slice),
            (S2D_CLIMATO_DATA_VAR_NAMES, climatology_slice),
            (S2D_SKILL_DATA_VAR_NAMES, skill_slice)]:
        values.update({var: dataset[var].item() for var in var_list})
    return values
