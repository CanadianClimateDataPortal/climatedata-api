import numpy as np
import xarray as xr
from flask import current_app as app
from flask import request
from werkzeug.exceptions import BadRequestKeyError

from climatedata_api.utils import (convert_time_series_dataset_to_dict,
                                   convert_time_series_dataset_to_list,
                                   open_dataset, open_dataset_by_path)


def _format_slices_to_highcharts_series(observations_location_slice, bccaq_location_slice, delta_30y_slice, var, decimals, dataset_name):
    """
    Format observations, bccaq and delta_30y slices to dictionary of series ready for highcharts
    """
    scenarios = app.config['SCENARIOS'][dataset_name]
    delta_naming = app.config['DELTA_NAMING'][dataset_name]

    if bccaq_location_slice[f'{scenarios[0]}_{var}_p50'].attrs.get('units') == 'K':
        bccaq_location_slice = bccaq_location_slice + app.config['KELVIN_TO_C']

    chart_series = {}

    if observations_location_slice and len(observations_location_slice.time) > 0:
        if observations_location_slice[var].attrs.get('units') == 'K':
            observations_location_slice = observations_location_slice + app.config['KELVIN_TO_C']

        observations_location_slice_30y = observations_location_slice.rolling({'time': 30}).mean().dropna('time')
        observations_location_slice_30y['time'] = observations_location_slice.time[0:len(observations_location_slice_30y.time)]
        observations_location_slice_30y = observations_location_slice_30y.sel(time=(observations_location_slice_30y.time.dt.year % 10 == 1))

        chart_series['observations'] = convert_time_series_dataset_to_list(observations_location_slice, decimals)
        chart_series['30y_observations'] = convert_time_series_dataset_to_dict(observations_location_slice_30y, decimals)

    else:
        chart_series['observations'] = []
        chart_series['30y_observations'] = []

    # we return the historical values for a single scenario before HISTORICAL_DATE_LIMIT
    bccaq_location_slice_historical = bccaq_location_slice.where(
        bccaq_location_slice.time <= np.datetime64(app.config['HISTORICAL_DATE_LIMIT_BEFORE'][dataset_name]), drop=True)
    chart_series['modeled_historical_median'] = convert_time_series_dataset_to_list(
        bccaq_location_slice_historical[f'{scenarios[0]}_{var}_p50'], decimals)
    chart_series['modeled_historical_range'] = convert_time_series_dataset_to_list(
        xr.merge([bccaq_location_slice_historical[f'{scenarios[0]}_{var}_p10'],
                  bccaq_location_slice_historical[f'{scenarios[0]}_{var}_p90']]), decimals)
    # we return values in historical for all scenarios after HISTORICAL_DATE_LIMIT
    bccaq_location_slice = bccaq_location_slice.where(
        bccaq_location_slice.time >= np.datetime64(app.config['HISTORICAL_DATE_LIMIT_AFTER'][dataset_name]), drop=True)

    for scenario in scenarios:
        # Skip the scenario if it's not available for this variable
        if f'{scenario}_{var}_p50' not in bccaq_location_slice:
            continue
        chart_series[scenario + '_median'] = convert_time_series_dataset_to_list(
            bccaq_location_slice[f'{scenario}_{var}_p50'], decimals)
        chart_series[scenario + '_range'] = convert_time_series_dataset_to_list(
            xr.merge([bccaq_location_slice[f'{scenario}_{var}_p10'],
                      bccaq_location_slice[f'{scenario}_{var}_p90']]), decimals)
        chart_series[f"delta7100_{scenario}_median"] = convert_time_series_dataset_to_dict(
            delta_30y_slice[f'{scenario}_{var}_{delta_naming}_p50'], decimals)
        chart_series[f"delta7100_{scenario}_range"] = convert_time_series_dataset_to_dict(
            xr.merge([delta_30y_slice[f'{scenario}_{var}_{delta_naming}_p10'],
                      delta_30y_slice[f'{scenario}_{var}_{delta_naming}_p90']]), decimals)

    if delta_30y_slice[f'{scenarios[0]}_{var}_p50'].attrs.get('units') == 'K':
        delta_30y_slice = delta_30y_slice + app.config['KELVIN_TO_C']

    for scenario in scenarios:
        # Skip the scenario if it's not available for this variable
        if f'{scenario}_{var}_p50' not in delta_30y_slice:
            continue
        chart_series[f"30y_{scenario}_median"] = convert_time_series_dataset_to_dict(
            delta_30y_slice[f'{scenario}_{var}_p50'],
            decimals)
        chart_series[f"30y_{scenario}_range"] = convert_time_series_dataset_to_dict(
            xr.merge([delta_30y_slice[f'{scenario}_{var}_p10'],
                      delta_30y_slice[f'{scenario}_{var}_p90']]), decimals)

    return chart_series


def generate_charts(var, lat, lon, month='ann'):
    """
    Rewrite of get_values, generating a JSON ready for highcharts
    ex: curl 'http://localhost:5000/generate-charts/60.31062731740045/-100.06347656250001/tx_max/ann'
        curl 'http://localhost:5000/generate-charts/60.31062731740045/-100.06347656250001/tx_max/ann?dataset_name=CMIP6'
    """
    try:
        lati = float(lat)
        loni = float(lon)
        monthpath, msys = app.config['MONTH_LUT'][month]
        monthnumber = app.config['MONTH_NUMBER_LUT'][month]
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

    # SPEI treatment is very different
    if var in app.config['SPEI_VARIABLES']:
        return generate_spei_charts(var, lati, loni, month, decimals)

    # so does sea level rise
    if var == 'slr':
        return generate_slr_charts(lati, loni)

    if var == 'allowance':
        if dataset_name != 'CMIP6':
            return f"Bad request : `allowance` variable only uses the CMIP6 dataset, and has no {dataset_name} data available.\n", 400
        return generate_allowance_charts(lati, loni)

    try:
        observations_dataset = open_dataset(app.config['OBSERVATIONS_DATASET'][dataset_name],
                                            'allyears', var, msys, monthpath)
        observations_location_slice = observations_dataset.sel(lon=loni, lat=lati, method='nearest').drop(['lat', 'lon']).dropna(
            'time')
        observations_location_slice = observations_location_slice.sel(time=(observations_location_slice.time.dt.month == monthnumber))
    except FileNotFoundError:
        # observations doesn't exist for some variable, ex: HXMax
        observations_location_slice = None
        observations_dataset = None

    bccaq_dataset = open_dataset(dataset_name, 'allyears', var, msys, monthpath)
    bccaq_location_slice = bccaq_dataset.sel(lon=loni, lat=lati, method='nearest').drop(['lat', 'lon']).dropna('time')

    delta_30y_dataset = bccaq_dataset = open_dataset(dataset_name, '30ygraph', var, msys, monthpath)
    delta_30y_slice = delta_30y_dataset.sel(lon=loni, lat=lati, method='nearest').drop(
        [i for i in delta_30y_dataset.coords if i != 'time']).dropna('time')

    chart_series = _format_slices_to_highcharts_series(observations_location_slice, bccaq_location_slice, delta_30y_slice,
                                                       var, decimals, dataset_name)
    bccaq_dataset.close()
    if observations_dataset:
        observations_dataset.close()
    delta_30y_dataset.close()
    return chart_series


def generate_spei_charts(var, lati, loni, month, decimals):
    """
        Copied from generate-charts, but handles the difference for SPEI (i.e. single file)
        ex: curl 'http://localhost:5000/generate-charts/60.31062731740045/-100.06347656250001/spei_12m/ann'
    """
    # very specific exception for location page, return december values (annual doesn't exist anyway for SPEI)
    if month == 'ann':
        month = 'dec'

    monthnumber = app.config['MONTH_NUMBER_LUT'][month]
    dataset = open_dataset_by_path(
        app.config['NETCDF_SPEI_FILENAME_FORMATS'].format(root=app.config['DATASETS_ROOT'], var=var))
    observed_dataset = open_dataset_by_path(
        app.config['NETCDF_SPEI_OBSERVED_FILENAME_FORMATS'].format(root=app.config['DATASETS_ROOT'], var=var))
    location_slice = dataset.sel(lon=loni, lat=lati, method='nearest').drop(['lat', 'lon', 'scale']).dropna('time')
    location_slice = location_slice.sel(time=(location_slice.time.dt.month == monthnumber))

    observed_location_slice = observed_dataset.sel(lon=loni, lat=lati, method='nearest').drop(
        ['lat', 'lon', 'scale']).dropna('time')
    observed_location_slice = observed_location_slice.sel(time=(observed_location_slice.time.dt.month == monthnumber))
    observed_location_slice = observed_location_slice.where(
        observed_location_slice.time >= np.datetime64(app.config['SPEI_DATE_LIMIT']), drop=True)
    chart_series = {}
    chart_series['observations'] = convert_time_series_dataset_to_list(observed_location_slice, decimals)

    # we return the historical values for a single scenario before HISTORICAL_DATE_LIMIT
    location_slice_historical = location_slice.where(
        location_slice.time <= np.datetime64(app.config['HISTORICAL_DATE_LIMIT_BEFORE']['CMIP5']), drop=True)
    location_slice_historical = location_slice_historical.where(
        location_slice.time >= np.datetime64(app.config['SPEI_DATE_LIMIT']), drop=True)
    chart_series['modeled_historical_median'] = convert_time_series_dataset_to_list(
        location_slice_historical['rcp26_spei_p50'], decimals)
    chart_series['modeled_historical_range'] = convert_time_series_dataset_to_list(
        xr.merge([location_slice_historical['rcp26_spei_p10'],
                  location_slice_historical['rcp26_spei_p90']]), decimals)
    # we return values in historical for all scenarios after HISTORICAL_DATE_LIMIT
    location_slice = location_slice.where(
        location_slice.time >= np.datetime64(app.config['HISTORICAL_DATE_LIMIT_AFTER']['CMIP5']), drop=True)

    for scenario in app.config['SCENARIOS']['CMIP5']:
        chart_series[scenario + '_median'] = convert_time_series_dataset_to_list(
            location_slice[f'{scenario}_spei_p50'], decimals)
        chart_series[scenario + '_range'] = convert_time_series_dataset_to_list(
            xr.merge([location_slice[f'{scenario}_spei_p10'], location_slice[f'{scenario}_spei_p90']]), decimals)
    return chart_series


def generate_slr_charts(lati, loni):
    """
        Copied from generate_spei_charts: no historical, no observations, single file
        ex: curl http://localhost:5000/generate-charts/58.031372421776396/-61.12792968750001/slr/ann?dataset_name=CMIP6
    """
    dataset = open_dataset_by_path(app.config['NETCDF_SLR_PATH'].format(root=app.config['DATASETS_ROOT']))
    location_slice = dataset.sel(lon=loni, lat=lati, method='nearest').drop(['lat', 'lon']).dropna('time')

    dataset_enhanced = open_dataset_by_path(
        app.config['NETCDF_SLR_ENHANCED_PATH'].format(root=app.config['DATASETS_ROOT']))
    enhanced_location_slice = dataset_enhanced.sel(lon=loni, lat=lati, method='nearest').drop(['lat', 'lon'])

    chart_series = {}

    for scenario in app.config['SCENARIOS']['CMIP5']:
        chart_series[scenario + '_median'] = convert_time_series_dataset_to_list(
            location_slice[f'{scenario}_slr_p50'], decimals=0)
        chart_series[scenario + '_range'] = convert_time_series_dataset_to_list(
            xr.merge([location_slice[f'{scenario}_slr_p05'],
                      location_slice[f'{scenario}_slr_p95']]), decimals=0)
    chart_series['rcp85_enhanced'] = [[dataset_enhanced['time'].item() / 10 ** 6,
                                       round(enhanced_location_slice['enhanced_p50'].item(), 0)]]

    return chart_series


def generate_allowance_charts(lati, loni):
    """
        Copied from generate_slr_charts: no historical, no observations, single file
        ex: curl http://localhost:5000/generate-charts/58.031372421776396/-61.12792968750001/allowance/ann?dataset_name=CMIP6
    """
    dataset = open_dataset_by_path(app.config['NETCDF_ALLOWANCE_PATH'].format(root=app.config['DATASETS_ROOT']))
    location_slice = dataset.sel(lon=loni, lat=lati, method='nearest').drop(['lat', 'lon']).dropna('time')

    chart_series = {}
    for scenario in app.config['SCENARIOS']['CMIP6']:
        chart_series[scenario] = convert_time_series_dataset_to_list(
            location_slice[f'{scenario}_allowance_p50'], decimals=0)

    return chart_series


def generate_regional_charts(partition, index, var, month='ann'):
    """
        Get data for specific region
        ex: curl 'http://localhost:5000/generate-regional-charts/census/4510/tx_max/ann'
            curl 'http://localhost:5000/generate-regional-charts/census/4510/tx_max/ann?dataset_name=CMIP6'
    """
    try:
        indexi = int(index)
        msys = app.config['MONTH_LUT'][month][1]
        monthnumber = app.config['MONTH_NUMBER_LUT'][month]
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

    try:
        observations_dataset = open_dataset(app.config['OBSERVATIONS_DATASET'][dataset_name],
                                            'allyears', var, msys, partition=partition)
        observations_location_slice = observations_dataset.sel(geom=indexi).drop(
            [i for i in observations_dataset.coords if i != 'time']).dropna('time')
    except FileNotFoundError:
        observations_dataset = None
        observations_location_slice = None

    bccaq_dataset = open_dataset(dataset_name, 'allyears', var, msys, partition=partition)
    bccaq_location_slice = bccaq_dataset.sel(geom=indexi).drop(
        [i for i in bccaq_dataset.coords if i != 'time']).dropna('time')

    delta_30y_dataset = open_dataset(dataset_name, '30ygraph', var, msys, partition=partition)
    delta_30y_slice = delta_30y_dataset.sel(geom=indexi).drop(
        [i for i in delta_30y_dataset.coords if i != 'time']).dropna('time')

    # we filter the appropriate month/season from the MS or QS-DEC file
    if msys in ["MS", "QS-DEC"]:
        bccaq_location_slice = bccaq_location_slice.sel(time=(bccaq_location_slice.time.dt.month == monthnumber))
        if observations_location_slice:
            observations_location_slice = observations_location_slice.sel(
                time=(observations_location_slice.time.dt.month == monthnumber))
        delta_30y_slice = delta_30y_slice.sel(
            time=(delta_30y_slice.time.dt.month == monthnumber))

    chart_series = _format_slices_to_highcharts_series(observations_location_slice, bccaq_location_slice, delta_30y_slice,
                                                       var, decimals, dataset_name)
    bccaq_dataset.close()
    if observations_dataset:
        observations_dataset.close()
    delta_30y_dataset.close()
    return chart_series
