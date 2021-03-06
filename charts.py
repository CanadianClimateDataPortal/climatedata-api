from flask import Flask, request, Response, send_file, current_app as app
from utils import open_dataset, open_dataset_by_path, convert_time_series_dataset_to_list, convert_time_series_dataset_to_dict
import numpy as np
import xarray as xr
from werkzeug.exceptions import BadRequestKeyError


def _format_slices_to_highcharts_series(anusplin_location_slice, bccaq_location_slice, delta_30y_slice, var, decimals):
    """
    Format anusplin, bccaq and delta_30y slices to dictionary of series ready for highcharts
    """
    if bccaq_location_slice[f'rcp26_{var}_p50'].attrs.get('units') == 'K':
        bccaq_location_slice = bccaq_location_slice + app.config['KELVIN_TO_C']

    if anusplin_location_slice[var].attrs.get('units') == 'K':
        anusplin_location_slice = anusplin_location_slice + app.config['KELVIN_TO_C']

    chart_series = {'observations': convert_time_series_dataset_to_list(anusplin_location_slice, decimals)}

    # we return the historical values for a single scenario before HISTORICAL_DATE_LIMIT
    bccaq_location_slice_historical = bccaq_location_slice.where(
        bccaq_location_slice.time <= np.datetime64(app.config['HISTORICAL_DATE_LIMIT_BEFORE']), drop=True)
    chart_series['modeled_historical_median'] = convert_time_series_dataset_to_list(
        bccaq_location_slice_historical[f'rcp26_{var}_p50'], decimals)
    chart_series['modeled_historical_range'] = convert_time_series_dataset_to_list(
        xr.merge([bccaq_location_slice_historical[f'rcp26_{var}_p10'],
                  bccaq_location_slice_historical[f'rcp26_{var}_p90']]), decimals)
    # we return values in historical for all scenarios after HISTORICAL_DATE_LIMIT
    bccaq_location_slice = bccaq_location_slice.where(
        bccaq_location_slice.time >= np.datetime64(app.config['HISTORICAL_DATE_LIMIT_AFTER']), drop=True)

    for scenario in app.config['SCENARIOS']:
        chart_series[scenario + '_median'] = convert_time_series_dataset_to_list(
            bccaq_location_slice[f'{scenario}_{var}_p50'], decimals)
        chart_series[scenario + '_range'] = convert_time_series_dataset_to_list(
            xr.merge([bccaq_location_slice[f'{scenario}_{var}_p10'],
                      bccaq_location_slice[f'{scenario}_{var}_p90']]), decimals)
        chart_series[f"delta7100_{scenario}_median"] = convert_time_series_dataset_to_dict(
            delta_30y_slice[f'{scenario}_{var}_delta7100_p50'], decimals)
        chart_series[f"delta7100_{scenario}_range"] = convert_time_series_dataset_to_dict(
            xr.merge([delta_30y_slice[f'{scenario}_{var}_delta7100_p10'],
                      delta_30y_slice[f'{scenario}_{var}_delta7100_p90']]), decimals)

    if delta_30y_slice[f'rcp26_{var}_p50'].attrs.get('units') == 'K':
        delta_30y_slice = delta_30y_slice + app.config['KELVIN_TO_C']

    for scenario in app.config['SCENARIOS']:
        chart_series[f"30y_{scenario}_median"] = convert_time_series_dataset_to_dict(delta_30y_slice[f'{scenario}_{var}_p50'],
                                                                                  decimals)
        chart_series[f"30y_{scenario}_range"] = convert_time_series_dataset_to_dict(
            xr.merge([delta_30y_slice[f'{scenario}_{var}_p10'],
                      delta_30y_slice[f'{scenario}_{var}_p90']]), decimals)

    return chart_series


def generate_charts(var, lat, lon, month='ann'):
    """
    Rewrite of get_values, generating a JSON ready for highcharts
    ex: curl 'http://localhost:5000/generate-charts/60.31062731740045/-100.06347656250001/tx_max/ann'
    """
    try:
        lati = float(lat)
        loni = float(lon)
        monthpath, msys = app.config['MONTH_LUT'][month]
        monthnumber = app.config['MONTH_NUMBER_LUT'][month]
        decimals = int(request.args.get('decimals', 2))
        if decimals < 0:
            return "Bad request: invalid number of decimals", 400

        if var not in app.config['VARIABLES']:
            raise ValueError
    except (ValueError, BadRequestKeyError, KeyError):
        return "Bad request", 400

    # SPEI treatment is very different
    if var in app.config['SPEI_VARIABLES']:
        return generate_spei_charts(var, lati, loni, month, decimals)

    # so does sea level rise
    if var == 'slr':
        return generate_slr_charts(lati, loni)

    anusplin_dataset = open_dataset(var, msys, monthpath,
                                    app.config['NETCDF_ANUSPLINV1_FILENAME_FORMATS'],
                                    app.config['NETCDF_ANUSPLINV1_YEARLY_FOLDER'])
    anusplin_location_slice = anusplin_dataset.sel(lon=loni, lat=lati, method='nearest').drop(['lat', 'lon']).dropna(
        'time')
    anusplin_location_slice = anusplin_location_slice.sel(time=(anusplin_location_slice.time.dt.month == monthnumber))

    bccaq_dataset = open_dataset(var, msys, monthpath,
                                 app.config['NETCDF_BCCAQV2_FILENAME_FORMATS'],
                                 app.config['NETCDF_BCCAQV2_YEARLY_FOLDER'])
    bccaq_location_slice = bccaq_dataset.sel(lon=loni, lat=lati, method='nearest').drop(['lat', 'lon']).dropna('time')

    delta_30y_dataset = open_dataset_by_path(
        app.config['NETCDF_BCCAQV2_30Y_FILENAME_FORMAT'].format(root=app.config['NETCDF_BCCAQV2_30Y_FOLDER'],
                                                                var=var,
                                                                msys=msys,
                                                                month=monthpath))
    delta_30y_slice = delta_30y_dataset.sel(lon=loni, lat=lati, method='nearest').drop(
        [i for i in delta_30y_dataset.coords if i != 'time']).dropna('time')

    chart_series = _format_slices_to_highcharts_series(anusplin_location_slice, bccaq_location_slice, delta_30y_slice,
                                                       var, decimals)
    bccaq_dataset.close()
    anusplin_dataset.close()
    delta_30y_dataset.close()
    return chart_series


def generate_spei_charts(var, lati, loni, month, decimals):
    """
        Copied from generate-charts, but handles the difference for SPEI (i.e. single file)
        ex: curl 'http://localhost:5000/generate-charts/60.31062731740045/-100.06347656250001/spei_12m/ann'
    """
    # very specific exception for location page, return december values (annual doesn't exists anyway for SPEI)
    if month == 'ann':
        month = 'dec'

    monthnumber = app.config['MONTH_NUMBER_LUT'][month]
    dataset = open_dataset_by_path(
        app.config['NETCDF_SPEI_FILENAME_FORMATS'].format(root=app.config['NETCDF_SPEI_FOLDER'], var=var))
    observed_dataset = open_dataset_by_path(
        app.config['NETCDF_SPEI_OBSERVED_FILENAME_FORMATS'].format(root=app.config['NETCDF_SPEI_FOLDER'], var=var))
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
        location_slice.time <= np.datetime64(app.config['HISTORICAL_DATE_LIMIT_BEFORE']), drop=True)
    location_slice_historical = location_slice_historical.where(
        location_slice.time >= np.datetime64(app.config['SPEI_DATE_LIMIT']), drop=True)
    chart_series['modeled_historical_median'] = convert_time_series_dataset_to_list(
        location_slice_historical['rcp26_spei_p50'], decimals)
    chart_series['modeled_historical_range'] = convert_time_series_dataset_to_list(
        xr.merge([location_slice_historical['rcp26_spei_p10'],
                  location_slice_historical['rcp26_spei_p90']]), decimals)
    # we return values in historical for all scenarios after HISTORICAL_DATE_LIMIT
    location_slice = location_slice.where(
        location_slice.time >= np.datetime64(app.config['HISTORICAL_DATE_LIMIT_AFTER']), drop=True)

    for scenario in app.config['SCENARIOS']:
        chart_series[scenario + '_median'] = convert_time_series_dataset_to_list(
            location_slice[f'{scenario}_spei_p50'], decimals)
        chart_series[scenario + '_range'] = convert_time_series_dataset_to_list(
            xr.merge([location_slice[f'{scenario}_spei_p10'], location_slice[f'{scenario}_spei_p90']]), decimals)
    return chart_series


def generate_slr_charts(lati, loni):
    """
        Copied from generate_spei_charts: no historical, no observations, single file
        ex: curl http://localhost:5000/generate-charts/58.031372421776396/-61.12792968750001/slr/ann
    """
    dataset = open_dataset_by_path(app.config['NETCDF_SLR_PATH'])
    location_slice = dataset.sel(lon=loni, lat=lati, method='nearest').drop(['lat', 'lon']).dropna('time')

    dataset_enhanced = open_dataset_by_path(app.config['NETCDF_SLR_ENHANCED_PATH'])
    enhanced_location_slice = dataset_enhanced.sel(lon=loni, lat=lati, method='nearest').drop(['lat', 'lon'])

    chart_series = {}

    for scenario in app.config['SCENARIOS']:
        chart_series[scenario + '_median'] = convert_time_series_dataset_to_list(
            location_slice[f'{scenario}_slr_p50'], decimals=0)
        chart_series[scenario + '_range'] = convert_time_series_dataset_to_list(
            xr.merge([location_slice[f'{scenario}_slr_p05'],
                      location_slice[f'{scenario}_slr_p95']]), decimals=0)
    chart_series['rcp85_enhanced'] = [[dataset_enhanced['time'].item() / 10 ** 6,
                                       round(enhanced_location_slice['enhanced_p50'].item(),0)]]

    return chart_series


def generate_regional_charts(partition, index, var, month='ann'):
    """
        Get data for specific region
        ex: curl 'http://localhost:5000/generate-regional-charts/census/4510/tx_max/ann'
    """
    try:
        indexi = int(index)
        msys = app.config['MONTH_LUT'][month][1]
        monthnumber = app.config['MONTH_NUMBER_LUT'][month]
        decimals = int(request.args.get('decimals', 2))
        if decimals < 0:
            return "Bad request: invalid number of decimals", 400

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
        delta30y_path = app.config['PARTITIONS_PATH_FORMATS'][partition]['30yGraph'].format(
            root=app.config['PARTITIONS_FOLDER'][partition]['30yGraph'],
            var=var,
            msys=msys)
    except (ValueError, BadRequestKeyError, KeyError):
        return "Bad request", 400

    anusplin_dataset = open_dataset_by_path(anusplin_path)
    anusplin_location_slice = anusplin_dataset.sel(region=indexi).drop(
        [i for i in anusplin_dataset.coords if i != 'time']).dropna('time')

    bccaq_dataset = open_dataset_by_path(bccaq_path)
    bccaq_location_slice = bccaq_dataset.sel(region=indexi).drop(
        [i for i in bccaq_dataset.coords if i != 'time']).dropna('time')

    delta_30y_dataset = open_dataset_by_path(delta30y_path)
    delta_30y_slice = delta_30y_dataset.sel(region=indexi).drop(
        [i for i in delta_30y_dataset.coords if i != 'time']).dropna('time')

    # we filter the appropriate month/season from the MS or QS-DEC file
    if msys in ["MS", "QS-DEC"]:
        bccaq_location_slice = bccaq_location_slice.sel(time=(bccaq_location_slice.time.dt.month == monthnumber))
        anusplin_location_slice = anusplin_location_slice.sel(
            time=(anusplin_location_slice.time.dt.month == monthnumber))
        delta_30y_slice = delta_30y_slice.sel(
            time=(delta_30y_slice.time.dt.month == monthnumber))

    chart_series = _format_slices_to_highcharts_series(anusplin_location_slice, bccaq_location_slice, delta_30y_slice,
                                                       var, decimals)
    bccaq_dataset.close()
    anusplin_dataset.close()
    delta_30y_dataset.close()
    return chart_series
