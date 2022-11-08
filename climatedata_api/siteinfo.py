import json

import numpy as np
import xarray as xr
from flask import current_app as app
from flask import request
from werkzeug.exceptions import BadRequestKeyError

from climatedata_api.utils import open_dataset


def get_location_values_allyears():
    """
    Return simple json containing key values used to render the location pages
    DEPRECATED: Will be removed when frontend will be upgraded.
    curl 'http://localhost:5000/get_location_values_allyears.php?lat=45.551538&lon=-73.744616&time=2100-01-01'
    :return:
    """
    try:
        lati = float(request.args['lat'])
        loni = float(request.args['lon'])
    except (ValueError, BadRequestKeyError, KeyError):
        return "Bad request", 400

    anusplin_dataset = xr.open_dataset(
        app.config['DATASETS_ROOT'] / "locations" / "SearchLocation_30yAvg_anusplin_nrcan_canada_tg_mean_prcptot_YS.nc")
    anusplin_location_slice = anusplin_dataset.sel(lat=lati, lon=loni, method='nearest')

    anusplin_1950_location_slice = anusplin_location_slice.sel(time='1951-01-01')
    anusplin_1980_location_slice = anusplin_location_slice.sel(time='1981-01-01')

    anusplin_1950_temp = round(anusplin_1950_location_slice.tg_mean.item() + app.config['KELVIN_TO_C'], 1)
    anusplin_1980_temp = round(anusplin_1980_location_slice.tg_mean.item() + app.config['KELVIN_TO_C'], 1)
    anusplin_1950_precip = round(anusplin_1950_location_slice.prcptot.item())

    bcc_dataset = xr.open_dataset(app.config['DATASETS_ROOT'] / "locations" /
                                  "SearchLocation_30yAvg_wDeltas_BCCAQv2+ANUSPLIN300_rcp85_tg_mean_prcptot_YS.nc")
    bcc_location_slice = bcc_dataset.sel(lat=lati, lon=loni, method='nearest')
    bcc_2020_location_slice = bcc_location_slice.sel(time='2021-01-01')
    bcc_2050_location_slice = bcc_location_slice.sel(time='2051-01-01')
    bcc_2070_location_slice = bcc_location_slice.sel(time='2071-01-01')

    bcc_2020_temp = round(bcc_2020_location_slice.tg_mean_p50.values.item() + app.config['KELVIN_TO_C'], 1)
    bcc_2020_precip = round(bcc_2020_location_slice.delta_prcptot_p50_vs_7100.values.item())

    bcc_2050_temp = round(bcc_2050_location_slice.tg_mean_p50.values.item() + app.config['KELVIN_TO_C'], 1)
    bcc_2050_precip = round(bcc_2050_location_slice.delta_prcptot_p50_vs_7100.values.item())

    bcc_2070_temp = round(bcc_2070_location_slice.tg_mean_p50.values.item() + app.config['KELVIN_TO_C'], 1)
    bcc_2070_precip = round(bcc_2070_location_slice.delta_prcptot_p50_vs_7100.values.item())

    return json.dumps({
        "anusplin_1950_temp": anusplin_1950_temp,
        "anusplin_2005_temp": anusplin_1980_temp,  # useless value kept for frontend transition
        "anusplin_1980_temp": anusplin_1980_temp,
        "anusplin_1980_precip": anusplin_1950_precip,  # useless value kept for frontend transition
        "anusplin_1950_precip": anusplin_1950_precip,
        "bcc_2020_temp": bcc_2020_temp,
        "bcc_2050_temp": bcc_2050_temp,
        "bcc_2070_temp": bcc_2070_temp,
        "bcc_2090_temp": bcc_2070_temp,  # useless value kept for frontend transition
        "bcc_2020_precip": bcc_2020_precip,
        "bcc_2050_precip": bcc_2050_precip,
        "bcc_2070_precip": bcc_2070_precip,
        "bcc_2090_precip": bcc_2070_precip  # useless value kept for frontend transition
    })


def get_location_values(lat, lon):
    """
    Return simple json containing the values required to render the location pages
    curl 'http://localhost:5000/get-location-values/45.551538/-73.744616?dataset_name=cmip5'
    """
    try:
        lati = float(lat)
        loni = float(lon)
        dataset_name = request.args.get('dataset_name', 'CMIP5').upper()
    except (ValueError, BadRequestKeyError, KeyError):
        return "Bad request", 400

    scenario = app.config['SCENARIOS'][dataset_name][-1]  # last scenario is the one with highest emissions
    delta_naming = app.config['DELTA_NAMING'][dataset_name]

    anusplin_ds = xr.open_dataset(
        app.config['DATASETS_ROOT'] / "locations" / "SearchLocation_30yAvg_anusplin_nrcan_canada_tg_mean_prcptot_YS.nc")
    anusplin_slice = anusplin_ds.sel(lat=lati, lon=loni, method='nearest')

    # return 404 when slice is empty to avoid "nan" in response
    if np.isnan(anusplin_slice.sel(time='1951-01-01').tg_mean.item()):
        return "Location not found", 404

    tg_mean = open_dataset(dataset_name, '30ygraph', 'tg_mean', 'YS', '').sel(lat=lati, lon=loni, method='nearest')
    tg_mean_var = f"{scenario}_tg_mean_p50"

    prcptot = open_dataset(dataset_name, '30ygraph', 'prcptot', 'YS', '').sel(lat=lati, lon=loni, method='nearest')
    prcptot_var = f"{scenario}_prcptot_p50"
    prcptot_delta_var = f"{scenario}_prcptot_{delta_naming}_p50"
    prcptot_reference = prcptot.sel(time='1971-01-01')[prcptot_var].item()

    return json.dumps({
        'tg_mean':
            {
                1951: f"{anusplin_slice.sel(time='1951-01-01').tg_mean.item() + app.config['KELVIN_TO_C']:.1f}",
                1971: f"{anusplin_slice.sel(time='1971-01-01').tg_mean.item() + app.config['KELVIN_TO_C']:.1f}",
                2021: f"{tg_mean.sel(time='2021-01-01')[tg_mean_var].item() + app.config['KELVIN_TO_C']:.1f}",
                2051: f"{tg_mean.sel(time='2051-01-01')[tg_mean_var].item() + app.config['KELVIN_TO_C']:.1f}",
                2071: f"{tg_mean.sel(time='2071-01-01')[tg_mean_var].item() + app.config['KELVIN_TO_C']:.1f}",
            },

        # It has been previously verified that all deltas in prcptot are positive
        'prcptot':
            {
                1951: f"{anusplin_slice.sel(time='1951-01-01').prcptot.item():.0f}",
                1971: f"{anusplin_slice.sel(time='1971-01-01').prcptot.item():.0f}",
                "delta_2021_percent": f"{prcptot.sel(time='2021-01-01')[prcptot_delta_var].item() / prcptot_reference * 100:.0f}",
                "delta_2051_percent": f"{prcptot.sel(time='2051-01-01')[prcptot_delta_var].item() / prcptot_reference * 100:.0f}",
                "delta_2071_percent": f"{prcptot.sel(time='2071-01-01')[prcptot_delta_var].item() / prcptot_reference * 100:.0f}",
            }
    })
