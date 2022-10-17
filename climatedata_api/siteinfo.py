import json

import xarray as xr
from flask import current_app as app
from flask import request
from werkzeug.exceptions import BadRequestKeyError

from climatedata_api.utils import open_dataset


def get_location_values_allyears():
    """
    Return simple json containing key values used to render the location pages
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
