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
