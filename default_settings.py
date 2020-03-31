import json
import mapbox_vector_tile

DEBUG = True

SENTRY_ENV="dev"

NETCDF_BCCAQV2_FILENAME_FORMATS=["{root}/{var}/allrcps_ensemble_stats/{msys}/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1951-2100_{var}_{msys}{month}.nc",
                         "{root}/{var}/allrcps_ensemble_stats/{msys}/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_{var}_{msys}{month}.nc"]

NETCDF_ANUSPLINV1_FILENAME_FORMATS=["{root}/{var}/{msys}/nrcan_canada_1950-2013_{var}_{msys}{month}.nc"]

NETCDF_BCCAQV2_YEARLY_FOLDER= "./netcdfs"
NETCDF_ANUSPLINV1_YEARLY_FOLDER = "./netcdfs"
NETCDF_LOCATIONS_FOLDER="./netcdfs/locations"

MODELS=['rcp26', 'rcp45', 'rcp85']

HISTORICAL_DATE_LIMIT_BEFORE = '2005-12-31'
HISTORICAL_DATE_LIMIT_AFTER = '2005-01-01'

VARIABLES = ['cddcold_18',
             'frost_days',
             'gddgrow_0',
             'gddgrow_10',
             'gddgrow_5',
             'hddheat_17',
             'ice_days',
             'prcptot',
             'r10mm',
             'r1mm',
             'r20mm',
             'rx1day',
             'tg_mean',
             'tnlt_-15',
             'tnlt_-25',
             'tn_mean',
             'tn_min',
             'tr_18',
             'tr_20',
             'tr_22',
             'txgt_25',
             'txgt_27',
             'txgt_29',
             'txgt_30',
             'txgt_32',
             'tx_max',
             'tx_mean']

MONTH_LUT= {
    'jan':('_01January','MS'),
    'feb':('_02February','MS'),
    'mar': ('_03March', 'MS'),
    'apr': ('_04April', 'MS'),
    'may': ('_05May', 'MS'),
    'jun': ('_06June', 'MS'),
    'jul': ('_07July', 'MS'),
    'aug': ('_08August', 'MS'),
    'sep': ('_09September', 'MS'),
    'oct': ('_10October', 'MS'),
    'nov': ('_11November', 'MS'),
    'dec': ('_12December', 'MS'),
    'ann': ('', 'YS')
}

MONTH_OUTPUT_LUT= {
    'jan': 'January',
    'feb': 'February',
    'mar': 'March',
    'apr': 'April',
    'may': 'May',
    'jun': 'June',
    'jul': 'July',
    'aug': 'August',
    'sep': 'September',
    'oct': 'October',
    'nov': 'November',
    'dec': 'December',
    'ann': ''
}

SYSTEM_CHECKS_HOST = "http://localhost"
SYSTEM_CHECKS= [
    {'name': 'get_values',
    'URL': "{}/get_values.php?lat=45.5833333&lon=-73.75&var=frost_days&month=ann",
    'validator': json.loads},
    {'name': 'get_location_values_allyears',
     'URL': "{}/get_location_values_allyears.php?lat=45.583333&lon=-73.75&time=2100-01-01",
     'validator': json.loads},
    {'name': 'canadagrid',
     'URL': "{}/geoserver/gwc/service/tms/1.0.0/CDC:canadagrid@EPSG%3A900913@pbf/11/610/1315.pbf",
     'validator': mapbox_vector_tile.decode},
    {'name': 'getFeatureInfo',
     'URL': ("{}/geoserver/ows?service=WMS&request=GetFeatureInfo&layers=CDC%3Atg_mean-ys-rcp85-p50-ann-30year"
             "&query_layers=CDC%3Atg_mean-ys-rcp85-p50-ann-30year&info_format=application%2Fjson&version=1.3.0"
             "&width=256&height=256&srs=EPSG%3A3857&TIME=2030-01-00T00%3A00%3A00Z%2F2040-01-01T00%3A00%3A00Z"
             "&bbox=-10331840.239250705%2C7200979.560689886%2C-10018754.171394622%2C7514065.628545967&i=33&j=33"),
     'validator': json.loads}
]
