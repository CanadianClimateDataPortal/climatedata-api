DEBUG = True

SENTRY_ENV="dev"

NETCDF_BCCAQV2_FILENAME_FORMATS=["{root}/{var}/allrcps_ensemble_stats/{msys}/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1951-2100_{var}_{msys}{month}.nc",
                         "{root}/{var}/allrcps_ensemble_stats/{msys}/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_{var}_{msys}{month}.nc"]

NETCDF_ANUSPLINV1_FILENAME_FORMATS=["{root}/{var}/{msys}/nrcan_canada_1950-2013_{var}_{msys}{month}.nc"]

NETCDF_BCCAQV2_YEARLY_FOLDER= "./netcdfs"
NETCDF_ANUSPLINV1_YEARLY_FOLDER = "./netcdfs"
NETCDF_LOCATIONS_FOLDER="./netcdfs/locations"

MODELS=['rcp26', 'rcp45', 'rcp85']

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
