DEBUG = True

SENTRY_ENV="dev"

NETCDF_BCCAQV2_FILENAME_FORMATS=["{root}/{var}/allrcps_ensemble_stats/{msys}/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1951-2100_{var}_{msys}{month}.nc",
                         "{root}/{var}/allrcps_ensemble_stats/{msys}/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_{var}_{msys}{month}.nc"]

NETCDF_ANUSPLINV1_FILENAME_FORMATS=["{root}/{var}/{msys}/nrcan_canada_1950-2013_{var}_{msys}{month}.nc"]
NETCDF_SPEI_FILENAME_FORMATS="{root}/{var}/SPEI_ensemble_percentiles_allrcps_MON_1900_2100_{var}.nc"

NETCDF_BCCAQV2_YEARLY_FOLDER= "./netcdfs"
NETCDF_ANUSPLINV1_YEARLY_FOLDER = "./netcdfs"
NETCDF_LOCATIONS_FOLDER="./netcdfs/locations"
NETCDF_SPEI_FOLDER="./netcdfs/SPEI/"

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
             'tx_mean',
             'spei_3m',
             'spei_12m']

SPEI_VARIABLES = ['spei_3m', 'spei_12m']

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

MONTH_NUMBER_LUT = {
    'jan': 1,
    'feb': 2,
    'mar': 3,
    'apr': 4,
    'may': 5,
    'jun': 6,
    'jul': 7,
    'aug': 8,
    'sep': 9,
    'oct': 10,
    'nov': 11,
    'dec': 12,
    'ann': 1
}

PARTITIONS_FOLDER = {'municipal': {'ANUSPLIN': './netcdfs',
                                   'BCCAQ': './netcdfs'}}

PARTITIONS_PATH_FORMATS = {'municipal': {'allyears': "{root}/{var}/{msys}/{msys}_{var}_allrcps_RegSummary_AllYears_Ensemble_percentiles.nc",
                                         'means': "{root}/{var}/{msys}/{msys}_{var}_allrcps_RegSummary_30y_Means_Ensemble_percentiles.nc",
                                         'ANUSPLIN': "{root}/{var}/{msys}/nrcan_canada_1950-2013_{var}_{msys}_regSummary.nc"},
                           'health':    {'allyears': "{root}/{var}/{msys}/{msys}_{var}_allrcps_RegSummary_AllYears_Ensemble_percentiles.nc",
                                         'means': "{root}/{var}/{msys}/{msys}_{var}_allrcps_RegSummary_30y_Means_Ensemble_percentiles.nc",
                                         'ANUSPLIN': "{root}/{var}/{msys}/nrcan_canada_1950-2013_{var}_{msys}_regSummary.nc"}}