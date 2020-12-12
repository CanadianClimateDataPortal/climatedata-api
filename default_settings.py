import json
import mapbox_vector_tile

DEBUG = True

SENTRY_ENV="dev"

NETCDF_BCCAQV2_FILENAME_FORMATS=["{root}/{var}/allrcps_ensemble_stats/{msys}/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1951-2100_{var}_{msys}{month}.nc",
                         "{root}/{var}/allrcps_ensemble_stats/{msys}/BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1950-2100_{var}_{msys}{month}.nc"]

NETCDF_ANUSPLINV1_FILENAME_FORMATS=["{root}/{var}/{msys}/nrcan_canada_1950-2013_{var}_{msys}.nc"]
NETCDF_SPEI_FILENAME_FORMATS="{root}/{var}/SPEI_ensemble_percentiles_allrcps_MON_1900_2100_{var}.nc"
NETCDF_SPEI_OBSERVED_FILENAME_FORMATS="{root}/{var}/CCRC_CANGRD_MON_1900_2014_spei_bc_ref_period_195001_200512_timev_{var}.nc"

NETCDF_BCCAQV2_YEARLY_FOLDER= "./netcdfs"
NETCDF_ANUSPLINV1_YEARLY_FOLDER = "./netcdfs"
NETCDF_LOCATIONS_FOLDER="./netcdfs/locations"
NETCDF_SPEI_FOLDER="./netcdfs/SPEI/"
NETCDF_SLR_PATH="./netcdfs/sealevel/Decadal_CMIP5_ensemble-percentiles_allrcps_2006-2100_slr_YS.nc"

MODELS=['rcp26', 'rcp45', 'rcp85']

HISTORICAL_DATE_LIMIT_BEFORE = '2005-12-31'
HISTORICAL_DATE_LIMIT_AFTER = '2005-01-01'
KELVIN_TO_C = -273.15

VARIABLES = ['cdd',
             'cddcold_18',
             'frost_days',
             'first_fall_frost',
             'frost_free_season',
             'gddgrow_0',
             'gddgrow_10',
             'gddgrow_5',
             'hddheat_17',
             'hddheat_18',
             'ice_days',
             'last_spring_frost',
             'nr_cdd',
             'prcptot',
             'r10mm',
             'r1mm',
             'r20mm',
             'rx1day',
             'rx5day',
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
             'slr',
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
    'ann': ('', 'YS'),
    '2qsapr': ('','2QS-APR'),
    'winter': ('_winterDJF', 'QS-DEC'),
    'spring': ('_springMAM', 'QS-DEC'),
    'summer': ('_summerJJA', 'QS-DEC'),
    'fall': ('_fallSON', 'QS-DEC')
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
    'ann': 1,
    '2qsapr': 4,
    'winter':12,
    'spring':3,
    'summer': 6,
    'fall': 9
}

PARTITIONS_FOLDER = {'census': {'ANUSPLIN': './netcdfs',
                                   'BCCAQ': './netcdfs'}}

PARTITIONS_PATH_FORMATS = {'census': {'allyears': "{root}/{var}/{msys}/{msys}_{var}_allrcps_RegSummary_AllYears_Ensemble_percentiles.nc",
                                         'means': "{root}/{var}/{msys}/{msys}_{var}_allrcps_RegSummary_30y_Means_Ensemble_percentiles.nc",
                                         'ANUSPLIN': "{root}/{var}/{msys}/nrcan_canada_1950-2013_{var}_{msys}_regSummary.nc"},
                           'health':    {'allyears': "{root}/{var}/{msys}/{msys}_{var}_allrcps_RegSummary_AllYears_Ensemble_percentiles.nc",
                                         'means': "{root}/{var}/{msys}/{msys}_{var}_allrcps_RegSummary_30y_Means_Ensemble_percentiles.nc",
                                         'ANUSPLIN': "{root}/{var}/{msys}/nrcan_canada_1950-2013_{var}_{msys}_regSummary.nc"},
                           'watershed': {'allyears': "{root}/{var}/{msys}/allrcps/{msys}_{var}_allrcps_RegSummary_AllYears_Ensemble_percentiles.nc",
                                         'means': "{root}/{var}/{msys}/allrcps/{msys}_{var}_allrcps_RegSummary_30y_Means_Ensemble_percentiles.nc",
                                         'ANUSPLIN': "{root}/{var}/{msys}/nrcan_canada_1950-2013_{var}_{msys}_regSummary.nc"}}

SYSTEM_CHECKS_HOST = "http://localhost"
SYSTEM_CHECKS= [
    {'name': 'generate-charts',
    'URL': "{}/generate-charts/45.5833333/-73.75/frost_days/ann",
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
