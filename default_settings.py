import json
import mapbox_vector_tile
from pathlib import Path

DEBUG = True

SENTRY_ENV = "dev"
TEMPDIR = "/tmp"

DATASETS_ROOT = Path("./datasets")
CACHE_FOLDER = Path("./cache")

FILENAME_FORMATS = {
    'ANUSPLIN_v1': {
        'allyears': ["nrcan_canada_1950-2013_{var}_{freq}.nc"],
        'partitions': {'allyears': ["nrcan_canada_1950-2013_{var}_{freq}_spatialAvg.nc"]}
    },
    'PCIC_Blended_Observations_v1': {
        'allyears': ["{var}_{freq}_PCIC_Blended_Observations_v1_1950-2012.nc"],
        'partitions': {'allyears': ["{var}_{freq}_PCIC_Blended_Observations_v1_1950-2012_spatialAvg.nc"]}
    },
    'CMIP5': {
        'allyears': ["BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1951-2100_{var}_{freq}{period}.nc"],
        'partitions': {'allyears': ["{var}_{freq}_BCCAQv2+ANUSPLIN300_historical_allrcps_spatialAvg_AllYears_Ensemble_percentiles.nc"],
                       '30ygraph': ["{var}_{freq}_BCCAQv2+ANUSPLIN300_historical_allrcps_spatialAvg_30yGraph_Means_Ensemble_percentiles.nc"],
                       '30ymeans': ["{var}_{freq}_BCCAQv2+ANUSPLIN300_historical_allrcps_spatialAvg_30y_Means_Ensemble_percentiles.nc"]},
        '30ygraph': ["30yAvg_BCCAQv2+ANUSPLIN300_ensemble-percentiles_historical+allrcps_1951-2100_{var}_{freq}{period}.nc"]
    },
    'CMIP6': {
        'allyears': ["{var}_{freq}_MBCn+PCIC-Blend_historical+allssps_1950-2100_AllYears_percentiles{period}.nc",
                     "{var}_{freq}_BCCAQ2v2+ANUSPLIN300_historical+allssps_1950-2100_AllYears_percentiles{period}.nc"],
        'partitions': {'allyears': ["{var}_{freq}_MBCn+PCIC-Blend_historical_allrcps_spatialAvg_AllYears_Ensemble_percentiles.nc",
                                    "{var}_{freq}_MBCn_ERA5-Land_historical_allrcps_spatialAvg_AllYears_Ensemble_percentiles.nc"],
                       '30ygraph': ["{var}_{freq}_MBCn+PCIC-Blend_historical_allrcps_spatialAvg_30yGraph_Means_Ensemble_percentiles.nc",
                                    "{var}_{freq}_MBCn_ERA5-Land_historical_allrcps_spatialAvg_30yGraph_Means_Ensemble_percentiles.nc"],
                       '30ymeans': ["{var}_{freq}_MBCn+PCIC-Blend_historical_allrcps_spatialAvg_30y_Means_Ensemble_percentiles.nc",
                                    "{var}_{freq}_MBCn_ERA5-Land_historical_allrcps_spatialAvg_30y_Means_Ensemble_percentiles.nc"]},
        '30ygraph': ["{var}_{freq}_MBCn+PCIC-Blend_historical+allssps_1950-2100_30yGraph_percentiles{period}.nc",
                     "{var}_{freq}_BCCAQ2v2+ANUSPLIN300_historical+allssps_1950-2100_30yGraph_percentiles{period}.nc"]
    }
}

OBSERVATIONS_DATASET = {
    'CMIP5': 'ANUSPLIN_v1',
    'CMIP6': 'PCIC_Blended_Observations_v1',
}

GRIDS = {'canadagrid': 'shapefiles/canadagrid/canadagrid.shp',
         '1degreegrid': 'shapefiles/1degreegrid/1degreegrid.shp'}

NETCDF_SPEI_FILENAME_FORMATS = "{root}/SPEI/{var}/SPEI_ensemble_percentiles_allrcps_MON_1900_2100_{var}.nc"
NETCDF_SPEI_OBSERVED_FILENAME_FORMATS = "{root}/SPEI/{var}/CCRC_CANGRD_MON_1900_2014_spei_bc_ref_period_195001_200512_timev_{var}.nc"

NETCDF_SLR_CMIP5_PATH = "{root}/sealevel/Decadal_CMIP5_ensemble-percentiles_allrcps_2006-2100_slr_YS.nc"
NETCDF_SLR_ENHANCED_PATH = "{root}/sealevel/Decadal_0.1degree_CMIP5_ensemble-percentiles_enhancedscenario_2100_slc_YS_geoserver.nc"
NETCDF_SLR_CMIP6_PATH = "{root}/sealevel/Decadal_CMIP6_ensemble-percentiles_allssps_2020-2150_slr_YS.nc"

NETCDF_ALLOWANCE_PATH =  "{root}/allowance/Decadal_CMIP6_median_allssps_2020-2150_allowance_YS.nc"

NETCDF_S2D_FORECAST_FILENAME_FORMATS = "{root}/s2d/data/forecast/{var}/s2d-forecast-{var}-{freq}-data.nc"
NETCDF_S2D_CLIMATOLOGY_FILENAME_FORMATS = "{root}/s2d/data/climatology/{var}/s2d-climatology-{var}-{freq}-data.nc"
NETCDF_S2D_SKILL_FILENAME_FORMATS = "{root}/s2d/data/skill/{var}/s2d-skill-{var}-{freq}-rel_to_{ref_period}-data.nc"

SCENARIOS = {
    'CMIP5': ['rcp26', 'rcp45', 'rcp85'],
    'CMIP6': ['ssp126', 'ssp245', 'ssp370', 'ssp585'],
}

DELTA_NAMING = {'CMIP5': 'delta7100',
                'CMIP6': 'delta_1971_2000'}

HISTORICAL_DATE_LIMIT_BEFORE = {'CMIP5': '2005-12-31',
                                'CMIP6': '2014-12-31'}
HISTORICAL_DATE_LIMIT_AFTER = {'CMIP5': '2005-01-01',
                               'CMIP6': '2014-01-01'}

SPEI_DATE_LIMIT = '1950-01-01'
KELVIN_TO_C = -273.15
DOWNLOAD_POINTS_LIMIT = 1000

DOWNLOAD_NETCDF_FORMAT = 'netcdf'
DOWNLOAD_JSON_FORMAT = 'json'
DOWNLOAD_CSV_FORMAT = 'csv'

VARIABLES = ['allowance',
             'cdd',
             'cddcold_18',
             'dlyfrzthw_tx0_tn-1',
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
             'txgt_37',
             'tx_max',
             'tx_mean',
             'slr',
             'spei_3m',
             'spei_12m',
             'HXmax30',
             'HXmax35',
             'HXmax40']

SPEI_VARIABLES = ['spei_3m', 'spei_12m']

MONTH_LUT = {
    'jan': ('_01January', 'MS'),
    'feb': ('_02February', 'MS'),
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
    'all': ('_', 'MS'),  # valid for download only
    'ann': ('', 'YS'),
    '2qsapr': ('', '2QS-APR'),
    'winter': ('_winterDJF', 'QS-DEC'),
    'spring': ('_springMAM', 'QS-DEC'),
    'summer': ('_summerJJA', 'QS-DEC'),
    'fall': ('_fallSON', 'QS-DEC')
}

ALLMONTHS = ['_01January',
             '_02February',
             '_03March',
             '_04April',
             '_05May',
             '_06June',
             '_07July',
             '_08August',
             '_09September',
             '_10October',
             '_11November',
             '_12December']

MONTH_OUTPUT_LUT = {
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
    'winter': 12,
    'spring': 3,
    'summer': 6,
    'fall': 9
}

AHCCD_FOLDER = '{root}/ahccd'
AHCCD_METADATA_COLUMNS = ['station_name', 'lon', 'lat', 'elev', 'prov']
AHCCD_VALUES_COLUMNS = ['tas', 'tas_flag', 'tasmax', 'tasmax_flag', 'tasmin', 'tasmin_flag',
               'pr', 'pr_flag', 'prlp', 'prlp_flag', 'prsn', 'prsn_flag']
AHCCD_ORDER = AHCCD_METADATA_COLUMNS + AHCCD_VALUES_COLUMNS
AHCCD_STATIONS_LIMIT = 15

CSV_COLUMNS_ORDER = ['time', 'lat', 'lon']

SYSTEM_CHECKS_HOST = "http://localhost"
SYSTEM_CHECKS = [
    {'name': 'generate-charts',
     'URL': "{}/generate-charts/45.5833333/-73.75/frost_days/ann",
     'validator': json.loads},
    {'name': 'get-location-values',
     'URL': "{}/get-location-values/45.551538/-73.744616?dataset_name=cmip6",
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

ALLOWED_DOMAINS = [
    "localhost:80",
    "localhost:5000",
    "climatedata.ca",
    "donneesclimatiques.ca",
    "dev-en.climatedata.ca",
    "dev-fr.climatedata.ca",
]

SALT = "override-me"

S2D_VARIABLE_AIR_TEMP = 'air_temp'
S2D_VARIABLE_PRECIP_ACCUM = 'precip_accum'

S2D_VARIABLES = [
    S2D_VARIABLE_AIR_TEMP,
    S2D_VARIABLE_PRECIP_ACCUM,
]

S2D_FREQUENCY_MONTHLY = 'monthly'
S2D_FREQUENCY_SEASONAL = 'seasonal'
#S2D_FREQUENCY_DECADAL = 'decadal'

S2D_FREQUENCIES = [
    S2D_FREQUENCY_MONTHLY,
    S2D_FREQUENCY_SEASONAL,
    # 'decadal',
]

S2D_FORECAST_TYPE_EXPECTED = 'expected'
S2D_FORECAST_TYPE_UNUSUAL = 'unusual'

S2D_FORECAST_TYPES = [
    S2D_FORECAST_TYPE_EXPECTED,
    S2D_FORECAST_TYPE_UNUSUAL
]

S2D_FORECAST_DATA_VAR_NAMES = [
    "prob_unusually_low",
    "prob_below_normal",
    "prob_near_normal",
    "prob_above_normal",
    "prob_unusually_high"
]

S2D_CLIMATO_DATA_VAR_NAMES = [
    "cutoff_unusually_low_p20",
    "cutoff_below_normal_p33",
    "historical_median_p50",
    "cutoff_above_normal_p66",
    "cutoff_unusually_high_p80"
]

S2D_SKILL_DATA_VAR_NAMES = [
    "skill_CRPSS",
    "skill_level"
]

S2D_SKILL_LEVEL_STR = {
    0: 'no skill',
    1: 'low',
    2: 'medium',
    3: 'high'
}

S2D_DOWNLOAD_DECIMALS = {
    "lat": 3,
    "lon": 3,
    "skill_CRPSS": 2,
    "default": 1,
}

S2D_FILENAME_VALUES = {
    S2D_VARIABLE_AIR_TEMP: 'MeanTemp',
    S2D_VARIABLE_PRECIP_ACCUM: 'TotalPrecip',
    S2D_FORECAST_TYPE_EXPECTED: 'ExpectedCond',
    S2D_FORECAST_TYPE_UNUSUAL: 'UnusualCond',
    S2D_FREQUENCY_MONTHLY: 'Monthly',
    S2D_FREQUENCY_SEASONAL: 'Seasonal',
    #S2D_FREQUENCY_DECADAL: 'Decadal',
}
