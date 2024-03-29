2023-07-17.01
  * Added 30y averages to anusplin dataset

2023-07-11.01
  * Added dataset\_type parameter to /download route

2023-06-01.01
  * Added support for Humidex indices

2023-02-22.01
  * Added /get-gids/ api call to get matching feature id from a list of points

2023-01-12.01
  * Updated metadata for AHCCD netcdf download for better Finch compatibility

2023-01-09.1
  * download-ahccd (csv): added sorting order and removed useless rows

2022-10-18.1
  * Added API online tests
  * Fix small bug with SPEI download
  * Add basic development tools

2022-10-14.01
  * Fixed AHCCD performance issue with xarray >= 2022.06.0

2022-09-09.01
  * Added CMIP6 support to climatedata-api

2022-08-22.01
  * Added bounding box support to /download route
  * Added netcdf format to download
  * Python >= 3.9 is now required

2022-02-24.01
  * Added variable\_type\_filter parameter to download-ahccd to return only temperature or precipitations variable

2022-01-10.01
  * Added get-slr-gridded-values
  * Renamed models to scenarios

2021-12-21.01
  * Added get-delta-30y-regional-values + get-delta-30y-gridded-values required for grid hovering

2021-11-18.01
  * Added decimals parameter to all relevant API calls

2021-11-16.01
  * Added delta features

2021-08-05.01
  * Added AHCCD download, close CLIM-39

2021-06-10.01
  * Added exceptions for sea-level in /generate-charts/, close CLIM-40

