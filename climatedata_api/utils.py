import xarray as xr
from flask import current_app as app


def open_dataset(dataset_name, filetype, var, freq, period=None, partition=None):
    """
    Open and return a xarray dataset. Try all path formats, since provided datasets had inconsistent naming convention
    :return: Dataset object
    """
    if partition:
        filename_format = app.config['FILENAME_FORMATS'][dataset_name]['partitions'][filetype]
        dataset_path = app.config['DATASETS_ROOT'] / dataset_name / "partitions" / partition / var / freq \
            / filename_format.format(var=var, freq=freq, period=period)
    else:
        filename_format = app.config['FILENAME_FORMATS'][dataset_name][filetype]
        dataset_path = app.config['DATASETS_ROOT'] / dataset_name / filetype / var / freq \
            / filename_format.format(var=var, freq=freq, period=period)
    if not dataset_path.exists():
        raise FileNotFoundError(f"Dataset not found {dataset_path}")
    dataset = xr.open_dataset(dataset_path, decode_times=False)
    dataset['time'] = xr.decode_cf(dataset).time
    return dataset


def open_dataset_by_path(path):
    """
    Open and return a xarray dataset
    :param path: path of the dataset
    :return: Dataset object
    """
    try:
        dataset = xr.open_dataset(path, decode_times=False)
        dataset['time'] = xr.decode_cf(dataset).time
        return dataset
    except FileNotFoundError:
        raise FileNotFoundError(f"Dataset file not found: {path}")


def convert_time_series_dataset_to_list(dataset, decimals):
    """
    Converts xarray dataset to a list.
    We assume that the coordinates are timestamps, which are converted to milliseconds since 1970-01-01 (integer)
    """

    # we cast to int if decimals is 0
    def _convert(float_list):
        return float_list if decimals > 0 else list(map(int, float_list))

    return [[int(a[0].timestamp() * 1000)] + _convert(a[1:]) for a in
            dataset.to_dataframe().astype('float64').round(decimals).reset_index().values.tolist()]


def convert_time_series_dataset_to_dict(dataset, decimals):
    """
    Converts xarray dataset to a dict.
    We assume that the coordinates are timestamps, which are converted to milliseconds since 1970-01-01 (integer)
    """

    # we cast to int if decimals is 0
    def _convert(float_list):
        return float_list if decimals > 0 else list(map(int, float_list))

    return {int(a[0].timestamp() * 1000): _convert(a[1:]) for a in
            dataset.to_dataframe().astype('float64').round(decimals).reset_index().values.tolist()}
