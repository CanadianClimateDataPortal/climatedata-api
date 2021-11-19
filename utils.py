import xarray as xr


def open_dataset(var, msys, month, formats, root):
    """
    Open and return an xarray dataset. Try all path formats, since provided datasets had inconsistent naming convention
    :return: Dataset object
    """
    for filename in formats:
        try:
            dataseturl = filename.format(root=root,
                                         var=var,
                                         msys=msys,
                                         month=month)
            dataset = xr.open_dataset(dataseturl, decode_times=False)
            dataset['time'] = xr.decode_cf(dataset).time
            return dataset
        except FileNotFoundError:
            pass
    raise FileNotFoundError("Dataset not found")


def open_dataset_by_path(path):
    """
        Open and return an xarray dataset.
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
