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


def convert_dataset_to_list(dataset):
    """
    Converts xarray dataset to a list.
    We assume that the coordinates are timestamps, which are converted to milliseconds since 1970-01-01 (integer)
    """
    return [[int(a[0].timestamp() * 1000)] + a[1:] for a in
            dataset.to_dataframe().astype('float64').round(2).reset_index().values.tolist()]


def convert_dataset_to_dict(dataset):
    """
    Converts xarray dataset to a dict.
    We assume that the coordinates are timestamps, which are converted to milliseconds since 1970-01-01 (integer)
    """
    return {int(a[0].timestamp() * 1000): a[1:] for a in
            dataset.to_dataframe().astype('float64').round(2).reset_index().values.tolist()}
