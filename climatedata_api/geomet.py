import requests
from flask import request
from urllib.parse import urlencode

GEOMET_API_BASE_URL = 'https://api.weather.gc.ca'
GEOMET_API_PAGE_SIZE = 10000
VALID_COLLECTION_IDS = [ "climate-normals", "climate-daily"]


def get_geomet_collection_download_links(collectionId):
    """
    Get paginated links to download all items of a GeoMet collection.

    Return the set of GeoMet API links (i.e. "paginated" links) required to download all
    items of the collection `collectionId`. Each link returns a maximum of
    `GEOMET_API_PAGE_SIZE` items. The links are sorted.

    GET parameters can be used to filter or alter the set of items. All GET parameters
    supported by the GeoMet `/collections/{collectionId}/items` endpoint can be used,
    except `limit`, `offset` and `resulttype` which will be overwritten.

    :param collectionId: The ID of the GeoMet collection to query.
    :return:
        Sorted list of links to the GeoMet API. Each element is a dictionary:
        - `url` (str): GeoMet API URL to download this set of items.
        - `start_index` (int): Index of the first item in the set.
        - `end_index` (int): Index of the last item in the set.
    """
    if not isinstance(collectionId, str) or collectionId not in VALID_COLLECTION_IDS:
        return f"Invalid collectionId. Valid values are : {VALID_COLLECTION_IDS}.", 400

    parameters = request.args.to_dict()

    for key in ['limit', 'offset', 'resulttype']:
        parameters.pop(key, None)

    # Transform the parameters to make a "hits" request returning only the number of records
    hits_parameters = parameters.copy()
    hits_parameters['resulttype'] = 'hits'
    hits_parameters['f'] = 'json'

    hits_url = f"{GEOMET_API_BASE_URL}/collections/{collectionId}/items"
    response = requests.get(hits_url, params=hits_parameters)

    try:
        response.raise_for_status()
    except requests.exceptions.HTTPError as http_err:
        return f"Error when calling GeoMet API : {http_err}", 500

    hits_result = response.json()

    if "numberMatched" not in hits_result:
        return "Unexpected response from GeoMet API. 'numberMatched' key is missing.", 500

    nb_total_results = hits_result["numberMatched"]

    links = []
    for offset in range(0, nb_total_results, GEOMET_API_PAGE_SIZE):
        # Build the URL to get this page, using the original parameters
        page_url = f"{GEOMET_API_BASE_URL}/collections/{collectionId}/items?{urlencode(parameters)}&limit={GEOMET_API_PAGE_SIZE}&offset={offset}"
        links.append({
            "start_index": offset,
            "end_index": min(offset + GEOMET_API_PAGE_SIZE, nb_total_results) - 1,
            "url": page_url,
        })

    return links
