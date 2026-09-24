#!/usr/bin/env python3

import requests
import json
import logging
import math
import re
import backoff
import pystac
from pystac import Collection, ItemCollection, SpatialExtent


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# The STAC API host has been observed to time out under a burst of catalog jobs (2026-09-22 LIS backfill probe:
# 3 of 24 jobs lost items to a single ConnectTimeout each). Every call to it is retried with backoff, and a call
# that still fails raises instead of being reported as a normal answer.
STAC_REQUEST_TIMEOUT_S = 60
# Errors raised before a request is sent (bad JSON payload, bad URL) are not transient: never retry them.
_CLIENT_SIDE_ERRORS = (requests.exceptions.InvalidJSONError, requests.exceptions.InvalidURL,
                       requests.exceptions.MissingSchema, requests.exceptions.InvalidSchema)


def _give_up(e: Exception) -> bool:
    if isinstance(e, _CLIENT_SIDE_ERRORS):
        return True
    resp = getattr(e, 'response', None)
    return resp is not None and 400 <= resp.status_code < 500 and resp.status_code != 429


_stac_retry = backoff.on_exception(
    backoff.expo, requests.exceptions.RequestException, max_tries=6, max_time=600, giveup=_give_up,
)


class StacApiError(RuntimeError):
    """A STAC API call failed after retries, or returned an error status."""


def find_non_finite(obj, path=""):
    """Yield (json_path, value) for every inf/nan float in a nested dict/list."""
    if isinstance(obj, dict):
        for k, v in obj.items():
            yield from find_non_finite(v, f"{path}.{k}" if path else str(k))
    elif isinstance(obj, (list, tuple)):
        for i, v in enumerate(obj):
            yield from find_non_finite(v, f"{path}[{i}]")
    elif isinstance(obj, float) and (math.isinf(obj) or math.isnan(obj)):
        yield path, obj


# Fields whose values position the data; a non-finite number here means the georeferencing is wrong and the
# record must not be written. Everything else (properties, asset metadata, summaries) is descriptive and the
# offending value is dropped instead, since JSON cannot carry inf/nan at all.
_STRUCTURAL_FIELDS = ("bbox", "geometry", "extent")


def _is_structural(path: str) -> bool:
    top = path.split(".")[0].split("[")[0]
    return top in _STRUCTURAL_FIELDS or ".proj:" in path  # proj:bbox / proj:transform / proj:geometry


def _drop_path(obj, path):
    parts = [p for p in re.split(r"\.|\[|\]", path) if p != ""]
    parent = obj
    for part in parts[:-1]:
        parent = parent[int(part)] if isinstance(parent, list) else parent[part]
    last = parts[-1]
    if isinstance(parent, list):
        parent[int(last)] = None
    else:
        del parent[last]


def sanitize_stac_dict(d: dict, label: str) -> dict:
    """
    Make a STAC dict JSON-serialisable: drop non-finite floats from descriptive fields (logged one by one) and
    raise StacApiError naming the exact path if one sits in bbox / geometry / extent.

    Motivating case (2026-09-23, LIS ROUTING files): every SurfElev collection POST failed with
    "Out of range float values are not JSON compliant: inf" and nothing in the log said which field.
    """
    bad = list(find_non_finite(d))
    if not bad:
        return d
    structural = [(pth, v) for pth, v in bad if _is_structural(pth)]
    if structural:
        where = ", ".join(f"{pth}={v}" for pth, v in structural[:8])
        raise StacApiError(f"{label} has non-finite coordinates and cannot be cataloged: {where}")
    for pth, v in bad:
        print(f"WARNING: {label}: dropping non-finite value {pth}={v} (not representable in JSON)")
        logger.warning(f"{label}: dropping non-finite value {pth}={v}")
        _drop_path(d, pth)
    return d


def get_min_max_dates_from_collections(collection1: pystac.Collection, collection2: pystac.Collection):
    """
    Gets the overall minimum and maximum dates from the temporal extents
    of two pystac Collections.

    Args:
        collection1: The first pystac Collection.
        collection2: The second pystac Collection.

    Returns:
        A tuple containing (min_date, max_date) or (None, None) if no dates are found.
    """
    all_dates = []


    # Extract dates from collection 1
    if collection1.extent and collection1.extent.temporal:
        for interval in collection1.extent.temporal.intervals:
            if interval[0] is not None:
                all_dates.append(interval[0])
            if interval[1] is not None:
                all_dates.append(interval[1])

    # Extract dates from collection 2
    if collection2.extent and collection2.extent.temporal:
        for interval in collection2.extent.temporal.intervals:
            if interval[0] is not None:
                all_dates.append(interval[0])
            if interval[1] is not None:
                all_dates.append(interval[1])

    if not all_dates:
        print("get_min_max_dates_from_collections function found no collection dates.")
        return None, None
    else:
        min_date = min(all_dates)
        max_date = max(all_dates)
        print(f"Min collection date: {min_date}; max collection date: {max_date}")
        return min_date, max_date


def _fix_spatial_extent(extent: SpatialExtent) -> SpatialExtent:
    copy = extent.clone()

    for i in range(len(extent.bboxes)):
        bbox = extent.bboxes[i]
        if len(bbox) == 4:
            min_lon, min_lat, max_lon, max_lat = bbox

            min_lon = max(min_lon, -180.0)
            min_lat = max(min_lat, -90.0)
            max_lon = min(max_lon, 180.0)
            max_lat = min(max_lat, 90.0)

            extent.bboxes[i] = [min_lon, min_lat, max_lon, max_lat]

            if copy.bboxes[i] != extent.bboxes[i]:
                print(f'Adjusted extent bbox: {copy.bboxes[i]} -> {extent.bboxes[i]}')

    return extent


def _normalize_base_url(url):
    """Strip whitespace and trailing slashes so f"{url}/path" never yields a double slash."""
    return url.strip().rstrip('/') if url else url


@_stac_retry
def _stac_get(url, token):
    response = requests.get(url, headers={'Authorization': f'Bearer {token}'}, timeout=STAC_REQUEST_TIMEOUT_S)
    if response.status_code >= 500 or response.status_code == 429:
        response.raise_for_status()  # retried by _stac_retry
    return response


def get_collection(mmgis_url, mmgis_token, collection_id):
    """
    Check if a STAC collection exists.
    Returns the collection if it exists, None if the API says it does not (404).

    Raises StacApiError if the API cannot be reached after retries: a network error must never be mistaken
    for "collection absent", or the caller will try to create it and fail with a 409 while the items go unwritten.
    """
    mmgis_url = _normalize_base_url(mmgis_url)
    url = f'{mmgis_url}/stac/collections/{collection_id}'

    try:
        response = _stac_get(url, mmgis_token)
    except requests.RequestException as e:
        raise StacApiError(f"Could not check existence of collection {collection_id} at {url}: {e}") from e
    if response.status_code == 200:
        return Collection.from_dict(json.loads(response.text))
    if response.status_code == 404:
        return None
    raise StacApiError(f"Unexpected response checking collection {collection_id}: {response.status_code} - {response.text[:300]}")


def upsert_collection(mmgis_url, mmgis_token, collection_id, collection, collection_items, upsert_items=False):
    """
    Upsert a STAC collection exists.
    Returns (collection: Collection)
    """
    mmgis_url = _normalize_base_url(mmgis_url)
    remote_collection = get_collection(mmgis_url, mmgis_token, collection_id)

    if remote_collection:
        print(f"Found existing collection with id {collection_id}.")

        if collection_items:            
            print(f"Updating temporal extent of collection {collection_id}...")
            print("Comparing min and max dates of new collection against existing, remote collection.")
            min_date, max_date = get_min_max_dates_from_collections(collection, remote_collection)   

            remote_collection.extent.temporal.intervals = [[min_date, max_date]]
            remote_collection.extent.spatial = _fix_spatial_extent(remote_collection.extent.spatial)

            # We have to clear existing links or duplicates will be inserted on PUT
            remote_collection.clear_links()

            try:
                response = _stac_send(
                    'put', f"{mmgis_url}/stac/collections/{collection_id}", mmgis_token,
                    json=sanitize_stac_dict(remote_collection.to_dict(), f"collection {collection_id}"),
                )
                response.raise_for_status()
            except requests.RequestException as e:
                resp = getattr(e, 'response', None)
                print(f"Failed to update collection {collection_id}: "
                      f"{resp.status_code if resp is not None else 'no response'} - {getattr(resp, 'text', e)}")
                raise StacApiError(f"Failed to update collection {collection_id}: {e}") from e

            print(f"Collection '{collection_id}' updated successfully.")

            upsert_collection_items(mmgis_url, mmgis_token, collection_id, collection.get_items(), True)

        return remote_collection
    else:
        print(f"No existing collection with id {collection_id}. Creating new collection...")

        try:
            # Insert collection
            payload = sanitize_stac_dict(collection.to_dict(), f"collection {collection_id}")
            response = _stac_send('post', f'{mmgis_url}/stac/collections', mmgis_token, json=payload)
        except requests.RequestException as e:
            raise StacApiError(f"Error creating collection {collection_id}: {e}") from e

        if 200 <= response.status_code < 300:
            print(f"Successfully created STAC collection: {collection_id}")
        else:
            print(f"Failed to create collection {collection_id}: {response.status_code} - {response.text}")
            raise StacApiError(f"Failed to create collection {collection_id}: {response.status_code} - {response.text[:300]}")

        upsert_collection_items(mmgis_url, mmgis_token, collection_id, collection.get_items(), upsert_items)

        return collection


@_stac_retry
def _stac_send(method, url, token, **kwargs):
    """POST/PUT to the STAC API; 5xx and 429 raise so _stac_retry retries them, other statuses are returned."""
    send = {'post': requests.post, 'put': requests.put}[method]
    response = send(
        url, headers={'Authorization': f'Bearer {token}', 'Content-Type': 'application/json'},
        timeout=STAC_REQUEST_TIMEOUT_S, **kwargs,
    )
    if response.status_code >= 500 or response.status_code == 429:
        response.raise_for_status()
    return response


def upsert_collection_items(mmgis_url, mmgis_token, collection_id, collection_items, upsert_items=False):
    """
    Bulk insert/upsert items into a collection.

    Raises StacApiError if the API cannot be reached after retries or rejects the request. Until 2026-09 a
    failure here was only printed, so a catalog job could finish "completed" with none of its items written.
    """
    mmgis_url = _normalize_base_url(mmgis_url)

    items_by_id = {item.id: item for item in collection_items}
    bulk_payload = prepare_bulk_items_dict(items_by_id)

    method = 'insert'
    if upsert_items is True:
        method = 'upsert'
        print(f'Using method: {method}.')
    else:
        print(f'Using method: {method}.')
        print(
            '    Note: The bulk insert may fail with a ConflictError if any item already exists. Consider using the --upsert flag if such replacement is intentional.')

    try:
        response = _stac_send(
            'post', f'{mmgis_url}/stac/collections/{collection_id}/bulk_items', mmgis_token,
            json={"items": bulk_payload, "method": method},
        )
    except requests.RequestException as e:
        raise StacApiError(f"Error upserting {len(items_by_id)} items into {collection_id}: {e}") from e

    if 200 <= response.status_code < 300:
        print(f"Successfully created STAC collection items for collection {collection_id}")
        logger.debug(f"Successfully created STAC collection items for collection {collection_id}\n{response.text}")
        return response
    print(f"Failed to create collection items for {collection_id}: {response.status_code} - {response.text}")
    raise StacApiError(f"Failed to write {len(items_by_id)} items into {collection_id}: "
                       f"{response.status_code} - {response.text[:300]}")


def prepare_bulk_items_dict(items_by_id: dict) -> dict:
    return {item_id: sanitize_stac_dict(item.to_dict(), f"item {item_id}") for item_id, item in items_by_id.items()}
