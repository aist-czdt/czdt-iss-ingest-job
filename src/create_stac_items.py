#!/usr/bin/env python3

import requests
import json
import pystac
from pystac import Collection, ItemCollection

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
        return None, None
    else:
        return min(all_dates), max(all_dates)


def get_collection(mmgis_url, mmgis_token, collection_id):
    """
    Check if a STAC collection exists.
    Returns collection if collection exists, None otherwise.
    """
    url = f'{mmgis_url}/stac/collections/{collection_id}'
    
    try:
        response = requests.get(url, headers={'Authorization': f'Bearer {mmgis_token}'})
        if response.status_code == 200:
            return Collection.from_dict(json.loads(response.text))
        else:
            return None
    except requests.RequestException as e:
        print(f"Error checking collection existence: {e}")
        return False


def upsert_collection(mmgis_url, mmgis_token, collection_id, collection, collection_items, upsert_items=False):
    """
    Upsert a STAC collection exists.
    Returns (collection: Collection)
    """
    remote_collection = get_collection(mmgis_url, mmgis_token, collection_id)

    if remote_collection:
        print(f"Found existing collection with id {collection_id}.")

        if collection_items:            
            print(f"Updating temporal extent of collection {collection_id}...")
            min_date, max_date = get_min_max_dates_from_collections(collection, remote_collection)            
            remote_collection.extent.temporal.intervals = [[min_date, max_date]]

            # We have to clear existing links or duplicates will be inserted on PUT
            remote_collection.clear_links()

            response = requests.put(
                f"{mmgis_url}/stac/collections/{collection_id}",
                json=remote_collection.to_dict(),
                headers={
                    'Authorization': f'Bearer {mmgis_token}',
                    'Content-Type': 'application/json'
                }
            )
            response.raise_for_status()

            print(f"Collection '{collection_id}' updated successfully.")
            
            upsert_collection_items(mmgis_url, mmgis_token, collection_id, collection.get_items(), True)

        return remote_collection
    else:
        print(f"No existing collection with id {collection_id}. Creating new collection...")

        try:
            # Insert collection
            response = requests.post(
                f'{mmgis_url}/stac/collections',
                json=collection.to_dict(),
                headers={
                    'Authorization': f'Bearer {mmgis_token}',
                    'Content-Type': 'application/json'
                }
            )

            if 200 <= response.status_code < 300:
                print(f"Successfully created STAC collection: {collection_id}")
            else:
                print(f"Failed to create collection {collection_id}: {response.status_code} - {response.text}")
                return None

            upsert_collection_items(mmgis_url, mmgis_token, collection_id, collection.get_items(), upsert_items)

            return collection

        except requests.RequestException as e:
            print(f"Error creating collection: {e}")
            return None


def upsert_collection_items(mmgis_url, mmgis_token, collection_id, collection_items, upsert_items=False):

    try:
        # Insert items
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

        response = requests.post(
            f'{mmgis_url}/stac/collections/{collection_id}/bulk_items',
            json={"items": bulk_payload, "method": method},
            headers={"Authorization": f'Bearer {mmgis_token}', "content-type": "application/json"}
        )

        if 200 <= response.status_code < 300:
            print(f"Successfully created STAC collection items for collection {collection_id}")
        else:
            print(f"Failed to create collection items for {collection_id}: {response.status_code} - {response.text}")

    except requests.RequestException as e:
        print(f"Error upserting collection items: {e}")
        return None


def prepare_bulk_items_dict(items_by_id: dict) -> dict:
    return {item_id: item.to_dict() for item_id, item in items_by_id.items()}
