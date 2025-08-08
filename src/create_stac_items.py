#!/usr/bin/env python3

# Developed on python 3.11.4
# ex: 
# create_stac_items.create_stac_items(
#     mmgis_url=f"{mmgis_host}",
#     mmgis_token=czdt_token,
#     collection_id=collection_id,
#     file_or_folder_path=tif_file,
#     starttime="2025-04-01T18:30:00Z")

import requests
import json
import os
import re
from datetime import datetime
from rio_stac import create_stac_item
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter
import pystac
from pystac import Collection, ItemCollection


def extract_variable_from_tif_filename(tif_file_path):
    """
    Extract variable name from TIF filename.
    Expected format: path/to/file_VARIABLE.tif or similar patterns
    """
    filename = os.path.basename(tif_file_path)
    base_name = os.path.splitext(filename)[0]
    
    # Try to extract variable name from common patterns
    # Pattern 1: filename ends with _VARIABLE (e.g., MERRA2_400.tavg1_2d_flx_Nx.20250331_PRECTOT.tif)
    if '_' in base_name:
        parts = base_name.split('_')
        # Try the last part as variable name
        potential_var = parts[-1]
        # Check if it looks like a variable name (alphanumeric, not just numbers/dates)
        if re.match(r'^[A-Za-z][A-Za-z0-9]*$', potential_var):
            return potential_var
    
    # Pattern 2: Extract from typical scientific variable naming
    # Look for common variable patterns in geoscience data
    var_patterns = [
        r'(?:_|^)(PREC[A-Z]*|TEMP[A-Z]*|WIND[A-Z]*|PRESS[A-Z]*|[A-Z]{2,}[A-Z0-9]*)(?:_|\.tif$)',
        r'(?:_|^)([A-Z][A-Z0-9_]*[A-Z])(?:_|\.tif$)'
    ]
    
    for pattern in var_patterns:
        match = re.search(pattern, base_name)
        if match:
            return match.group(1)
    
    # Fallback: use the last meaningful part of the filename
    if '_' in base_name:
        return base_name.split('_')[-1]
    
    # Final fallback: return "data" as a generic variable name
    return "data"


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


def get_item_collection(mmgis_url, mmgis_token, collection_id):
    """
    Returns item collection if collection exists, None otherwise.
    """
    url = f'{mmgis_url}/stac/collections/{collection_id}/items'
    
    try:
        response = requests.get(url, headers={'Authorization': f'Bearer {mmgis_token}'})
        if response.status_code == 200:
            return ItemCollection.from_dict(json.loads(response.text))
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
            print(f"Adding items to collection id {collection_id} and updating extent...")
            remote_collection.add_items(collection_items)
            remote_collection.update_extent_from_items()

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

            # Insert items
            items_by_id = {item.id: item for item in collection.get_items()}
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
                return None

            return collection

        except requests.RequestException as e:
            print(f"Error creating collection: {e}")
            return None


def prepare_bulk_items_dict(items_by_id: dict) -> dict:
    return {item_id: item.to_dict() for item_id, item in items_by_id.items()}
