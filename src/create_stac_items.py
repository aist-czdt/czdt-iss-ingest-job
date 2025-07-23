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


def check_collection_exists(mmgis_url, mmgis_token, collection_id):
    """
    Check if a STAC collection exists.
    Returns True if collection exists, False otherwise.
    """
    url = f'{mmgis_url}/stac/collections/{collection_id}'
    
    try:
        response = requests.get(url, headers={'Authorization': f'Bearer {mmgis_token}'})
        return response.status_code == 200
    except requests.RequestException as e:
        print(f"Error checking collection existence: {e}")
        return False


def create_stac_collection(mmgis_url, mmgis_token, collection_id, variable_name, description=None):
    """
    Create a new STAC collection.
    """
    if description is None:
        description = f"CZDT collection for {collection_id} variable {variable_name}"
    
    collection_data = {
        "id": collection_id,
        "type": "Collection",
        "title": f"{collection_id} - {variable_name}",
        "description": description,
        "stac_version": "1.0.0",
        "license": "proprietary",
        "extent": {
            "spatial": {
                "bbox": [[-180, -90, 180, 90]]  # Global extent as default
            },
            "temporal": {
                "interval": [["1900-01-01T00:00:00Z", None]]  # Open-ended temporal extent
            }
        },
        "links": [
            {
                "rel": "root",
                "href": f"{mmgis_url}/stac/",
                "type": "application/json",
                "title": "STAC Catalog"
            },
            {
                "rel": "parent",
                "href": f"{mmgis_url}/stac/",
                "type": "application/json",
                "title": "STAC Catalog"
            },
            {
                "rel": "self",
                "href": f"{mmgis_url}/stac/collections/{collection_id}",
                "type": "application/json",
                "title": f"Collection {collection_id}"
            },
            {
                "rel": "items",
                "href": f"{mmgis_url}/stac/collections/{collection_id}/items",
                "type": "application/geo+json",
                "title": "Items"
            }
        ]
    }
    
    url = f'{mmgis_url}/stac/collections'
    
    try:
        response = requests.post(
            url,
            json=collection_data,
            headers={
                'Authorization': f'Bearer {mmgis_token}',
                'Content-Type': 'application/json'
            }
        )
        
        if response.status_code >= 200 and response.status_code < 300:
            print(f"Successfully created STAC collection: {collection_id}")
            return True
        else:
            print(f"Failed to create collection {collection_id}: {response.status_code} - {response.text}")
            return False
            
    except requests.RequestException as e:
        print(f"Error creating collection: {e}")
        return False


def ensure_collection_exists(mmgis_url, mmgis_token, collection_id, tif_file_path):
    """
    Ensure a STAC collection exists, creating it if necessary.
    Returns (success: bool, actual_collection_id: str)
    """
    # Check if original collection exists
    if check_collection_exists(mmgis_url, mmgis_token, collection_id):
        print(f"Collection {collection_id} already exists")
        return True, collection_id
    
    # Extract variable name from TIF file
    variable_name = extract_variable_from_tif_filename(tif_file_path)
    
    # Create new collection with variable name appended
    full_collection_id = f"{collection_id}-{variable_name}"
    
    # Check if the full collection ID already exists
    if check_collection_exists(mmgis_url, mmgis_token, full_collection_id):
        print(f"Collection {full_collection_id} already exists")
        return True, full_collection_id
    
    print(f"Creating new collection: {full_collection_id} (original: {collection_id}, variable: {variable_name})")
    
    success = create_stac_collection(
        mmgis_url=mmgis_url,
        mmgis_token=mmgis_token,
        collection_id=full_collection_id,
        variable_name=variable_name,
        description=f"CZDT ISS collection for {collection_id} processing variable {variable_name}"
    )
    
    return success, full_collection_id


def create_stac_items(mmgis_url, mmgis_token, collection_id, file_or_folder_path, path_remove="", path_replace_with="", upsert=False, regex=None, time_from_fn=None, starttime=None, endtime=None):
    """Create STAC items from a collection id and input file location.

    Args:
        mmgis_url: URL to MMGIS
        mmgis_token: MMGIS API Token
        collection_id: Pre-existing STAC collection id
        file_or_folder_path: Input file or folder path
        path_remove: Portion of internal file path to remove. Useful if the current filepath differs from what titiler should hit internally.
        path_replace_with: If --path_remove, instead of setting that portion of the internal path to "", replaces it with this value.
        upsert: Allow overwriting existing STAC items
        regex: If folder, only create stac items for files that match this regex
        time_from_fn: time format to read from filename

    Returns:
        Created STAC records
    """

    isDir = os.path.isdir(file_or_folder_path)
    print('Finding files...')

    files = []
    stac_records = []
    if isDir:
        print('    Note: regexing in folders is not implemented.')
        filelist = os.listdir(file_or_folder_path)
        for file in filelist[:]: # filelist[:] makes a copy of filelist.
            if file.lower().endswith(".tif"):
                files.append(os.path.join(file_or_folder_path, file))
    else:
        filename, file_extension = os.path.splitext(file_or_folder_path)
        if file_extension.lower() == '.tif':
            files.append(file_or_folder_path)

    # Ensure collection exists before creating items
    # Use the first file to determine variable name and collection ID
    if files:
        first_file = files[0]
        success, actual_collection_id = ensure_collection_exists(mmgis_url, mmgis_token, collection_id, first_file)
        if not success:
            print(f"Failed to create/verify collection for {collection_id}")
            return []
        collection_id = actual_collection_id
        print(f"Using collection ID: {collection_id}")
    else:
        print("No TIF files found to process")
        return []

    items = {}

    url = f'{mmgis_url}/stac/collections/{collection_id}/bulk_items'

    for idx, file in enumerate(files, start=1):
        print(f'Gathering metadata {idx}/{len(files)}...', end='\r', flush=True)
        asset_href = file
        if path_remove is not None:
            if path_replace_with is not None:
                asset_href = file.replace(path_remove, path_replace_with)
            else:
                asset_href = file.replace(path_remove, "")

        property = {}
        input_datetime = None
        iso_time_fmt = '%Y-%m-%dT%H:%M:%SZ'
        time_fmt = iso_time_fmt
        if time_from_fn is not None:
            time_fmt = time_from_fn
            input_datetime = datetime.strptime(os.path.basename(file),
                                               time_fmt)
            property = {'start_datetime': input_datetime.strftime(iso_time_fmt),
                        'end_datetime': input_datetime.strftime(iso_time_fmt)
                        }     
        if starttime is not None:
            input_datetime = datetime.strptime(starttime, time_fmt)
            property['start_datetime'] = input_datetime.strftime(iso_time_fmt)
        if endtime is not None:
            input_endtime = datetime.strptime(endtime, time_fmt)
            property['end_datetime'] = input_endtime.strftime(iso_time_fmt)

        item = create_stac_item(
            file,
            input_datetime=input_datetime,
            #extensions=extensions,
            #collection=collection,
            #collection_url=collection_url,
            properties=property,
            #id=id,
            #asset_name=asset_name,
            asset_href=asset_href,
            #asset_media_type=asset_mediatype,
            with_proj=True,
            with_raster=True,
            with_eo=True,
            #raster_max_size=max_raster_size,
            #geom_densify_pts=densify_geom,
            #geom_precision=geom_precision,
        )
        item_dict = item.to_dict()
        items[item_dict.get('id')] = item_dict

        stac_record_url = mmgis_url + '/stac/collections/' + collection_id + '/items/' + os.path.basename(file)
        stac_records.append(stac_record_url)

    print(f'Gathering metadata {len(files)}/{len(files)}...')

    print('Sending bulk item creation request...')

    method = 'insert'
    if upsert is True:
        method = 'upsert'
        print(f'Using method: {method}.')
    else:
        print(f'Using method: {method}.')
        print('    Note: The bulk insert may fail with a ConflictError if any item already exists. Consider using the --upsert flag if such replacement is intentional.')

    req = requests.post(url, json = { "items": items, "method": method }, headers = { "Authorization": f'Bearer {mmgis_token}', "content-type": "application/json" } )
    print(json.loads(req.text))

    # Check if the request was successful
    if req.status_code >= 200 and req.status_code < 300:
        print("Request was successful!\n")
        return stac_records
    else:
        print(f"Request failed with status code {req.status_code}")
        return ''
