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
from datetime import datetime
from rio_stac import create_stac_item
from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter


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