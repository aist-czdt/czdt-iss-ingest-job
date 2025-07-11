import argparse
import os
import sys
import logging
import boto3
import json
from maap.maap import MAAP
from maap.dps.dps_job import DPSJob
import asyncio
from async_job import AsyncJob
import create_stac_items
import requests
from os.path import basename, join
from common_utils import (
    AWSUtils, MaapUtils, LoggingUtils, ConfigUtils
)

# Configure basic logging to provide feedback on the script's progress and any errors.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

# --- Argument Parsing ---
def parse_arguments():
    """
    Defines and parses command-line arguments for the script.
    Returns:
        argparse.Namespace: An object containing the parsed command-line arguments.
    """
    parser = ConfigUtils.get_generic_argument_parser()
    parser.description = "Process NetCDF files from S3 through CZDT_NETCDF_TO_ZARR pipeline, convert to COG, and catalog to STAC."
    
    return parser.parse_args()

def get_zarr_output_name(input_s3_url: str) -> str:
    """Generate zarr output name from input S3 URL"""
    filename = os.path.basename(input_s3_url)
    base_name = os.path.splitext(filename)[0]
    return f"{base_name}.zarr"


def upload_to_s3(s3_client, local_file_path: str, bucket_name: str, s3_prefix: str):
    file_name = os.path.basename(local_file_path)

    # Construct the S3 object key
    s3_key_parts = []
    if s3_prefix:
        s3_key_parts.append(s3_prefix.strip('/'))
    s3_key_parts.append(file_name)
    s3_key = "/".join(part for part in s3_key_parts if part)

    AWSUtils.upload_to_s3(local_file_path, bucket_name, s3_key, s3_client)


# ETL FUNCTIONS
async def convert_to_zarr(args, maap, input_s3_url):
    """Convert NetCDF file to Zarr using CZDT_NETCDF_TO_ZARR job"""
    msg = f"Started ZARR conversion for {input_s3_url}"
    print(msg)
    LoggingUtils.cmss_logger(str(msg), args.cmss_logger_host)
    
    # Extract filename and determine pattern
    filename = os.path.basename(input_s3_url)
    if filename.endswith('.nc4'):
        pattern = "*.nc4"
    elif filename.endswith('.nc'):
        pattern = "*.nc"
    else:
        raise ValueError(f"Unsupported file extension for {filename}")
    
    output_zarr_name = get_zarr_output_name(input_s3_url)
    
    print(f"Submitting CZDT_NETCDF_TO_ZARR job for {input_s3_url}")
    job = maap.submitJob(
        identifier=f"NetCDF-Pipeline_netcdf_2_zarr_{filename[-10:]}",
        algo_id="CZDT_NETCDF_TO_ZARR",
        version="master",
        queue=args.job_queue,
        input_s3=input_s3_url,
        zarr_access="stage",
        config=args.zarr_config_url,
        config_path=join('input', basename(args.zarr_config_url)),
        pattern=pattern,
        output=output_zarr_name,
        variables=args.variables
    )
    
    if not job.id:
        error_msg = MaapUtils.job_error_message(job)
        raise RuntimeError(f"Failed to submit CZDT_NETCDF_TO_ZARR job: {error_msg}")
    
    # Wait for job completion
    aj = AsyncJob(job.id)
    await aj.get_job_status()
    
    return job

async def convert_to_concatenated_zarr(args, maap, convert_to_zarr_result):
    """Convert individual zarr to concatenated zarr"""
    msg = f"Started ZARR concatenation for job {convert_to_zarr_result.id}"
    maap_username = maap.profile.account_info()['username']
    print(msg)
    LoggingUtils.cmss_logger(str(msg), args.cmss_logger_host)

    # Create zarr manifest
    s3_client = boto3.client('s3')
    s3_zarr_urls = MaapUtils.get_dps_output([convert_to_zarr_result], ".zarr", True)
    local_zarr_path = f"output/{convert_to_zarr_result.id}.json"
    
    # Ensure output directory exists
    os.makedirs("output", exist_ok=True)
    
    with open(local_zarr_path, 'w') as fp:
        json.dump(s3_zarr_urls, fp, indent=2)

    # Upload manifest to S3
    upload_to_s3(
        s3_client,
        local_zarr_path,
        "maap-ops-workspace",
        f"{maap_username}/zarr_concat_manifests"
    )

    job = maap.submitJob(
        identifier=f"NetCDF-Pipeline_zarr_concat_{convert_to_zarr_result.id[-7:]}",
        algo_id="CZDT_ZARR_CONCAT",
        version="master",
        queue=args.job_queue,
        config="s3://maap-ops-workspace/rileykk/sample_merra2_cfg.yaml",
        config_path='input/sample_merra2_cfg.yaml',
        zarr_manifest=f"s3://maap-ops-workspace/{maap_username}/zarr_concat_manifests/{convert_to_zarr_result.id}.json",
        zarr_access="mount",
        duration="P5D",
        output=f"concat.{convert_to_zarr_result.id}.zarr",
    )

    if not job.id:
        error_msg = MaapUtils.job_error_message(job)
        raise RuntimeError(f"Failed to submit CZDT_ZARR_CONCAT job: {error_msg}")
    
    # Wait for job completion
    aj = AsyncJob(job.id)
    await aj.get_job_status()
    
    return job

async def convert_zarr_to_cog(args, maap, convert_to_concatenated_zarr_result):
    """Convert Zarr to Cloud Optimized GeoTIFF"""
    zarr_files = MaapUtils.get_dps_output([convert_to_concatenated_zarr_result], ".zarr", True)
    msg = f"Converting {len(zarr_files)} ZARR(s) to COG(s)"
    print(msg)
    LoggingUtils.cmss_logger(str(msg), args.cmss_logger_host)

    print(f"Submitting CZDT_ZARR_TO_COG job(s)")
    jobs = []
    
    for zarr_file in zarr_files:
        job = maap.submitJob(
            identifier=f"NetCDF-Pipeline_zarr_2_cog_{zarr_file[-7:]}",
            algo_id="CZDT_ZARR_TO_COG",
            version="master",
            queue=args.job_queue,
            zarr=f"{zarr_file}/",
            zarr_access="stage",
            time="time",
            latitude="lat",
            longitude='lon',
            output_name=zarr_file.split("/")[-1].replace(".zarr", ""),
        )
        
        if job.id:
            jobs.append(job)
        else:
            error_msg = MaapUtils.job_error_message(job)
            print(f"Failed to submit CZDT_ZARR_TO_COG job: {error_msg}", file=sys.stderr)
    
    if not jobs:
        raise RuntimeError("No CZDT_ZARR_TO_COG jobs were successfully submitted")
    
    # Wait for all jobs to complete
    job_ids = [job.id for job in jobs]
    tasks = [AsyncJob(job_id).get_job_status() for job_id in job_ids]
    await asyncio.gather(*tasks)
    
    return jobs

def catalog(args, maap, convert_zarr_to_cog_result, convert_to_zarr_result):
    """Catalog the processed COG files to STAC"""
    tif_files = MaapUtils.get_dps_output(convert_zarr_to_cog_result, ".tif")
    zarr_files = MaapUtils.get_dps_output([convert_to_zarr_result], ".zarr", True)
    czdt_token = maap.secrets.get_secret(f"{args.titiler_token_secret_name}")

    msg = f"Cataloging {len(tif_files)} COG file(s) to STAC"
    print(msg)
    LoggingUtils.cmss_logger(str(msg), args.cmss_logger_host)

    for tif_file in tif_files:
        create_stac_items.create_stac_items(
            mmgis_url=f"{args.mmgis_host}",
            mmgis_token=czdt_token,
            collection_id=args.collection_id,
            file_or_folder_path=tif_file,
            starttime="2025-04-01T18:30:00Z"
        )

    # Include both COG and Zarr files in product details
    all_uris = tif_files + zarr_files
    product_details = {
        "collection": args.collection_id,
        "ogc": f"{args.mmgis_host}/stac/collections/{args.collection_id}/items",
        "uris": all_uris,
        "job_id": MaapUtils.get_job_id()
    }
    LoggingUtils.cmss_product_available(product_details, args.cmss_logger_host)
    LoggingUtils.cmss_logger(f"Product available for collection {args.collection_id}", args.cmss_logger_host)


# --- Main Execution Block ---
async def main():
    """
    Main function to orchestrate the NetCDF to COG pipeline.
    Handles argument parsing and top-level error management.
    """
    args = parse_arguments()

    try:
        # Initialize MAAP client
        maap_host_to_use = os.environ.get('MAAP_API_HOST', args.maap_host)
        maap = MaapUtils.get_maap_instance(maap_host_to_use)

        # Validate input S3 URL
        if not args.input_s3_url.startswith('s3://'):
            raise ValueError(f"Invalid S3 URL: {args.input_s3_url}")

        # Execute pipeline steps
        convert_to_zarr_result = await convert_to_zarr(args, maap, args.input_s3_url)
        # Since this is a one time ingest we do not need zar concatenation 
        #convert_to_concatenated_zarr_result = await convert_to_concatenated_zarr(args, maap, convert_to_zarr_result)
        convert_zarr_to_cog_result = await convert_zarr_to_cog(args, maap, convert_to_zarr_result)
        catalog(args, maap, convert_zarr_to_cog_result)

        logging.info("The NetCDF pipeline completed successfully!")

    except ValueError as e:
        logging.error(f"TERMINATED: Invalid argument or value. Details: {e}")
        sys.exit(6)
    except RuntimeError as e:
        logging.error(f"TERMINATED: Runtime error. Details: {e}")
        sys.exit(7)
    except Exception as e:
        logging.error(f"TERMINATED: An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())