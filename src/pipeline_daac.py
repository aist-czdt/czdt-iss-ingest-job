import argparse
import os
import sys
import logging
import boto3
import json
from botocore.exceptions import ClientError
from maap.maap import MAAP  # Confirmed import for maap-py
from maap.dps.dps_job import DPSJob
import asyncio
from async_job import AsyncJob
import create_stac_items
import requests
from os.path import basename, join
from urllib.parse import urlparse

# Configure basic logging to provide feedback on the script's progress and any errors.
# The logging level can be adjusted (e.g., to logging.DEBUG for more verbose output).
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

# --- Custom Exceptions ---
class UploadError(Exception):
    """Custom exception for errors encountered during S3 upload."""
    pass


# --- Argument Parsing ---
def parse_arguments():
    """
    Defines and parses command-line arguments for the script.
    Returns:
        argparse.Namespace: An object containing the parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Search for a MAAP granule, download it, and upload it to an AWS S3 bucket.")
    parser.add_argument("--granule-id", required=True,
                        help="The Granule ID to search for (e.g., Granule UR or a producer granule ID recognizable by MAAP's CMR search).")
    parser.add_argument("--collection-id", required=True,
                        help="The Collection Concept ID (e.g., C123456789-MAAP) for the granule's collection.")
    parser.add_argument("--s3-bucket", required=True,
                        help="The name of the target S3 bucket for uploading the granule.")
    parser.add_argument("--s3-prefix", default="",
                        help="Optional S3 prefix (folder path) within the bucket. Do not use leading/trailing slashes. "
                             "The granule will be placed under <s3-prefix>/<collection-id>/<filename>.")
    parser.add_argument("--local-download-path", required=False, default="output",
                        help="Local directory path where the granule will be temporarily downloaded.")
    parser.add_argument("--role-arn", required=True,
                        help="Optional AWS IAM Role ARN to assume for S3 upload. "
                             "Useful for cross-account S3 bucket access.")
    parser.add_argument("--cmss-logger-host", required=True,
                        help="Used for logging pipeline messages.")
    parser.add_argument("--mmgis-host", required=True,
                        help="Used for cataloging stac items.")
    parser.add_argument("--titiler-token-secret-name", required=True,
                        help="The name of the maap secret used for storing the mmgis-host token.")
    parser.add_argument("--job-queue", required=True,
                        help="The name of the queue for running all pipeline jobs.")
    parser.add_argument("--zarr-config-url", required=True,
                        help="The s3 url of the ZARR config file.")
    parser.add_argument("--maap-host", default="api.maap-project.org",  # Default MAAP API host
                        help="MAAP API host. Defaults to 'api.ops.maap-project.org' if not overridden by MAAP_API_HOST env var.")
    return parser.parse_args()


# --- MAAP Operations ---
def get_maap_instance(maap_host_url: str) -> MAAP:
    """
    Initializes and returns a MAAP client instance.
    This function assumes that MAAP credentials (e.g., a .maaprc file or
    MAAP_PGT/MAAP_API_USER/MAAP_API_PASSWORD environment variables) are properly configured.

    Args:
        maap_host_url (str): The URL of the MAAP API host.

    Returns:
        MAAP: An initialized instance of the maap.maap.MAAP client.

    Raises:
        RuntimeError: If the MAAP instance cannot be initialized.
    """
    try:
        logging.info(f"Initializing MAAP client for host: {maap_host_url}")
        maap_client = MAAP(maap_host=maap_host_url)
        logging.info("MAAP client initialized successfully.")
        return maap_client
    except Exception as e:
        logging.error(f"Failed to initialize MAAP instance for host '{maap_host_url}': {e}", exc_info=True)
        raise RuntimeError(f"Could not initialize MAAP instance: {e}")


# --- File Cleanup ---
def cleanup_local_file(local_file_path: str):
    """
    Deletes the specified local file. Logs a warning if deletion fails.

    Args:
        local_file_path (str): The path to the local file to be deleted.
    """
    if not local_file_path or not isinstance(local_file_path, str):
        logging.warning("Invalid or empty file path provided for cleanup. Skipping.")
        return

    if os.path.exists(local_file_path):
        try:
            os.remove(local_file_path)
            logging.info(f"Successfully deleted local file: {local_file_path}")
        except OSError as e:
            logging.warning(f"Could not delete local file '{local_file_path}': {e}", exc_info=True)
    else:
        # This is not an error; file might have been cleaned up already or never created due to prior failure.
        logging.info(
            f"Local file '{local_file_path}' not found for cleanup (possibly already deleted or download failed).")


# HELPER FUNCTIONS
def cmss_logger(args, level, msg):
    endpoint = "log"
    url = f"{args.cmss_logger_host}/{endpoint}"
    body = {"level": level, "msg_body": str(msg)}
    r = requests.post(url, json=body)

def cmss_product_available(args, details):
    endpoint = "product"
    url = f"{args.cmss_logger_host}/{endpoint}"
    r = requests.post(url, json=details)

def job_error_message(job: DPSJob) -> str:
    if isinstance(job.error_details, str):
        try:
            return json.loads(job.error_details)["message"]
        except (json.JSONDecodeError, KeyError):
            return job.error_details

    return job.response_code or "Unknown error"

def parse_s3_path(s3_path: str) -> tuple[str, str]:
    """
    Parse an S3 path into first folder and remaining key components.
    Example: s3://s3-us-west-2.amazonaws.com:80/maap-ops-workspace/bsatoriu/dps_output/algo/master/ -> ('maap-ops-workspace', 'bsatoriu/dps_output/algo/master/')
    """
    if not s3_path.startswith("s3://"):
        raise ValueError(f"{s3_path} is not a valid s3 key")

    _, _, host, key = s3_path.split("/", 3)

    # Split key into first folder and the rest
    parts = key.split("/", 1)
    first_folder = parts[0]
    remaining_path = parts[1] if len(parts) > 1 else ""

    return first_folder, remaining_path

def get_dps_output(jobs: [DPSJob], file_ext: str, prefixes_only: bool=False) -> [str]:
    s3 = boto3.resource('s3')
    output = set()
    job_outputs = [next((path for path in j.retrieve_result() if path.startswith("s3")), None) for j in jobs]
    
    for job_output in job_outputs:
        bucket_name, path = parse_s3_path(job_output)
        dps_bucket = s3.Bucket(bucket_name)
        for obj in dps_bucket.objects.filter(Prefix=path):   
            if prefixes_only:
                folder_prefix = os.path.dirname(obj.key)
                if folder_prefix.endswith(file_ext):
                    output.add(f"s3://{bucket_name}/{folder_prefix}")
            else:
                if obj.key.endswith(file_ext):
                    output.add(f"s3://{bucket_name}/{obj.key}")

    return list(output)

def get_granule_from_file(args, file):
    granule_file_name = file.split("/")[-1]
    ingested_granule = f"s3://{args.s3_bucket}/{args.s3_prefix}/{args.collection_id}/{granule_file_name}"
    return ingested_granule

def get_zarr_from_file(args, file, ext):
    output_zarr_name = os.path.basename(get_granule_from_file(args, file)).replace(ext, ".zarr")
    return output_zarr_name

def upload_to_s3(s3_client, local_file_path: str, bucket_name: str, s3_prefix: str):
    """
    Uploads the specified local file to an S3 bucket. The file is placed under a
    constructed key: <s3_prefix>/<collection_id>/<filename>.

    Args:
        s3_client (boto3.client): The S3 client to use for the upload.
        local_file_path (str): The path to the local file to be uploaded.
        bucket_name (str): The name of the S3 bucket.
        s3_prefix (str): The S3 prefix (acts like a folder). Can be empty.

    Raises:
        FileNotFoundError: If the local file does not exist.
        UploadError: If the S3 upload fails.
    """
    if not os.path.exists(local_file_path):
        raise FileNotFoundError(f"Local file '{local_file_path}' intended for upload does not exist.")

    file_name = os.path.basename(local_file_path)

    # Construct the S3 object key, ensuring no leading/trailing slashes are mishandled.
    s3_key_parts = []
    if s3_prefix:
        s3_key_parts.append(s3_prefix.strip('/'))  # Remove slashes to prevent issues
    s3_key_parts.append(file_name)  # The actual filename

    # Join parts with '/', filtering out any empty strings (e.g., if s3_prefix was empty)
    s3_key = "/".join(part for part in s3_key_parts if part)

    logging.info(f"Starting upload of '{local_file_path}' to S3 bucket '{bucket_name}' with key '{s3_key}'.")

    try:
        s3_client.upload_file(local_file_path, bucket_name, s3_key)
        logging.info(f"Successfully uploaded '{file_name}' to 's3://{bucket_name}/{s3_key}'.")
    except ClientError as e:
        logging.error(f"Failed to upload '{local_file_path}' to S3 (s3://{bucket_name}/{s3_key}): {e}", exc_info=True)
        raise UploadError(f"S3 upload of {local_file_path} failed: {e}")
    except Exception as e:  # Catch other potential errors (e.g., file read issues not caught by boto3)
        logging.error(f"An unexpected error occurred during S3 upload of '{local_file_path}': {e}", exc_info=True)
        raise UploadError(f"Unexpected error during S3 upload of {local_file_path}: {e}")


# ETL FUNCTIONS
# TODO: exit with error code on any async_job failure
async def ingest(args, maap):
    print(f"Submitting staging job using collection id {args.collection_id}; granule id {args.granule_id}")        
    staging_job = maap.submitJob(identifier=f"ISS-Pipeline_ingest_{args.granule_id[-10:]}",
        algo_id="czdt-iss-ingest",
        version="main",
        queue=args.job_queue,
        granule_id=args.granule_id,
        collection_id=args.collection_id,
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        role_arn=args.role_arn)

    aj = AsyncJob(staging_job.id)
    await aj.get_job_status()
    return staging_job

async def transform(args, maap, ingest_result):
    convert_to_zarr_result = await convert_to_zarr(args, maap, ingest_result)
    convert_to_concatenated_zarr_result = await convert_to_concatenated_zarr(args, maap, convert_to_zarr_result)
    convert_zarr_to_cog_result = await convert_zarr_to_cog(args, maap, convert_to_concatenated_zarr_result)
    return convert_zarr_to_cog_result

async def convert_to_zarr(args, maap, ingest_result):
    EXT_NC = ".nc"
    EXT_NC4 = ".nc4"
    msg = f"Started ZARR extraction for {args.granule_id}"
    print(msg)   
    cmss_logger(args, "INFO", msg)
    #TODO: parameterize
    variables_to_extract = "PRECTOTCORR PRECTOT PRECCON"
    # Support .nc4 OR .nc
    nc_pattern = EXT_NC4
    nc_files = get_dps_output([ingest_result], EXT_NC4)   

    if len(nc_files) == 0:
        # No .nc4 output files found. Now try looking for .nc files
        nc_files = get_dps_output([ingest_result], EXT_NC)  

        if len(nc_files) > 0:
            nc_pattern = EXT_NC
            # TODO: determine variables dynamically based on file type
            variables_to_extract = ""

    maap_username = maap.profile.account_info()['username']

    print(f"Submitting CZDT_NETCDF_TO_ZARR job(s)")  
    jobs = [
        maap.submitJob(identifier=f"ISS-Pipeline_netcdf_2_zarr_{nc_file[-7:]}",
            algo_id="CZDT_NETCDF_TO_ZARR",
            version="master",
            queue=args.job_queue,
            input_s3=get_granule_from_file(args, nc_file),
            zarr_access="stage",
            config=args.zarr_config_url,
            config_path=join('input', basename(args.zarr_config_url)),
            pattern=f"*{nc_pattern}",
            output=get_zarr_from_file(args, nc_file, nc_pattern),
            variables=variables_to_extract
        )
        for nc_file in nc_files
    ]
    
    error_messages = [job_error_message(job) for job in jobs if not job.id]
    job_ids = [job.id for job in jobs if job.id]
    
    for error_message in error_messages:
        print(f"Failed to submit job: {error_message}", file=sys.stderr)
    
    # Generate tasks from array
    tasks = [AsyncJob(i).get_job_status() for i in job_ids]  
    results = await asyncio.gather(*tasks)
    print(jobs)

    s3_client = boto3.client('s3')

    for job in jobs:
        s3_zarr_urls = get_dps_output([job], ".zarr", True)  
        local_zarr_path = f"output/{job.id}.json"
        with open(local_zarr_path, 'w') as fp:
            json.dump(s3_zarr_urls, fp, indent=2) 

        # Step 4: Upload the downloaded granule to S3
        upload_to_s3(
            s3_client,
            local_zarr_path,
            "maap-ops-workspace",
            f"{maap_username}/zarr_concat_manifests"
        )

    return jobs


async def convert_to_concatenated_zarr(args, maap, convert_to_zarr_results):
    msg = f"Started ZARR concatenation for {args.granule_id}"
    maap_username = maap.profile.account_info()['username']
    print(msg)   
    cmss_logger(args, "INFO", msg)

    jobs = [
        maap.submitJob(
            identifier=f"ISS-Pipeline_zarr_concat_{convert_to_zarr_result.id[-7:]}",
            algo_id="CZDT_ZARR_CONCAT",
            version="master",
            queue=args.job_queue,
            config="s3://maap-ops-workspace/rileykk/sample_merra2_cfg.yaml",
            config_path='input/sample_merra2_cfg.yaml',
            zarr_manifest=f"s3://maap-ops-workspace/{maap_username}/zarr_concat_manifests/{convert_to_zarr_result.id}.json",
            zarr_access="mount",
            duration="P5D",
            output=f"concat.{convert_to_zarr_result.id}.zarr",
        ) for convert_to_zarr_result in convert_to_zarr_results
    ]

    error_messages = [job_error_message(job) for job in jobs if not job.id]
    job_ids = [job.id for job in jobs if job.id]
    
    for error_message in error_messages:
        print(f"Failed to submit job: {error_message}", file=sys.stderr)
    
    # Generate tasks from array
    tasks = [AsyncJob(i).get_job_status() for i in job_ids]  
    results = await asyncio.gather(*tasks)
    print(jobs)

    return jobs


async def convert_zarr_to_cog(args, maap, convert_to_concatenated_zarr_result):
    zarr_files = get_dps_output(convert_to_concatenated_zarr_result, ".zarr", True)       
    msg = f"Converting {len(zarr_files)} ZARR(s) to COG(s)"
    print(msg)   
    cmss_logger(args, "INFO", msg)

    print(f"Submitting CZDT_ZARR_TO_COG job(s)")  
    jobs = [
        maap.submitJob(
            identifier=f"ISS-Pipeline_zarr_2_cog_{zarr_file[-7:]}",
            algo_id="CZDT_ZARR_TO_COG",
            version="master",
            queue=args.job_queue,
            zarr=f"{zarr_file}/",
            zarr_access="stage",
            time="time",
            latitude="lat",
            longitude='lon',
            output_name=zarr_file.split("/")[-1].replace(".zarr", ""), # MERRA2_400.tavg1_2d_flx_Nx.20250331
        )
        for zarr_file in zarr_files
    ]
    
    error_messages = [job_error_message(job) for job in jobs if not job.id]
    job_ids = [job.id for job in jobs if job.id]
    
    for error_message in error_messages:
        print(f"Failed to submit job: {error_message}", file=sys.stderr)
    
    # Generate tasks from array
    tasks = [AsyncJob(i).get_job_status() for i in job_ids]  
    results = await asyncio.gather(*tasks)
    print(jobs)

    return jobs


def catalog(args, maap, convert_zarr_to_cog_result):
    tif_files = get_dps_output(convert_zarr_to_cog_result, ".tif")                   
    czdt_token = maap.secrets.get_secret(f"{args.titiler_token_secret_name}")

    #TODO: Parallelize and fix starttime
    for tif_file in tif_files:  
        create_stac_items.create_stac_items(
            mmgis_url=f"{args.mmgis_host}",
            mmgis_token=czdt_token,
            collection_id=args.collection_id,
            file_or_folder_path=tif_file,
            starttime="2025-04-01T18:30:00Z")

    product_details = {"collection":args.collection_id,
                       "ogc":f"{args.mmgis_host}/stac/collections/{args.collection_id}/items", "uris": tif_files}
    cmss_product_available(args, product_details)
    cmss_logger(args, "INFO", f"Product available for collection {args.collection_id}")


# --- Main Execution Block ---
async def main():
    """
    Main function to orchestrate the ingest, transform and catalog pipeline.
    Handles argument parsing and top-level error management.
    """
    args = parse_arguments()
    downloaded_granule_path = None  # Initialize to ensure it's defined for the finally block

    try:
        # Initialize MAAP client
        # The MAAP host can be overridden by the MAAP_API_HOST environment variable if maap-py supports it,
        # otherwise, it uses the --maap-host argument.
        maap_host_to_use = os.environ.get('MAAP_API_HOST', args.maap_host)
        maap = get_maap_instance(maap_host_to_use)

        ingest_result = await ingest(args, maap)
        transform_result = await transform(args, maap, ingest_result)
        catalog(args, maap, transform_result)

        # TODO: FILE CLEAN-UP

        logging.info("The DAAC pipeline completed successfully!")

    except ValueError as e:  # E.g. invalid local_download_path
        logging.error(f"TERMINATED: Invalid argument or value. Details: {e}")
        sys.exit(6)
    except RuntimeError as e:  # E.g. MAAP client init failed
        logging.error(f"TERMINATED: Runtime error. Details: {e}")
        sys.exit(7)
    except Exception as e:
        logging.error(f"TERMINATED: An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)  # General error

if __name__ == "__main__":
    asyncio.run(main())
