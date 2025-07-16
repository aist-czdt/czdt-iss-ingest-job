import argparse
import os
import sys
import logging
import boto3
import json
from botocore.exceptions import ClientError
from maap.maap import MAAP
from maap.dps.dps_job import DPSJob
import asyncio
import backoff
import create_stac_items
import requests
from os.path import basename, join
from urllib.parse import urlparse

# Configure basic logging to provide feedback on the script's progress and any errors.
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
        description="Process NetCDF files from S3 through CZDT_NETCDF_TO_ZARR pipeline, convert to COG, and catalog to STAC.")
    parser.add_argument("--input-s3-url", required=True,
                        help="The S3 URL of the input NetCDF file to process.")
    parser.add_argument("--collection-id", required=True,
                        help="The Collection Concept ID (e.g., C123456789-MAAP) for the granule's collection.")
    parser.add_argument("--s3-bucket", required=True,
                        help="The name of the target S3 bucket for uploading processed files.")
    parser.add_argument("--s3-prefix", default="",
                        help="Optional S3 prefix (folder path) within the bucket. Do not use leading/trailing slashes.")
    parser.add_argument("--role-arn", required=True,
                        help="AWS IAM Role ARN to assume for S3 upload.")
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
    parser.add_argument("--variables", default="",
                        help="Space-separated list of variables to extract from NetCDF.")
    parser.add_argument("--maap-host", default="api.maap-project.org",
                        help="MAAP API host. Defaults to 'api.maap-project.org'.")
    return parser.parse_args()


# --- MAAP Operations ---
def get_maap_instance(maap_host_url: str) -> MAAP:
    """
    Initializes and returns a MAAP client instance.
    """
    try:
        logging.info(f"Initializing MAAP client for host: {maap_host_url}")
        maap_client = MAAP(maap_host=maap_host_url)
        logging.info("MAAP client initialized successfully.")
        return maap_client
    except Exception as e:
        logging.error(f"Failed to initialize MAAP instance for host '{maap_host_url}': {e}", exc_info=True)
        raise RuntimeError(f"Could not initialize MAAP instance: {e}")


# HELPER FUNCTIONS
def cmss_logger(args, level, msg):
    endpoint = "log"
    url = f"{args.cmss_logger_host}/{endpoint}"
    body = {"level": level, "msg_body": str(msg)}
    requests.post(url, json=body)

def cmss_product_available(args, details):
    endpoint = "product"
    url = f"{args.cmss_logger_host}/{endpoint}"
    requests.post(url, json=details)

def job_error_message(job: DPSJob) -> str:
    if isinstance(job.error_details, str):
        try:
            return json.loads(job.error_details)["message"]
        except (json.JSONDecodeError, KeyError):
            return job.error_details
    return job.response_code or "Unknown error"

def parse_s3_path(s3_path: str) -> tuple[str, str]:
    """
    Parse an S3 path into bucket and key components.
    Example: s3://bucket-name/path/to/file.nc -> ('bucket-name', 'path/to/file.nc')
    """
    if not s3_path.startswith("s3://"):
        raise ValueError(f"{s3_path} is not a valid s3 path")
    
    # Remove s3:// prefix and split on first /
    path_without_prefix = s3_path[5:]  # Remove 's3://'
    if '/' not in path_without_prefix:
        return path_without_prefix, ""
    
    bucket, key = path_without_prefix.split('/', 1)
    return bucket, key

def get_dps_output(jobs: list[DPSJob], file_ext: str, prefixes_only: bool=False) -> list[str]:
    s3 = boto3.resource('s3')
    output = set()
    job_outputs = [next((path for path in j.retrieve_result() if path.startswith("s3")), None) for j in jobs]
    
    for job_output in job_outputs:
        if job_output is None:
            continue
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

def get_zarr_output_name(input_s3_url: str) -> str:
    """Generate zarr output name from input S3 URL"""
    filename = os.path.basename(input_s3_url)
    base_name = os.path.splitext(filename)[0]
    return f"{base_name}.zarr"

def get_job_id():
    if os.path.exists("_job.json"):
        with open("_job.json", 'r') as fr:
            job_info = json.load(fr)
            job_id = job_info.get("job_id", "")
            return job_id
    return ""

@backoff.on_exception(backoff.expo, Exception, max_value=64, max_time=172800)
def wait_for_completion(job: DPSJob):
    job.retrieve_status()
    if job.status.lower() in ["deleted", "accepted", "running"]:
        logger.debug('Current Status is {}. Backing off.'.format(job.status))
        raise RuntimeError
    return job


def upload_to_s3(s3_client, local_file_path: str, bucket_name: str, s3_prefix: str):
    """
    Uploads the specified local file to an S3 bucket.
    """
    if not os.path.exists(local_file_path):
        raise FileNotFoundError(f"Local file '{local_file_path}' intended for upload does not exist.")

    file_name = os.path.basename(local_file_path)

    # Construct the S3 object key
    s3_key_parts = []
    if s3_prefix:
        s3_key_parts.append(s3_prefix.strip('/'))
    s3_key_parts.append(file_name)

    s3_key = "/".join(part for part in s3_key_parts if part)

    logging.info(f"Starting upload of '{local_file_path}' to S3 bucket '{bucket_name}' with key '{s3_key}'.")

    try:
        s3_client.upload_file(local_file_path, bucket_name, s3_key)
        logging.info(f"Successfully uploaded '{file_name}' to 's3://{bucket_name}/{s3_key}'.")
    except ClientError as e:
        logging.error(f"Failed to upload '{local_file_path}' to S3 (s3://{bucket_name}/{s3_key}): {e}", exc_info=True)
        raise UploadError(f"S3 upload of {local_file_path} failed: {e}")


# ETL FUNCTIONS
async def convert_to_zarr(args, maap, input_s3_url):
    """Convert NetCDF file to Zarr using CZDT_NETCDF_TO_ZARR job"""
    msg = f"Started ZARR conversion for {input_s3_url}"
    print(msg)   
    cmss_logger(args, "INFO", msg)
    
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
        error_msg = job_error_message(job)
        raise RuntimeError(f"Failed to submit CZDT_NETCDF_TO_ZARR job: {error_msg}")
    
    wait_for_completion(job)
    
    return job

async def convert_to_concatenated_zarr(args, maap, convert_to_zarr_result):
    """Convert individual zarr to concatenated zarr"""
    msg = f"Started ZARR concatenation for job {convert_to_zarr_result.id}"
    maap_username = maap.profile.account_info()['username']
    print(msg)   
    cmss_logger(args, "INFO", msg)

    # Create zarr manifest
    s3_client = boto3.client('s3')
    s3_zarr_urls = get_dps_output([convert_to_zarr_result], ".zarr", True)
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
        error_msg = job_error_message(job)
        raise RuntimeError(f"Failed to submit CZDT_ZARR_CONCAT job: {error_msg}")
    
    wait_for_completion(job)
    
    return job

async def convert_zarr_to_cog(args, maap, convert_to_concatenated_zarr_result):
    """Convert Zarr to Cloud Optimized GeoTIFF"""
    zarr_files = get_dps_output([convert_to_concatenated_zarr_result], ".zarr", True)
    msg = f"Converting {len(zarr_files)} ZARR(s) to COG(s)"
    print(msg)   
    cmss_logger(args, "INFO", msg)

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
            error_msg = job_error_message(job)
            print(f"Failed to submit CZDT_ZARR_TO_COG job: {error_msg}", file=sys.stderr)
    
    if not jobs:
        raise RuntimeError("No CZDT_ZARR_TO_COG jobs were successfully submitted")
    
    # Wait for all jobs to complete
    for job in jobs:
        wait_for_completion(job)
    
    return jobs

def catalog(args, maap, convert_zarr_to_cog_result, convert_to_zarr_result):
    """Catalog the processed COG files to STAC"""
    tif_files = get_dps_output(convert_zarr_to_cog_result, ".tif")
    zarr_files = get_dps_output([convert_to_zarr_result], ".zarr", True)
    czdt_token = maap.secrets.get_secret(f"{args.titiler_token_secret_name}")

    msg = f"Cataloging {len(tif_files)} COG file(s) to STAC"
    print(msg)
    cmss_logger(args, "INFO", msg)

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
        "job_id": get_job_id()
    }
    cmss_product_available(args, product_details)
    cmss_logger(args, "INFO", f"Product available for collection {args.collection_id}")


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
        maap = get_maap_instance(maap_host_to_use)

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