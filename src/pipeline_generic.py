import argparse
import os
import sys
import logging
import boto3
import json
from maap.maap import MAAP
from maap.dps.dps_job import DPSJob
import asyncio
import backoff
import create_stac_items
import requests
from os.path import basename, join
from datetime import datetime, timezone

# Configure logging: DEBUG for this module, INFO for dependencies
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

class UploadError(Exception):
    """Custom exception for errors encountered during S3 upload."""
    pass

class GranuleNotFoundError(Exception):
    """Custom exception for when a granule is not found via MAAP search."""
    pass

class DownloadError(Exception):
    """Custom exception for errors encountered during granule download."""
    pass

def parse_arguments():
    """
    Defines and parses command-line arguments for the generic pipeline script.
    """
    logger.debug("Starting argument parsing")
    parser = argparse.ArgumentParser(
        description="Generic CZDT pipeline that processes data from DAAC, S3 NetCDF, or S3 Zarr inputs.")
    
    # Input source options (mutually exclusive)
    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument("--granule-id", 
                           help="DAAC Granule ID for download from DAAC archives")
    input_group.add_argument("--input-s3", 
                           help="S3 URL of input file (NetCDF or Zarr)")
    
    # Required for DAAC input
    parser.add_argument("--collection-id", 
                        help="Collection Concept ID (required for DAAC input)")
    
    # Common required arguments
    parser.add_argument("--s3-bucket", required=True,
                        help="Target S3 bucket for uploading processed files")
    parser.add_argument("--s3-prefix", default="",
                        help="Optional S3 prefix (folder path) within the bucket")
    parser.add_argument("--role-arn", required=True,
                        help="AWS IAM Role ARN to assume for S3 upload")
    parser.add_argument("--cmss-logger-host", required=True,
                        help="Host for logging pipeline messages")
    parser.add_argument("--mmgis-host", required=True,
                        help="Host for cataloging STAC items")
    parser.add_argument("--titiler-token-secret-name", required=True,
                        help="MAAP secret name for MMGIS host token")
    parser.add_argument("--job-queue", required=True,
                        help="Queue name for running pipeline jobs")
    parser.add_argument("--zarr-config-url", required=True,
                        help="S3 URL of the ZARR config file")
    
    # Optional processing parameters
    parser.add_argument("--variables", default="*",
                        help="Variables to extract from NetCDF (default: all variables '*')")
    parser.add_argument("--enable-concat", action="store_true",
                        help="Enable Zarr concatenation step (default: skip)")
    parser.add_argument("--local-download-path", default="output",
                        help="Local directory for temporary downloads")
    parser.add_argument("--maap-host", default="api.maap-project.org",
                        help="MAAP API host")
    
    args = parser.parse_args()
    logger.debug(f"Parsed arguments: {vars(args)}")
    return args

def validate_arguments(args):
    """Validate argument combinations"""
    logger.debug("Starting argument validation")
    if args.granule_id and not args.collection_id:
        logger.debug("Validation failed: collection-id required for granule-id")
        raise ValueError("--collection-id is required when using --granule-id")
    
    if args.input_s3:
        logger.debug(f"Validating S3 URL: {args.input_s3}")
        if not args.input_s3.startswith('s3://'):
            logger.debug(f"Validation failed: Invalid S3 URL format: {args.input_s3}")
            raise ValueError(f"Invalid S3 URL: {args.input_s3}")
    
    logger.debug("Argument validation completed successfully")

def detect_input_type(args):
    """Detect the type of input and processing needed"""
    logger.debug("Detecting input type")
    if args.granule_id and len(args.granule_id) > 0 and args.granule_id != "none":
        logger.debug(f"Detected DAAC input type for granule: {args.granule_id}")
        return "daac"
    elif args.input_s3:
        logger.debug(f"Analyzing S3 input URL: {args.input_s3}")
        if args.input_s3.endswith(('.nc', '.nc4')):
            logger.debug("Detected S3 NetCDF input type")
            return "s3_netcdf"
        elif args.input_s3.endswith('.zarr') or args.input_s3.endswith('.zarr/'):
            logger.debug("Detected S3 Zarr input type")
            return "s3_zarr"
        else:
            logger.debug(f"Unsupported file type detected: {args.input_s3}")
            raise ValueError(f"Unsupported file type in S3 URL: {args.input_s3}")
    
    logger.debug("No valid input type could be determined")

def get_maap_instance(maap_host_url: str) -> MAAP:
    """Initialize and return a MAAP client instance."""
    logger.debug(f"Starting MAAP client initialization for host: {maap_host_url}")
    try:
        logging.info(f"Initializing MAAP client for host: {maap_host_url}")
        maap_client = MAAP(maap_host=maap_host_url)
        logging.info("MAAP client initialized successfully.")
        logger.debug("MAAP client object created and ready for use")
        return maap_client
    except Exception as e:
        logger.debug(f"Exception during MAAP initialization: {type(e).__name__}: {e}")
        logging.error(f"Failed to initialize MAAP instance for host '{maap_host_url}': {e}", exc_info=True)
        raise RuntimeError(f"Could not initialize MAAP instance: {e}")

# Helper functions
def cmss_logger(args, level, msg):
    logger.debug(f"Sending CMSS log message: level={level}, msg={msg}")
    endpoint = "log"
    url = f"{args.cmss_logger_host}/{endpoint}"
    body = {"level": level, "msg_body": str(msg)}
    logger.debug(f"CMSS logger URL: {url}, body: {body}")
    response = requests.post(url, json=body)
    logger.debug(f"CMSS logger response status: {response.status_code}")

def cmss_product_available(args, details):
    logger.debug(f"Sending CMSS product availability notification: {details}")
    endpoint = "product"
    url = f"{args.cmss_logger_host}/{endpoint}"
    logger.debug(f"CMSS product URL: {url}")
    response = requests.post(url, json=details)
    logger.debug(f"CMSS product response status: {response.status_code}")

def job_error_message(job: DPSJob) -> str:
    logger.debug(f"Extracting error message from job: {job.id if hasattr(job, 'id') else 'unknown'}")
    if isinstance(job.error_details, str):
        logger.debug(f"Job error details (string): {job.error_details}")
        try:
            parsed_error = json.loads(job.error_details)["message"]
            logger.debug(f"Parsed error message: {parsed_error}")
            return parsed_error
        except (json.JSONDecodeError, KeyError) as e:
            logger.debug(f"Failed to parse error details as JSON: {e}")
            return job.error_details
    error_msg = job.response_code or "Unknown error"
    logger.debug(f"Returning error message: {error_msg}")
    return error_msg

@backoff.on_exception(backoff.expo, Exception, max_value=64, max_time=172800)
async def wait_for_completion(job: DPSJob):
    await asyncio.to_thread(job.retrieve_status)
    if job.status.lower() in ["deleted", "accepted", "running"]:
        logger.debug('Current Status is {}. Backing off.'.format(job.status))
        raise RuntimeError
    return job

def parse_s3_path(s3_path: str) -> tuple[str, str]:
    """Parse an S3 path into bucket and key components."""
    logger.debug(f"Parsing S3 path: {s3_path}")
    if not s3_path.startswith("s3://"):
        logger.debug(f"Invalid S3 path format: {s3_path}")
        raise ValueError(f"{s3_path} is not a valid s3 path")
    
    path_without_prefix = s3_path[5:]
    logger.debug(f"Path without s3:// prefix: {path_without_prefix}")
    
    # Handle both formats: s3://bucket/key and s3://hostname:port/bucket/key
    if path_without_prefix.startswith(('s3-', 's3.')):
        # Format: s3://s3-region.amazonaws.com:port/bucket/key
        logger.debug("Detected hostname format S3 path")
        if '/' not in path_without_prefix:
            logger.debug("No bucket/key found in hostname format")
            raise ValueError(f"Invalid S3 path format: {s3_path}")
        
        # Split at first slash after hostname
        hostname_part, remaining_path = path_without_prefix.split('/', 1)
        logger.debug(f"Hostname part: {hostname_part}, remaining path: {remaining_path}")
        
        if '/' not in remaining_path:
            # Only bucket, no key
            logger.debug(f"No key found, returning bucket only: {remaining_path}")
            return remaining_path, ""
        
        bucket, key = remaining_path.split('/', 1)
        logger.debug(f"Parsed S3 path (hostname format) - bucket: {bucket}, key: {key}")
        return bucket, key
    else:
        # Standard format: s3://bucket/key
        logger.debug("Detected standard format S3 path")
        if '/' not in path_without_prefix:
            logger.debug(f"No key found, returning bucket only: {path_without_prefix}")
            return path_without_prefix, ""
        
        bucket, key = path_without_prefix.split('/', 1)
        logger.debug(f"Parsed S3 path (standard format) - bucket: {bucket}, key: {key}")
        return bucket, key

def get_dps_output(jobs: list[DPSJob], file_ext: str, prefixes_only: bool = False) -> list[str]:
    logger.debug(f"Getting DPS output for {len(jobs)} jobs, file extension: {file_ext}, prefixes_only: {prefixes_only}")
    s3 = boto3.resource('s3')
    output = set()
    job_outputs = [next((path for path in j.retrieve_result() if path.startswith("s3")), None) for j in jobs]
    logger.debug(f"Job outputs: {job_outputs}")
    
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

def get_job_id():
    logger.debug("Attempting to retrieve job ID from _job.json")
    if os.path.exists("_job.json"):
        logger.debug("_job.json file found, reading job info")
        with open("_job.json", 'r') as fr:
            job_info = json.load(fr)
            job_id = job_info.get("job_id", "")
            logger.debug(f"Retrieved job ID: {job_id}")
            return job_id
    logger.debug("_job.json file not found, returning empty job ID")
    return ""

# DAAC processing functions
async def stage_from_daac(args, maap):
    """Stage granule from DAAC"""
    logger.debug(f"Starting DAAC staging for granule: {args.granule_id}")
    msg = f"Staging granule {args.granule_id} from DAAC"
    print(msg)
    cmss_logger(args, "INFO", msg)
    
    job_params = {
        "identifier": f"Generic-Pipeline_stage_{args.granule_id[-10:]}",
        "algo_id": "czdt-iss-ingest",
        "version": "main",
        "queue": args.job_queue,
        "granule_id": args.granule_id,
        "collection_id": args.collection_id,
        "s3_bucket": args.s3_bucket,
        "s3_prefix": args.s3_prefix,
        "role_arn": args.role_arn
    }
    logger.debug(f"Submitting DAAC staging job with parameters: {job_params}")
    staging_job = maap.submitJob(**job_params)
    
    if not staging_job.id:
        error_msg = job_error_message(staging_job)
        logger.debug(f"DAAC staging job submission failed: {error_msg}")
        raise RuntimeError(f"Failed to submit DAAC staging job: {error_msg}")
    
    logger.debug(f"DAAC staging job submitted successfully with ID: {staging_job.id}")
    logger.debug("Waiting for DAAC staging job to complete")
    await wait_for_completion(staging_job)
    logger.debug("DAAC staging job completed")
    return staging_job

# NetCDF processing functions
async def convert_netcdf_to_zarr(args, maap, input_source):
    """Convert NetCDF to Zarr"""
    logger.debug(f"Starting NetCDF to Zarr conversion for input: {input_source}")
    if isinstance(input_source, str):  # S3 URL
        logger.debug("Input source is S3 URL")
        input_s3_url = input_source
        identifier_suffix = input_s3_url[-10:]
        logger.debug(f"Using S3 URL: {input_s3_url}, identifier suffix: {identifier_suffix}")
    else:  # DPS Job result
        logger.debug("Input source is DPS Job result, searching for NetCDF files")
        nc_files = get_dps_output([input_source], ".nc4")
        if not nc_files:
            logger.debug("No .nc4 files found, searching for .nc files")
            nc_files = get_dps_output([input_source], ".nc")
        if not nc_files:
            logger.debug("No NetCDF files found in staging job output")
            raise RuntimeError("No NetCDF files found in staging job output")
        input_s3_url = nc_files[0]  # Process first file
        identifier_suffix = input_source.id[-7:]
        logger.debug(f"Found NetCDF files: {nc_files}, using: {input_s3_url}, identifier suffix: {identifier_suffix}")
    
    msg = f"Converting NetCDF to Zarr: {input_s3_url}"
    print(msg)
    cmss_logger(args, "INFO", msg)
    
    # Determine file pattern
    filename = os.path.basename(input_s3_url)
    logger.debug(f"Processing filename: {filename}")
    if filename.endswith('.nc4'):
        pattern = "*.nc4"
        logger.debug("Detected .nc4 file pattern")
    elif filename.endswith('.nc'):
        pattern = "*.nc"
        logger.debug("Detected .nc file pattern")
    else:
        logger.debug(f"Unsupported file extension in filename: {filename}")
        raise ValueError(f"Unsupported file extension: {filename}")
    
    # Generate output name
    base_name = os.path.splitext(filename)[0]
    output_zarr_name = f"{base_name}.zarr"
    logger.debug(f"Generated output Zarr name: {output_zarr_name}")
    
    job_params = {
        "identifier": f"Generic-Pipeline_netcdf_2_zarr_{identifier_suffix}",
        "algo_id": "CZDT_NETCDF_TO_ZARR",
        "version": "master",
        "queue": args.job_queue,
        "input_s3": input_s3_url,
        "zarr_access": "stage",
        "config": args.zarr_config_url,
        "config_path": join('input', basename(args.zarr_config_url)),
        "pattern": pattern,
        "output": output_zarr_name,
        "variables": args.variables
    }
    logger.debug(f"Submitting NetCDF to Zarr job with parameters: {job_params}")
    job = maap.submitJob(**job_params)
    
    if not job.id:
        error_msg = job_error_message(job)
        logger.debug(f"NetCDF to Zarr job submission failed: {error_msg}")
        raise RuntimeError(f"Failed to submit NetCDF to Zarr job: {error_msg}")
    
    logger.debug(f"NetCDF to Zarr job submitted successfully with ID: {job.id}")
    logger.debug("Waiting for NetCDF to Zarr job to complete")
    await wait_for_completion(job)
    logger.debug("NetCDF to Zarr job completed")
    return job

async def concatenate_zarr(args, maap, zarr_job):
    """Concatenate Zarr files (optional step)"""
    logger.debug(f"Starting Zarr concatenation for job: {zarr_job.id}")
    msg = f"Concatenating Zarr files from job {zarr_job.id}"
    print(msg)
    cmss_logger(args, "INFO", msg)
    
    maap_username = maap.profile.account_info()['username']
    logger.debug(f"MAAP username: {maap_username}")
    s3_client = boto3.client('s3')
    s3_zarr_urls = get_dps_output([zarr_job], ".zarr", True)
    logger.debug(f"Found Zarr URLs for concatenation: {s3_zarr_urls}")
    
    # Create manifest
    os.makedirs("output", exist_ok=True)
    local_zarr_path = f"output/{zarr_job.id}.json"
    logger.debug(f"Creating Zarr manifest at: {local_zarr_path}")
    
    with open(local_zarr_path, 'w') as fp:
        json.dump(s3_zarr_urls, fp, indent=2)
    logger.debug(f"Zarr manifest created with {len(s3_zarr_urls)} URLs")
    
    # Upload manifest
    manifest_key = f"{maap_username}/zarr_concat_manifests/{zarr_job.id}.json"
    logger.debug(f"Uploading manifest to S3: maap-ops-workspace/{manifest_key}")
    s3_client.upload_file(
        local_zarr_path,
        "maap-ops-workspace",
        manifest_key
    )
    logger.debug("Manifest uploaded successfully")
    
    job_params = {
        "identifier": f"Generic-Pipeline_zarr_concat_{zarr_job.id[-7:]}",
        "algo_id": "CZDT_ZARR_CONCAT",
        "version": "master",
        "queue": args.job_queue,
        "config": "s3://maap-ops-workspace/rileykk/sample_merra2_cfg.yaml",
        "config_path": 'input/sample_merra2_cfg.yaml',
        "zarr_manifest": f"s3://maap-ops-workspace/{maap_username}/zarr_concat_manifests/{zarr_job.id}.json",
        "zarr_access": "mount",
        "duration": "P5D",
        "output": f"concat.{zarr_job.id}.zarr"
    }
    logger.debug(f"Submitting Zarr concatenation job with parameters: {job_params}")
    job = maap.submitJob(**job_params)
    
    if not job.id:
        error_msg = job_error_message(job)
        logger.debug(f"Zarr concatenation job submission failed: {error_msg}")
        raise RuntimeError(f"Failed to submit Zarr concatenation job: {error_msg}")
    
    logger.debug(f"Zarr concatenation job submitted successfully with ID: {job.id}")
    logger.debug("Waiting for Zarr concatenation job to complete")
    await wait_for_completion(job)
    logger.debug("Zarr concatenation job completed")
    return job

async def convert_zarr_to_cog(args, maap, zarr_source):
    """Convert Zarr to COG"""
    logger.debug(f"Starting Zarr to COG conversion for source: {zarr_source}")
    if isinstance(zarr_source, str):  # Direct S3 Zarr URL
        logger.debug("Zarr source is S3 URL")
        zarr_files = [zarr_source.rstrip('/')]
        identifier_suffix = zarr_source[-10:]
        logger.debug(f"Using S3 Zarr URL: {zarr_files[0]}, identifier suffix: {identifier_suffix}")
    else:  # DPS Job result
        logger.debug("Zarr source is DPS Job result, searching for Zarr files")
        zarr_files = get_dps_output([zarr_source], ".zarr", True)
        identifier_suffix = zarr_source.id[-7:]
        logger.debug(f"Found Zarr files: {zarr_files}, identifier suffix: {identifier_suffix}")
    
    if not zarr_files:
        logger.debug("No Zarr files found for COG conversion")
        raise RuntimeError("No Zarr files found for COG conversion")
    
    msg = f"Converting {len(zarr_files)} Zarr file(s) to COG"
    print(msg)
    cmss_logger(args, "INFO", msg)
    logger.debug(f"Processing {len(zarr_files)} Zarr files: {zarr_files}")
    
    jobs = []
    for i, zarr_file in enumerate(zarr_files):
        output_name = zarr_file.split("/")[-1].replace(".zarr", "")
        logger.debug(f"Processing Zarr file {i+1}/{len(zarr_files)}: {zarr_file}, output name: {output_name}")
        
        job_params = {
            "identifier": f"Generic-Pipeline_zarr_2_cog_{identifier_suffix}",
            "algo_id": "CZDT_ZARR_TO_COG",
            "version": "master",
            "queue": args.job_queue,
            "zarr": f"{zarr_file}/",
            "zarr_access": "stage",
            "time": "time",
            "latitude": "lat",
            "longitude": "lon",
            "output_name": output_name
        }
        logger.debug(f"Submitting Zarr to COG job with parameters: {job_params}")
        job = maap.submitJob(**job_params)
        
        if job.id:
            logger.debug(f"Zarr to COG job submitted successfully with ID: {job.id}")
            jobs.append(job)
        else:
            error_msg = job_error_message(job)
            logger.debug(f"Failed to submit Zarr to COG job: {error_msg}")
            print(f"Failed to submit Zarr to COG job: {error_msg}", file=sys.stderr)
    
    if not jobs:
        logger.debug("No Zarr to COG jobs were successfully submitted")
        raise RuntimeError("No Zarr to COG jobs were successfully submitted")
    
    # Wait for all jobs to complete
    job_ids = [job.id for job in jobs]
    logger.debug(f"Waiting for {len(jobs)} Zarr to COG jobs to complete: {job_ids}")
    for job in jobs:
        await wait_for_completion(job)
    logger.debug("All Zarr to COG jobs completed")
    
    return jobs

def catalog_products(args, maap, cog_jobs, zarr_job):
    """Catalog processed products to STAC"""
    logger.debug(f"Starting product cataloging for {len(cog_jobs)} COG jobs")
    tif_files = get_dps_output(cog_jobs, ".tif")
    logger.debug(f"Found {len(tif_files)} TIF files for cataloging: {tif_files}")
    czdt_token = maap.secrets.get_secret(args.titiler_token_secret_name)
    logger.debug(f"Retrieved CZDT token from secret: {args.titiler_token_secret_name}")
    
    msg = f"Cataloging {len(tif_files)} COG file(s) to STAC"
    print(msg)
    cmss_logger(args, "INFO", msg)

    utc_now = datetime.now(timezone.utc)
    # Format as ISO 8601 with 'Z' for UTC
    formatted_utc_time = utc_now.strftime("%Y-%m-%dT%H:%M:%SZ")

    stac_records = []
    
    for i, tif_file in enumerate(tif_files):
        logger.debug(f"Cataloging TIF file {i+1}/{len(tif_files)}: {tif_file}")
        stac_record = create_stac_items.create_stac_items(
            mmgis_url=args.mmgis_host,
            mmgis_token=czdt_token,
            collection_id=args.collection_id,
            file_or_folder_path=tif_file,
            starttime=formatted_utc_time
        )
        stac_records += stac_record
        logger.debug(f"STAC item : {stac_record}")

    product_uris = tif_files

    if zarr_job is not None:
        s3_zarr_urls = get_dps_output([zarr_job], ".zarr", True)
        logger.debug(f"Found {len(s3_zarr_urls)} ZARR files for cataloging: {s3_zarr_urls}")
        product_uris += s3_zarr_urls
    
    product_details = {
        "collection": args.collection_id,
        "ogc": stac_records,
        "uris": product_uris,
        "job_id": get_job_id()
    }
    logger.debug(f"Product details for notification: {product_details}")
    cmss_product_available(args, product_details)
    cmss_logger(args, "INFO", f"Product available for collection {args.collection_id}")
    logger.debug("Product cataloging completed successfully")

async def main():
    """Main function orchestrating the generic pipeline"""
    logger.debug("Starting generic pipeline main function")
    args = parse_arguments()
    
    try:
        validate_arguments(args)
        input_type = detect_input_type(args)
        logger.debug(f"Detected input type: {input_type}")
        
        maap_host_to_use = os.environ.get('MAAP_API_HOST', args.maap_host)
        logger.debug(f"Using MAAP host: {maap_host_to_use}")
        maap = get_maap_instance(maap_host_to_use)
        
        logging.info(f"Processing {input_type} input")
        
        if input_type == "daac":
            # DAAC → NetCDF → Zarr → (optional concat) → COG → Catalog
            logger.debug("Starting DAAC pipeline: stage → netcdf2zarr → concat? → zarr2cog → catalog")
            staged_job = await stage_from_daac(args, maap)
            zarr_job = await convert_netcdf_to_zarr(args, maap, staged_job)
            
            if args.enable_concat:
                logger.debug("Concatenation enabled, performing Zarr concatenation")
                zarr_job = await concatenate_zarr(args, maap, zarr_job)
            else:
                logger.debug("Concatenation disabled, skipping concatenation step")
            
            cog_jobs = await convert_zarr_to_cog(args, maap, zarr_job)
            catalog_products(args, maap, cog_jobs, zarr_job)
            logger.debug("DAAC pipeline completed successfully")
            
        elif input_type == "s3_netcdf":
            # S3 NetCDF → Zarr → (optional concat) → COG → Catalog
            logger.debug("Starting S3 NetCDF pipeline: netcdf2zarr → concat? → zarr2cog → catalog")
            zarr_job = await convert_netcdf_to_zarr(args, maap, args.input_s3)
            
            if args.enable_concat:
                logger.debug("Concatenation enabled, performing Zarr concatenation")
                zarr_job = await concatenate_zarr(args, maap, zarr_job)
            else:
                logger.debug("Concatenation disabled, skipping concatenation step")
            
            cog_jobs = await convert_zarr_to_cog(args, maap, zarr_job)
            catalog_products(args, maap, cog_jobs, zarr_job)
            logger.debug("S3 NetCDF pipeline completed successfully")
            
        elif input_type == "s3_zarr":
            # S3 Zarr → COG → Catalog (skip NetCDF conversion and concat)
            logger.debug("Starting S3 Zarr pipeline: zarr2cog → catalog")
            cog_jobs = await convert_zarr_to_cog(args, maap, args.input_s3)
            catalog_products(args, maap, cog_jobs, None)
            logger.debug("S3 Zarr pipeline completed successfully")
        
        logging.info("Generic pipeline completed successfully!")
        logger.debug("All pipeline steps completed without errors")
        
    except ValueError as e:
        logger.debug(f"ValueError caught: {e}")
        logging.error(f"TERMINATED: Invalid argument or value. Details: {e}")
        sys.exit(6)
    except RuntimeError as e:
        logger.debug(f"RuntimeError caught: {e}")
        logging.error(f"TERMINATED: Runtime error. Details: {e}")
        sys.exit(7)
    except Exception as e:
        logger.debug(f"Unexpected exception caught: {type(e).__name__}: {e}")
        logging.error(f"TERMINATED: An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    logger.debug("Script started as main module")
    asyncio.run(main())