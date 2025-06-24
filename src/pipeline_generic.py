import argparse
import os
import sys
import logging
import boto3
import json
from maap.maap import MAAP
from maap.dps.dps_job import DPSJob
import asyncio
from async_job import AsyncJob, JobFailedException
import create_stac_items
import requests
from os.path import basename, join

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

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
    
    return parser.parse_args()

def validate_arguments(args):
    """Validate argument combinations"""
    if args.granule_id and not args.collection_id:
        raise ValueError("--collection-id is required when using --granule-id")
    
    if args.input_s3:
        if not args.input_s3.startswith('s3://'):
            raise ValueError(f"Invalid S3 URL: {args.input_s3}")

def detect_input_type(args):
    """Detect the type of input and processing needed"""
    if args.granule_id:
        return "daac"
    elif args.input_s3:
        if args.input_s3.endswith(('.nc', '.nc4')):
            return "s3_netcdf"
        elif args.input_s3.endswith('.zarr') or args.input_s3.endswith('.zarr/'):
            return "s3_zarr"
        else:
            raise ValueError(f"Unsupported file type in S3 URL: {args.input_s3}")

def get_maap_instance(maap_host_url: str) -> MAAP:
    """Initialize and return a MAAP client instance."""
    try:
        logging.info(f"Initializing MAAP client for host: {maap_host_url}")
        maap_client = MAAP(maap_host=maap_host_url)
        logging.info("MAAP client initialized successfully.")
        return maap_client
    except Exception as e:
        logging.error(f"Failed to initialize MAAP instance for host '{maap_host_url}': {e}", exc_info=True)
        raise RuntimeError(f"Could not initialize MAAP instance: {e}")

# Helper functions
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
    """Parse an S3 path into bucket and key components."""
    if not s3_path.startswith("s3://"):
        raise ValueError(f"{s3_path} is not a valid s3 path")
    
    path_without_prefix = s3_path[5:]
    if '/' not in path_without_prefix:
        return path_without_prefix, ""
    
    bucket, key = path_without_prefix.split('/', 1)
    return bucket, key

def get_dps_output(jobs: list[DPSJob], file_ext: str, prefixes_only: bool = False) -> list[str]:
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

def get_job_id():
    if os.path.exists("_job.json"):
        with open("_job.json", 'r') as fr:
            job_info = json.load(fr)
            return job_info.get("job_id", "")
    return ""

# DAAC processing functions
async def stage_from_daac(args, maap):
    """Stage granule from DAAC"""
    msg = f"Staging granule {args.granule_id} from DAAC"
    print(msg)
    cmss_logger(args, "INFO", msg)
    
    staging_job = maap.submitJob(
        identifier=f"Generic-Pipeline_stage_{args.granule_id[-10:]}",
        algo_id="czdt-iss-ingest",
        version="main",
        queue=args.job_queue,
        granule_id=args.granule_id,
        collection_id=args.collection_id,
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
        role_arn=args.role_arn
    )
    
    if not staging_job.id:
        error_msg = job_error_message(staging_job)
        raise RuntimeError(f"Failed to submit DAAC staging job: {error_msg}")
    
    aj = AsyncJob(staging_job.id)
    await aj.get_job_status()
    return staging_job

# NetCDF processing functions
async def convert_netcdf_to_zarr(args, maap, input_source):
    """Convert NetCDF to Zarr"""
    if isinstance(input_source, str):  # S3 URL
        input_s3_url = input_source
        identifier_suffix = input_s3_url[-10:]
    else:  # DPS Job result
        nc_files = get_dps_output([input_source], ".nc4")
        if not nc_files:
            nc_files = get_dps_output([input_source], ".nc")
        if not nc_files:
            raise RuntimeError("No NetCDF files found in staging job output")
        input_s3_url = nc_files[0]  # Process first file
        identifier_suffix = input_source.id[-7:]
    
    msg = f"Converting NetCDF to Zarr: {input_s3_url}"
    print(msg)
    cmss_logger(args, "INFO", msg)
    
    # Determine file pattern
    filename = os.path.basename(input_s3_url)
    if filename.endswith('.nc4'):
        pattern = "*.nc4"
    elif filename.endswith('.nc'):
        pattern = "*.nc"
    else:
        raise ValueError(f"Unsupported file extension: {filename}")
    
    # Generate output name
    base_name = os.path.splitext(filename)[0]
    output_zarr_name = f"{base_name}.zarr"
    
    job = maap.submitJob(
        identifier=f"Generic-Pipeline_netcdf_2_zarr_{identifier_suffix}",
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
        raise RuntimeError(f"Failed to submit NetCDF to Zarr job: {error_msg}")
    
    aj = AsyncJob(job.id)
    await aj.get_job_status()
    return job

async def concatenate_zarr(args, maap, zarr_job):
    """Concatenate Zarr files (optional step)"""
    msg = f"Concatenating Zarr files from job {zarr_job.id}"
    print(msg)
    cmss_logger(args, "INFO", msg)
    
    maap_username = maap.profile.account_info()['username']
    s3_client = boto3.client('s3')
    s3_zarr_urls = get_dps_output([zarr_job], ".zarr", True)
    
    # Create manifest
    os.makedirs("output", exist_ok=True)
    local_zarr_path = f"output/{zarr_job.id}.json"
    
    with open(local_zarr_path, 'w') as fp:
        json.dump(s3_zarr_urls, fp, indent=2)
    
    # Upload manifest
    s3_client.upload_file(
        local_zarr_path,
        "maap-ops-workspace",
        f"{maap_username}/zarr_concat_manifests/{zarr_job.id}.json"
    )
    
    job = maap.submitJob(
        identifier=f"Generic-Pipeline_zarr_concat_{zarr_job.id[-7:]}",
        algo_id="CZDT_ZARR_CONCAT",
        version="master",
        queue=args.job_queue,
        config="s3://maap-ops-workspace/rileykk/sample_merra2_cfg.yaml",
        config_path='input/sample_merra2_cfg.yaml',
        zarr_manifest=f"s3://maap-ops-workspace/{maap_username}/zarr_concat_manifests/{zarr_job.id}.json",
        zarr_access="mount",
        duration="P5D",
        output=f"concat.{zarr_job.id}.zarr"
    )
    
    if not job.id:
        error_msg = job_error_message(job)
        raise RuntimeError(f"Failed to submit Zarr concatenation job: {error_msg}")
    
    aj = AsyncJob(job.id)
    await aj.get_job_status()
    return job

async def convert_zarr_to_cog(args, maap, zarr_source):
    """Convert Zarr to COG"""
    if isinstance(zarr_source, str):  # Direct S3 Zarr URL
        zarr_files = [zarr_source.rstrip('/')]
        identifier_suffix = zarr_source[-10:]
    else:  # DPS Job result
        zarr_files = get_dps_output([zarr_source], ".zarr", True)
        identifier_suffix = zarr_source.id[-7:]
    
    if not zarr_files:
        raise RuntimeError("No Zarr files found for COG conversion")
    
    msg = f"Converting {len(zarr_files)} Zarr file(s) to COG"
    print(msg)
    cmss_logger(args, "INFO", msg)
    
    jobs = []
    for zarr_file in zarr_files:
        output_name = zarr_file.split("/")[-1].replace(".zarr", "")
        
        job = maap.submitJob(
            identifier=f"Generic-Pipeline_zarr_2_cog_{identifier_suffix}",
            algo_id="CZDT_ZARR_TO_COG",
            version="master",
            queue=args.job_queue,
            zarr=f"{zarr_file}/",
            zarr_access="stage",
            time="time",
            latitude="lat",
            longitude="lon",
            output_name=output_name
        )
        
        if job.id:
            jobs.append(job)
        else:
            error_msg = job_error_message(job)
            print(f"Failed to submit Zarr to COG job: {error_msg}", file=sys.stderr)
    
    if not jobs:
        raise RuntimeError("No Zarr to COG jobs were successfully submitted")
    
    # Wait for all jobs to complete
    job_ids = [job.id for job in jobs]
    tasks = [AsyncJob(job_id).get_job_status() for job_id in job_ids]
    await asyncio.gather(*tasks)
    
    return jobs

def catalog_products(args, maap, cog_jobs):
    """Catalog processed products to STAC"""
    tif_files = get_dps_output(cog_jobs, ".tif")
    czdt_token = maap.secrets.get_secret(args.titiler_token_secret_name)
    
    msg = f"Cataloging {len(tif_files)} COG file(s) to STAC"
    print(msg)
    cmss_logger(args, "INFO", msg)
    
    for tif_file in tif_files:
        create_stac_items.create_stac_items(
            mmgis_url=args.mmgis_host,
            mmgis_token=czdt_token,
            collection_id=args.collection_id,
            file_or_folder_path=tif_file,
            starttime="2025-04-01T18:30:00Z"
        )
    
    product_details = {
        "collection": args.collection_id,
        "ogc": f"{args.mmgis_host}/stac/collections/{args.collection_id}/items",
        "uris": tif_files,
        "job_id": get_job_id()
    }
    cmss_product_available(args, product_details)
    cmss_logger(args, "INFO", f"Product available for collection {args.collection_id}")

async def main():
    """Main function orchestrating the generic pipeline"""
    args = parse_arguments()
    
    try:
        validate_arguments(args)
        input_type = detect_input_type(args)
        
        maap_host_to_use = os.environ.get('MAAP_API_HOST', args.maap_host)
        maap = get_maap_instance(maap_host_to_use)
        
        logging.info(f"Processing {input_type} input")
        
        if input_type == "daac":
            # DAAC → NetCDF → Zarr → (optional concat) → COG → Catalog
            staged_job = await stage_from_daac(args, maap)
            zarr_job = await convert_netcdf_to_zarr(args, maap, staged_job)
            
            if args.enable_concat:
                zarr_job = await concatenate_zarr(args, maap, zarr_job)
            
            cog_jobs = await convert_zarr_to_cog(args, maap, zarr_job)
            catalog_products(args, maap, cog_jobs)
            
        elif input_type == "s3_netcdf":
            # S3 NetCDF → Zarr → (optional concat) → COG → Catalog
            zarr_job = await convert_netcdf_to_zarr(args, maap, args.input_s3)
            
            if args.enable_concat:
                zarr_job = await concatenate_zarr(args, maap, zarr_job)
            
            cog_jobs = await convert_zarr_to_cog(args, maap, zarr_job)
            catalog_products(args, maap, cog_jobs)
            
        elif input_type == "s3_zarr":
            # S3 Zarr → COG → Catalog (skip NetCDF conversion and concat)
            cog_jobs = await convert_zarr_to_cog(args, maap, args.input_s3)
            catalog_products(args, maap, cog_jobs)
        
        logging.info("Generic pipeline completed successfully!")
        
    except JobFailedException as e:
        logging.error(f"TERMINATED: Pipeline job failed. Details: {e}")
        sys.exit(5)
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