import argparse
import os
import sys
import logging
import boto3
import json
import backoff
from maap.maap import MAAP
from maap.dps.dps_job import DPSJob
import asyncio
import create_stac_items
from os.path import basename, join
import fsspec
import pystac
from common_utils import (
    MaapUtils, LoggingUtils, ConfigUtils, AWSUtils
)

# Configure logging: DEBUG for this module, INFO for dependencies
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def parse_arguments():
    """
    Defines and parses command-line arguments for the generic pipeline script.
    """
    logger.debug("Starting argument parsing")
    parser = ConfigUtils.get_generic_argument_parser()
    args = parser.parse_args()
    logger.debug(f"Parsed arguments: {vars(args)}")
    return args

@backoff.on_exception(backoff.expo, Exception, max_value=64, max_time=172800)
async def wait_for_completion(job: DPSJob):
    await asyncio.to_thread(job.retrieve_status)
    if job.status.lower() in ["deleted", "accepted", "running"]:
        logger.debug('Current Status is {}. Backing off.'.format(job.status))
        raise RuntimeError
    return job

# DAAC processing functions
async def stage_from_daac(args, maap):
    """Stage granule from DAAC"""
    logger.debug(f"Starting DAAC staging for granule: {args.granule_id}")
    msg = f"Staging granule {args.granule_id} from DAAC"
    print(msg)
    LoggingUtils.cmss_logger(str(msg), args.cmss_logger_host)
    
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
        error_msg =  MaapUtils.job_error_message(staging_job)
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
        nc_files = MaapUtils.get_dps_output([input_source], ".nc4")

        if not nc_files:
            logger.debug("No .nc4 files found, searching for .nc files")
            nc_files = MaapUtils.get_dps_output([input_source], ".nc")
        if not nc_files:
            logger.debug("No NetCDF files found in staging job output")
            raise RuntimeError("No NetCDF files found in staging job output")
        input_s3_url = nc_files[0]  # Process first file
        identifier_suffix = input_source.id[-7:]
        logger.debug(f"Found NetCDF files: {nc_files}, using: {input_s3_url}, identifier suffix: {identifier_suffix}")
    
    msg = f"Converting NetCDF to Zarr: {input_s3_url}"
    print(msg)
    LoggingUtils.cmss_logger(str(msg), args.cmss_logger_host)
    
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
        error_msg = MaapUtils.job_error_message(job)
        logger.debug(f"NetCDF to Zarr job submission failed: {error_msg}")
        raise RuntimeError(f"Failed to submit NetCDF to Zarr job: {error_msg}")
    
    logger.debug(f"NetCDF to Zarr job submitted successfully with ID: {job.id}")
    logger.debug("Waiting for NetCDF to Zarr job to complete")
    await wait_for_completion(job)
    logger.debug("NetCDF to Zarr job completed")

    s3_zarr_urls = MaapUtils.get_dps_output([job], ".zarr", True)
    logger.debug(f"Uploading zarr file directories to S3: {s3_zarr_urls}")
    aws_region_for_s3 = os.environ.get('AWS_REGION', 'us-west-2')
    s3_client = AWSUtils.get_s3_client(role_arn=args.role_arn, aws_region=aws_region_for_s3)

    for s3_zarr_url in s3_zarr_urls:
        src_bucket_name, src_path = AWSUtils.parse_s3_path(s3_zarr_url)
        dst_bucket_name = args.s3_bucket
        dst_path = f"{args.s3_prefix}/{args.collection_id}/"

        AWSUtils.copy_s3_folder(src_bucket_name, f"{src_path}/", dst_bucket_name, dst_path, s3_client)
    
    if s3_zarr_urls:        
        product_details = {
            "collection": args.collection_id,
            "ogc": [],
            "uris": s3_zarr_urls,
            "job_id": MaapUtils.get_job_id()
        }
        logger.debug(f"Product details for notification: {product_details}")
        LoggingUtils.cmss_product_available(product_details, args.cmss_logger_host)
        LoggingUtils.cmss_logger(f"Product available for collection {args.collection_id}", args.cmss_logger_host)

    return job

async def concatenate_zarr(args, maap, zarr_job):
    """Concatenate Zarr files (optional step)"""
    logger.debug(f"Starting Zarr concatenation for job: {zarr_job.id}")
    msg = f"Concatenating Zarr files from job {zarr_job.id}"
    print(msg)
    LoggingUtils.cmss_logger(str(msg), args.cmss_logger_host)
    
    maap_username = maap.profile.account_info()['username']
    logger.debug(f"MAAP username: {maap_username}")
    s3_client = boto3.client('s3')
    s3_zarr_urls = MaapUtils.get_dps_output([zarr_job], ".zarr", True)
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
        error_msg = MaapUtils.job_error_message(job)
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
        zarr_files = MaapUtils.get_dps_output([zarr_source], ".zarr", True)
        identifier_suffix = zarr_source.id[-7:]
        logger.debug(f"Found Zarr files: {zarr_files}, identifier suffix: {identifier_suffix}")
    
    if not zarr_files:
        logger.debug("No Zarr files found for COG conversion")
        raise RuntimeError("No Zarr files found for COG conversion")
    
    msg = f"Converting {len(zarr_files)} Zarr file(s) to COG"
    print(msg)
    LoggingUtils.cmss_logger(str(msg), args.cmss_logger_host)
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
            error_msg = MaapUtils.job_error_message(job)
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
    stac_cat_file = MaapUtils.get_dps_output(cog_jobs, "catalog.json")[0]
    logger.debug(f"Found STAC file for cataloging: {stac_cat_file}.")
    czdt_token = maap.secrets.get_secret(args.titiler_token_secret_name)
    logger.debug(f"Retrieved CZDT token from secret: {args.titiler_token_secret_name}")

    bucket_name, catalog_path = AWSUtils.parse_s3_path(stac_cat_file)
    presigned_url = maap.aws.s3_signed_url(bucket_name, catalog_path)['url']

    with fsspec.open(presigned_url, "r") as f:
        data = json.load(f)

    with open("catalog.json", 'w') as fr: 
        fr.write(json.dumps(data, indent=4))

    catalog = pystac.Catalog.from_dict(data)
    catalog.set_self_href(presigned_url)
    catalog.make_all_asset_hrefs_absolute()

    coll_count = len(list(catalog.get_collections()))
    item_count = len(list(catalog.get_items(recursive=True)))

    msg = f"Updating STAC catalog with {item_count} items across {coll_count} collections."
    print(msg)
    LoggingUtils.cmss_logger(str(msg), args.cmss_logger_host)

    for root, collections, items in catalog.walk():
        if collections:
            for coll in collections:
                collection_data = coll.to_dict()
                collection_id = collection_data['id']
                collection_items = coll.get_items()

                upserted_collection = create_stac_items.upsert_collection(
                    mmgis_url=args.mmgis_host,
                    mmgis_token=czdt_token,
                    collection_id=collection_id,
                    collection=coll,
                    collection_items=collection_items
                )

                msg = f"STAC catalog update complete for collection {collection_id}."
                print(msg)
                LoggingUtils.cmss_logger(str(msg), args.cmss_logger_host)

                asset_urls = []
                for item in collection_items:
                    for asset_key, asset in item.assets.items():
                        asset_urls.append(asset.href)

                if asset_urls:
                    product_details = {
                        "collection": collection_id,
                        "ogc": upserted_collection.to_dict(),
                        "uris": asset_urls,
                        "job_id": MaapUtils.get_job_id()
                    }

                    logger.debug(f"Product details for notification: {product_details}")
                    LoggingUtils.cmss_product_available(product_details, args.cmss_logger_host)
                    LoggingUtils.cmss_logger(f"Products available for collection {collection_id}", args.cmss_logger_host)

    logger.debug("Product cataloging completed successfully")

async def main():
    """Main function orchestrating the generic pipeline"""
    logger.debug("Starting generic pipeline main function")
    args = parse_arguments()
    
    try:
        ConfigUtils.validate_arguments(args)
        input_type = ConfigUtils.detect_input_type(args)
        logger.debug(f"Detected input type: {input_type}")
        
        maap_host_to_use = os.environ.get('MAAP_API_HOST', args.maap_host)
        logger.debug(f"Using MAAP host: {maap_host_to_use}")
        maap = MaapUtils.get_maap_instance(maap_host_to_use)
        
        logging.info(f"Processing {input_type} input")

        cog_jobs = [maap.getJob("853116bf-cda9-446c-9ab1-5d475d4e51d8")]

        catalog_products(args, maap, cog_jobs, None)
        
        # if input_type == "daac":
        #     # DAAC → NetCDF → Zarr → (optional concat) → COG → Catalog
        #     logger.debug("Starting DAAC pipeline: stage → netcdf2zarr → concat? → zarr2cog → catalog")
        #     staged_job = await stage_from_daac(args, maap)
        #     zarr_job = await convert_netcdf_to_zarr(args, maap, staged_job)
        #
        #     if args.enable_concat:
        #         logger.debug("Concatenation enabled, performing Zarr concatenation")
        #         zarr_job = await concatenate_zarr(args, maap, zarr_job)
        #     else:
        #         logger.debug("Concatenation disabled, skipping concatenation step")
        #
        #     cog_jobs = await convert_zarr_to_cog(args, maap, zarr_job)
        #     catalog_products(args, maap, cog_jobs, zarr_job)
        #     logger.debug("DAAC pipeline completed successfully")
        #
        # elif input_type == "s3_netcdf":
        #     # S3 NetCDF → Zarr → (optional concat) → COG → Catalog
        #     logger.debug("Starting S3 NetCDF pipeline: netcdf2zarr → concat? → zarr2cog → catalog")
        #     zarr_job = await convert_netcdf_to_zarr(args, maap, args.input_s3)
        #
        #     if args.enable_concat:
        #         logger.debug("Concatenation enabled, performing Zarr concatenation")
        #         zarr_job = await concatenate_zarr(args, maap, zarr_job)
        #     else:
        #         logger.debug("Concatenation disabled, skipping concatenation step")
        #
        #     cog_jobs = await convert_zarr_to_cog(args, maap, zarr_job)
        #     catalog_products(args, maap, cog_jobs, zarr_job)
        #     logger.debug("S3 NetCDF pipeline completed successfully")
        #
        # elif input_type == "s3_zarr":
        #     # S3 Zarr → COG → Catalog (skip NetCDF conversion and concat)
        #     logger.debug("Starting S3 Zarr pipeline: zarr2cog → catalog")
        #     cog_jobs = await convert_zarr_to_cog(args, maap, args.input_s3)
        #     catalog_products(args, maap, cog_jobs, None)
        #     logger.debug("S3 Zarr pipeline completed successfully")
        
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