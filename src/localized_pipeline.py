import os
import sys
import logging
from pathlib import Path
from typing import List

import pystac
from geoserver_ingest import GeoServerClient
from common_utils import (
    MaapUtils, LoggingUtils, ConfigUtils, AWSUtils,
    GranuleNotFoundError, DownloadError, UploadError
)
from stage_from_daac import search_and_download_granule
import czdt_iss_transformers.cf2zarr as cf2zarr
import czdt_iss_transformers.zarr_concat as zarr_concat
import czdt_iss_transformers.zarr2cog as zarr2cog 
# Configure logging: DEBUG for this module, INFO for dependencies
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Note: Direct imports of czdt_iss_transformers modules replaced subprocess calls

# Geoserver configuration
GEOSERVER_WORKSPACE = "czdt"
GEOSERVER_USER = "ingest"
GEOSERVER_PASSWORD_SECRET_NAME = "geoserver_secret"

def parse_arguments():
    """
    Defines and parses command-line arguments for the localized pipeline script.
    """
    logger.debug("Starting argument parsing")
    parser = ConfigUtils.get_generic_argument_parser()
    
    # Add step selection parameter
    parser.add_argument(
        '--steps',
        type=str,
        default='all',
        help='Comma-separated list of steps to run: stage,netcdf2zarr,concat,zarr2cog,catalog (default: all)'
    )
    
    # Note: transformers-path argument removed - using direct module imports
    
    # Add geoserver host parameter
    parser.add_argument(
        '--geoserver-host',
        type=str,
        required=False,
        help='Geoserver host URL (required for GeoPackage processing)'
    )
    
    # Add upsert parameter for catalog job
    parser.add_argument(
        '--upsert',
        action='store_true',
        help='Enable upsert mode for catalog job (update existing items instead of failing on conflicts)'
    )
    
    args = parser.parse_args()
    logger.debug(f"Parsed arguments: {vars(args)}")
    return args

# Note: validate_transformers_path function removed - using direct module imports

def get_enabled_steps(steps_arg: str, input_type: str) -> List[str]:
    """
    Parse the steps argument and return list of enabled steps based on input type.
    """
    if steps_arg == 'all':
        if input_type == "daac":
            return ['stage', 'netcdf2zarr', 'zarr2cog', 'catalog']
        elif input_type == "s3_netcdf":
            return ['netcdf2zarr', 'zarr2cog', 'catalog']
        elif input_type == "s3_zarr":
            return ['zarr2cog', 'catalog']
        elif input_type == "s3_gpkg":
            return ['catalog']
    else:
        steps = [step.strip() for step in steps_arg.split(',')]
        valid_steps = ['stage', 'netcdf2zarr', 'concat', 'zarr2cog', 'catalog']
        invalid_steps = [step for step in steps if step not in valid_steps]
        if invalid_steps:
            raise ValueError(f"Invalid steps: {invalid_steps}. Valid steps: {valid_steps}")
        return steps

# Note: run_transformer_command function removed - using direct function calls

def stage_from_daac_local(args, maap) -> str:
    """
    Local implementation of DAAC staging.
    Downloads granule from DAAC using MAAP and returns local file path.
    
    Args:
        args: Arguments containing granule_id, collection_id
        maap: MAAP client instance
        
    Returns:
        str: Path to the locally downloaded granule file
        
    Raises:
        GranuleNotFoundError: If the granule cannot be found
        DownloadError: If download fails
    """
    logger.debug(f"Starting DAAC staging for granule '{args.granule_id}' in collection '{args.collection_id}'")
    
    # Set local download directory
    local_download_dir = getattr(args, 'local_download_path', 'output')
    
    # Search and download granule using imported function
    downloaded_file_path = search_and_download_granule(
        maap, args.granule_id, args.collection_id, local_download_dir
    )
    
    logger.debug(f"DAAC staging completed successfully, local file: {downloaded_file_path}")
    return downloaded_file_path

def convert_netcdf_to_zarr_local(args, input_source: str) -> str:
    """
    Convert NetCDF to Zarr using direct cf2zarr function call.
    """
    logger.debug(f"Starting local NetCDF to Zarr conversion for input: {input_source}")
    
    # Prepare output directory
    os.makedirs("output", exist_ok=True)
    
    # Generate output name based on input
    filename = os.path.basename(input_source)
    base_name = os.path.splitext(filename)[0]
    output_zarr_name = f"{base_name}.zarr"
    output_path = os.path.join("output", output_zarr_name)
    
    print(f"Running NetCDF to Zarr conversion")
    
    try:
        # Prepare variables list
        variables = None
        if hasattr(args, 'variables') and args.variables:
            variables = args.variables.split(',')
        
        # Call cf2zarr function directly
        cf2zarr.convert_cf_to_zarr(
            config_path=args.zarr_config_url,
            input_path=input_source,
            output_path=output_path,
            pattern="*.nc*",  # Support both .nc and .nc4
            variables=variables
        )
        
        logger.debug(f"NetCDF to Zarr conversion completed successfully")
        logger.debug(f"NetCDF to Zarr conversion completed, output: {output_path}")
        return output_path
        
    except Exception as e:
        logger.error(f"NetCDF to Zarr conversion failed: {e}")
        raise RuntimeError(f"NetCDF to Zarr conversion failed: {e}")

def concatenate_zarr_local(args, zarr_paths: List[str]) -> str:
    """
    Concatenate Zarr files using direct zarr_concat main function call.
    """
    logger.debug(f"Starting local Zarr concatenation for paths: {zarr_paths}")
    
    # Generate output name
    output_zarr_name = "concatenated.zarr"
    output_path = os.path.join("output", output_zarr_name)
    
    print(f"Running Zarr concatenation")
    
    try:
        # Create argparse-like object for zarr_concat.main()
        class ConcatArgs:
            def __init__(self):
                self.config = args.zarr_config_url
                self.zarr = [os.path.abspath(p) if not p.startswith('/') else p for p in zarr_paths]
                self.zarr_manifest = None
                self.output = output_path
                self.zarr_access = "stage"
        
        concat_args = ConcatArgs()
        
        # Call zarr_concat main function directly
        zarr_concat.main(concat_args)
        
        logger.debug(f"Zarr concatenation completed successfully")
        logger.debug(f"Zarr concatenation completed, output: {output_path}")
        return output_path
        
    except Exception as e:
        logger.error(f"Zarr concatenation failed: {e}")
        raise RuntimeError(f"Zarr concatenation failed: {e}")

def convert_zarr_to_cog_local(args, zarr_path: str) -> List[str]:
    """
    Convert Zarr to COG using direct zarr2cog main function call.
    """
    logger.debug(f"Starting local Zarr to COG conversion for: {zarr_path}")
    
    print(f"Running Zarr to COG conversion")
    
    try:
        # Create argparse-like object for zarr2cog.main()
        class CogArgs:
            def __init__(self):
                self.zarr = zarr_path
                self.concept_id = args.collection_id
                self.output = "cog"
                self.time = "time"
                self.latitude = "lat"
                self.longitude = "lon"
                self.zarr_access = "stage"
        
        cog_args = CogArgs()
        
        # Call zarr2cog main function directly
        zarr2cog.main(cog_args)
        
        # Find generated COG files
        output_dir = Path("output")
        cog_files = list(output_dir.glob("*.tif"))
        cog_paths = [str(f) for f in cog_files]
        
        logger.debug(f"Zarr to COG conversion completed successfully")
        logger.debug(f"Zarr to COG conversion completed, generated {len(cog_paths)} COG files")
        return cog_paths
        
    except Exception as e:
        logger.error(f"Zarr to COG conversion failed: {e}")
        raise RuntimeError(f"Zarr to COG conversion failed: {e}")

def catalog_products_local(args, cog_paths: List[str]):
    """
    Local implementation of product cataloging.
    The zarr2cog.py transformer already creates STAC catalog, so we just need to handle STAC catalog processing.
    """
    logger.debug(f"Starting local product cataloging for {len(cog_paths)} COG files")
    # Note: args parameter preserved for future use (collection_id, etc.)
    del args  # Suppress unused variable warning for now
    
    # Look for the STAC catalog created by zarr2cog.py
    catalog_file = os.path.join("output", "catalog.json")
    
    if not os.path.exists(catalog_file):
        logger.warning("No STAC catalog file found, cataloging may not have been completed by zarr2cog")
        return
    
    try:
        # Load the catalog created by zarr2cog
        catalog = pystac.Catalog.from_file(catalog_file)
        
        # Count collections and items
        collections = list(catalog.get_collections())
        total_items = sum(len(list(coll.get_items())) for coll in collections)
        
        msg = f"Local STAC catalog contains {total_items} items across {len(collections)} collections."
        print(msg)
        logger.debug(msg)
        
        # Log collection details
        for collection in collections:
            items = list(collection.get_items())
            logger.debug(f"Collection {collection.id}: {len(items)} items")
        
        logger.debug("Local product cataloging completed successfully")
        
    except Exception as e:
        logger.error(f"Error processing STAC catalog: {e}")
        raise RuntimeError(f"Failed to process STAC catalog: {e}")


def submit_catalog_job(args):
    """
    Submit a separate MAAP DPS job to handle catalog ingestion to STAC API.
    This job will wait for the current job to complete, then process the catalog.json output.
    """
    logger.info("Submitting catalog job to handle STAC API ingestion")
    
    try:
        # Get current job ID to pass as parent_job_id
        current_job_id = MaapUtils.get_job_id()
        if not current_job_id:
            logger.warning("Could not get current job ID, catalog job may not work properly")
            current_job_id = "unknown"
        
        # Get MAAP instance
        maap = MaapUtils.get_maap_instance(args.maap_host)
        
        # Prepare catalog job parameters
        job_params = {
            "identifier": f"Catalog_Job_for_{current_job_id}",
            "algo_id": "czdt-iss-catalog-job",
            "version": "main",
            "queue": "maap-dps-czdt-worker-8gb",
            "parent_job_id": current_job_id,
            "mmgis_host": args.mmgis_host,
            "titiler_token_secret_name": args.titiler_token_secret_name,
            "cmss_logger_host": args.cmss_logger_host,
            "collection_id": args.collection_id,
            "maap_host": args.maap_host
        }
        
        # Add optional parameters if provided
        if hasattr(args, 'upsert') and args.upsert:
            job_params["upsert"] = "true"
        
        # Submit catalog job
        catalog_job = maap.submitJob(**job_params)
        
        msg = f"Catalog job {catalog_job.id} submitted to process outputs from parent job {current_job_id}"
        print(msg)
        LoggingUtils.cmss_logger(str(msg), args.cmss_logger_host)
        logger.info(f"Catalog job submitted: {catalog_job.id}")
        
        return catalog_job
        
    except Exception as e:
        logger.error(f"Failed to submit catalog job: {e}")
        # Don't fail the pipeline for catalog job submission failure
        msg = f"Warning: Failed to submit catalog job: {e}. Products may not be cataloged to STAC API."
        print(msg)
        LoggingUtils.cmss_logger(str(msg), args.cmss_logger_host)
        return None

def main():
    """
    Main function orchestrating the localized pipeline.
    """
    logger.debug("Starting localized pipeline main function")
    args = parse_arguments()
    
    try:
        # Validate arguments
        ConfigUtils.validate_arguments(args)
        
        # Note: transformers path validation removed - using direct module imports
        
        # Detect input type and get enabled steps
        input_type = ConfigUtils.detect_input_type(args)
        enabled_steps = get_enabled_steps(args.steps, input_type)
        
        logger.debug(f"Detected input type: {input_type}")
        logger.debug(f"Enabled steps: {enabled_steps}")
        
        # Initialize MAAP if needed for certain operations
        maap = None
        if 'stage' in enabled_steps or input_type == "s3_gpkg":
            maap_host_to_use = os.environ.get('MAAP_API_HOST', args.maap_host)
            logger.debug(f"Using MAAP host: {maap_host_to_use}")
            maap = MaapUtils.get_maap_instance(maap_host_to_use)
        
        logging.info(f"Processing {input_type} input with steps: {', '.join(enabled_steps)}")
        
        # Track intermediate outputs
        current_output = None
        
        if input_type == "daac":
            # DAAC pipeline: stage � netcdf2zarr � concat? � zarr2cog � catalog
            if 'stage' in enabled_steps:
                current_output = stage_from_daac_local(args, maap)
            
            if 'netcdf2zarr' in enabled_steps and current_output:
                current_output = convert_netcdf_to_zarr_local(args, current_output)
                
            if 'concat' in enabled_steps and args.enable_concat and current_output:
                logger.debug("Concatenation enabled, performing Zarr concatenation")
                current_output = concatenate_zarr_local(args, [current_output])
            elif 'concat' in enabled_steps:
                logger.debug("Concatenation requested but conditions not met, skipping")
                
            if 'zarr2cog' in enabled_steps and current_output:
                cog_paths = convert_zarr_to_cog_local(args, current_output)
                
            if 'catalog' in enabled_steps:
                submit_catalog_job(args)
                
        elif input_type == "s3_netcdf":
            # S3 NetCDF pipeline: netcdf2zarr � concat? � zarr2cog � catalog
            if 'netcdf2zarr' in enabled_steps:
                current_output = convert_netcdf_to_zarr_local(args, args.input_s3)
                
            if 'concat' in enabled_steps and args.enable_concat and current_output:
                logger.debug("Concatenation enabled, performing Zarr concatenation")
                current_output = concatenate_zarr_local(args, [current_output])
            elif 'concat' in enabled_steps:
                logger.debug("Concatenation requested but conditions not met, skipping")
                
            if 'zarr2cog' in enabled_steps and current_output:
                cog_paths = convert_zarr_to_cog_local(args, current_output)
                
            if 'catalog' in enabled_steps:
                submit_catalog_job(args)
                
        elif input_type == "s3_zarr":
            # S3 Zarr pipeline: zarr2cog � catalog
            if 'zarr2cog' in enabled_steps:
                cog_paths = convert_zarr_to_cog_local(args, args.input_s3)
                
            if 'catalog' in enabled_steps:
                submit_catalog_job(args)
                
        elif input_type == "s3_gpkg":
            # S3 GeoPackage pipeline: catalog (geoserver upload)
            if 'catalog' in enabled_steps:
                logger.debug("Starting S3 GeoPackage pipeline: geoserver upload")
                
                # Validate geoserver host is provided for GeoPackage processing
                if not args.geoserver_host:
                    raise ValueError("--geoserver-host is required for GeoPackage processing")
                
                aws_region_for_s3 = os.environ.get('AWS_REGION', 'us-west-2')
                s3_client = AWSUtils.get_s3_client(role_arn=args.role_arn, aws_region=aws_region_for_s3)
                bucket_name, gpkg_path = AWSUtils.parse_s3_path(args.input_s3)
                
                # Download the file
                os.makedirs("output", exist_ok=True)
                file_name = os.path.basename(gpkg_path)
                local_file_path = f"output/{file_name}"
                s3_client.download_file(bucket_name, gpkg_path, local_file_path)
                
                print(f"File '{gpkg_path}' downloaded successfully to '{local_file_path}'")
                logger.debug("Starting Geoserver upload")
                
                client = GeoServerClient(args.geoserver_host, GEOSERVER_USER, maap.secrets.get_secret(GEOSERVER_PASSWORD_SECRET_NAME))
                client.create_workspace(GEOSERVER_WORKSPACE)
                success, layer_names = client.upload_geopackage(local_file_path, GEOSERVER_WORKSPACE)
                
                if success:
                    logger.debug("S3 GeoPackage pipeline completed successfully")
                    asset_uris = []
                    
                    for l in layer_names:
                        asset_uris.append(f"{args.geoserver_host}{GEOSERVER_WORKSPACE}/ows?service=WFS&version=1.0.0&request=GetFeature&typeName={GEOSERVER_WORKSPACE}%3A{l}&outputFormat=application%2Fjson&maxFeatures=10000")
                    
                    product_details = {
                        "concept_id": args.collection_id,
                        "uris": asset_uris,
                        "job_id": MaapUtils.get_job_id()
                    }
                    
                    logger.debug(f"Product details for notification: {product_details}")
                    LoggingUtils.cmss_product_available(product_details, args.cmss_logger_host)
                    LoggingUtils.cmss_logger(f"Products available for collection {args.collection_id}", args.cmss_logger_host)
        
        logging.info("Localized pipeline completed successfully!")
        logger.debug("All pipeline steps completed without errors")
        
    except ValueError as e:
        logger.debug(f"ValueError caught: {e}")
        logging.error(f"TERMINATED: Invalid argument or value. Details: {e}")
        sys.exit(6)
    except RuntimeError as e:
        logger.debug(f"RuntimeError caught: {e}")
        logging.error(f"TERMINATED: Runtime error. Details: {e}")
        sys.exit(7)
    except NotImplementedError as e:
        logger.debug(f"NotImplementedError caught: {e}")
        logging.error(f"TERMINATED: Feature not yet implemented. Details: {e}")
        sys.exit(8)
    except Exception as e:
        logger.debug(f"Unexpected exception caught: {type(e).__name__}: {e}")
        logging.error(f"TERMINATED: An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    logger.debug("Script started as main module")
    main()