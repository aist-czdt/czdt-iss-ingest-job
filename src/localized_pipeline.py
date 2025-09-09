import os
import sys
import logging
import subprocess
from pathlib import Path
from typing import List

import pystac
from geoserver_ingest import GeoServerClient
from common_utils import (
    MaapUtils, LoggingUtils, ConfigUtils, AWSUtils
)

# Configure logging: DEBUG for this module, INFO for dependencies
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Path to the czdt-iss-transformers repository (default to parent directory)
DEFAULT_TRANSFORMERS_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "czdt-iss-transformers")
TRANSFORMERS_PATH = DEFAULT_TRANSFORMERS_PATH
TRANSFORMERS_SRC = os.path.join(TRANSFORMERS_PATH, "src")

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
    
    # Add transformers path override
    parser.add_argument(
        '--transformers-path',
        type=str,
        default=DEFAULT_TRANSFORMERS_PATH,
        help=f'Path to czdt-iss-transformers repository (default: {DEFAULT_TRANSFORMERS_PATH})'
    )
    
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

def validate_transformers_path(transformers_path: str) -> bool:
    """
    Validate that the transformers path exists and contains required modules.
    """
    src_path = os.path.join(transformers_path, "src")
    required_files = ["cf2zarr.py", "zarr_concat.py", "zarr2cog.py", "util.py"]
    
    if not os.path.exists(src_path):
        logger.error(f"Transformers src directory not found: {src_path}")
        return False
    
    for file in required_files:
        file_path = os.path.join(src_path, file)
        if not os.path.exists(file_path):
            logger.error(f"Required transformer file not found: {file_path}")
            return False
    
    return True

def get_enabled_steps(steps_arg: str, input_type: str) -> List[str]:
    """
    Parse the steps argument and return list of enabled steps based on input type.
    """
    if steps_arg == 'all':
        if input_type == "daac":
            return ['stage', 'netcdf2zarr', 'concat', 'zarr2cog', 'catalog']
        elif input_type == "s3_netcdf":
            return ['netcdf2zarr', 'concat', 'zarr2cog', 'catalog']
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

def run_transformer_command(cmd: List[str], description: str) -> subprocess.CompletedProcess:
    """
    Run a transformer command and handle logging/errors.
    """
    logger.debug(f"Running {description}: {' '.join(cmd)}")
    msg = f"Running {description}"
    print(msg)
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        logger.debug(f"{description} completed successfully")
        logger.debug(f"Command output: {result.stdout}")
        return result
    except subprocess.CalledProcessError as e:
        logger.error(f"{description} failed with exit code {e.returncode}")
        logger.error(f"Command output: {e.stdout}")
        logger.error(f"Command error: {e.stderr}")
        raise RuntimeError(f"{description} failed: {e.stderr}")

def stage_from_daac_local(args, maap):
    """
    Local implementation of DAAC staging.
    For now, delegate to the original MAAP job since this requires MAAP infrastructure.
    """
    logger.debug("DAAC staging not yet implemented locally, would need MAAP job")
    # Note: args and maap parameters preserved for future implementation
    del args, maap  # Suppress unused variable warnings
    raise NotImplementedError("DAAC staging requires MAAP infrastructure and is not yet implemented for local execution")

def convert_netcdf_to_zarr_local(args, input_source: str, transformers_path: str) -> str:
    """
    Convert NetCDF to Zarr using local cf2zarr.py transformer.
    """
    logger.debug(f"Starting local NetCDF to Zarr conversion for input: {input_source}")
    
    # Prepare output directory
    os.makedirs("output", exist_ok=True)
    
    # Generate output name based on input
    if input_source.startswith('s3://'):
        filename = os.path.basename(input_source)
        base_name = os.path.splitext(filename)[0]
        output_zarr_name = f"{base_name}.zarr"
    else:
        # For other inputs, use a generic name
        output_zarr_name = "converted.zarr"
    
    # Build cf2zarr command
    cf2zarr_script = os.path.join(transformers_path, "src", "cf2zarr.py")
    cmd = [
        sys.executable, cf2zarr_script,
        args.zarr_config_url,  # config file
        "--input-s3", input_source,
        "--output", output_zarr_name,
        "--pattern", "*.nc*",  # Support both .nc and .nc4
    ]
    
    # Add variables if specified
    if hasattr(args, 'variables') and args.variables:
        cmd.extend(["--variables"] + args.variables.split(','))
    
    # Run the conversion
    run_transformer_command(cmd, "NetCDF to Zarr conversion")
    
    output_path = os.path.join("output", output_zarr_name)
    logger.debug(f"NetCDF to Zarr conversion completed, output: {output_path}")
    return output_path

def concatenate_zarr_local(args, zarr_paths: List[str], transformers_path: str) -> str:
    """
    Concatenate Zarr files using local zarr_concat.py transformer.
    """
    logger.debug(f"Starting local Zarr concatenation for paths: {zarr_paths}")
    
    # Generate output name
    output_zarr_name = "concatenated.zarr"
    
    # Build zarr_concat command
    zarr_concat_script = os.path.join(transformers_path, "src", "zarr_concat.py")
    cmd = [
        sys.executable, zarr_concat_script,
        args.zarr_config_url,  # config file
        "--zarr"
    ]
    
    # Add zarr paths - convert to local paths if needed
    for zarr_path in zarr_paths:
        if zarr_path.startswith('/'):
            cmd.append(zarr_path)  # Local absolute path
        else:
            cmd.append(os.path.abspath(zarr_path))  # Make relative paths absolute
    
    cmd.extend([
        "--output", output_zarr_name,
        "--zarr-access", "stage"
    ])
    
    # Run the concatenation
    run_transformer_command(cmd, "Zarr concatenation")
    
    output_path = os.path.join("output", output_zarr_name)
    logger.debug(f"Zarr concatenation completed, output: {output_path}")
    return output_path

def convert_zarr_to_cog_local(args, zarr_path: str, transformers_path: str) -> List[str]:
    """
    Convert Zarr to COG using local zarr2cog.py transformer.
    """
    logger.debug(f"Starting local Zarr to COG conversion for: {zarr_path}")
    
    # Build zarr2cog command
    zarr2cog_script = os.path.join(transformers_path, "src", "zarr2cog.py")
    cmd = [
        sys.executable, zarr2cog_script,
        zarr_path,  # zarr input
        "--concept_id", args.collection_id,
        "--output", "cog",
        "--time", "time",
        "--latitude", "lat", 
        "--longitude", "lon"
    ]
    
    # Run the conversion
    run_transformer_command(cmd, "Zarr to COG conversion")
    
    # Find generated COG files
    output_dir = Path("output")
    cog_files = list(output_dir.glob("*.tif"))
    cog_paths = [str(f) for f in cog_files]
    
    logger.debug(f"Zarr to COG conversion completed, generated {len(cog_paths)} COG files")
    return cog_paths

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
        
        # Validate transformers path
        if not validate_transformers_path(args.transformers_path):
            raise RuntimeError(f"Invalid transformers path: {args.transformers_path}")
        
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
                # For now, staging from DAAC is not implemented locally
                raise NotImplementedError("DAAC staging not yet supported in localized pipeline")
            
            if 'netcdf2zarr' in enabled_steps:
                # This would use staged files, not implemented yet
                raise NotImplementedError("NetCDF to Zarr from DAAC staging not yet supported")
                
        elif input_type == "s3_netcdf":
            # S3 NetCDF pipeline: netcdf2zarr � concat? � zarr2cog � catalog
            if 'netcdf2zarr' in enabled_steps:
                current_output = convert_netcdf_to_zarr_local(args, args.input_s3, args.transformers_path)
                
            if 'concat' in enabled_steps and args.enable_concat and current_output:
                logger.debug("Concatenation enabled, performing Zarr concatenation")
                current_output = concatenate_zarr_local(args, [current_output], args.transformers_path)
            elif 'concat' in enabled_steps:
                logger.debug("Concatenation requested but conditions not met, skipping")
                
            if 'zarr2cog' in enabled_steps and current_output:
                cog_paths = convert_zarr_to_cog_local(args, current_output, args.transformers_path)
                
            if 'catalog' in enabled_steps:
                submit_catalog_job(args)
                
        elif input_type == "s3_zarr":
            # S3 Zarr pipeline: zarr2cog � catalog
            if 'zarr2cog' in enabled_steps:
                cog_paths = convert_zarr_to_cog_local(args, args.input_s3, args.transformers_path)
                
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