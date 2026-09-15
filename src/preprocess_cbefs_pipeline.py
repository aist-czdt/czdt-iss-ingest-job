#!/usr/bin/env python3
"""
CBEFS Preprocessing Pipeline Script

This script runs CBEFS data preprocessing followed by the full localized pipeline.
Designed to be run as a separate MAAP algorithm for CBEFS data processing.
"""

import os
import sys
import logging
import subprocess
from pathlib import Path
import requests
from urllib.parse import urlparse

from common_utils import (
    MaapUtils, LoggingUtils, ConfigUtils, AWSUtils
)

# Configure logging: DEBUG for this module, INFO for dependencies
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def parse_arguments():
    """
    Defines and parses command-line arguments for the CBEFS preprocessing pipeline script.
    """
    logger.debug("Starting argument parsing")
    parser = ConfigUtils.get_generic_argument_parser()
    
    # Add CBEFS-specific parameters
    parser.add_argument(
        '--resolution',
        type=float,
        default=0.005,
        help='Grid resolution for CBEFS regridding (default: 0.005)'
    )
    
    args, unknown = parser.parse_known_args()
    logger.debug(f"Parsed arguments: {vars(args)}")
    logger.debug(f"Ignored unknown arguments: {unknown}")

    return args

def run_cbefs_preprocessor(args):
    """
    Run CBEFS preprocessor on the input S3 file.
    
    Returns:
        str: Path to the first preprocessed file (CBEFS creates multiple files)
    """
    logger.info(f"CBEFS_PREPROCESS - Args: input_s3='{args.input_s3}', resolution='{args.resolution}', variables='{getattr(args, 'variables', ['oxygen', 'salt'])}'")
    logger.debug(f"Starting CBEFS preprocessing for input: {args.input_s3}")
    
    try:
        # Create input and output directories
        os.makedirs("input", exist_ok=True)
        os.makedirs("output", exist_ok=True)
        
        # Import and run CBEFS preprocessor
        from czdt_iss_transformers.preprocessors.cbefs.cbefs_preprocessor import main as cbefs_main
        
        # Create args object for CBEFS preprocessor
        class CBEFSArgs:
            def __init__(self):
                self.url = args.input_s3  # Use downloaded file as URL/path
                self.resolution = args.resolution
                self.variables = getattr(args, 'variables', ['oxygen', 'salt'])
                if isinstance(self.variables, str):
                    self.variables = self.variables.split(',')
        
        cbefs_args = CBEFSArgs()
        logger.debug(f"Running CBEFS preprocessor with resolution={cbefs_args.resolution}, variables={cbefs_args.variables}")
        cbefs_main(cbefs_args)
        
        # CBEFS creates multiple output files, find all of them
        output_files = list(Path("output").glob("*.nc"))
        if not output_files:
            raise RuntimeError("CBEFS preprocessing did not generate any output files")
        
        # Sort files to get consistent ordering
        preprocessed_files = sorted([str(f) for f in output_files])
        logger.debug(f"CBEFS preprocessing completed successfully, generated {len(preprocessed_files)} files")
        
        # Return the time-sliced files
        return preprocessed_files
        
    except Exception as e:
        logger.error(f"CBEFS preprocessing failed: {e}")
        raise RuntimeError(f"CBEFS preprocessing failed: {e}")

def run_localized_pipeline(preprocessed_file: str, original_args, unknown_args=None):
    """
    Run the main localized pipeline with the preprocessed file.
    """
    logger.info(f"Running localized pipeline with preprocessed file: {preprocessed_file}")
    
    # Build command for localized pipeline
    pipeline_script = os.path.join(os.path.dirname(__file__), 'localized_pipeline.py')
    cmd = [sys.executable, pipeline_script, '--input-netcdf', preprocessed_file]
    
    # Add all required arguments - these must be present for localized pipeline to work
    required_args = [
        ('s3_bucket', '--s3-bucket'),
        ('role_arn', '--role-arn'),
        ('zarr_config_url', '--zarr-config-url'),
        ('maap_host', '--maap-host'),
        ('mmgis_host', '--mmgis-host'),
        ('titiler_token_secret_name', '--titiler-token-secret-name'),
        ('cmss_logger_host', '--cmss-logger-host'),
        ('job_queue', '--job-queue')
    ]
    
    for attr_name, arg_name in required_args:
        if hasattr(original_args, attr_name) and getattr(original_args, attr_name):
            cmd.extend([arg_name, getattr(original_args, attr_name)])
        else:
            logger.warning(f"Required argument {arg_name} is missing or None")
    
    # Add optional arguments if provided
    optional_args = [
        ('collection_id', '--collection-id'),
        ('variables', '--variables'),
        ('s3_prefix', '--s3-prefix'),
        ('local_download_path', '--local-download-path')
    ]
    
    for attr_name, arg_name in optional_args:
        if hasattr(original_args, attr_name) and getattr(original_args, attr_name):
            cmd.extend([arg_name, getattr(original_args, attr_name)])
    
    # Pass through any unknown arguments to the localized pipeline
    if unknown_args:
        cmd.extend(unknown_args)
        logger.debug(f"Passing through unknown arguments: {unknown_args}")
    
    logger.debug(f"Running command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True)
        logger.info("Localized pipeline completed successfully")
        logger.debug(f"Pipeline output: {result.stdout}")
        return result
    except subprocess.CalledProcessError as e:
        logger.error(f"Localized pipeline failed with return code {e.returncode}")
        logger.error(f"Pipeline stderr: {e.stderr}")
        raise RuntimeError(f"Localized pipeline failed: {e}")
    
def find_file(files, endswith):
    for name in files:
        if name.endswith(endswith):
            return name

    return None


def main():
    """
    Main function orchestrating the CBEFS preprocessing pipeline.
    """
    logger.debug("Starting CBEFS preprocessing pipeline main function")
    args = parse_arguments()
    
    try:        
        logging.info("Processing CBEFS input with preprocessing followed by full pipeline")

        bucket_name, zarr_config_path = AWSUtils.parse_s3_path(args.zarr_config_url)
        s3_client = AWSUtils.get_s3_client(role_arn=args.role_arn, bucket_name=bucket_name)
        
        # Download the file
        os.makedirs("output", exist_ok=True)
        file_name = os.path.basename(zarr_config_path)
        
        local_file_path = f"output/{file_name}"

        logging.info(f"zarr_config_path: {zarr_config_path}")
        logging.info(f"bucket_name: {bucket_name}, file_name: {file_name}, local_file_path: {local_file_path}")

        s3_client.download_file(bucket_name, zarr_config_path, local_file_path)
        args.zarr_config_url = local_file_path  
        
        # Step 1: Run CBEFS preprocessor
        preprocessed_files = run_cbefs_preprocessor(args)

        coll_prefix = "chesroms_ECB_HR_avg-"

        # Step 2: Run the main localized pipeline with the preprocessed files
        args.collection_id = coll_prefix + "surface-nowcast"
        run_localized_pipeline(find_file(preprocessed_files, "s0_1.nc"), args)

        args.collection_id = coll_prefix + "surface-forecast"
        run_localized_pipeline(find_file(preprocessed_files, "s0_2.nc"), args)

        args.collection_id = coll_prefix + "bottom-nowcast"
        run_localized_pipeline(find_file(preprocessed_files, "s19_1.nc"), args)

        args.collection_id = coll_prefix + "bottom-forecast"
        run_localized_pipeline(find_file(preprocessed_files, "s19_2.nc"), args)
        
        logging.info("CBEFS preprocessing pipeline completed successfully!")
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