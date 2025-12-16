#!/usr/bin/env python3
"""
LIS Preprocessing Pipeline Script

This script runs LIS data preprocessing followed by the full localized pipeline.
Designed to be run as a separate MAAP algorithm for LIS data processing.
"""

import os
import sys
import logging
import subprocess

from common_utils import (
    MaapUtils, LoggingUtils, ConfigUtils, AWSUtils
)

# Configure logging: DEBUG for this module, INFO for dependencies
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def parse_arguments():
    """
    Defines and parses command-line arguments for the LIS preprocessing pipeline script.
    """
    logger.debug("Starting argument parsing")
    parser = ConfigUtils.get_generic_argument_parser()
    
    # The --input-s3 argument is already defined in the generic parser
    # No need to redefine it here
    
    args, unknown_args = parser.parse_known_args()
    logger.debug(f"Parsed arguments: {vars(args)}")
    logger.debug(f"Unknown arguments: {unknown_args}")
    return args, unknown_args

def run_lis_preprocessor(args) -> str:
    """
    Run LIS preprocessor on the input S3 file.
    
    Returns:
        str: Path to the preprocessed file
    """
    logger.info(f"LIS_PREPROCESS - Args: input_s3='{args.input_s3}'")
    logger.debug(f"Starting LIS preprocessing for input: {args.input_s3}")
    
    try:
        # Create input and output directories
        os.makedirs("input", exist_ok=True)
        os.makedirs("output", exist_ok=True)
        
        # Download S3 file to local input directory  
        bucket_name, s3_path = AWSUtils.parse_s3_path(args.input_s3)
        s3_client = AWSUtils.get_s3_client(role_arn=args.role_arn, bucket_name=bucket_name)
        
        # Get filename from S3 path
        input_filename = os.path.basename(s3_path)
        local_input_path = os.path.join("input", input_filename)
        
        logger.debug(f"Downloading {args.input_s3} to {local_input_path}")
        s3_client.download_file(bucket_name, s3_path, local_input_path)
        
        # Import and run LIS preprocessor
        from czdt_iss_transformers.preprocessors.lis.lis_preprocessor import preprocess_lis_data
        
        # Generate output filename
        base_name = os.path.splitext(input_filename)[0]
        output_filename = f"{base_name}_preprocessed.nc"
        local_output_path = os.path.join("output", output_filename)
        
        logger.debug(f"Running LIS preprocessor: {local_input_path} -> {local_output_path}")
        preprocess_lis_data(local_input_path, local_output_path)
        
        logger.debug(f"LIS preprocessing completed successfully")
        return local_output_path
        
    except Exception as e:
        logger.error(f"LIS preprocessing failed: {e}")
        raise RuntimeError(f"LIS preprocessing failed: {e}")

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

def main():
    """
    Main function orchestrating the LIS preprocessing pipeline.
    """
    logger.debug("Starting LIS preprocessing pipeline main function")
    args, unknown_args = parse_arguments()
    
    try:
        # Validate arguments
        ConfigUtils.validate_arguments(args)
        
        logging.info("Processing LIS input with preprocessing followed by full pipeline")
        
        # Step 1: Run LIS preprocessor
        preprocessed_file = run_lis_preprocessor(args)
        
        # Step 2: Run the main localized pipeline with the preprocessed file
        run_localized_pipeline(preprocessed_file, args, unknown_args)
        
        logging.info("LIS preprocessing pipeline completed successfully!")
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