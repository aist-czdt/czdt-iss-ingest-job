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
    
    # Override the input group to only accept input-s3 for preprocessing
    parser.add_argument(
        '--input-s3',
        required=True,
        help='S3 path to raw CBEFS NetCDF file for preprocessing'
    )
    
    # Add CBEFS-specific parameters
    parser.add_argument(
        '--resolution',
        type=float,
        default=0.005,
        help='Grid resolution for CBEFS regridding (default: 0.005)'
    )
    
    args = parser.parse_args()
    logger.debug(f"Parsed arguments: {vars(args)}")
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
        
        # Download S3 file to local input directory
        bucket_name, s3_path = AWSUtils.parse_s3_path(args.input_s3)
        s3_client = AWSUtils.get_s3_client(role_arn=args.role_arn, bucket_name=bucket_name)
        
        # Get filename from S3 path
        input_filename = os.path.basename(s3_path)
        local_input_path = os.path.join("input", input_filename)
        
        logger.debug(f"Downloading {args.input_s3} to {local_input_path}")
        s3_client.download_file(bucket_name, s3_path, local_input_path)
        
        # Import and run CBEFS preprocessor
        from czdt_iss_transformers.preprocessors.cbefs.cbefs_preprocessor import main as cbefs_main
        
        # Create args object for CBEFS preprocessor
        class CBEFSArgs:
            def __init__(self):
                self.url = local_input_path  # Use downloaded file as URL/path
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
        
        # Return the first file for the pipeline (CBEFS creates time-sliced files)
        return preprocessed_files[0]
        
    except Exception as e:
        logger.error(f"CBEFS preprocessing failed: {e}")
        raise RuntimeError(f"CBEFS preprocessing failed: {e}")

def run_localized_pipeline(preprocessed_file: str, original_args):
    """
    Run the main localized pipeline with the preprocessed file.
    """
    logger.info(f"Running localized pipeline with preprocessed file: {preprocessed_file}")
    
    # Build command for localized pipeline
    pipeline_script = os.path.join(os.path.dirname(__file__), 'localized_pipeline.py')
    cmd = [
        sys.executable, pipeline_script,
        '--input-netcdf', preprocessed_file,
        '--collection-id', original_args.collection_id,
        '--zarr-config-url', original_args.zarr_config_url,
        '--maap-host', original_args.maap_host,
        '--mmgis-host', original_args.mmgis_host,
        '--titiler-token-secret-name', original_args.titiler_token_secret_name,
        '--cmss-logger-host', original_args.cmss_logger_host,
    ]
    
    # Add role-arn if provided
    if hasattr(original_args, 'role_arn') and original_args.role_arn:
        cmd.extend(['--role-arn', original_args.role_arn])
    
    # Add variables if provided
    if hasattr(original_args, 'variables') and original_args.variables:
        cmd.extend(['--variables', original_args.variables])
    
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
    Main function orchestrating the CBEFS preprocessing pipeline.
    """
    logger.debug("Starting CBEFS preprocessing pipeline main function")
    args = parse_arguments()
    
    try:
        # Validate arguments
        ConfigUtils.validate_arguments(args)
        
        logging.info("Processing CBEFS input with preprocessing followed by full pipeline")
        
        # Step 1: Run CBEFS preprocessor
        preprocessed_file = run_cbefs_preprocessor(args)
        
        # Step 2: Run the main localized pipeline with the first preprocessed file
        run_localized_pipeline(preprocessed_file, args)
        
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