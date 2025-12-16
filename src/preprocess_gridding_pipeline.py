#!/usr/bin/env python3
"""
Gridding Preprocessing Pipeline Script

This script runs Gridding data preprocessing followed by the full localized pipeline.
Designed to be run as a separate MAAP algorithm for Gridding data processing.
"""

import os
import sys
import logging
import subprocess
from stage_from_daac import search_and_download_granule

from common_utils import (
    MaapUtils, LoggingUtils, ConfigUtils, AWSUtils
)

# Configure logging: DEBUG for this module, INFO for dependencies
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

def parse_arguments():
    """
    Defines and parses command-line arguments for the Gridding preprocessing pipeline script.
    """
    logger.debug("Starting argument parsing")
    parser = ConfigUtils.get_generic_argument_parser()
    
    # The --input-s3 argument is already defined in the generic parser
    # No need to redefine it here

    parser.add_argument(
        '--gridding-config-url',
        nargs='+',
        help='Path to config file (should be local). If more than one files are provided, the gridding preprocessor '
             'will be executed for all configurations and the results will be combined by the method in the '
             '--combine parameter'
    )

    parser.add_argument(
        '--concept-id',
        type=str,
        default='',
        help='Concept ID for zarr2cog and catalog operations (overrides collection-id for these steps)'
    )

    def _extent_type(s):
        parts = s.split(' ')

        if len(parts) != 4:
            raise ValueError(f'Unexpected number of fields in --output-extent param: expected 4, get {len(parts)}')

        try:
            min_lon, min_lat, max_lon, max_lat = [float(p) for p in parts]
        except ValueError as e:
            raise ValueError(f'Non-float part in --output-extent param: {e}')

        for lon in [min_lon, max_lon]:
            if lon < -180 or lon > 180:
                raise ValueError(f'Longitude {lon} is outside of [-180, 180]')

        for lat in [min_lat, max_lat]:
            if lat < -90 or lat > 90:
                raise ValueError(f'Latitude {lat} is outside of [-90, 90]')

        if min_lon >= max_lon:
            raise ValueError(f'Extent minimum longitude is greater than or equal to maximum longitude')

        if min_lat >= max_lat:
            raise ValueError(f'Extent minimum latitude is greater than or equal to maximum latitude')

        return min_lon, min_lat, max_lon, max_lat

    parser.add_argument(
        '--output-extent',
        required=False,
        default=(-180, -90, 180, 90),
        type=_extent_type,
        help='Output extent of gridded dataset. Must be comma-separated real numbers in order '
             'min-lon,min-lat,max-lon,max-lat. Longitudes must be in [-180, 180] and latitudes in [-90, 90]. Minimum '
             'extent values must be less than maximum extent values.'
    )

    resolution_group = parser.add_mutually_exclusive_group()

    def _resolution_deg_type(s):
        v = float(s)

        if v <=0:
            raise ValueError(f'Resolution in degrees must be greater than or equal to zero')

        return v

    resolution_group.add_argument(
        '--grid-resolution',
        default=None,
        type=_resolution_deg_type,
        help='Output grid resolution in degrees'
    )

    resolution_group.add_argument(
        '--grid-size',
        default=None,
        type=int,
        nargs=2,
        help='Output grid size in pixels. X Y'
    )

    parser.add_argument(
        '-f', '--format',
        default='netcdf',
        choices=['netcdf', 'zarr'],
        help='Output file format. Either netcdf or zarr'
    )

    parser.add_argument(
        '-v', '--zarr-version',
        type=int,
        choices=[2, 3],
        default=3,
        help='Version of zarr standard to output. Only applies if --format=zarr, otherwise does nothing.'
    )

    parser.add_argument(
        '--input-url',
        type=str,
        default='',
        help='Input url. Clone of input_s3.'
    )

    parser.add_argument(
        '--pattern',
        default='*.nc',
        help='Glob pattern to match input files. Important if input is a directory with non-input files present'
    )

    parser.add_argument(
        '--config',
        type=str,
        default='',
        help='Path to config file (should be local). If more than one files are provided, the gridding preprocessor '
             'will be executed for all configurations and the results will be combined by the method in the '
             '--combine parameter'
    )

    parser.add_argument(
        '--output',
        default='gridded',
        help='Name of output file/store',
        required=False
    )

    parser.add_argument(
        '-c', '--combine',
        choices=['sum', 'mean', 'median', 'min', 'max'],
        default='mean',
        help='Method to combine the output datasets. Can be sum, mean, median, min, max'
    )

    args = parser.parse_args()
    logger.debug(f"Parsed arguments: {vars(args)}")
    return args

def run_gridding_preprocessor(args, input = None) -> str:
    """
    Run Gridding preprocessor on the input S3 file.
    
    Returns:
        str: Path to the preprocessed file
    """
    logger.info(f"Gridding_PREPROCESS - Args: input_s3='{args.input_s3}'")
    logger.debug(f"Starting Gridding preprocessing for input: {args.input_s3}")
    
    try:
        # Create input and output directories
        os.makedirs("input", exist_ok=True)
        os.makedirs("output", exist_ok=True)

        if args.config:
            args.config = args.config.split(",")

        # if input:
        #     input_filename = input
        # else:    
        #     # Download S3 file to local input directory  
        #     bucket_name, s3_path = AWSUtils.parse_s3_path(args.input_s3)
            
        #     # Get filename from S3 path
        #     input_filename = os.path.basename(s3_path)
        #     local_input_path = os.path.join("input", input_filename)
            
        #     logger.debug(f"Downloading {args.input_s3} to {local_input_path}")
        #     AWSUtils.download_s3_prefix(bucket_name, s3_path, local_input_path)
        
        # Import and run Gridding preprocessor
        from czdt_iss_transformers.preprocessors.gridding.gridding_preprocessor import main
        
        # Generate output filename
        # base_name = os.path.splitext(input_filename)[0]
        # output_filename = f"{base_name}output.nc" # TODO: derive file type from full path
        local_output_path = os.path.join("output", f"{args.local_download_path}.nc")

        logger.debug("Running Gridding preprocessor...")
        main(args)
        logger.debug(f"Gridding preprocessing completed successfully")

        return local_output_path
        
    except Exception as e:
        logger.error(f"Gridding preprocessing failed: {e}")
        raise RuntimeError(f"Gridding preprocessing failed: {e}")

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
    
# TODO: Move this into stage_from_daac.py
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
    logger.info(f"STAGE_FROM_DAAC - Args: granule_id='{args.granule_id}', collection_id='{args.collection_id}', local_download_path='{getattr(args, 'local_download_path', 'output')}'")
    logger.debug(f"Starting DAAC staging for granule '{args.granule_id}' in collection '{args.collection_id}'")
    
    # Set local download directory
    local_download_dir = getattr(args, 'local_download_path', 'output')
    
    # Search and download granule using imported function
    downloaded_file_path = search_and_download_granule(
        maap, args.granule_id, args.collection_id, local_download_dir
    )
    
    logger.debug(f"DAAC staging completed successfully, local file: {downloaded_file_path}")
    return downloaded_file_path


def main():
    """
    Main function orchestrating the Gridding preprocessing pipeline.
    """
    logger.debug("Starting Gridding preprocessing pipeline main function")
    args = parse_arguments()
    
    try:
        # Validate arguments
        ConfigUtils.validate_arguments(args)

        input_type = ConfigUtils.detect_input_type(args)

        current_output = None

        if input_type == "daac":
            maap_host_to_use = os.environ.get('MAAP_API_HOST', args.maap_host)
            maap = MaapUtils.get_maap_instance(maap_host_to_use)
            current_output = stage_from_daac_local(args, maap)     
            logger.debug(f"DAAC input: {current_output}")
            args.input_url = current_output 
            logger.debug(f"args.input_url: {args.input_url}")

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

        logging.info("Processing Gridding input with preprocessing followed by full pipeline")

        # Step 1: Run Gridding preprocessor
        preprocessed_file = run_gridding_preprocessor(args, current_output)
        
        # Step 2: Run the main localized pipeline with the preprocessed file
        run_localized_pipeline(preprocessed_file, args)
        
        logging.info("Gridding preprocessing pipeline completed successfully!")
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