import argparse
import os
import sys
import logging
import boto3
from botocore.exceptions import ClientError
from maap.maap import MAAP  # Confirmed import for maap-py
from common_utils import (
    AWSUtils, MaapUtils, ConfigUtils,
    UploadError, DownloadError, GranuleNotFoundError
)

# Configure basic logging to provide feedback on the script's progress and any errors.
# The logging level can be adjusted (e.g., to logging.DEBUG for more verbose output).
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')


# --- Argument Parsing ---
def parse_arguments():
    """
    Defines and parses command-line arguments for the script.
    Returns:
        argparse.Namespace: An object containing the parsed command-line arguments.
    """
    parser = ConfigUtils.get_common_argument_parser()
    parser.description = "Search for a MAAP granule, download it, and upload it to an AWS S3 bucket."
    
    # Add script-specific arguments
    parser.add_argument("--local-download-path", required=False, default="output",
                        help="Local directory path where the granule will be temporarily downloaded.")
    
    return parser.parse_args()



def search_and_download_granule(maap_client: MAAP, granule_id: str, collection_id: str, local_download_dir: str) -> str:
    """
    Searches for the specified granule using the MAAP client and downloads it to the local directory.

    Args:
        maap_client (MAAP): The initialized MAAP client.
        granule_id (str): The granule identifier (e.g., producer_granule_id or Granule UR).
        collection_id (str): The MAAP collection concept ID.
        local_download_dir (str): The directory to download the granule into.

    Returns:
        str: The full path to the locally downloaded granule file.

    Raises:
        GranuleNotFoundError: If the granule cannot be found.
        DownloadError: If any error occurs during the download process.
        ValueError: If the local download path is invalid.
    """
    logging.info(f"Searching for granule ID '{granule_id}' in collection '{collection_id}'...")
    try:
        search_results = maap_client.searchGranule(cmr_host="cmr.earthdata.nasa.gov",
                                                   concept_id=collection_id,
                                                   readable_granule_name=granule_id, limit=2)

        if not search_results:
            raise GranuleNotFoundError(f"Granule '{granule_id}' not found in collection '{collection_id}'.")
        if len(search_results) > 1:
            logging.warning(f"Multiple granules found for '{granule_id}' in '{collection_id}'. Using the first result.")
            # Potentially add logic here to select the correct one if ambiguity is common.

        granule_metadata = search_results[0]
        granule_ur = granule_metadata.get('Granule', {}).get('GranuleUR')

        if not granule_ur:
            raise GranuleNotFoundError(
                f"Could not determine GranuleUR for '{granule_id}'. Metadata received: {granule_metadata}")

        logging.info(f"Granule found: {granule_ur}. Preparing to download.")

        # Ensure the local download directory exists and is a directory
        if not os.path.exists(local_download_dir):
            logging.info(f"Local download directory '{local_download_dir}' does not exist. Creating it.")
            os.makedirs(local_download_dir, exist_ok=True)
        elif not os.path.isdir(local_download_dir):
            raise ValueError(f"The specified local download path '{local_download_dir}' exists but is not a directory.")

        # Attempt to download using MAAP.getGranule()
        logging.info(f"Attempting download of '{granule_ur}' to '{local_download_dir}' using maap.getGranule().")
        # The maap.getGranule method should download the file and return its local path.
        # Behavior might vary slightly by maap-py version.
        granule_url = granule_metadata.get("Granule", {}).get("OnlineAccessURLs").get("OnlineAccessURL")[0]['URL']
        downloaded_file_path_or_status = maap_client.downloadGranule(online_access_url=granule_url,
                                                                     destination_path=local_download_dir)

        # Interpret the result of getGranule
        actual_downloaded_path = None
        if isinstance(downloaded_file_path_or_status, str) and os.path.exists(downloaded_file_path_or_status):
            actual_downloaded_path = downloaded_file_path_or_status
            logging.info(f"Granule successfully downloaded by getGranule to: {actual_downloaded_path}")

        if not actual_downloaded_path or not os.path.exists(actual_downloaded_path):
            raise DownloadError(
                f"Download failed for granule '{granule_ur}'. Expected file at '{actual_downloaded_path}' not found.")

        return actual_downloaded_path

    except GranuleNotFoundError:  # Re-raise specific exception
        raise
    except ClientError as e:  # Catch Boto3/AWS related errors if MAAP uses them internally for some S3 access
        logging.error(f"AWS ClientError during MAAP operation for '{granule_id}': {e}", exc_info=True)
        raise DownloadError(f"An AWS ClientError occurred during MAAP operations for '{granule_id}': {e}")
    except Exception as e:  # Catch other exceptions like requests.exceptions.HTTPError
        logging.error(f"An error occurred during granule search or download for '{granule_id}': {e}", exc_info=True)
        raise DownloadError(f"Failed to search or download granule '{granule_id}': {e}")

def upload_to_s3(s3_client, local_file_path: str, bucket_name: str, s3_prefix: str, collection_id: str):
    """
    Uploads the specified local file to an S3 bucket. The file is placed under a
    constructed key: <s3_prefix>/<collection_id>/<filename>.

    Args:
        s3_client (boto3.client): The S3 client to use for the upload.
        local_file_path (str): The path to the local file to be uploaded.
        bucket_name (str): The name of the S3 bucket.
        s3_prefix (str): The S3 prefix (acts like a folder). Can be empty.
        collection_id (str): The collection ID, used to create a subfolder in S3.

    Raises:
        FileNotFoundError: If the local file does not exist.
        UploadError: If the S3 upload fails.
    """
    file_name = os.path.basename(local_file_path)

    # Construct the S3 object key, ensuring no leading/trailing slashes are mishandled.
    s3_key_parts = []
    if s3_prefix:
        s3_key_parts.append(s3_prefix.strip('/'))  # Remove slashes to prevent issues
    s3_key_parts.append(collection_id.strip('/'))  # Collection ID as a folder
    s3_key_parts.append(file_name)  # The actual filename

    # Join parts with '/', filtering out any empty strings (e.g., if s3_prefix was empty)
    s3_key = "/".join(part for part in s3_key_parts if part)

    return AWSUtils.upload_to_s3(local_file_path, bucket_name, s3_key, s3_client)



# --- Main Execution Block ---
def main():
    """
    Main function to orchestrate the granule search, download, S3 upload, and cleanup.
    Handles argument parsing and top-level error management.
    """
    args = parse_arguments()
    downloaded_granule_path = None  # Initialize to ensure it's defined for the finally block

    try:
        # Step 1: Initialize MAAP client
        # The MAAP host can be overridden by the MAAP_API_HOST environment variable if maap-py supports it,
        # otherwise, it uses the --maap-host argument.
        maap_host_to_use = os.environ.get('MAAP_API_HOST', args.maap_host)
        maap = MaapUtils.get_maap_instance(maap_host_to_use)

        # Step 2: Search for and download the granule
        downloaded_granule_path = search_and_download_granule(
            maap,
            args.granule_id,
            args.collection_id,
            args.local_download_path
        )

        # Step 3: Get S3 client (with optional role assumption)
        # Determine AWS region (can be None to use default configured for boto3)
        aws_region_for_s3 = os.environ.get('AWS_REGION', 'us-west-2')  # Or get from args if you add it
        s3_client = AWSUtils.get_s3_client(role_arn=args.role_arn, aws_region=aws_region_for_s3)

        # Step 4: Upload the downloaded granule to S3
        upload_to_s3(
            s3_client,
            downloaded_granule_path,
            args.s3_bucket,
            args.s3_prefix,
            args.collection_id
        )

        logging.info("Granule processing and upload completed successfully!")

    except GranuleNotFoundError as e:
        logging.error(f"TERMINATED: Granule not found. Details: {e}")
        sys.exit(2)  # Specific exit code for granule not found
    except DownloadError as e:
        logging.error(f"TERMINATED: Download failed. Details: {e}")
        sys.exit(3)  # Specific exit code for download error
    except UploadError as e:
        logging.error(f"TERMINATED: S3 upload failed. Details: {e}")
        # IMPORTANT: Local file is NOT cleaned up if upload fails, for inspection.
        sys.exit(4)  # Specific exit code for upload error
    except FileNotFoundError as e:  # Can be raised by upload_to_s3 if local file disappears
        logging.error(f"TERMINATED: File operation error. Details: {e}")
        sys.exit(5)
    except ValueError as e:  # E.g. invalid local_download_path
        logging.error(f"TERMINATED: Invalid argument or value. Details: {e}")
        sys.exit(6)
    except RuntimeError as e:  # E.g. MAAP client init failed
        logging.error(f"TERMINATED: Runtime error. Details: {e}")
        sys.exit(7)
    except Exception as e:
        logging.error(f"TERMINATED: An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)  # General error
    finally:
        # Step 5: Cleanup the local file if it was downloaded AND upload was successful.
        # Check if an exception occurred. If so, and it's an UploadError, don't delete.
        # If no exception, or if the exception was something else (like a later part of script, not here),
        # and downloaded_granule_path is set, then cleanup.
        current_exception = sys.exc_info()[1]
        if downloaded_granule_path and isinstance(current_exception, UploadError):
            logging.warning(
                f"Upload failed. Inspect the file '{downloaded_granule_path}' for errors.")


if __name__ == "__main__":
    main()
