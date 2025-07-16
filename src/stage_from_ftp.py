import argparse
import os
import sys
import logging
import boto3
import ftplib 
from typing import List
from botocore.exceptions import ClientError

# Configure basic logging to provide feedback on the script's progress and any errors.
# The logging level can be adjusted (e.g., to logging.DEBUG for more verbose output).
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')


# --- Custom Exceptions ---
class FtpFileNotFoundError(Exception):
    """Custom exception for when a ftp file is not found via keyword search."""
    pass

class DownloadError(Exception):
    """Custom exception for errors encountered during ftp download."""
    pass


class UploadError(Exception):
    """Custom exception for errors encountered during S3 upload."""
    pass


# --- Argument Parsing ---
def parse_arguments():
    """
    Defines and parses command-line arguments for the script.
    Returns:
        argparse.Namespace: An object containing the parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(
        description="Search for a batch of ftp files, download them, and upload them into an AWS S3 bucket.")
    parser.add_argument("--ftp-server", required=True,
                        help="The ftp server to use for the file downloads (e.g., floodlight.ssec.wisc.edu).")
    parser.add_argument("--area-of-interest", required=True,
                        help="The area of interest to filter on within the ftp files.")
    parser.add_argument("--s3-bucket", required=True,
                        help="The name of the target S3 bucket for uploading the ftp files.")
    parser.add_argument("--s3-prefix", default="",
                        help="Optional S3 prefix (folder path) within the bucket. Do not use leading/trailing slashes. "
                             "The ftp files will be placed under <s3-prefix>/<ftp-server>/<filename>.")
    parser.add_argument("--local-download-path", required=False, default="output",
                        help="Local directory path where the files will be temporarily downloaded.")
    parser.add_argument("--role-arn",
                        help="Optional AWS IAM Role ARN to assume for S3 upload. "
                             "Useful for cross-account S3 bucket access.")
    parser.add_argument("--cmss-logger-host", required=True,
                        help="Host for logging pipeline messages")
    parser.add_argument("--mmgis-host", required=True,
                        help="Host for cataloging STAC items")
    parser.add_argument("--titiler-token-secret-name", required=True,
                        help="MAAP secret name for MMGIS host token")
    parser.add_argument("--overwrite-existing", default="false",
                        help="If set to true, ftp files will be processed and re-uploaded even if already previously uploaded.")
    return parser.parse_args()


def _process_line(line: str, keywords: List[str], matches: List[str]) -> None:
    """
    Processes a line from the FTP LIST output, extracting matching names.

    Args:
        line: A line of text from the FTP LIST command.
        keywords: a list of required keywords within ftp file name.
        matches: A list to append file names to.
    """    
    
    parts = line.split(maxsplit=8)
    if len(parts) > 1 and all(x in parts[-1] for x in keywords):
        matches.append(parts[-1])


def list_ftp_files(ftp: ftplib.FTP, path: str, keywords: List[str]) -> List[str]:
    """
    Lists only matching files within a given FTP path.

    Args:
        ftp: An initialized ftplib.FTP object.
        path: The path to list.
        keywords: a list of required keywords within ftp file name.

    Returns:
        A list of matching ftp file names.
    """
    matches = []
    
    try:
        ftp.cwd(path)
        ftp.retrlines('LIST', lambda line: _process_line(line, keywords, matches))
    except ftplib.error_perm as e:
        print(f"Error: {e}")
    return matches


def search_and_download_ftp_files(
    ftp_server: str, 
    area_of_interest: str, 
    local_download_dir: str, 
    overwrite_existing: bool,
    s3_client, 
    bucket_name: str, 
    s3_prefix: str
    ) -> List[str]:
    """
    Searches for ftp files containing the specified keyword and downloads them to the local directory.

    Args:
        ftp_server (str): The ftp server to use for the file downloads.
        area_of_interest (str): The area of interest to search within the ftp files.
        local_download_dir (str): The directory to download the ftp files into.
        overwrite_existing (bool): Whether to re-upload if already previously uploaded.
        s3_client (boto3.client): The S3 client to use for the upload.
        bucket_name (str): The name of the S3 bucket.
        s3_prefix (str): The S3 prefix (acts like a folder). Can be empty.

    Returns:
        str: The full path to the locally downloaded ftp files.

    Raises:
        DownloadError: If any error occurs during the download process.
        ValueError: If the local download path is invalid.
    """
    logging.info(f"Searching for file files containing '{area_of_interest}' in server '{ftp_server}'...")
    ftp = ftplib.FTP(ftp_server)

    try:
        ftp.login(user="anonymous", passwd="anonymous@domain.com")
        path = "/composite"

        keywords = [area_of_interest, "."]
        
        file_results = list_ftp_files(ftp, path, keywords)

        if not file_results:
            raise FtpFileNotFoundError(f"Keyword '{area_of_interest}' yielded no results in '{ftp_server}'.")

        logging.info(f"{len(file_results)} file(s) found.")

        if overwrite_existing:
            logging.info("Overwrite = true. Preparing to download...")
        else:
            logging.info("Overwrite = false. Checking for previous non-uploaded files...")  

        # Ensure the local download directory exists and is a directory
        if not os.path.exists(local_download_dir):
            logging.info(f"Local download directory '{local_download_dir}' does not exist. Creating it.")
            os.makedirs(local_download_dir, exist_ok=True)
        elif not os.path.isdir(local_download_dir):
            raise ValueError(f"The specified local download path '{local_download_dir}' exists but is not a directory.")

        local_files = []

        for filename in file_results: 
            if not overwrite_existing:
                s3_key = get_s3_key(s3_prefix, ftp_server, filename)
                file_exists = s3_file_exists(s3_client, bucket_name, s3_key)

                if file_exists:
                    logging.info(f"Previous upload found for {filename}. Skipping.")
                    continue

            # Attempt to download using ftp.retrbinary()
            ftp.retrbinary("RETR " + filename ,open(f"{local_download_dir}/{filename}", 'wb').write)
            local_files.append(f"{local_download_dir}/{filename}")

        return local_files

    except FtpFileNotFoundError:  # Re-raise specific exception
        raise
    except ClientError as e:  # Catch Boto3/AWS related errors if MAAP uses them internally for some S3 access
        logging.error(f"AWS ClientError during MAAP operation for '{area_of_interest}': {e}", exc_info=True)
        raise DownloadError(f"An AWS ClientError occurred during ftp operations for aoi '{area_of_interest}': {e}")
    except Exception as e:  # Catch other exceptions like requests.exceptions.HTTPError
        logging.error(f"An error occurred during ftp search or download for aoi '{area_of_interest}': {e}", exc_info=True)
        raise DownloadError(f"Failed to search or download file for aoi '{area_of_interest}': {e}")
    finally:
        ftp.quit()


# --- AWS S3 Operations ---
def get_s3_client(role_arn: str = None, aws_region: str = None):
    """
    Creates and returns a Boto3 S3 client. If a role_arn is provided,
    it attempts to assume this role.

    Args:
        role_arn (str, optional): The ARN of the IAM role to assume. Defaults to None.
        aws_region (str, optional): The AWS region for the STS and S3 clients.
                                   If None, uses the default region from AWS config.

    Returns:
        boto3.client: An S3 client instance.

    Raises:
        UploadError: If role assumption fails.
    """
    if role_arn:
        try:
            logging.info(f"Attempting to assume IAM role: {role_arn}")
            sts_client = boto3.client('sts', region_name=aws_region)
            # Define a unique session name for the assumed role session
            role_session_name = f"MAAPGranuleUpload-{os.getpid()}"
            assumed_role_object = sts_client.assume_role(
                RoleArn=role_arn,
                RoleSessionName=role_session_name
            )
            credentials = assumed_role_object['Credentials']
            s3_client = boto3.client(
                's3',
                aws_access_key_id=credentials['AccessKeyId'],
                aws_secret_access_key=credentials['SecretAccessKey'],
                aws_session_token=credentials['SessionToken'],
                region_name=aws_region  # Pass region to S3 client as well
            )
            logging.info(f"Successfully assumed role '{role_arn}' and created S3 client.")
            return s3_client
        except ClientError as e:
            logging.error(f"Failed to assume AWS role '{role_arn}': {e}", exc_info=True)
            raise UploadError(f"AWS role assumption failed for {role_arn}: {e}")
    else:
        logging.info("Using default AWS credentials to create S3 client.")
        return boto3.client('s3', region_name=aws_region)


def upload_to_s3(s3_client, local_file_paths: List[str], bucket_name: str, s3_prefix: str, ftp_server: str):
    """
    Uploads the specified local files to an S3 bucket. The files are placed under a
    constructed key: <s3_prefix>/<ftp_server>/<filename>.

    Args:
        s3_client (boto3.client): The S3 client to use for the upload.
        local_file_paths (str): The paths to the local files to be uploaded.
        bucket_name (str): The name of the S3 bucket.
        s3_prefix (str): The S3 prefix (acts like a folder). Can be empty.
        ftp_server (str): The ftp server, used to create a subfolder in S3.

    Raises:
        FileNotFoundError: If the local file does not exist.
        UploadError: If the S3 upload fails.
    """

    for local_file_path in local_file_paths:
        if not os.path.exists(local_file_path):
            raise FileNotFoundError(f"Local file '{local_file_path}' intended for upload does not exist.")

        file_name = os.path.basename(local_file_path)
        s3_key = get_s3_key(s3_prefix, ftp_server, file_name)

        logging.info(f"Starting upload of '{local_file_path}' to S3 bucket '{bucket_name}' with key '{s3_key}'.")

        try:
            s3_client.upload_file(local_file_path, bucket_name, s3_key)
            logging.info(f"Successfully uploaded '{file_name}' to 's3://{bucket_name}/{s3_key}'.")
        except ClientError as e:
            logging.error(f"Failed to upload '{local_file_path}' to S3 (s3://{bucket_name}/{s3_key}): {e}", exc_info=True)
            raise UploadError(f"S3 upload of {local_file_path} failed: {e}")
        except Exception as e:  # Catch other potential errors (e.g., file read issues not caught by boto3)
            logging.error(f"An unexpected error occurred during S3 upload of '{local_file_path}': {e}", exc_info=True)
            raise UploadError(f"Unexpected error during S3 upload of {local_file_path}: {e}")


def get_s3_key(s3_prefix: str, ftp_server: str, file_name: str):
    """
    Get the full s3 key for a given file name.

    Args:
        s3_prefix (str): The S3 prefix (acts like a folder). Can be empty.
        ftp_server (str): The ftp server, used to create a subfolder in S3.
        file_name (str): The file name.
    """  
    # Construct the S3 object key, ensuring no leading/trailing slashes are mishandled.
    s3_key_parts = []
    if s3_prefix:
        s3_key_parts.append(s3_prefix.strip('/'))  # Remove slashes to prevent issues
    s3_key_parts.append(f"ftp_{ftp_server.replace('.', '-')}")  # ftp server as a folder
    s3_key_parts.append(file_name)  # The actual filename

    # Join parts with '/', filtering out any empty strings (e.g., if s3_prefix was empty)
    s3_key = "/".join(part for part in s3_key_parts if part)

    return s3_key


def s3_file_exists(s3_client, bucket_name: str, key: str):
    """
    Checks if an s3 file exists.

    Args:
        s3_client (boto3.client): The S3 client to use for the upload.
        bucket_name (str): The name of the S3 bucket.
        key: The s3 file key.
    """  
    try:
        s3_client.head_object(Bucket=bucket_name, Key=key)
        return True
    except:
        return False


# --- File Cleanup ---
def cleanup_local_files(local_file_paths: List[str]):
    """
    Deletes the specified local files. Logs a warning if deletion fails.

    Args:
        local_file_paths (List[str]): The paths to the local files to be deleted.
    """
    for local_file_path in local_file_paths:
        if not local_file_path or not isinstance(local_file_path, str):
            logging.warning("Invalid or empty file path provided for cleanup. Skipping.")
            return

        if os.path.exists(local_file_path):
            try:
                os.remove(local_file_path)
                logging.info(f"Successfully deleted local file: {local_file_path}")
            except OSError as e:
                logging.warning(f"Could not delete local file '{local_file_path}': {e}", exc_info=True)
        else:
            # This is not an error; file might have been cleaned up already or never created due to prior failure.
            logging.info(
                f"Local file '{local_file_path}' not found for cleanup (possibly already deleted or download failed).")


# --- Main Execution Block ---
def main():
    """
    Main function to orchestrate the ftp search, download, S3 upload, and cleanup.
    Handles argument parsing and top-level error management.
    """
    args = parse_arguments()
    downloaded_file_paths = []  # Initialize to ensure it's defined for the finally block
    true = "true"

    try:
        # Step 1: Get S3 client (with optional role assumption)
        # Determine AWS region (can be None to use default configured for boto3)
        aws_region_for_s3 = os.environ.get('AWS_REGION', 'us-west-2')  # Or get from args if you add it
        s3_client = get_s3_client(role_arn=args.role_arn, aws_region=aws_region_for_s3)

        _overwrite_existing = args.overwrite_existing.casefold() == true.casefold()

        # Step 2: Search for and download the ftp files
        downloaded_file_paths = search_and_download_ftp_files(
            args.ftp_server,
            args.area_of_interest,
            args.local_download_path,
            _overwrite_existing,
            s3_client,
            args.s3_bucket,
            args.s3_prefix
        )

        # Step 3: Upload the downloaded ftp files to S3
        upload_to_s3(
            s3_client,
            downloaded_file_paths,
            args.s3_bucket,
            args.s3_prefix,
            args.ftp_server
        )

        logging.info("FTP file processing and upload completed successfully!")

    except FtpFileNotFoundError as e:
        logging.error(f"TERMINATED: FTP file not found. Details: {e}")
        sys.exit(2)  # Specific exit code for file not found
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
    except RuntimeError as e:  # E.g. client library init failed
        logging.error(f"TERMINATED: Runtime error. Details: {e}")
        sys.exit(7)
    except Exception as e:
        logging.error(f"TERMINATED: An unexpected error occurred: {e}", exc_info=True)
        sys.exit(1)  # General error
    finally:
        # Step 4: Cleanup the local files if they were downloaded AND upload was successful.
        # Check if an exception occurred. If so, and it's an UploadError, don't delete.
        # If no exception, or if the exception was something else (like a later part of script, not here),
        # and downloaded_file_paths is set, then cleanup.
        current_exception = sys.exc_info()[1]
        if downloaded_file_paths and not isinstance(current_exception, UploadError):
            if current_exception:  # An error occurred, but it wasn't an UploadError
                logging.info(
                    f"An error ({type(current_exception).__name__}) occurred, but attempting cleanup of downloaded "
                    f"file as it was not an UploadError.")
            else:  # No error occurred, proceed with cleanup
                logging.info("Process completed (or an error not related to upload occurred), cleaning up local file.")
            cleanup_local_files(downloaded_file_paths)
        elif downloaded_file_paths and isinstance(current_exception, UploadError):
            logging.warning(
                f"Upload failed. Local files '{downloaded_file_paths}' will NOT be deleted to allow for inspection.")


if __name__ == "__main__":
    main()
