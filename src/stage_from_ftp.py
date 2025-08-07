import argparse
import os
import sys
import logging
import ftplib 
from typing import List
from botocore.exceptions import ClientError
from common_utils import (
    DownloadError
)

# Configure basic logging to provide feedback on the script's progress and any errors.
# The logging level can be adjusted (e.g., to logging.DEBUG for more verbose output).
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')

# --- Custom Exceptions ---
class FtpFileNotFoundError(Exception):
    """Custom exception for when a ftp file is not found via keyword search."""
    pass

# --- Argument Parsing ---
def parse_arguments():
    """
    Defines and parses command-line arguments for the script.
    Returns:
        argparse.Namespace: An object containing the parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(description='Search for a batch of ftp files, download them, and upload them into an AWS S3 bucket.')

    parser.add_argument("--ftp-server", required=True,
                        help="The ftp server to use for the file downloads (e.g., floodlight.ssec.wisc.edu).")
    parser.add_argument("--area-of-interest", required=True,
                        help="The area of interest to filter on within the ftp files.")
    parser.add_argument("--local-download-path", required=False, default="output",
                        help="Local directory path where the granule will be temporarily downloaded.")
    
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
    local_download_dir: str
    ) -> List[str]:
    """
    Searches for ftp files containing the specified keyword and downloads them to the local directory.

    Args:
        ftp_server (str): The ftp server to use for the file downloads.
        area_of_interest (str): The area of interest to search within the ftp files.
        local_download_dir (str): The directory to download the ftp files into.

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

        # Ensure the local download directory exists and is a directory
        if not os.path.exists(local_download_dir):
            logging.info(f"Local download directory '{local_download_dir}' does not exist. Creating it.")
            os.makedirs(local_download_dir, exist_ok=True)
        elif not os.path.isdir(local_download_dir):
            raise ValueError(f"The specified local download path '{local_download_dir}' exists but is not a directory.")

        local_files = []

        for filename in file_results:
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


# --- Main Execution Block ---
def main():
    """
    Main function to orchestrate the ftp search, download, S3 upload, and cleanup.
    Handles argument parsing and top-level error management.
    """
    args = parse_arguments()

    try:
        # Search for and download the ftp files
        downloaded_file_paths = search_and_download_ftp_files(
            args.ftp_server,
            args.area_of_interest,
            args.local_download_path
        )

        logging.info(f"FTP file processing completed successfully: {downloaded_file_paths}")

    except FtpFileNotFoundError as e:
        logging.error(f"TERMINATED: FTP file not found. Details: {e}")
        sys.exit(2)  # Specific exit code for file not found
    except DownloadError as e:
        logging.error(f"TERMINATED: Download failed. Details: {e}")
        sys.exit(3)  # Specific exit code for download error
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


if __name__ == "__main__":
    main()
