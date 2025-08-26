"""
Common utility functions for the CZDT ISS Ingest Job pipeline.

This module consolidates duplicated functions across the codebase into organized utility classes.
"""

import os
import re
import logging
import argparse
import boto3
from typing import Optional, Tuple, List, Dict, Any
from botocore.exceptions import ClientError, NoCredentialsError
from maap.maap import MAAP
from maap.dps.dps_job import DPSJob
import json
import requests
from pathlib import Path


class AWSUtils:
    """AWS-related utility functions for S3 operations and client management."""
    
    @staticmethod
    def get_s3_client(role_arn: str = None, aws_region: str = None):
        """
        Create and return an S3 client with optional role assumption.
        
        Args:
            role_arn: Optional ARN of the role to assume
            aws_region: AWS region for the client
            
        Returns:
            boto3 S3 client instance
        """
        if role_arn:
            try:
                sts_client = boto3.client('sts')
                assumed_role = sts_client.assume_role(
                    RoleArn=role_arn,
                    RoleSessionName=f"czdt-iss-session-{os.getpid()}"
                )
                credentials = assumed_role['Credentials']
                
                return boto3.client(
                    's3',
                    region_name=aws_region,
                    aws_access_key_id=credentials['AccessKeyId'],
                    aws_secret_access_key=credentials['SecretAccessKey'],
                    aws_session_token=credentials['SessionToken']
                )
            except Exception as e:
                logging.error(f"Failed to assume role {role_arn}: {e}")
                raise
        else:
            return boto3.client('s3', region_name=aws_region)
    
    @staticmethod
    def parse_s3_path(s3_path: str) -> Tuple[str, str]:
        """
        Parse S3 path into bucket and key components.
        Enhanced version that handles multiple S3 URL formats.
        
        Args:
            s3_path: S3 path in format s3://bucket/key or s3://hostname:port/bucket/key
            
        Returns:
            Tuple of (bucket_name, key)
        """
        if not s3_path.startswith('s3://'):
            raise ValueError(f"Invalid S3 path format: {s3_path}")
        
        # Remove s3:// prefix
        path_without_prefix = s3_path[5:]
        
        # Handle both formats: s3://bucket/key and s3://hostname:port/bucket/key
        if path_without_prefix.startswith(('s3-', 's3.')):
            # Format: s3://s3-region.amazonaws.com:port/bucket/key
            if '/' not in path_without_prefix:
                raise ValueError(f"Invalid S3 path format: {s3_path}")
            
            # Split at first slash after hostname
            hostname_part, remaining_path = path_without_prefix.split('/', 1)
            
            if '/' not in remaining_path:
                # Only bucket, no key
                return remaining_path, ""
            
            bucket, key = remaining_path.split('/', 1)
            return bucket, key
        else:
            # Standard format: s3://bucket/key
            if '/' not in path_without_prefix:
                return path_without_prefix, ""
            
            bucket, key = path_without_prefix.split('/', 1)
            return bucket, key
    
    @staticmethod
    def upload_to_s3(file_path: str, bucket: str, key: str, s3_client=None, role_arn: str = None) -> str:
        """
        Upload a file to S3 with error handling.
        
        Args:
            file_path: Local file path to upload
            bucket: S3 bucket name
            key: S3 key (path within bucket)
            s3_client: Optional existing S3 client
            role_arn: Optional role ARN for authentication
            
        Returns:
            S3 URL of uploaded file
            
        Raises:
            Exception: If upload fails
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        if not s3_client:
            s3_client = AWSUtils.get_s3_client(role_arn=role_arn)
        
        try:
            logging.info(f"Uploading {file_path} to s3://{bucket}/{key}")
            s3_client.upload_file(file_path, bucket, key)
            s3_url = f"s3://{bucket}/{key}"
            logging.info(f"Successfully uploaded to {s3_url}")
            return s3_url
        except Exception as e:
            logging.error(f"Failed to upload {file_path} to s3://{bucket}/{key}: {e}")
            raise

    @staticmethod
    def copy_s3_folder(source_bucket, source_prefix, destination_bucket, destination_prefix, s3_client=None, role_arn: str = None):
        """
        Copies all objects within a specified "folder" (key prefix) from one S3 bucket to another.

        Args:
            source_bucket (str): The name of the source S3 bucket.
            source_prefix (str): The key prefix of the "folder" to copy (e.g., "myfolder/").
            destination_bucket (str): The name of the destination S3 bucket.
            destination_prefix (str): The desired key prefix for the copied objects in the destination bucket (e.g., "newfolder/").
            s3_client: Optional existing S3 client
            role_arn: Optional role ARN for authentication
        """
        
        if not s3_client:
            s3_client = AWSUtils.get_s3_client(role_arn=role_arn)

        # List objects in the source "folder"
        objects_to_copy = []
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=source_bucket, Prefix=source_prefix)

        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    objects_to_copy.append(obj['Key'])

        # Copy each object individually
        for object_key in objects_to_copy:
            # Construct the new key for the destination bucket
            new_object_key = object_key.replace(source_prefix, destination_prefix, 1)

            copy_source = {
                'Bucket': source_bucket,
                'Key': object_key
            }

            try:
                s3_client.copy_object(
                    CopySource=copy_source,
                    Bucket=destination_bucket,
                    Key=new_object_key
                )
                logging.info(f"Successfully copied: {object_key} to {destination_bucket}/{new_object_key}")
            except Exception as e:
                logging.error(f"Error copying {source_bucket}/{object_key} to {destination_bucket}/{new_object_key}: {e}")
    
    @staticmethod
    def file_exists_in_s3(bucket: str, key: str, s3_client=None) -> bool:
        """
        Check if a file exists in S3.
        
        Args:
            bucket: S3 bucket name
            key: S3 key to check
            s3_client: Optional existing S3 client
            
        Returns:
            True if file exists, False otherwise
        """
        if not s3_client:
            s3_client = boto3.client('s3')
        
        try:
            s3_client.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return False
            else:
                raise

    @staticmethod
    def convert_s3_http_to_s3_uri(http_s3_link):
        """
        Converts an S3 HTTP or HTTPS link to an S3 URI.

        Args:
            http_s3_link (str): The S3 HTTP or HTTPS link (e.g.,
                                "https://bucket-name.s3.amazonaws.com/object-key" or
                                "https://s3.amazonaws.com/bucket-name/object-key").

        Returns:
            str: The corresponding S3 URI (e.g., "s3://bucket-name/object-key"),
                or None if the input link is not a valid S3 HTTP/HTTPS link.
        """
        # Regex for path-style URLs: https://s3.amazonaws.com/bucket-name/object-key
        path_style_pattern = r"https?://s3\.amazonaws\.com/([^/]+)/(.*)"
        # Regex for virtual-hosted-style URLs: https://bucket-name.s3.amazonaws.com/object-key
        virtual_hosted_style_pattern = r"https?://([^.]+)\.s3\.amazonaws\.com/(.*)"

        match_path_style = re.match(path_style_pattern, http_s3_link)
        if match_path_style:
            bucket_name = match_path_style.group(1)
            object_key = match_path_style.group(2)
            return f"s3://{bucket_name}/{object_key}"

        match_virtual_hosted_style = re.match(virtual_hosted_style_pattern, http_s3_link)
        if match_virtual_hosted_style:
            bucket_name = match_virtual_hosted_style.group(1)
            object_key = match_virtual_hosted_style.group(2)
            return f"s3://{bucket_name}/{object_key}"

        return None


class MaapUtils:
    """MAAP-related utility functions for client management and operations."""
    
    @staticmethod
    def get_maap_instance(maap_host_url: str) -> MAAP:
        """
        Initialize and return a MAAP client instance.
        
        Args:
            maap_host_url: MAAP host URL
            
        Returns:
            MAAP client instance
            
        Raises:
            RuntimeError: If MAAP client initialization fails
        """
        try:
            logging.info(f"Initializing MAAP client for host: {maap_host_url}")
            maap_client = MAAP(maap_host=maap_host_url)
            logging.info("MAAP client initialized successfully.")
            return maap_client
        except Exception as e:
            logging.error(f"Failed to initialize MAAP instance for host '{maap_host_url}': {e}", exc_info=True)
            raise RuntimeError(f"Could not initialize MAAP instance: {e}")
    
    @staticmethod
    def job_error_message(job) -> str:
        """
        Extract error message from DPS job result.
        
        Args:
            job: DPS job object
            
        Returns:
            Error message string
        """
        if isinstance(job.error_details, str):
            try:
                parsed_error = json.loads(job.error_details)["message"]
                return parsed_error
            except (json.JSONDecodeError, KeyError):
                return job.error_details
        return job.response_code or "Unknown error"
    
    @staticmethod
    def get_job_id() -> str:
        """
        Retrieve current job ID from _job.json file.
        
        Returns:
            Job ID string, empty if not found
        """
        logging.debug("Attempting to retrieve job ID from _job.json")

        if os.path.exists("_job.json"):
            with open("_job.json", 'r') as fr:
                job_info = json.load(fr).get("job_info", {})
                job_id = job_info.get("job_payload", {}).get("payload_task_id", "")
                logging.debug(f"Retrieved job ID: {job_id}")
                return job_id
        return ""

    @staticmethod
    def get_dps_output(jobs: List, file_ext: str, prefixes_only: bool = False) -> List[str]:
        """
        Get DPS job output files from S3.
        Enhanced version that works with multiple DPS jobs.
        
        Args:
            jobs: List of DPS job objects
            file_ext: File extension filter
            prefixes_only: If True, return only prefixes instead of full paths
            
        Returns:
            List of S3 paths or prefixes
        """
        s3 = boto3.resource('s3')
        output = set()
        job_outputs = [next((path for path in j.retrieve_result() if path.startswith("s3")), None) for j in jobs]
        
        for job_output in job_outputs:
            if job_output is None:
                continue
            bucket_name, path = AWSUtils.parse_s3_path(job_output)
            dps_bucket = s3.Bucket(bucket_name)
            for obj in dps_bucket.objects.filter(Prefix=path):
                if prefixes_only:
                    folder_prefix = os.path.dirname(obj.key)
                    if folder_prefix.endswith(file_ext):
                        output.add(f"s3://{bucket_name}/{folder_prefix}")
                else:
                    if obj.key.endswith(file_ext):
                        output.add(f"s3://{bucket_name}/{obj.key}")

        return list(output)

class FileUtils:
    """File system utility functions for file operations and cleanup."""
    
    @staticmethod
    def cleanup_local_file(file_path: str) -> None:
        """
        Remove a local file with error handling.
        
        Args:
            file_path: Path to file to remove
        """
        if os.path.exists(file_path):
            try:
                os.remove(file_path)
                logging.info(f"Cleaned up local file: {file_path}")
            except Exception as e:
                logging.warning(f"Failed to remove {file_path}: {e}")
    
    @staticmethod
    def cleanup_local_files(file_paths: List[str]) -> None:
        """
        Remove multiple local files with error handling.
        
        Args:
            file_paths: List of file paths to remove
        """
        for file_path in file_paths:
            FileUtils.cleanup_local_file(file_path)
    
    @staticmethod
    def ensure_directory_exists(directory_path: str) -> None:
        """
        Create directory if it doesn't exist.
        
        Args:
            directory_path: Path to directory to create
        """
        os.makedirs(directory_path, exist_ok=True)
    
    @staticmethod
    def get_file_size(file_path: str) -> int:
        """
        Get file size in bytes.
        
        Args:
            file_path: Path to file
            
        Returns:
            File size in bytes
        """
        return os.path.getsize(file_path)


class LoggingUtils:
    """Logging and monitoring utility functions."""
    
    @staticmethod
    def cmss_logger(message: str, host: str, token: str = None) -> None:
        """
        Send log message to CMSS logging service.
        
        Args:
            message: Log message to send
            host: CMSS host URL
            token: Optional authentication token
        """
        if not host:
            logging.debug("CMSS host not provided, skipping external logging")
            return
        
        try:
            endpoint = "log"
            url = f"{host}/{endpoint}"
            body = {"level": "info", "msg_body": str(message)}
            logging.debug(f"CMSS logger URL: {url}, body: {body}")
            response = requests.post(url, json=body)
            logging.debug(f"CMSS logger response status: {response.status_code}")
            
            if response.status_code == 200:
                logging.debug("Successfully sent log to CMSS")
            else:
                logging.warning(f"CMSS logging failed with status {response.status_code}")
                
        except Exception as e:
            logging.warning(f"Failed to send log to CMSS: {e}")
    
    @staticmethod
    def cmss_product_available(product_info: Dict[str, Any], host: str, token: str = None) -> None:
        """
        Notify CMSS that a product is available.
        
        Args:
            product_info: Product information dictionary
            host: CMSS host URL
            token: Optional authentication token
        """
        if not host:
            logging.debug("CMSS host not provided, skipping product notification")
            return
        
        try:
            headers = {'Content-Type': 'application/json'}
            if token:
                headers['Authorization'] = f'Bearer {token}'
            
            response = requests.post(
                f"{host}/product",
                json=product_info,
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                logging.info("Successfully notified CMSS of product availability")
            else:
                logging.warning(f"CMSS product notification failed with status {response.status_code}")
                
        except Exception as e:
            logging.warning(f"Failed to notify CMSS of product availability: {e}")


class ConfigUtils:
    """Configuration and argument parsing utilities."""
    
    @staticmethod
    def get_common_argument_parser() -> argparse.ArgumentParser:
        """
        Create argument parser with common arguments used across pipeline scripts.
        
        Returns:
            ArgumentParser with common arguments
        """
        parser = argparse.ArgumentParser(description='CZDT ISS Ingest Pipeline')
        
        # S3 configuration
        parser.add_argument('--s3-bucket', type=str, required=True,
                          help='S3 bucket for outputs')
        parser.add_argument('--s3-prefix', type=str, default='',
                          help='S3 prefix for outputs')
        parser.add_argument('--role-arn', type=str,
                          help='AWS role ARN to assume for S3 access')
        
        # MAAP configuration
        parser.add_argument('--maap-host', type=str, 
                          default='https://api.maap-project.org',
                          help='MAAP host URL')
        parser.add_argument('--job-queue', type=str,
                          help='DPS job queue name')
        
        # External services
        parser.add_argument('--cmss-logger-host', type=str,
                          help='CMSS logger host URL')
        parser.add_argument('--mmgis-host', type=str,
                          help='MMGIS host URL')
        parser.add_argument('--titiler-token-secret-name', type=str,
                          help='TiTiler token secret name')
        
        # Configuration
        parser.add_argument('--zarr-config-url', type=str,
                          help='Zarr configuration URL')
        
        return parser
    
    @staticmethod
    def get_generic_argument_parser() -> argparse.ArgumentParser:
        """
        Create argument parser for the generic pipeline with enhanced features.
        
        Returns:
            ArgumentParser with generic pipeline arguments
        """
        parser = argparse.ArgumentParser(
            description="Generic CZDT pipeline that processes data from DAAC, S3 NetCDF, or S3 Zarr inputs.")
        
        # Input source options (mutually exclusive)
        input_group = parser.add_mutually_exclusive_group(required=True)
        input_group.add_argument("--granule-id", 
                               help="DAAC Granule ID for download from DAAC archives")
        input_group.add_argument("--input-s3", 
                               help="S3 URL of input file (NetCDF or Zarr)")
        
        # Required for DAAC input
        parser.add_argument("--collection-id", 
                            help="Collection Concept ID (required for DAAC input)")
        
        # Common required arguments
        parser.add_argument("--s3-bucket", required=True,
                            help="Target S3 bucket for uploading processed files")
        parser.add_argument("--s3-prefix", default="",
                            help="Optional S3 prefix (folder path) within the bucket")
        parser.add_argument("--role-arn", required=True,
                            help="AWS IAM Role ARN to assume for S3 upload")
        parser.add_argument("--cmss-logger-host", required=True,
                            help="Host for logging pipeline messages")
        parser.add_argument("--mmgis-host", required=True,
                            help="Host for cataloging STAC items")
        parser.add_argument("--titiler-token-secret-name", required=True,
                            help="MAAP secret name for MMGIS host token")
        parser.add_argument("--job-queue", required=True,
                            help="Queue name for running pipeline jobs")
        parser.add_argument("--zarr-config-url", required=True,
                            help="S3 URL of the ZARR config file")
        
        # Optional processing parameters
        parser.add_argument("--variables", default="*",
                            help="Variables to extract from NetCDF (default: all variables '*')")
        parser.add_argument("--enable-concat", action="store_true",
                            help="Enable Zarr concatenation step (default: skip)")
        parser.add_argument("--local-download-path", default="output",
                            help="Local directory for temporary downloads")
        parser.add_argument("--maap-host", default="api.maap-project.org",
                            help="MAAP API host")
        
        return parser
    
    @staticmethod
    def validate_arguments(args):
        """Validate argument combinations"""
        logging.debug("Starting argument validation")
        if args.granule_id and not args.collection_id:
            raise ValueError("--collection-id is required when using --granule-id")
        
        if args.input_s3:
            if not args.input_s3.startswith('s3://'):
                raise ValueError(f"Invalid S3 URL: {args.input_s3}")

        logging.debug("Argument validation completed successfully")
    
    @staticmethod
    def detect_input_type(args):
        """Detect the type of input and processing needed"""
        logging.debug("Detecting input type")
        if args.granule_id and len(args.granule_id) > 0 and args.granule_id != "none":
            logging.debug(f"Detected DAAC input type for granule: {args.granule_id}")
            return "daac"
        elif args.input_s3:
            logging.debug(f"Analyzing S3 input URL: {args.input_s3}")
            if args.input_s3.endswith(('.nc', '.nc4')):
                logging.debug("Detected S3 NetCDF input type")
                return "s3_netcdf"
            elif args.input_s3.endswith('.zarr') or args.input_s3.endswith('.zarr/'):
                logging.debug("Detected S3 Zarr input type")
                return "s3_zarr"
            elif args.input_s3.endswith('.gpkg'):
                logging.debug("Detected S3 GeoPackage input type")
                return "s3_gpkg"
            else:
                logging.debug(f"Unsupported file type detected: {args.input_s3}")
                raise ValueError(f"Unsupported file type in S3 URL: {args.input_s3}")
        
        logging.debug("No valid input type could be determined")
    
    @staticmethod
    def load_config_from_url(config_url: str) -> Dict[str, Any]:
        """
        Load configuration from a URL.
        
        Args:
            config_url: URL to configuration file
            
        Returns:
            Configuration dictionary
        """
        try:
            response = requests.get(config_url, timeout=10)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logging.error(f"Failed to load configuration from {config_url}: {e}")
            raise
    
    @staticmethod
    def get_env_or_default(env_var: str, default_value: str = None) -> str:
        """
        Get environment variable with optional default value.
        
        Args:
            env_var: Environment variable name
            default_value: Default value if env var is not set
            
        Returns:
            Environment variable value or default
        """
        return os.environ.get(env_var, default_value)


class ValidationUtils:
    """Data validation and error handling utilities."""
    
    @staticmethod
    def validate_s3_path(s3_path: str) -> bool:
        """
        Validate S3 path format.
        
        Args:
            s3_path: S3 path to validate
            
        Returns:
            True if valid, False otherwise
        """
        return s3_path.startswith('s3://') and len(s3_path) > 5
    
    @staticmethod
    def validate_required_args(args: argparse.Namespace, required_fields: List[str]) -> None:
        """
        Validate that required arguments are present.
        
        Args:
            args: Parsed arguments namespace
            required_fields: List of required field names
            
        Raises:
            ValueError: If any required field is missing
        """
        missing_fields = []
        for field in required_fields:
            if not hasattr(args, field) or getattr(args, field) is None:
                missing_fields.append(field)
        
        if missing_fields:
            raise ValueError(f"Missing required arguments: {', '.join(missing_fields)}")
    
    @staticmethod
    def validate_file_exists(file_path: str) -> None:
        """
        Validate that a file exists.
        
        Args:
            file_path: Path to file to check
            
        Raises:
            FileNotFoundError: If file doesn't exist
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")


# Exception classes for common errors
class UploadError(Exception):
    """Exception raised for S3 upload errors."""
    pass


class DownloadError(Exception):
    """Exception raised for download errors."""
    pass


class GranuleNotFoundError(Exception):
    """Exception raised when a granule is not found."""
    pass


class ConfigurationError(Exception):
    """Exception raised for configuration errors."""
    pass
