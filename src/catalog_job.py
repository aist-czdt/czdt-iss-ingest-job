#!/usr/bin/env python3
"""
CZDT ISS Catalog Job - Waits for parent job completion and catalogs products to STAC API.

This script monitors a parent MAAP DPS job until completion, then retrieves the 
catalog.json output and ingests all collections and items into a STAC API.
"""

import argparse
import json
import logging
import sys
from typing import Dict, Any
import fsspec
import pystac
import backoff

# Import existing utility functions
from common_utils import AWSUtils, MaapUtils, LoggingUtils
import create_stac_items

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(module)s - %(message)s'
)
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


def parse_arguments():
    """Parse command line arguments for the catalog job."""
    parser = argparse.ArgumentParser(
        description="Wait for parent job completion and catalog products to STAC API",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage
  python catalog_job.py \\
    --parent-job-id job-12345 \\
    --mmgis-host https://stac.example.com \\
    --titiler-token-secret-name czdt-token \\
    --cmss-logger-host https://logger.example.com \\
    --collection-id CZDT_ISS_001
    
  # With custom wait times
  python catalog_job.py \\
    --parent-job-id job-12345 \\
    --mmgis-host https://stac.example.com \\
    --titiler-token-secret-name czdt-token \\
    --cmss-logger-host https://logger.example.com \\
    --collection-id CZDT_ISS_001 \\
    --max-wait-time 86400 \\
    --max-backoff 120
        """
    )
    
    parser.add_argument(
        '--parent-job-id',
        required=True,
        help='Parent job ID to wait for completion'
    )
    
    parser.add_argument(
        '--mmgis-host',
        required=True,
        help='STAC API host URL (e.g., https://stac.example.com)'
    )
    
    parser.add_argument(
        '--titiler-token-secret-name',
        required=True,
        help='AWS Secrets Manager secret name containing the authentication token'
    )
    
    parser.add_argument(
        '--cmss-logger-host',
        required=True,
        help='Host for logging pipeline messages'
    )
    
    parser.add_argument(
        '--collection-id',
        required=True,
        help='Collection Concept ID for product notifications'
    )
    
    parser.add_argument(
        '--maap-host',
        default='api.maap-project.org',
        help='MAAP host (default: api.maap-project.org)'
    )
    
    parser.add_argument(
        '--max-wait-time',
        type=int,
        default=172800,
        help='Maximum wait time for parent job completion in seconds (default: 172800 = 48 hours)'
    )
    
    parser.add_argument(
        '--max-backoff',
        type=int,
        default=64,
        help='Maximum backoff interval in seconds (default: 64)'
    )
    
    parser.add_argument(
        '--upsert',
        action='store_true',
        help='Enable upsert mode (update existing items instead of failing on conflicts)'
    )
    
    return parser.parse_args()


@backoff.on_exception(backoff.expo, Exception, max_value=64, max_time=172800)
def wait_for_parent_completion(parent_job):
    """
    Wait for parent job to complete with exponential backoff.
    
    Args:
        parent_job: MAAP DPS job object
        
    Raises:
        RuntimeError: If job is still running/pending (triggers backoff)
        ValueError: If job failed or was deleted (no retry)
    """
    parent_job.retrieve_status()
    status = parent_job.status.lower()
    
    logger.info(f"Parent job {parent_job.id} status: {status}")
    
    if status in ["failed", "deleted", "revoked"]:
        error_msg = f"Parent job {parent_job.id} ended with status: {status}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    
    if status in ["accepted", "running", "queued"]:
        logger.debug(f'Parent job {parent_job.id} status is {status}. Backing off.')
        raise RuntimeError(f"Job still running: {status}")
    
    if status == "successful":
        logger.info(f"Parent job {parent_job.id} completed successfully")
        return parent_job
    
    # Unknown status - log and retry
    logger.warning(f"Unknown parent job status: {status}. Will retry.")
    raise RuntimeError(f"Unknown job status: {status}")


def get_authentication_token(token_secret_name: str, maap_host: str) -> str:
    """
    Retrieve authentication token from AWS Secrets Manager via MAAP.
    
    Args:
        token_secret_name: Name of the secret in AWS Secrets Manager
        maap_host: MAAP host to use for secret retrieval
        
    Returns:
        Authentication token string
    """
    logger.debug(f"Retrieving authentication token from secret: {token_secret_name}")
    
    try:
        maap = MaapUtils.get_maap_instance(maap_host)
        token = maap.secrets.get_secret(token_secret_name)
        logger.debug("Successfully retrieved authentication token")
        return token
    except Exception as e:
        logger.error(f"Failed to retrieve authentication token: {e}")
        raise


def get_catalog_from_parent_job(parent_job, maap_host: str) -> Dict[str, Any]:
    """
    Get catalog.json from parent job outputs.
    
    Args:
        parent_job: Completed parent job
        maap_host: MAAP host for presigned URL generation
        
    Returns:
        Parsed catalog dictionary
    """
    logger.info(f"Retrieving catalog.json from parent job {parent_job.id}")
    
    try:
        # Get catalog.json files from parent job outputs
        stac_cat_files = MaapUtils.get_dps_output([parent_job], "catalog.json")
        if not stac_cat_files:
            error_msg = f"No STAC catalog files found from parent job {parent_job.id}"
            logger.error(error_msg)
            raise RuntimeError(error_msg)
        
        stac_cat_file = stac_cat_files[0]
        logger.info(f"Found STAC catalog file: {stac_cat_file}")
        
        # Generate presigned URL for catalog file
        maap = MaapUtils.get_maap_instance(maap_host)
        bucket_name, catalog_path = AWSUtils.parse_s3_path(stac_cat_file)
        presigned_url = maap.aws.s3_signed_url(bucket_name, catalog_path)['url']
        
        logger.debug(f"Generated presigned URL for catalog.json")
        
        # Download and parse catalog
        with fsspec.open(presigned_url, "r") as f:
            catalog_data = json.load(f)
        
        # Write local copy for reference
        with open("catalog.json", 'w') as fr:
            fr.write(json.dumps(catalog_data, indent=4))
        
        logger.info(f"Successfully downloaded catalog with {len(catalog_data.get('links', []))} links")
        return catalog_data
        
    except Exception as e:
        logger.error(f"Failed to get catalog from parent job {parent_job.id}: {e}")
        raise


def process_catalog_items(catalog: pystac.Catalog) -> Dict[str, int]:
    """
    Process catalog and convert asset hrefs to S3 URIs where applicable.
    
    Args:
        catalog: PySTAC Catalog object
        
    Returns:
        Dictionary with processing statistics
    """
    logger.info("Processing catalog items and converting asset hrefs")
    
    collections_processed = 0
    items_processed = 0
    assets_converted = 0
    
    for _, collections, _ in catalog.walk():
        if collections:
            for coll in collections:
                collections_processed += 1
                collection_items = coll.get_items()
                
                # Convert item hrefs to s3 uris
                for item in collection_items:
                    items_processed += 1
                    for _, asset in item.assets.items():
                        if asset.href.startswith("https://") and ".s3." in asset.href:
                            original_href = asset.href
                            asset.href = AWSUtils.convert_s3_http_to_s3_uri(asset.href)
                            if asset.href != original_href:
                                assets_converted += 1
                                logger.debug(f"Converted asset href: {original_href} -> {asset.href}")
    
    stats = {
        'collections': collections_processed,
        'items': items_processed,
        'assets_converted': assets_converted
    }
    
    logger.info(f"Processing complete: {collections_processed} collections, "
                f"{items_processed} items, {assets_converted} assets converted")
    
    return stats


def ingest_catalog_to_stac(catalog: pystac.Catalog, mmgis_host: str, token: str, 
                          collection_id: str, cmss_logger_host: str, 
                          upsert_mode: bool = False) -> Dict[str, Any]:
    """
    Ingest all collections and items from catalog into STAC API.
    
    Args:
        catalog: PySTAC Catalog object to ingest
        mmgis_host: STAC API host URL
        token: Authentication token
        collection_id: Collection concept ID for notifications
        cmss_logger_host: CMSS logging host
        upsert_mode: Whether to use upsert mode for existing items
        
    Returns:
        Dictionary with ingestion results
    """
    logger.info(f"Starting STAC API ingestion to {mmgis_host} (upsert_mode={upsert_mode})")
    
    collections_ingested = 0
    items_ingested = 0
    failed_collections = []
    ogc_uris = []
    asset_uris = []
    
    # Get collection and item counts for logging
    coll_count = len(list(catalog.get_collections()))
    item_count = len(list(catalog.get_items(recursive=True)))
    
    msg = f"Updating STAC catalog with {item_count} items across {coll_count} collections."
    print(msg)
    LoggingUtils.cmss_logger(str(msg), cmss_logger_host)
    
    for _, collections, _ in catalog.walk():
        if collections:
            for coll in collections:
                collection_id_current = coll.id
                collection_items = list(coll.get_items())
                
                logger.info(f"Processing collection '{collection_id_current}' with {len(collection_items)} items")
                
                try:
                    # Collect URIs (href conversion already done in process_catalog_items)
                    for item in collection_items:
                        for asset_key, asset in item.assets.items():
                            ogc_uris.append(f"{mmgis_host}/stac/collections/{collection_id_current}/items/{item.id}")
                            
                            if asset_key == "asset" and asset.href not in asset_uris:
                                asset_uris.append(asset.href)
                    
                    # Upsert collection to STAC API
                    upserted_collection = create_stac_items.upsert_collection(
                        mmgis_url=mmgis_host,
                        mmgis_token=token,
                        collection_id=collection_id_current,
                        collection=coll,
                        collection_items=collection_items,
                        upsert_items=upsert_mode
                    )
                    
                    if upserted_collection:
                        collections_ingested += 1
                        items_ingested += len(collection_items)
                        
                        msg = f"STAC catalog update complete for collection {collection_id_current}."
                        print(msg)
                        LoggingUtils.cmss_logger(str(msg), cmss_logger_host)
                        logger.info(f"Successfully ingested collection '{collection_id_current}'")
                    else:
                        logger.error(f"Failed to ingest collection '{collection_id_current}'")
                        failed_collections.append(collection_id_current)
                        
                except Exception as e:
                    logger.error(f"Error ingesting collection '{collection_id_current}': {e}")
                    failed_collections.append(collection_id_current)
    
    # Send product availability notification
    job_id = MaapUtils.get_job_id()
    product_details = {
        "concept_id": collection_id,
        "ogc": ogc_uris,
        "uris": asset_uris,
        "job_id": job_id
    }
    
    logger.debug(f"Product details for notification: {product_details}")
    LoggingUtils.cmss_product_available(product_details, cmss_logger_host)
    LoggingUtils.cmss_logger(f"Products available for collection {collection_id}", cmss_logger_host)
    
    results = {
        'collections_ingested': collections_ingested,
        'items_ingested': items_ingested,
        'failed_collections': failed_collections,
        'ogc_uris': ogc_uris,
        'asset_uris': asset_uris
    }
    
    logger.info(f"Ingestion complete: {collections_ingested} collections, {items_ingested} items ingested")
    if failed_collections:
        logger.warning(f"Failed collections: {failed_collections}")
    
    return results


def main():
    """Main function orchestrating the catalog job process."""
    logger.info("Starting CZDT ISS Catalog Job")
    
    try:
        args = parse_arguments()
        
        # Get MAAP instance
        maap = MaapUtils.get_maap_instance(args.maap_host)
        
        # Get parent job by ID
        logger.info(f"Getting parent job: {args.parent_job_id}")
        parent_job = maap.getJob(args.parent_job_id)
        
        if not parent_job:
            raise ValueError(f"Parent job {args.parent_job_id} not found")
        
        # Wait for parent job completion with custom backoff settings
        logger.info(f"Waiting for parent job {args.parent_job_id} to complete...")
        msg = f"Catalog job waiting for parent job {args.parent_job_id} completion"
        print(msg)
        LoggingUtils.cmss_logger(str(msg), args.cmss_logger_host)
        
        # Configure backoff decorator with custom values
        wait_func = backoff.on_exception(
            backoff.expo, 
            Exception, 
            max_value=args.max_backoff,
            max_time=args.max_wait_time
        )(lambda: wait_for_parent_completion(parent_job))
        
        completed_job = wait_func()
        
        msg = f"Parent job {args.parent_job_id} completed successfully"
        print(msg)
        LoggingUtils.cmss_logger(str(msg), args.cmss_logger_host)
        
        # Get catalog from parent job outputs
        catalog_data = get_catalog_from_parent_job(completed_job, args.maap_host)
        
        # Create PySTAC catalog object
        catalog = pystac.Catalog.from_dict(catalog_data)
        catalog.set_self_href(f"s3://temp-catalog-{args.parent_job_id}.json")
        catalog.make_all_asset_hrefs_absolute()
        
        logger.info(f"Created PySTAC catalog: '{catalog.id}' - {catalog.description}")
        
        # Process catalog items
        processing_stats = process_catalog_items(catalog)
        logger.info(f"Processed {processing_stats['collections']} collections, "
                   f"{processing_stats['items']} items")
        
        # Get authentication token
        auth_token = get_authentication_token(args.titiler_token_secret_name, args.maap_host)
        
        # Ingest catalog into STAC API
        ingestion_results = ingest_catalog_to_stac(
            catalog=catalog,
            mmgis_host=args.mmgis_host,
            token=auth_token,
            collection_id=args.collection_id,
            cmss_logger_host=args.cmss_logger_host,
            upsert_mode=args.upsert
        )
        
        # Print summary
        print("\\n=== CATALOG JOB SUMMARY ===")
        print(f"Parent Job ID: {args.parent_job_id}")
        print(f"Collections ingested: {ingestion_results['collections_ingested']}")
        print(f"Items ingested: {ingestion_results['items_ingested']}")
        print(f"Failed collections: {len(ingestion_results['failed_collections'])}")
        if ingestion_results['failed_collections']:
            print(f"Failed collection IDs: {ingestion_results['failed_collections']}")
        
        print(f"Generated {len(ingestion_results['ogc_uris'])} OGC URIs")
        print(f"Found {len(ingestion_results['asset_uris'])} unique asset URIs")
        
        msg = f"Catalog job completed successfully for parent job {args.parent_job_id}"
        print(msg)
        LoggingUtils.cmss_logger(str(msg), args.cmss_logger_host)
        logger.info("Catalog job completed successfully")
        
    except ValueError as e:
        # Non-retryable errors (job failed, not found, etc.)
        logger.error(f"Catalog job failed due to invalid input: {e}")
        msg = f"Catalog job failed: {e}"
        LoggingUtils.cmss_logger(str(msg), args.cmss_logger_host if 'args' in locals() else "localhost")
        sys.exit(1)
        
    except Exception as e:
        # General errors
        logger.error(f"Catalog job failed: {e}", exc_info=True)
        msg = f"Catalog job failed with error: {e}"
        LoggingUtils.cmss_logger(str(msg), args.cmss_logger_host if 'args' in locals() else "localhost")
        sys.exit(1)


if __name__ == "__main__":
    main()