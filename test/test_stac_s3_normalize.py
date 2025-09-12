#!/usr/bin/env python3
"""
Test script for reading STAC catalogs from S3 and normalizing hrefs to absolute URLs.

This script demonstrates how to:
1. Read a STAC catalog from an S3 bucket
2. Normalize all item hrefs to use absolute URLs
3. Handle both direct S3 access and HTTP endpoints

Dependencies:
- boto3: AWS S3 client
- requests: HTTP client for STAC API endpoints
- json: JSON parsing
- urllib.parse: URL manipulation
"""

import boto3
import json
import requests
from urllib.parse import urljoin, urlparse
import argparse
from typing import Dict, List, Any, Optional


class STACCatalogNormalizer:
    """Utility class for reading and normalizing STAC catalogs from S3."""
    
    def __init__(self, s3_bucket: str = None, aws_region: str = 'us-east-1'):
        """
        Initialize the STAC catalog normalizer.
        
        Args:
            s3_bucket: S3 bucket name (optional, can be specified per operation)
            aws_region: AWS region for S3 client
        """
        self.s3_bucket = s3_bucket
        self.aws_region = aws_region
        self.s3_client = boto3.client('s3', region_name=aws_region)
    
    def read_stac_from_s3(self, s3_key: str, bucket: str = None) -> Dict[str, Any]:
        """
        Read a STAC catalog/collection/item directly from S3.
        
        Args:
            s3_key: S3 object key (path to the STAC JSON file)
            bucket: S3 bucket name (uses instance bucket if not provided)
            
        Returns:
            Dict containing the STAC JSON content
        """
        bucket = bucket or self.s3_bucket
        if not bucket:
            raise ValueError("S3 bucket must be specified either during initialization or as parameter")
        
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=s3_key)
            content = response['Body'].read().decode('utf-8')
            return json.loads(content)
        except Exception as e:
            print(f"Error reading STAC from S3 s3://{bucket}/{s3_key}: {e}")
            raise
    
    def read_stac_from_url(self, url: str) -> Dict[str, Any]:
        """
        Read a STAC catalog/collection/item from HTTP endpoint.
        
        Args:
            url: HTTP URL to the STAC JSON
            
        Returns:
            Dict containing the STAC JSON content
        """
        try:
            response = requests.get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error reading STAC from URL {url}: {e}")
            raise
    
    def normalize_href_to_absolute(self, href: str, base_url: str) -> str:
        """
        Convert a relative href to an absolute URL.
        
        Args:
            href: The href to normalize (could be relative or already absolute)
            base_url: Base URL to resolve relative hrefs against
            
        Returns:
            Absolute URL
        """
        if href.startswith(('http://', 'https://', 's3://')):
            # Already absolute
            return href
        
        # Handle S3 paths that start with bucket name
        if '/' in href and not href.startswith('./'):
            # Could be s3://bucket/key format or full path
            if base_url.startswith('s3://'):
                # Base URL is S3, maintain S3 format
                base_parts = urlparse(base_url)
                if href.startswith(base_parts.netloc):
                    # href starts with bucket name
                    return f"s3://{href}"
                else:
                    # Relative path within same bucket
                    return urljoin(base_url, href)
            elif base_url.startswith('http'):
                # Base URL is HTTP, convert to HTTP
                return urljoin(base_url, href)
        
        # Standard relative path resolution
        return urljoin(base_url, href)
    
    def normalize_stac_links(self, stac_obj: Dict[str, Any], base_url: str) -> Dict[str, Any]:
        """
        Normalize all hrefs in STAC links to absolute URLs.
        
        Args:
            stac_obj: STAC object (catalog, collection, or item)
            base_url: Base URL for resolving relative hrefs
            
        Returns:
            STAC object with normalized hrefs
        """
        if 'links' in stac_obj:
            for link in stac_obj['links']:
                if 'href' in link:
                    link['href'] = self.normalize_href_to_absolute(link['href'], base_url)
        
        return stac_obj
    
    def normalize_stac_assets(self, stac_item: Dict[str, Any], base_url: str) -> Dict[str, Any]:
        """
        Normalize all hrefs in STAC item assets to absolute URLs.
        
        Args:
            stac_item: STAC item object
            base_url: Base URL for resolving relative hrefs
            
        Returns:
            STAC item with normalized asset hrefs
        """
        if 'assets' in stac_item:
            for asset_key, asset in stac_item['assets'].items():
                if 'href' in asset:
                    asset['href'] = self.normalize_href_to_absolute(asset['href'], base_url)
        
        return stac_item
    
    def normalize_collection_from_s3(self, s3_key: str, bucket: str = None, 
                                   collection_base_url: str = None) -> Dict[str, Any]:
        """
        Read a STAC collection from S3 and normalize all hrefs.
        
        Args:
            s3_key: S3 key to the collection JSON
            bucket: S3 bucket name
            collection_base_url: Base URL for the collection (defaults to S3 URL)
            
        Returns:
            Normalized STAC collection
        """
        bucket = bucket or self.s3_bucket
        collection = self.read_stac_from_s3(s3_key, bucket)
        
        if collection_base_url is None:
            collection_base_url = f"s3://{bucket}/{s3_key}"
        
        # Normalize links
        self.normalize_stac_links(collection, collection_base_url)
        
        return collection
    
    def normalize_catalog_items_from_s3(self, collection_s3_key: str, 
                                      bucket: str = None,
                                      item_base_url: str = None,
                                      max_items: int = None) -> List[Dict[str, Any]]:
        """
        Read STAC collection from S3 and normalize all item hrefs.
        
        Args:
            collection_s3_key: S3 key to the collection JSON
            bucket: S3 bucket name  
            item_base_url: Base URL for items (defaults to S3 URL)
            max_items: Maximum number of items to process (for testing)
            
        Returns:
            List of normalized STAC items
        """
        bucket = bucket or self.s3_bucket
        
        # Read the collection
        collection = self.read_stac_from_s3(collection_s3_key, bucket)
        
        # Find items link
        items_href = None
        if 'links' in collection:
            for link in collection['links']:
                if link.get('rel') == 'items':
                    items_href = link.get('href')
                    break
        
        if not items_href:
            print("No items link found in collection")
            return []
        
        # If items_href is relative, make it absolute
        if item_base_url is None:
            item_base_url = f"s3://{bucket}/"
        
        items_url = self.normalize_href_to_absolute(items_href, item_base_url)
        print(f"Reading items from: {items_url}")
        
        # Read items (this is simplified - in practice you might need pagination)
        try:
            if items_url.startswith('s3://'):
                # Parse S3 URL
                parsed = urlparse(items_url)
                items_bucket = parsed.netloc
                items_key = parsed.path.lstrip('/')
                items_response = self.read_stac_from_s3(items_key, items_bucket)
            else:
                items_response = self.read_stac_from_url(items_url)
            
            items = []
            if 'features' in items_response:
                features = items_response['features']
                if max_items:
                    features = features[:max_items]
                
                for item in features:
                    # Normalize item links and assets
                    self.normalize_stac_links(item, item_base_url)
                    self.normalize_stac_assets(item, item_base_url)
                    items.append(item)
            
            return items
            
        except Exception as e:
            print(f"Error reading items: {e}")
            return []


def test_s3_stac_normalization():
    """Test function demonstrating STAC S3 normalization."""
    
    # Example usage with hypothetical S3 bucket
    normalizer = STACCatalogNormalizer(
        s3_bucket="my-stac-bucket",
        aws_region="us-west-2"
    )
    
    try:
        print("=== Testing STAC Collection Normalization ===")
        
        # Test reading and normalizing a collection from S3
        collection = normalizer.normalize_collection_from_s3(
            s3_key="collections/my-collection/collection.json",
            collection_base_url="https://my-stac-api.com/collections/my-collection"
        )
        
        print(f"Collection ID: {collection.get('id')}")
        print(f"Collection links: {len(collection.get('links', []))}")
        
        # Print normalized links
        for link in collection.get('links', []):
            print(f"  - {link.get('rel')}: {link.get('href')}")
        
        print("\n=== Testing STAC Items Normalization ===")
        
        # Test reading and normalizing items
        items = normalizer.normalize_catalog_items_from_s3(
            collection_s3_key="collections/my-collection/collection.json",
            item_base_url="https://my-stac-api.com/collections/my-collection/items/",
            max_items=5  # Limit for testing
        )
        
        print(f"Processed {len(items)} items")
        
        for i, item in enumerate(items[:2]):  # Show first 2 items
            print(f"\nItem {i+1}: {item.get('id')}")
            print(f"  Links: {len(item.get('links', []))}")
            print(f"  Assets: {len(item.get('assets', {}))}")
            
            # Show asset hrefs
            for asset_key, asset in item.get('assets', {}).items():
                print(f"    {asset_key}: {asset.get('href')}")
    
    except Exception as e:
        print(f"Test failed: {e}")
        print("\nNote: This test uses example S3 paths. Update with real S3 bucket and keys to test.")


def main():
    """Main function with command line interface."""
    parser = argparse.ArgumentParser(
        description="Read STAC catalog from S3 and normalize hrefs to absolute URLs",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument("--bucket", required=True, help="S3 bucket name")
    parser.add_argument("--collection-key", required=True, 
                       help="S3 key to STAC collection JSON file")
    parser.add_argument("--region", default="us-east-1", 
                       help="AWS region")
    parser.add_argument("--base-url", 
                       help="Base URL for normalizing hrefs (defaults to S3 URL)")
    parser.add_argument("--max-items", type=int, default=10,
                       help="Maximum items to process (for testing)")
    parser.add_argument("--output", help="Output file for normalized catalog")
    
    args = parser.parse_args()
    
    normalizer = STACCatalogNormalizer(
        s3_bucket=args.bucket,
        aws_region=args.region
    )
    
    try:
        print(f"Reading STAC collection from s3://{args.bucket}/{args.collection_key}")
        
        # Normalize collection
        collection = normalizer.normalize_collection_from_s3(
            s3_key=args.collection_key,
            collection_base_url=args.base_url
        )
        
        print(f"Collection: {collection.get('id')} - {collection.get('title')}")
        print(f"Links: {len(collection.get('links', []))}")
        
        # Normalize items
        items = normalizer.normalize_catalog_items_from_s3(
            collection_s3_key=args.collection_key,
            item_base_url=args.base_url,
            max_items=args.max_items
        )
        
        print(f"Processed {len(items)} items")
        
        # Create normalized catalog
        normalized_catalog = {
            "type": "FeatureCollection",
            "collection": collection,
            "features": items
        }
        
        if args.output:
            with open(args.output, 'w') as f:
                json.dump(normalized_catalog, f, indent=2)
            print(f"Normalized catalog saved to: {args.output}")
        else:
            print("\nSample normalized item:")
            if items:
                print(json.dumps(items[0], indent=2))
    
    except Exception as e:
        print(f"Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    if len(sys.argv) == 1:
        # Run test mode if no arguments provided
        print("Running test mode with example data...")
        test_s3_stac_normalization()
    else:
        import sys
        sys.exit(main())