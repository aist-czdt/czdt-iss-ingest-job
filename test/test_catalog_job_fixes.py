#!/usr/bin/env python3
"""
Tests for two catalog_job.py bugs found while inspecting a real DPS run:

1. AWSUtils.convert_s3_http_to_s3_uri doesn't recognize region-qualified S3
   URLs (e.g. bucket.s3.us-west-2.amazonaws.com), returns None, and
   process_catalog_items blindly overwrites a good href with that None.
2. ingest_catalog_to_stac appends the same OGC item URL once per asset
   instead of once per item, producing duplicates in the CMSS notification.
"""

import os
import sys
from datetime import datetime, timezone
from unittest.mock import patch

import pystac

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from common_utils import AWSUtils  # noqa: E402
import catalog_job  # noqa: E402


class TestConvertS3HttpToS3Uri:
    def test_global_path_style(self):
        assert AWSUtils.convert_s3_http_to_s3_uri(
            "https://s3.amazonaws.com/bucket-name/object-key"
        ) == "s3://bucket-name/object-key"

    def test_global_virtual_hosted_style(self):
        assert AWSUtils.convert_s3_http_to_s3_uri(
            "https://bucket-name.s3.amazonaws.com/object-key"
        ) == "s3://bucket-name/object-key"

    def test_region_qualified_virtual_hosted_style(self):
        assert AWSUtils.convert_s3_http_to_s3_uri(
            "https://bucket-name.s3.us-west-2.amazonaws.com/object-key"
        ) == "s3://bucket-name/object-key"

    def test_region_qualified_path_style(self):
        assert AWSUtils.convert_s3_http_to_s3_uri(
            "https://s3.us-west-2.amazonaws.com/bucket-name/object-key"
        ) == "s3://bucket-name/object-key"

    def test_non_s3_url_returns_none(self):
        assert AWSUtils.convert_s3_http_to_s3_uri("https://example.com/foo") is None


def _make_item(item_id, asset_href, zarr_href="https://czdt-iass.s3.amazonaws.com/output_data/x/y.zarr"):
    item = pystac.Item(
        id=item_id,
        geometry={"type": "Point", "coordinates": [0, 0]},
        bbox=[0, 0, 0, 0],
        datetime=datetime(2026, 5, 13, tzinfo=timezone.utc),
        properties={},
    )
    item.add_asset("asset", pystac.Asset(href=asset_href))
    item.add_asset("zarr", pystac.Asset(href=zarr_href))
    return item


def _make_catalog_with_items(items, collection_id="test-collection"):
    catalog = pystac.Catalog(id="test-catalog", description="test")
    collection = pystac.Collection(
        id=collection_id,
        description="test collection",
        extent=pystac.Extent(
            spatial=pystac.SpatialExtent([[0, 0, 0, 0]]),
            temporal=pystac.TemporalExtent([[None, None]]),
        ),
    )
    catalog.add_child(collection)
    for item in items:
        collection.add_item(item)
    return catalog


class TestProcessCatalogItemsSafeConversion:
    def test_unconvertible_href_is_left_unchanged(self):
        original_href = "https://bucket-name.s3.unrecognized-shape.example.com/object-key"
        item = _make_item("item-1", original_href)
        catalog = _make_catalog_with_items([item])

        with patch.object(AWSUtils, "convert_s3_http_to_s3_uri", return_value=None):
            stats = catalog_job.process_catalog_items(catalog)

        got_item = next(catalog.get_items(recursive=True))
        assert got_item.assets["asset"].href == original_href
        assert stats["assets_converted"] == 0

    def test_convertible_href_is_rewritten(self):
        original_href = "https://maap-ops-workspace.s3.us-west-2.amazonaws.com/some/key.tif"
        item = _make_item("item-1", original_href)
        catalog = _make_catalog_with_items([item])

        stats = catalog_job.process_catalog_items(catalog)

        got_item = next(catalog.get_items(recursive=True))
        assert got_item.assets["asset"].href == "s3://maap-ops-workspace/some/key.tif"
        assert stats["assets_converted"] >= 1


class TestIngestCatalogToStacOgcUris:
    def test_one_ogc_uri_per_item_not_per_asset(self):
        items = [_make_item("item-1", "https://bucket.s3.amazonaws.com/a.tif"),
                 _make_item("item-2", "https://bucket.s3.amazonaws.com/b.tif")]
        catalog = _make_catalog_with_items(items)

        with patch.object(catalog_job.create_stac_items, "upsert_collection", return_value=True), \
             patch.object(catalog_job.LoggingUtils, "cmss_product_available"), \
             patch.object(catalog_job.LoggingUtils, "cmss_logger"):
            results = catalog_job.ingest_catalog_to_stac(
                catalog=catalog,
                mmgis_host="http://stac-api.example.com",
                token="token",
                collection_id="concept-id",
                cmss_logger_host="http://cmss.example.com",
                parent_job_id="job-id",
            )

        assert len(results["ogc_uris"]) == 2
        assert len(set(results["ogc_uris"])) == 2
