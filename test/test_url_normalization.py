#!/usr/bin/env python3
"""
Tests that base URLs/hosts with trailing slashes (e.g. mmgis_host of
"http://100.21.202.199:8888/") are normalized so joined URLs never contain a
double slash, which MMGIS answers with a 404 HTML page.
"""

import os
import sys
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from common_utils import normalize_base_url, ConfigUtils, LoggingUtils  # noqa: E402
import create_stac_items  # noqa: E402
import catalog_job  # noqa: E402


class TestNormalizeBaseUrl:
    def test_strips_single_trailing_slash(self):
        assert normalize_base_url("http://100.21.202.199:8888/") == "http://100.21.202.199:8888"

    def test_strips_multiple_trailing_slashes(self):
        assert normalize_base_url("https://stac.example.com///") == "https://stac.example.com"

    def test_strips_whitespace(self):
        assert normalize_base_url("  https://stac.example.com/ \n") == "https://stac.example.com"

    def test_leaves_clean_url_alone(self):
        assert normalize_base_url("https://stac.example.com") == "https://stac.example.com"

    def test_bare_host(self):
        assert normalize_base_url("api.maap-project.org/") == "api.maap-project.org"

    def test_none_and_empty_pass_through(self):
        assert normalize_base_url(None) is None
        assert normalize_base_url("") == ""


class TestArgumentParsers:
    def test_common_parser_normalizes_hosts(self):
        parser = ConfigUtils.get_common_argument_parser()
        args = parser.parse_args([
            "--s3-bucket", "b",
            "--mmgis-host", "http://100.21.202.199:8888/",
            "--cmss-logger-host", "http://44.242.188.25:8000/",
            "--maap-host", "https://api.maap-project.org/",
        ])
        assert args.mmgis_host == "http://100.21.202.199:8888"
        assert args.cmss_logger_host == "http://44.242.188.25:8000"
        assert args.maap_host == "https://api.maap-project.org"

    def test_generic_parser_normalizes_hosts(self):
        parser = ConfigUtils.get_generic_argument_parser()
        args = parser.parse_args([
            "--granule-id", "g", "--collection-id", "c",
            "--s3-bucket", "b", "--role-arn", "r",
            "--cmss-logger-host", "http://44.242.188.25:8000/",
            "--mmgis-host", "http://100.21.202.199:8888/",
            "--titiler-token-secret-name", "t", "--job-queue", "q",
            "--maap-host", "api.maap-project.org/",
        ])
        assert args.mmgis_host == "http://100.21.202.199:8888"
        assert args.cmss_logger_host == "http://44.242.188.25:8000"
        assert args.maap_host == "api.maap-project.org"

    def test_catalog_job_parser_normalizes_hosts(self):
        with patch.object(sys, "argv", [
            "catalog_job.py",
            "--parent-job-id", "p",
            "--mmgis-host", "http://100.21.202.199:8888/",
            "--titiler-token-secret-name", "t",
            "--cmss-logger-host", "http://44.242.188.25:8000/",
            "--collection-id", "c",
            "--maap-host", "api.maap-project.org/",
        ]):
            args = catalog_job.parse_arguments()
        assert args.mmgis_host == "http://100.21.202.199:8888"
        assert args.cmss_logger_host == "http://44.242.188.25:8000"
        assert args.maap_host == "api.maap-project.org"


class TestUrlJoinsAtPointOfUse:
    """Even if a caller bypasses argparse, joins must not produce '//'."""

    def test_get_collection_url(self):
        with patch("create_stac_items.requests.get") as mock_get:
            mock_get.return_value = MagicMock(status_code=404)
            create_stac_items.get_collection("http://100.21.202.199:8888/", "tok", "C1")
        url = mock_get.call_args.args[0]
        assert url == "http://100.21.202.199:8888/stac/collections/C1"

    def test_upsert_collection_items_url(self):
        with patch("create_stac_items.requests.post") as mock_post, \
             patch("create_stac_items.prepare_bulk_items_dict", return_value=[]):
            mock_post.return_value = MagicMock(status_code=200)
            create_stac_items.upsert_collection_items("http://100.21.202.199:8888/", "tok", "C1", [])
        url = mock_post.call_args.args[0]
        assert url == "http://100.21.202.199:8888/stac/collections/C1/bulk_items"

    def test_cmss_logger_url(self):
        with patch("common_utils.requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            LoggingUtils.cmss_logger("hello", "http://44.242.188.25:8000/")
        assert mock_post.call_args.args[0] == "http://44.242.188.25:8000/log"

    def test_cmss_product_available_url(self):
        with patch("common_utils.requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            LoggingUtils.cmss_product_available({}, "http://44.242.188.25:8000/")
        assert mock_post.call_args.args[0] == "http://44.242.188.25:8000/product"
