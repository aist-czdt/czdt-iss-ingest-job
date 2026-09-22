#!/usr/bin/env python3
"""
The catalog job version submitted by the localized pipeline used to be a hard-coded constant, so every
catalog fix (e.g. the July 2026 href-conversion fix) required rebuilding every pipeline that embeds it.
It is now an optional input, --catalog-job-version / algorithm input `catalog_job_version`, that falls
back to the build's pinned default when unset, empty, or the registered-default string "none".
"""

import os
import sys
import types
from argparse import Namespace
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

# localized_pipeline imports the czdt-iss-transformers PGE modules at import time; they are not a
# test dependency, so stub them before the import.
for _name in ("czdt_iss_transformers", "czdt_iss_transformers.cf2zarr",
              "czdt_iss_transformers.zarr_concat", "czdt_iss_transformers.zarr2cog"):
    sys.modules.setdefault(_name, types.ModuleType(_name))
# geoserver_ingest hard-requires the geoserver-rest package (GeoPackage path only); stub it likewise.
if "geoserver_ingest" not in sys.modules:
    _gs = types.ModuleType("geoserver_ingest")
    _gs.GeoServerClient = MagicMock
    sys.modules["geoserver_ingest"] = _gs

from common_utils import ConfigUtils  # noqa: E402
import localized_pipeline  # noqa: E402
import preprocess_lis_pipeline  # noqa: E402


class TestResolveCatalogJobVersion:
    def test_default_when_not_given(self):
        assert localized_pipeline.resolve_catalog_job_version(Namespace()) == localized_pipeline.CATALOG_JOB_VERSION

    def test_default_when_empty_or_none_string(self):
        for value in (None, "", "  ", "none", "None"):
            args = Namespace(catalog_job_version=value)
            assert localized_pipeline.resolve_catalog_job_version(args) == localized_pipeline.CATALOG_JOB_VERSION

    def test_override_wins(self):
        args = Namespace(catalog_job_version=" catalog-fix-dev2 ")
        assert localized_pipeline.resolve_catalog_job_version(args) == "catalog-fix-dev2"


class TestArgumentParsing:
    def test_generic_parser_accepts_catalog_job_version(self):
        parser = ConfigUtils.get_generic_argument_parser()
        args, _ = parser.parse_known_args([
            "--input-s3", "s3://bucket/file.nc", "--s3-bucket", "b", "--role-arn", "r",
            "--cmss-logger-host", "http://c", "--mmgis-host", "http://m",
            "--titiler-token-secret-name", "t", "--job-queue", "q",
            "--catalog-job-version", "v0.3.0",
        ])
        assert args.catalog_job_version == "v0.3.0"

    def test_generic_parser_default_is_unset(self):
        parser = ConfigUtils.get_generic_argument_parser()
        args, _ = parser.parse_known_args([
            "--input-s3", "s3://bucket/file.nc", "--s3-bucket", "b", "--role-arn", "r",
            "--cmss-logger-host", "http://c", "--mmgis-host", "http://m",
            "--titiler-token-secret-name", "t", "--job-queue", "q",
        ])
        assert args.catalog_job_version is None


def _pipeline_args(**overrides):
    base = dict(collection_id="C1-TEST", concept_id=None, maap_host="api.maap-project.org",
                mmgis_host="http://stac", titiler_token_secret_name="tok", cmss_logger_host="http://cmss",
                upsert=False, catalog_job_version=None)
    base.update(overrides)
    return Namespace(**base)


class TestSubmitCatalogJobVersion:
    def _submit(self, args):
        maap = MagicMock()
        maap.submitJob.return_value = MagicMock(id="cat-job-id")
        with patch.object(localized_pipeline.MaapUtils, "get_maap_instance", return_value=maap), \
             patch.object(localized_pipeline.MaapUtils, "get_job_id", return_value="parent-id"), \
             patch.object(localized_pipeline.MaapUtils, "get_job_tag", return_value="TAG"), \
             patch.object(localized_pipeline.LoggingUtils, "cmss_logger"):
            localized_pipeline.submit_catalog_job(args)
        return maap.submitJob.call_args.kwargs

    def test_submits_pinned_default_when_unset(self):
        kwargs = self._submit(_pipeline_args())
        assert kwargs["algo_id"] == "czdt-iss-catalog-job"
        assert kwargs["version"] == localized_pipeline.CATALOG_JOB_VERSION

    def test_submits_override(self):
        kwargs = self._submit(_pipeline_args(catalog_job_version="v0.3.0"))
        assert kwargs["version"] == "v0.3.0"
        assert kwargs["parent_job_id"] == "parent-id"


class TestPreprocessPipelinesForwardTheOverride:
    def test_lis_forwards_catalog_job_version(self):
        args = Namespace(s3_bucket="b", role_arn="r", zarr_config_url="z", maap_host="h", mmgis_host="m",
                         titiler_token_secret_name="t", cmss_logger_host="c", job_queue="q",
                         catalog_job_version="v0.3.0")
        with patch.object(preprocess_lis_pipeline.subprocess, "run") as run:
            preprocess_lis_pipeline.run_localized_pipeline("out/file_preprocessed.nc", args)
        cmd = run.call_args.args[0]
        assert cmd[cmd.index("--catalog-job-version") + 1] == "v0.3.0"

    def test_lis_omits_flag_when_unset(self):
        args = Namespace(s3_bucket="b", role_arn="r", zarr_config_url="z", maap_host="h", mmgis_host="m",
                         titiler_token_secret_name="t", cmss_logger_host="c", job_queue="q",
                         catalog_job_version=None)
        with patch.object(preprocess_lis_pipeline.subprocess, "run") as run:
            preprocess_lis_pipeline.run_localized_pipeline("out/file_preprocessed.nc", args)
        assert "--catalog-job-version" not in run.call_args.args[0]
