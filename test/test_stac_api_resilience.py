#!/usr/bin/env python3
"""
STAC / MAAP API calls must retry transient failures and must never turn a network error into a "normal"
answer. Motivating incident (2026-09-22, LIS backfill probe, 3 of 24 jobs):
  * get_collection() returned False on a ConnectTimeout -> caller created the collection -> 409 -> items lost
  * upsert_collection_items() printed a failed bulk POST and returned None -> job "completed", items still empty
  * submit_catalog_job() timed out, logged a warning, and the pipeline completed with nothing cataloged
"""

import os
import sys
import types
from argparse import Namespace
from unittest.mock import MagicMock, patch

import pytest
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

for _name in ("czdt_iss_transformers", "czdt_iss_transformers.cf2zarr",
              "czdt_iss_transformers.zarr_concat", "czdt_iss_transformers.zarr2cog"):
    sys.modules.setdefault(_name, types.ModuleType(_name))
if "geoserver_ingest" not in sys.modules:
    _gs = types.ModuleType("geoserver_ingest")
    _gs.GeoServerClient = MagicMock
    sys.modules["geoserver_ingest"] = _gs

import create_stac_items  # noqa: E402
import localized_pipeline  # noqa: E402


def _resp(status, text="{}"):
    r = MagicMock()
    r.status_code = status
    r.text = text
    r.raise_for_status.side_effect = (
        requests.exceptions.HTTPError(f"{status}", response=r) if status >= 400 else None
    )
    return r


@pytest.fixture(autouse=True)
def no_sleep():
    with patch("backoff._sync.time.sleep"):
        yield


class TestGetCollection:
    def test_timeout_is_retried_then_raises(self):
        with patch.object(create_stac_items.requests, "get",
                          side_effect=requests.exceptions.ConnectTimeout("timed out")) as get, \
             pytest.raises(create_stac_items.StacApiError):
            create_stac_items.get_collection("http://stac", "tok", "c1")
        assert get.call_count > 1  # retried, not answered with False

    def test_timeout_then_success(self):
        ok = _resp(200, '{"type":"Collection","id":"c1","stac_version":"1.0.0","description":"d","license":"x",'
                        '"extent":{"spatial":{"bbox":[[0,0,1,1]]},"temporal":{"interval":[[null,null]]}},"links":[]}')
        with patch.object(create_stac_items.requests, "get",
                          side_effect=[requests.exceptions.ConnectTimeout("t"), ok]):
            coll = create_stac_items.get_collection("http://stac", "tok", "c1")
        assert coll.id == "c1"

    def test_404_means_absent(self):
        with patch.object(create_stac_items.requests, "get", return_value=_resp(404, "")):
            assert create_stac_items.get_collection("http://stac", "tok", "c1") is None

    def test_5xx_is_retried(self):
        ok = _resp(404, "")
        with patch.object(create_stac_items.requests, "get", side_effect=[_resp(503, "busy"), ok]) as get:
            assert create_stac_items.get_collection("http://stac", "tok", "c1") is None
        assert get.call_count == 2


class TestUpsertCollectionItems:
    def test_rejected_bulk_write_raises(self):
        with patch.object(create_stac_items.requests, "post", return_value=_resp(409, "conflict")), \
             pytest.raises(create_stac_items.StacApiError):
            create_stac_items.upsert_collection_items("http://stac", "tok", "c1", [], upsert_items=True)

    def test_timeout_raises_after_retries(self):
        with patch.object(create_stac_items.requests, "post",
                          side_effect=requests.exceptions.ConnectTimeout("t")) as req, \
             pytest.raises(create_stac_items.StacApiError):
            create_stac_items.upsert_collection_items("http://stac", "tok", "c1", [], upsert_items=True)
        assert req.call_count > 1

    def test_success_returns_response(self):
        with patch.object(create_stac_items.requests, "post", return_value=_resp(200, "ok")):
            assert create_stac_items.upsert_collection_items("http://stac", "tok", "c1", [], True) is not None


class TestSubmitCatalogJobRetry:
    def _args(self):
        return Namespace(collection_id="c1", concept_id=None, maap_host="h", mmgis_host="m",
                         titiler_token_secret_name="t", cmss_logger_host="c", upsert=True, catalog_job_version=None)

    def test_transient_submit_error_is_retried(self):
        maap = MagicMock()
        maap.submitJob.side_effect = [requests.exceptions.ConnectTimeout("t"), MagicMock(id="cat-1")]
        with patch.object(localized_pipeline.MaapUtils, "get_maap_instance", return_value=maap), \
             patch.object(localized_pipeline.MaapUtils, "get_job_id", return_value="p"), \
             patch.object(localized_pipeline.MaapUtils, "get_job_tag", return_value="T"), \
             patch.object(localized_pipeline.LoggingUtils, "cmss_logger"):
            job = localized_pipeline.submit_catalog_job(self._args())
        assert job.id == "cat-1"
        assert maap.submitJob.call_count == 2

    def test_persistent_failure_fails_the_pipeline(self):
        maap = MagicMock()
        maap.submitJob.side_effect = requests.exceptions.ConnectTimeout("t")
        with patch.object(localized_pipeline.MaapUtils, "get_maap_instance", return_value=maap), \
             patch.object(localized_pipeline.MaapUtils, "get_job_id", return_value="p"), \
             patch.object(localized_pipeline.MaapUtils, "get_job_tag", return_value="T"), \
             patch.object(localized_pipeline.LoggingUtils, "cmss_logger"), \
             pytest.raises(RuntimeError):
            localized_pipeline.submit_catalog_job(self._args())
