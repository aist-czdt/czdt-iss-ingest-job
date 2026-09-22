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


class TestCatalogJobMaapApiRetries:
    """
    100 concurrent catalog jobs against api.maap-project.org (2026-09-22): 35 ConnectTimeouts at client
    construction and 34 'TypeError: exceptions must derive from BaseException' from maap-py's
    Secrets.get_secret, which raises a str. Both must be retried.
    """

    def test_get_maap_retries_connect_timeout(self):
        import catalog_job
        with patch.object(catalog_job.MaapUtils, "get_maap_instance",
                          side_effect=[requests.exceptions.ConnectTimeout("t"), "client"]) as g:
            assert catalog_job.get_maap("host") == "client"
        assert g.call_count == 2

    def test_secret_fetch_retries_maap_py_str_raise(self):
        import catalog_job
        maap = MagicMock()
        maap.secrets.get_secret.side_effect = [TypeError("exceptions must derive from BaseException"), "tok"]
        with patch.object(catalog_job.MaapUtils, "get_maap_instance", return_value=maap):
            assert catalog_job.get_authentication_token("secret", "host") == "tok"
        assert maap.secrets.get_secret.call_count == 2

    def test_load_catalog_retries_s3_throttle(self):
        import catalog_job
        import pystac
        cat = MagicMock(spec=pystac.Catalog)
        cat.walk.side_effect = [pystac.errors.STACError("HREF does not resolve"), iter([])]
        with patch.object(catalog_job.pystac.Catalog, "from_dict", return_value=cat):
            assert catalog_job.load_catalog({"id": "c"}, "https://x/catalog.json") is cat
        assert cat.walk.call_count == 2

    def test_download_json_retries(self):
        import catalog_job
        good = MagicMock()
        good.__enter__ = lambda s: s
        good.__exit__ = lambda s, *a: False
        good.read = lambda: '{"ok": 1}'
        with patch.object(catalog_job.fsspec, "open", side_effect=[FileNotFoundError("glob characters"), good]), \
             patch.object(catalog_job.json, "load", return_value={"ok": 1}):
            assert catalog_job._download_json("https://x/catalog.json") == {"ok": 1}


class TestWaitForParent:
    """
    2026-09-22: a catalog job whose parent had failed kept polling for 42+ minutes (it would have gone on for
    the full 48 h max_wait_time) because the retry wrapper retried the exception meant to be terminal.
    """

    def _job(self, statuses):
        import catalog_job
        job = MagicMock(id="parent-1")
        it = iter(statuses)

        def _set():
            job.status = next(it)
        job.retrieve_status.side_effect = _set
        catalog_job._deleted_first_seen.clear()
        return job

    def test_failed_parent_gives_up_immediately(self):
        import catalog_job
        job = self._job(["Failed", "Failed", "Failed"])
        with pytest.raises(catalog_job.ParentJobFailed):
            catalog_job.wait_for_parent(job, max_backoff=1, max_wait_time=60)
        assert job.retrieve_status.call_count == 1

    def test_running_then_succeeded_returns(self):
        import catalog_job
        job = self._job(["Accepted", "Running", "Succeeded"])
        assert catalog_job.wait_for_parent(job, max_backoff=1, max_wait_time=60) is job
        assert job.retrieve_status.call_count == 3

    def test_deleted_is_retried_within_grace(self):
        import catalog_job
        job = self._job(["Deleted", "Running", "Succeeded"])
        assert catalog_job.wait_for_parent(job, max_backoff=1, max_wait_time=60) is job

    def test_dismissed_is_terminal(self):
        import catalog_job
        job = self._job(["Dismissed"])
        with pytest.raises(catalog_job.ParentJobFailed):
            catalog_job.wait_for_parent(job, max_backoff=1, max_wait_time=60)


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
