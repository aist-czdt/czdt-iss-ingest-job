#!/usr/bin/env python3
"""
DAAC granules that are GeoTIFFs (OPERA DSWx-HLS, 2026-11-16) used to be handed to cf2zarr, which only reads
NetCDF, and an s3:// --zarr-config-url was passed straight to a plain open(). The localized pipeline now
downloads S3 configs first and converts GeoTIFF granules with cog2zarr, giving their COGs a per-granule name.
"""

import os
import sys
import types
from argparse import Namespace
from unittest.mock import MagicMock, patch

import pytest

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

import localized_pipeline  # noqa: E402

GRANULE = "OPERA_L3_DSWx-HLS_T18SUG_20261104T154047Z_20261107T013750Z_L9_30_v1.1"
TIFF = f"output/{GRANULE}_B01_WTR.tif"
ROLE = "arn:aws:iam::000000000000:role/test-role"


@pytest.fixture
def workdir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    return tmp_path


@pytest.fixture
def cog2zarr_stub():
    """Stand-in for czdt_iss_transformers.cog2zarr, which the pipeline imports lazily."""
    stub = types.ModuleType("czdt_iss_transformers.cog2zarr")
    stub.convert_cog_to_zarr = MagicMock()
    with patch.dict(sys.modules, {"czdt_iss_transformers.cog2zarr": stub}), \
            patch.object(sys.modules["czdt_iss_transformers"], "cog2zarr", stub, create=True):
        yield stub


class TestIsGeotiff:
    def test_tiff_extensions(self):
        for path in (TIFF, "a.tiff", "A.TIF", "/x/y/z.TiFf"):
            assert localized_pipeline.is_geotiff(path)

    def test_other_inputs(self):
        for path in ("a.nc", "a.nc4", "a.zarr", "a.tif.aux.xml", "", None):
            assert not localized_pipeline.is_geotiff(path)


class TestLocalizeConfig:
    def test_local_and_unset_values_pass_through(self):
        with patch.object(localized_pipeline.AWSUtils, "get_s3_client") as get_client:
            assert localized_pipeline.localize_config("configs/mur.yaml", ROLE) == "configs/mur.yaml"
            assert localized_pipeline.localize_config(None, ROLE) is None
            assert localized_pipeline.localize_config("", ROLE) == ""
        get_client.assert_not_called()

    def test_s3_url_is_downloaded_with_the_job_role(self, workdir):
        client = MagicMock()
        with patch.object(localized_pipeline.AWSUtils, "get_s3_client", return_value=client) as get_client:
            local = localized_pipeline.localize_config("s3://bucket/user/dswx-hls.yaml", ROLE)

        assert local == os.path.join("input", "dswx-hls.yaml")
        assert (workdir / "input").is_dir()
        get_client.assert_called_once_with(role_arn=ROLE, bucket_name="bucket")
        client.download_file.assert_called_once_with("bucket", "user/dswx-hls.yaml", local)

    def test_falls_back_to_worker_credentials_when_the_role_cannot_read(self, workdir):
        denied, allowed = MagicMock(), MagicMock()
        denied.download_file.side_effect = RuntimeError("AccessDenied")
        with patch.object(localized_pipeline.AWSUtils, "get_s3_client", side_effect=[denied, allowed]) as get_client:
            local = localized_pipeline.localize_config("s3://bucket/user/dswx-hls.yaml", ROLE)

        assert get_client.call_args_list[1].kwargs == {"bucket_name": "bucket"}
        allowed.download_file.assert_called_once_with("bucket", "user/dswx-hls.yaml", local)

    def test_failure_without_a_role_is_raised(self, workdir):
        client = MagicMock()
        client.download_file.side_effect = RuntimeError("AccessDenied")
        with patch.object(localized_pipeline.AWSUtils, "get_s3_client", return_value=client):
            with pytest.raises(RuntimeError, match="AccessDenied"):
                localized_pipeline.localize_config("s3://bucket/user/dswx-hls.yaml", None)


class TestConvertGeotiffToZarr:
    def test_calls_cog2zarr_with_a_localized_config(self, workdir, cog2zarr_stub):
        args = Namespace(zarr_config_url="s3://bucket/user/dswx-hls.yaml", role_arn=ROLE)
        with patch.object(localized_pipeline, "localize_config", return_value="input/dswx-hls.yaml") as localize:
            out = localized_pipeline.convert_geotiff_to_zarr_local(args, TIFF)

        assert out == os.path.join("output", f"{GRANULE}_B01_WTR.zarr")
        localize.assert_called_once_with("s3://bucket/user/dswx-hls.yaml", ROLE)
        cog2zarr_stub.convert_cog_to_zarr.assert_called_once_with(
            config_path="input/dswx-hls.yaml", input_path=TIFF, output_path=out)

    def test_config_is_required(self, workdir, cog2zarr_stub):
        with pytest.raises(ValueError, match="zarr-config-url"):
            localized_pipeline.convert_geotiff_to_zarr_local(Namespace(zarr_config_url=None, role_arn=None), TIFF)
        cog2zarr_stub.convert_cog_to_zarr.assert_not_called()

    def test_conversion_errors_become_runtime_errors(self, workdir, cog2zarr_stub):
        cog2zarr_stub.convert_cog_to_zarr.side_effect = ValueError("does not match pattern")
        args = Namespace(zarr_config_url="cfg.yaml", role_arn=None)
        with pytest.raises(RuntimeError, match="GeoTIFF to Zarr conversion failed: does not match pattern"):
            localized_pipeline.convert_geotiff_to_zarr_local(args, TIFF)


class TestNetcdfConfigIsLocalized:
    def test_cf2zarr_receives_the_downloaded_config(self, workdir):
        convert = MagicMock()
        args = Namespace(input_netcdf="output/granule.nc", input_s3=None, variables="sst",
                         zarr_config_url="s3://bucket/user/mur.yaml", role_arn=ROLE)
        with patch.object(localized_pipeline.cf2zarr, "convert_cf_to_zarr", convert, create=True), \
                patch.object(localized_pipeline, "localize_config", return_value="input/mur.yaml") as localize:
            localized_pipeline.convert_netcdf_to_zarr_local(args)

        localize.assert_called_once_with("s3://bucket/user/mur.yaml", ROLE)
        assert convert.call_args.kwargs["config_path"] == "input/mur.yaml"


class TestCogOutputPrefix:
    def _run(self, **extra):
        main = MagicMock()
        args = Namespace(collection_id="C1-POCLOUD", concept_id=None, **extra)
        with patch.object(localized_pipeline.zarr2cog, "main", main, create=True):
            localized_pipeline.convert_zarr_to_cog_local(args, "output/x.zarr")
        return main.call_args.args[0]

    def test_default_prefix_is_unchanged(self, workdir):
        assert self._run().output == "cog"

    def test_per_granule_prefix(self, workdir):
        assert self._run(cog_output_prefix=GRANULE).output == GRANULE


class TestDaacDispatch:
    """main() routes a staged GeoTIFF to cog2zarr and a staged NetCDF to cf2zarr."""

    def _run_main(self, staged_file):
        argv = ["localized_pipeline.py", "--granule-id", GRANULE, "--collection-id", "C1-POCLOUD",
                "--s3-bucket", "b", "--role-arn", ROLE, "--cmss-logger-host", "http://c", "--mmgis-host", "http://m",
                "--titiler-token-secret-name", "t", "--zarr-config-url", "s3://bucket/user/cfg.yaml",
                "--job-queue", "q", "--steps", "stage,netcdf2zarr,zarr2cog"]
        seen = {}

        def to_cog(args, zarr_path):
            seen["cog_args"] = (zarr_path, args.time_coord, args.lat_coord, args.lon_coord,
                                getattr(args, "cog_output_prefix", None))
            return []

        with patch.object(sys, "argv", argv), \
                patch.object(localized_pipeline.MaapUtils, "get_maap_instance", return_value=MagicMock()), \
                patch.object(localized_pipeline, "stage_from_daac_local", return_value=(staged_file, GRANULE)), \
                patch.object(localized_pipeline, "convert_geotiff_to_zarr_local", return_value="output/t.zarr") as tif, \
                patch.object(localized_pipeline, "convert_netcdf_to_zarr_local", return_value="output/n.zarr") as nc, \
                patch.object(localized_pipeline, "convert_zarr_to_cog_local", side_effect=to_cog):
            localized_pipeline.main()
        return tif, nc, seen["cog_args"]

    def test_geotiff_granule(self, workdir):
        tif, nc, cog_args = self._run_main(TIFF)
        assert tif.call_args.args[1] == TIFF
        nc.assert_not_called()
        assert cog_args == ("output/t.zarr", "time", "latitude", "longitude", GRANULE)

    def test_netcdf_granule(self, workdir):
        tif, nc, cog_args = self._run_main("output/granule.nc")
        tif.assert_not_called()
        nc.assert_called_once()
        assert cog_args == ("output/n.zarr", "time", "lat", "lon", None)
