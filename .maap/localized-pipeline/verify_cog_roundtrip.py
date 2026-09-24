#!/usr/bin/env python3
"""
Build-time verification that the installed stack writes COGs with the right numbers.

Writes a small float64 Zarr v3 exactly the way cf2zarr does (blosc, NaN fill, CF-style attributes), runs
zarr2cog.convert_timeslice_to_cog on it, reads the COG back with rasterio, and fails the build if the values
differ. Motivation: in September 2026 an image built from unpinned conda-forge packages produced MUR SST COGs
holding Kelvin modulo 256 (0..51, land = 0) from Zarrs that held 271..307 K; the same code and data were fine
on other library versions. Prints the versions and per-stage statistics so a failing build log says where
the values change.
"""
import importlib
import os
import shutil
import sys
import tempfile

import numpy as np

for m in ("numpy", "xarray", "zarr", "numcodecs", "rioxarray", "rasterio", "rio_stac", "pandas"):
    try:
        mod = importlib.import_module(m)
        extra = f" (GDAL {mod.gdal_version()})" if m == "rasterio" else ""
        print(f"  {m:10} {getattr(mod, '__version__', '?')}{extra}")
    except Exception as e:  # noqa: BLE001
        print(f"  {m:10} not importable: {e}")

import xarray as xr  # noqa: E402
import rasterio  # noqa: E402
from zarr.codecs import BloscCodec  # noqa: E402
from czdt_iss_transformers import zarr2cog  # noqa: E402

work = tempfile.mkdtemp(prefix="cog-verify-")
os.chdir(work)
lat = np.arange(-89.875, 90, 0.25)
lon = np.arange(-179.875, 180, 0.25)
vals = np.full((1, lat.size, lon.size), np.nan)
vals[0, 400:500, 300:600] = 302.556      # warm ocean
vals[0, 100:200, 100:200] = 271.35       # cold ocean
attrs = {"units": "kelvin", "valid_min": -32767, "valid_max": 32767, "coordinates": "lon lat",
         "long_name": "analysed sea surface temperature", "standard_name": "sea_surface_foundation_temperature"}
ds = xr.Dataset({"analysed_sst": (("time", "lat", "lon"), vals, attrs)},
                coords={"time": [np.datetime64("2018-09-14T09:00:00")], "lat": lat, "lon": lon})
ds["analysed_sst"] = ds["analysed_sst"].chunk({"time": 24, "lat": 100, "lon": 100})
ds.to_zarr("sst.zarr", mode="w-", encoding={"analysed_sst": {"compressor": BloscCodec(cname="blosclz", clevel=9)}},
           consolidated=True, write_empty_chunks=False)


def stats(label, a):
    a = np.asarray(a, dtype="float64")
    print(f"  {label:34} dtype={a.dtype} min={np.nanmin(a):.3f} max={np.nanmax(a):.3f} nan%={100 * np.isnan(a).mean():.1f}")


da = xr.open_zarr("sst.zarr", consolidated=True)["analysed_sst"]
stats("after open_zarr", da.values)
sl = da.sel(time=da.time[0])
stats("after sel(time)", sl.values)
_, path = zarr2cog.convert_timeslice_to_cog(da, da.time[0], "analysed_sst", "lat", "lon", "cog", output_dir="out")
with rasterio.open(path) as src:
    band = src.read(1)
    print(f"  COG dtype={src.dtypes[0]} nodata={src.nodata} scales={src.scales} offsets={src.offsets}")
stats("COG read back", band)

ok = (np.isclose(np.nanmax(band), 302.556, atol=1e-3) and np.isclose(np.nanmin(band), 271.35, atol=1e-3)
      and np.isnan(band[0, 0]) and src.nodata is not None and np.isnan(src.nodata))
shutil.rmtree(work, ignore_errors=True)
if not ok:
    print("COG ROUNDTRIP FAILED: values or nodata do not match the Zarr", file=sys.stderr)
    sys.exit(1)
print("COG roundtrip OK")
