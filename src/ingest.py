# Script to take input NetCDF file and convert it into COG for ingest into
# Info Sub System
import argparse
import os.path

import xarray as xr
from osgeo import gdal


def create_cog(dataset, variable, cog_path):
    # Convert to GeoTIFF using rioxarray
    data_array = dataset[variable]
    data_array.rio.set_spatial_dims(x_dim=dataset[variable].dims[1], y_dim=dataset[variable].dims[0], inplace=True)
    data_array.rio.write_crs("EPSG:4326", inplace=True)  # Set CRS (modify if needed)
    tiff_path = "output.tif"
    data_array.rio.to_raster(tiff_path)

    gdal.Translate(
        cog_path,
        tiff_path,
        format="COG",
        creationOptions=[
            "COMPRESS=DEFLATE",
            "PREDICTOR=2"
            ]
    )


def main(input_file, variables=None, output_dir="output"):
    if variables is None:
        variables = list()
    ds = xr.open_dataset(input_file)
    variable_name = list(ds.data_vars.keys())[0]  # Automatically pick the first variable
    if len(variables) == 0:
        variables = [variable_name]

    os.makedirs(output_dir, exist_ok=True)

    for variable in variables:
        variable_name = variable
        cog_path = os.path.join(output_dir, f"{os.path.splitext(os.path.basename(input_file))[0]}_{variable_name}.tif")
        create_cog(ds, variable, cog_path)


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument("-i", "--input_file", required=True, help="Input NeCDF File")
    ap.add_argument("--variables", required=False, help="Variables to extract into COGs",
                    nargs="*")
    ap.add_argument("-o", "--output_dir", required=True, help="Output directory")
    params = ap.parse_args()
    main(params.input_file, params.variables, params.output_dir)
