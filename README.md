# CZDT ISS Ingest Job

A comprehensive data ingestion and processing pipeline for NASA MAAP platform that transforms satellite/remote sensing data into Cloud Optimized GeoTIFFs (COGs) with STAC metadata.

## Overview

This project provides two main pipeline workflows for processing geospatial data:

1. **DAAC Pipeline** (`pipeline_daac.py`) - Complete end-to-end ingestion from NASA DAAC archives
2. **NetCDF Pipeline** (`pipeline_netcdf.py`) - Direct processing of NetCDF files from S3 URLs

Both pipelines transform NetCDF data through Zarr intermediate format to COGs and catalog the results using STAC standards.

## Pipeline Architecture

### Data Flow
```
Input → NetCDF Processing → Zarr Conversion → Zarr Concatenation → COG Generation → STAC Cataloging
```

### Core Components
- **CZDT_NETCDF_TO_ZARR** - Converts NetCDF files to Zarr format
- **CZDT_ZARR_CONCAT** - Concatenates individual Zarr files
- **CZDT_ZARR_TO_COG** - Converts Zarr to Cloud Optimized GeoTIFFs
- **STAC Cataloging** - Creates metadata for cloud discovery

## Usage

### DAAC Pipeline

Process granules directly from NASA DAAC archives:

```bash
python src/pipeline_daac.py \
  --granule-id "GRANULE_ID" \
  --collection-id "C123456789-MAAP" \
  --s3-bucket "target-bucket" \
  --s3-prefix "data/processed" \
  --role-arn "arn:aws:iam::123456789:role/S3AccessRole" \
  --cmss-logger-host "https://logger.example.com" \
  --mmgis-host "https://mmgis.example.com" \
  --titiler-token-secret-name "mmgis-token" \
  --job-queue "maap-dps-worker-queue" \
  --zarr-config-url "s3://config-bucket/zarr-config.yaml"
```

**Parameters:**
- `--granule-id`: MAAP granule identifier to process
- `--collection-id`: Collection concept ID for the granule
- `--s3-bucket`: Target S3 bucket for outputs
- `--s3-prefix`: S3 prefix/folder path
- `--role-arn`: AWS IAM role for S3 access
- `--cmss-logger-host`: Pipeline logging endpoint
- `--mmgis-host`: STAC cataloging service
- `--titiler-token-secret-name`: MAAP secret name for authentication
- `--job-queue`: MAAP job queue name
- `--zarr-config-url`: S3 URL to Zarr configuration file

### NetCDF Pipeline

Process NetCDF files directly from S3 URLs:

```bash
python src/pipeline_netcdf.py \
  --input-s3-url "s3://source-bucket/path/to/file.nc" \
  --collection-id "C123456789-MAAP" \
  --s3-bucket "target-bucket" \
  --s3-prefix "data/processed" \
  --role-arn "arn:aws:iam::123456789:role/S3AccessRole" \
  --cmss-logger-host "https://logger.example.com" \
  --mmgis-host "https://mmgis.example.com" \
  --titiler-token-secret-name "mmgis-token" \
  --job-queue "maap-dps-worker-queue" \
  --zarr-config-url "s3://config-bucket/zarr-config.yaml" \
  --variables "PRECTOT PRECCON"
```

**Additional Parameters:**
- `--input-s3-url`: S3 URL of the input NetCDF file to process
- `--variables`: Space-separated list of variables to extract (optional)

### Environment Setup

```bash
# Install dependencies
conda env update -f environment.yml
conda activate ingest

# For MAAP platform execution
.maap/run_pipeline_from_daac.sh
```

## Key Features

- **Multi-source Ingestion**: Support for DAAC archives and direct S3 files
- **Asynchronous Processing**: Handles large datasets with job queue management
- **Format Conversion**: NetCDF → Zarr → COG transformation pipeline
- **Cloud Optimization**: Generates COGs with proper overviews and compression
- **STAC Compliance**: Creates discoverable metadata following OGC standards
- **AWS Integration**: Full S3 integration with role-based access
- **Error Handling**: Comprehensive logging and error recovery

## Output Products

Both pipelines generate:
- **Cloud Optimized GeoTIFFs (.tif)** - Analysis-ready raster data
- **Zarr Files (.zarr)** - Intermediate chunked array format
- **STAC Items** - Standardized metadata for discovery
- **Product Notifications** - Automated cataloging to MMGIS/STAC services

The final product details include URIs for both COG and Zarr outputs for maximum flexibility in downstream applications.

## Dependencies

- Python 3.11+
- xarray, rioxarray - Geospatial array processing
- netCDF4, gdal - File format support
- boto3 - AWS S3 integration
- maap-py - NASA MAAP platform client
- rio-stac - STAC metadata generation

## License

Apache 2.0
