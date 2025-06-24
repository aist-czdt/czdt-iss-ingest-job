# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

CZDT ISS Ingest Job is a NASA MAAP platform data ingestion pipeline that transforms NetCDF satellite data into Cloud Optimized GeoTIFFs (COGs) with STAC metadata. The pipeline handles multi-source data ingestion (DAAC archives, FTP servers), asynchronous processing, and AWS S3 cloud storage integration.

## Common Commands

### Environment Setup
```bash
# Install dependencies
conda env update -f environment.yml
conda activate ingest
```

### Development and Testing
```bash
# Test individual components
python src/stage_from_daac.py --granule-id <id> --collection-id <id> --s3-bucket <bucket>
python src/ingest.py -i input.nc -o output_dir --variables var1 var2
python src/create_stac_items.py -i processed_file.tif -o stac_output

# Run full pipeline
python src/pipeline_daac.py --granule-id <id> --collection-id <id> --s3-bucket <bucket> --variables <vars>
```

### MAAP Platform Execution
```bash
# Via MAAP shell scripts
.maap/run_pipeline_from_daac.sh
.maap/run_ingest_from_daac.sh  
.maap/run_ingest_from_ftp.sh
```

## Architecture

### Pipeline Flow
1. **Staging** (`stage_from_daac.py`, `stage_from_ftp.py`) - Downloads granules from NASA DAAC or FTP servers
2. **Ingestion** (`ingest.py`) - Converts NetCDF → Zarr → COG with geospatial transformations
3. **Cataloging** (`create_stac_items.py`) - Generates STAC metadata for cloud discovery
4. **Orchestration** (`pipeline_daac.py`) - Coordinates the full pipeline with async job monitoring

### Key Components
- **Async Job Management** (`async_job.py`) - Handles MAAP platform job queuing and monitoring
- **S3 Integration** - All outputs stored in AWS S3 with role-based access
- **STAC Compliance** - Uses `rio-stac` for metadata generation following OGC standards

### Data Flow Architecture
```
DAAC/FTP → Local Staging → NetCDF Processing → Zarr Intermediate → COG Generation → S3 Upload → STAC Catalog
```

## Project Structure

### Core Pipeline Files
- `src/pipeline_daac.py` - Main orchestrator with end-to-end pipeline logic
- `src/stage_from_daac.py` - NASA DAAC data retrieval using `maap-py`
- `src/stage_from_ftp.py` - FTP server data retrieval with pattern matching
- `src/ingest.py` - Core NetCDF→COG conversion using `xarray` and `rioxarray`
- `src/create_stac_items.py` - STAC metadata generation and validation

### Configuration
- `environment.yml` - Conda environment with geospatial stack (`xarray`, `rioxarray`, `gdal`, `netCDF4`, `boto3`)
- `.maap/sample-algo-configs/` - MAAP platform algorithm configurations for different job types
- `.maap/*.sh` - Shell scripts for MAAP platform execution

## Important Implementation Details

### Geospatial Processing
- Uses `xarray` for N-dimensional array operations on NetCDF files
- Leverages `rioxarray` for CRS transformations and geospatial indexing
- Implements chunked processing for large datasets using Zarr intermediate format
- Generates COGs with proper overviews and compression using GDAL

### Cloud Integration
- All processing designed for AWS S3 backend storage
- Uses `boto3` with role-based authentication (no hardcoded credentials)
- Implements proper error handling for network failures and retry logic

### MAAP Platform Integration
- Jobs run on 8GB worker nodes with 50GB disk space allocation
- Uses async job queuing system for large dataset processing
- Follows MAAP naming conventions and output path structures

## Dependencies

Primary geospatial stack managed via Conda:
- `xarray`, `rioxarray` - NetCDF and geospatial array processing  
- `netCDF4`, `gdal` - File format support
- `boto3` - AWS S3 integration
- `maap-py` - NASA MAAP platform client
- `rio-stac` - STAC metadata generation