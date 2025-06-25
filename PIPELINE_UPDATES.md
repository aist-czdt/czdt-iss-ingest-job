# Generic Pipeline Updates - Implementation Guide

This document summarizes the comprehensive updates made to create a unified generic pipeline for CZDT ISS data processing.

## Overview

The generic pipeline (`pipeline_generic.py`) unifies three distinct processing workflows:
- **DAAC Input**: Download from NASA DAAC archives
- **S3 NetCDF Input**: Process NetCDF files from S3 URLs
- **S3 Zarr Input**: Direct Zarr-to-COG conversion

## Core Components Updated

### 1. Pipeline Script: `src/pipeline_generic.py`

**Key Features:**
- **Auto-detection**: Input type determined by parameter presence (`granule_id` vs `input_s3`)
- **Flexible Processing**: Optional Zarr concatenation via `--enable-concat` flag
- **Variable Control**: User-specified variables via `--variables` (default: "*" for all)
- **Fixed Dimensions**: Zarr-to-COG uses standardized dimensions (time="time", lat="lat", lon="lon")
- **Error Handling**: Integrated JobFailedException for pipeline failure management

**Processing Flows:**
```
DAAC:        Stage → NetCDF→Zarr → [concat] → COG → Catalog
S3 NetCDF:   NetCDF→Zarr → [concat] → COG → Catalog  
S3 Zarr:     Zarr→COG → Catalog (skip conversion)
```

**Usage Examples:**
```bash
# DAAC Processing
python src/pipeline_generic.py \
  --granule-id "GRANULE_ID" \
  --collection-id "C123456789-MAAP" \
  --s3-bucket "bucket" \
  --variables "PRECTOT PRECCON" \
  --enable-concat

# S3 NetCDF Processing  
python src/pipeline_generic.py \
  --input-s3 "s3://bucket/file.nc" \
  --collection-id "C123456789-MAAP" \
  --s3-bucket "bucket" \
  --variables "*"

# S3 Zarr Processing
python src/pipeline_generic.py \
  --input-s3 "s3://bucket/file.zarr" \
  --collection-id "C123456789-MAAP" \
  --s3-bucket "bucket"
```

### 2. Async Job Error Handling: `src/async_job.py`

**Enhancements:**
- **JobFailedException**: New exception class for job failure scenarios
- **Status Monitoring**: Enhanced `check_status()` detects "Failed" status
- **Error Propagation**: Failures bubble up through pipeline with exit code 5

**Implementation:**
```python
if self.status == "Failed":
    raise JobFailedException(f"Job {self.job_id} failed with status: {self.status}")
```

### 3. STAC Collection Management: `src/create_stac_items.py`

**New Functions:**
- **`extract_variable_from_tif_filename()`**: Intelligent variable name extraction from filenames
- **`check_collection_exists()`**: API-based collection verification  
- **`create_stac_collection()`**: STAC 1.0.0 compliant collection creation
- **`ensure_collection_exists()`**: Unified collection management

**Collection Naming Convention:**
- **Existing**: Uses provided `collection_id` if collection exists
- **New**: Creates `{collection_id}_{variable_name}` (e.g., "C123456789-MAAP_PRECTOT")

**Variable Extraction Patterns:**
```python
# Pattern 1: filename_VARIABLE.tif
"MERRA2_400.tavg1_2d_flx_Nx.20250331_PRECTOT.tif" → "PRECTOT"

# Pattern 2: Scientific naming conventions
"data_TEMP_analysis.tif" → "TEMP"

# Fallback: Last meaningful part or "data"
```

### 4. MAAP Platform Integration

**Algorithm Configuration: `.maap/sample-algo-configs/czdt-iss-product-ingest.yml`**
```yaml
algorithm_name: czdt-iss-product-ingest
inputs:
  positional:
  - name: granule_id
    required: false
    description: "DAAC Granule ID (mutually exclusive with input_s3)"
  - name: input_s3  
    required: false
    description: "S3 URL of input file - NetCDF or Zarr"
  - name: variables
    default: "*"
    description: "Variables to extract from NetCDF"
  - name: enable_concat
    default: "false"
    description: "Enable Zarr concatenation step (true/false)"
```

**Run Script: `.maap/run_product_ingest.sh`**
- **JSON Parsing**: Reads parameters from `_job.json` using `jq`
- **Input Validation**: Comprehensive parameter checking with clear error messages
- **Type Detection**: Automatic input type detection and argument building
- **Debug Output**: Parameter display for troubleshooting

**JSON Structure:**
```json
{
  "params": {
    "granule_id": "optional_granule_id",
    "input_s3": "optional_s3_url", 
    "collection_id": "required_collection_id",
    "s3_bucket": "required_bucket",
    "variables": "*",
    "enable_concat": "false"
  }
}
```

### 5. Environment Dependencies

**Added to `environment.yml`:**
```yaml
dependencies:
  - jq  # For JSON parsing in shell scripts
```

## Integration Architecture

### Parameter Flow
```
MAAP Job Submission → _job.json → run_product_ingest.sh → pipeline_generic.py
```

### Error Handling Chain
```
DPS Job Failure → AsyncJob.check_status() → JobFailedException → Pipeline Exit (code 5)
```

### Collection Management Flow
```
TIF File → Variable Extraction → Collection Check → Create if Needed → STAC Item Creation
```

## Deployment Considerations

### Backward Compatibility
- Existing pipeline scripts (`pipeline_daac.py`, `pipeline_netcdf.py`) remain functional
- Generic pipeline provides unified interface without breaking existing workflows
- STAC collection creation handles both new and existing collections gracefully

### Error Recovery
- Pipeline stops on first job failure for data integrity
- Clear error messages with specific exit codes for different failure types
- Debug output helps troubleshoot parameter and job issues

### Performance
- Parallel job execution using asyncio for multiple file processing
- Optional concatenation step reduces processing overhead when not needed
- Efficient collection checking minimizes API calls

## Usage Guidelines

### When to Use Generic Pipeline
- **Multi-source Processing**: When input source varies (DAAC vs S3)
- **Variable Processing**: When different variables need different handling
- **Conditional Steps**: When concatenation is sometimes needed
- **Collection Management**: When collections need to be created dynamically

### When to Use Specific Pipelines
- **Fixed Workflows**: When processing is always the same type
- **Performance Critical**: When minimal overhead is required
- **Legacy Integration**: When existing systems depend on specific interfaces

## Testing and Validation

### Key Test Scenarios
1. **DAAC Processing**: End-to-end granule download and processing
2. **S3 NetCDF**: Direct file processing with variable extraction
3. **S3 Zarr**: Skip conversion steps, direct COG generation
4. **Collection Creation**: New collection creation with variable naming
5. **Error Handling**: Job failure detection and proper exit codes

### Validation Points
- Input type detection accuracy
- Variable extraction from various filename patterns
- Collection existence checking and creation
- Parameter parsing from JSON
- Error propagation through async job chain

This generic pipeline architecture provides a robust, flexible foundation for CZDT ISS data processing while maintaining the reliability and performance of the original specialized pipelines.