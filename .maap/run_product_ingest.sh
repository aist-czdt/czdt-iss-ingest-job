#!/usr/bin/env bash

# CZDT ISS Product Ingest - Generic Pipeline Runner
# Reads job parameters from _job.json file instead of command line arguments
# Supports DAAC, S3 NetCDF, and S3 Zarr inputs with automatic detection

# Get current location of build script
basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname "${basedir}")

# Check if _job.json exists
if [[ ! -f "_job.json" ]]; then
    echo "ERROR: _job.json file not found"
    exit 1
fi
source activate ingest
# Read parameters from _job.json using jq
granule_id=$(jq -r '.params.granule_id // empty' _job.json)
input_s3=$(jq -r '.params.input_s3 // empty' _job.json)
collection_id=$(jq -r '.params.collection_id // empty' _job.json)
s3_bucket=$(jq -r '.params.s3_bucket // empty' _job.json)
s3_prefix=$(jq -r '.params.s3_prefix // ""' _job.json)
role_arn=$(jq -r '.params.role_arn // empty' _job.json)
cmss_logger_host=$(jq -r '.params.cmss_logger_host // empty' _job.json)
mmgis_host=$(jq -r '.params.mmgis_host // empty' _job.json)
titiler_token_secret_name=$(jq -r '.params.titiler_token_secret_name // empty' _job.json)
job_queue=$(jq -r '.job_info.job_queue // empty' _job.json)
zarr_config_url=$(jq -r '.params.zarr_config_url // empty' _job.json)
variables=$(jq -r '.params.variables // "*"' _job.json)
enable_concat=$(jq -r '.params.enable_concat // "false"' _job.json)
local_download_path=$(jq -r '.params.local_download_path // "output"' _job.json)
maap_host=$(jq -r '.params.maap_host // "api.maap-project.org"' _job.json)

# Validate required parameters
if [[ -z "${s3_bucket}" ]]; then
    echo "ERROR: s3_bucket is required"
    exit 1
fi
if [[ -z "${role_arn}" ]]; then
    echo "ERROR: role_arn is required"
    exit 1
fi
if [[ -z "${cmss_logger_host}" ]]; then
    echo "ERROR: cmss_logger_host is required"
    exit 1
fi
if [[ -z "${mmgis_host}" ]]; then
    echo "ERROR: mmgis_host is required"
    exit 1
fi
if [[ -z "${titiler_token_secret_name}" ]]; then
    echo "ERROR: titiler_token_secret_name is required"
    exit 1
fi
if [[ -z "${job_queue}" ]]; then
    echo "ERROR: job_queue is required"
    exit 1
fi
if [[ -z "${zarr_config_url}" ]]; then
    echo "ERROR: zarr_config_url is required"
    exit 1
fi

# Debug: Show parsed parameters
echo "=== Parsed Parameters from _job.json ==="
echo "granule_id: ${granule_id}"
echo "input_s3: ${input_s3}"
echo "collection_id: ${collection_id}"
echo "s3_bucket: ${s3_bucket}"
echo "s3_prefix: ${s3_prefix}"
echo "variables: ${variables}"
echo "enable_concat: ${enable_concat}"
echo "========================================"

mkdir -p "${local_download_path}"

# Build arguments for the generic pipeline
args=(
    --s3-bucket "${s3_bucket}"
    --role-arn "${role_arn}" 
    --cmss-logger-host "${cmss_logger_host}"
    --mmgis-host "${mmgis_host}"
    --titiler-token-secret-name "${titiler_token_secret_name}"
    --job-queue "${job_queue}"
    --zarr-config-url "${zarr_config_url}"
    --variables "${variables}"
    --local-download-path "${local_download_path}"
    --maap-host "${maap_host}"
)

# Add optional s3-prefix if provided
if [[ -n "${s3_prefix}" ]]; then
    args+=(--s3-prefix "${s3_prefix}")
fi

# Add enable-concat flag if true
if [[ "${enable_concat}" == "true" ]]; then
    args+=(--enable-concat)
fi

# Determine input type and add appropriate arguments
if [[ -n "${granule_id}" && "${granule_id}" != "none" ]]; then
    # DAAC input
    args+=(--granule-id "${granule_id}")
    if [[ -n "${collection_id}" ]]; then
        args+=(--collection-id "${collection_id}")
    else
        echo "ERROR: collection-id is required when using granule-id"
        exit 1
    fi
elif [[ -n "${input_s3}" ]]; then
    # S3 input
    args+=(--input-s3 "${input_s3}")
    # collection-id is still needed for cataloging
    if [[ -n "${collection_id}" ]]; then
        args+=(--collection-id "${collection_id}")
    fi
else
    echo "ERROR: Either granule-id or input-s3 must be provided"
    exit 1
fi

echo "Running generic pipeline with arguments: ${args[@]}"

# Execute the generic pipeline
python "${root_dir}"/src/pipeline_generic.py "${args[@]}"