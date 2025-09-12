#!/usr/bin/env bash

# CZDT ISS LIS Preprocessing - Pipeline Runner
# Reads job parameters from _job.json file instead of command line arguments

# Get current location of script
basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

echo "Running LIS preprocessing pipeline..."

# Check if _job.json exists
if [[ ! -f "_job.json" ]]; then
    echo "ERROR: _job.json file not found"
    exit 1
fi

# Read parameters from _job.json using jq
input_s3=$(jq -r '.params.input_s3 // empty' _job.json)
collection_id=$(jq -r '.params.collection_id // empty' _job.json)
s3_bucket=$(jq -r '.params.s3_bucket // empty' _job.json)
s3_prefix=$(jq -r '.params.s3_prefix // ""' _job.json)
role_arn=$(jq -r '.params.role_arn // empty' _job.json)
cmss_logger_host=$(jq -r '.params.cmss_logger_host // empty' _job.json)
mmgis_host=$(jq -r '.params.mmgis_host // empty' _job.json)
titiler_token_secret_name=$(jq -r '.params.titiler_token_secret_name // empty' _job.json)
zarr_config_url=$(jq -r '.params.zarr_config_url // empty' _job.json)
variables=$(jq -r '.params.variables // "*"' _job.json)
enable_concat=$(jq -r '.params.enable_concat // "false"' _job.json)
local_download_path=$(jq -r '.params.local_download_path // "output"' _job.json)
maap_host=$(jq -r '.params.maap_host // "api.maap-project.org"' _job.json)
upsert=$(jq -r '.params.upsert // "false"' _job.json)
concept_id=$(jq -r '.params.concept_id // empty' _job.json)
time_coord=$(jq -r '.params.time_coord // "time"' _job.json)
lat_coord=$(jq -r '.params.lat_coord // "lat"' _job.json)
lon_coord=$(jq -r '.params.lon_coord // "lon"' _job.json)
default_queue=$(jq -r '.job_info.job_queue // empty' _job.json)
job_queue=$(jq -r '.params.job_queue // empty' _job.json)

# Use default queue if job_queue not specified
if [[ -z "${job_queue}" ]]; then
    job_queue="${default_queue}"
fi

# Debug: Show parsed parameters
echo "=== Parsed Parameters from _job.json ==="
echo "input_s3: ${input_s3}"
echo "collection_id: ${collection_id}"
echo "s3_bucket: ${s3_bucket}"
echo "s3_prefix: ${s3_prefix}"
echo "role_arn: ${role_arn}"
echo "variables: ${variables}"
echo "enable_concat: ${enable_concat}"
echo "========================================"

# Build arguments for the LIS preprocessing pipeline
args=()

# Add parameters if present
if [[ -n "${input_s3}" ]]; then
    args+=(--input-s3 "${input_s3}")
fi
if [[ -n "${collection_id}" ]]; then
    args+=(--collection-id "${collection_id}")
fi
if [[ -n "${s3_bucket}" ]]; then
    args+=(--s3-bucket "${s3_bucket}")
fi
if [[ -n "${s3_prefix}" ]]; then
    args+=(--s3-prefix "${s3_prefix}")
fi
if [[ -n "${role_arn}" ]]; then
    args+=(--role-arn "${role_arn}")
fi
if [[ -n "${cmss_logger_host}" ]]; then
    args+=(--cmss-logger-host "${cmss_logger_host}")
fi
if [[ -n "${mmgis_host}" ]]; then
    args+=(--mmgis-host "${mmgis_host}")
fi
if [[ -n "${titiler_token_secret_name}" ]]; then
    args+=(--titiler-token-secret-name "${titiler_token_secret_name}")
fi
if [[ -n "${zarr_config_url}" ]]; then
    args+=(--zarr-config-url "${zarr_config_url}")
fi
if [[ -n "${variables}" && "${variables}" != "*" ]]; then
    args+=(--variables "${variables}")
fi
if [[ "${enable_concat}" == "true" ]]; then
    args+=(--enable-concat)
fi
if [[ -n "${local_download_path}" && "${local_download_path}" != "output" ]]; then
    args+=(--local-download-path "${local_download_path}")
fi
if [[ -n "${maap_host}" && "${maap_host}" != "api.maap-project.org" ]]; then
    args+=(--maap-host "${maap_host}")
fi
if [[ "${upsert}" == "true" ]]; then
    args+=(--upsert)
fi
if [[ -n "${concept_id}" ]]; then
    args+=(--concept-id "${concept_id}")
fi
if [[ -n "${time_coord}" && "${time_coord}" != "time" ]]; then
    args+=(--time-coord "${time_coord}")
fi
if [[ -n "${lat_coord}" && "${lat_coord}" != "lat" ]]; then
    args+=(--lat-coord "${lat_coord}")
fi
if [[ -n "${lon_coord}" && "${lon_coord}" != "lon" ]]; then
    args+=(--lon-coord "${lon_coord}")
fi
if [[ -n "${job_queue}" ]]; then
    args+=(--job-queue "${job_queue}")
fi

echo "Executing: python ${root_dir}/src/preprocess_lis_pipeline.py ${args[@]}"
conda run -n ingest --live-stream python "${root_dir}/src/preprocess_lis_pipeline.py" "${args[@]}"