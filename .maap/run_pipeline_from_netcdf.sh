#!/usr/bin/env bash

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
echo "========================================"

mkdir -p output
source activate ingest

python "${root_dir}"/src/pipeline_netcdf.py \
    --input-s3-url "${input_s3_url}" \
    --collection-id "${collection_id}" \
    --s3-bucket "${s3_bucket}" \
    --s3-prefix "${s3_prefix}" \
    --role-arn "${role_arn}" \
    --cmss-logger-host "${cmss_logger_host}" \
    --mmgis-host "${mmgis_host}" \
    --titiler-token-secret-name "${titiler_token_secret_name}" \
    --job-queue "${job_queue}" \
    --zarr-config-url "${zarr_config_url}" \
    --variables "${variables}"