#!/usr/bin/env bash

# Get current location of build script
basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname "${basedir}")

set -x

# Check if _job.json exists
if [[ ! -f "_job.json" ]]; then
    echo "ERROR: _job.json file not found"
    exit 1
fi

source activate ingest

# Read parameters from _job.json using jq
mmgis_host=$(jq -r '.params.mmgis_host // empty' _job.json)
titiler_token_secret_name=$(jq -r '.params.titiler_token_secret_name // empty' _job.json)
job_queue=$(jq -r '.params.job_queue // empty' _job.json)
zarr_config_url=$(jq -r '.params.zarr_config_url // empty' _job.json)
maap_host=$(jq -r '.params.maap_host // "api.maap-project.org"' _job.json)
stac_collection=$(jq -r '.params.stac_collection // empty' _job.json)
start_date=$(jq -r '.params.start_date // "1970-01-01T00:00:00Z"' _job.json)
end_date=$(jq -r '.params.end_date // "2999-12-31T23:59:59Z"' _job.json)
days_back=$(jq -r '.params.days_back // empty' _job.json)
sdap_collection=$(jq -r '.params.sdap_collection // empty' _job.json)
sdap_base_url=$(jq -r '.params.sdap_base_url // empty' _job.json)
variable=$(jq -r '.params.variable // empty' _job.json)

# Validate required parameters
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
if [[ -z "${stac_collection}" ]]; then
    echo "ERROR: stac_collection is required"
    exit 1
fi
if [[ -z "${sdap_collection}" ]]; then
    echo "ERROR: sdap_collection is required"
    exit 1
fi
if [[ -z "${sdap_base_url}" ]]; then
    echo "ERROR: sdap_base_url is required"
    exit 1
fi
if [[ -z "${variable}" ]]; then
    echo "ERROR: variable is required"
    exit 1
fi

# Debug: Show parsed parameters
echo "=== Parsed Parameters from _job.json ==="
echo "mmgis_host: ${mmgis_host}"
echo "titiler_token_secret_name: ${titiler_token_secret_name}"
echo "job_queue: ${job_queue}"
echo "zarr_config_url: ${zarr_config_url}"
echo "maap_host: ${maap_host}"
echo "stac_collection: ${stac_collection}"
echo "start_date: ${start_date}"
echo "end_date: ${end_date}"
echo "days_back: ${days_back}"
echo "sdap_collection: ${sdap_collection}"
echo "sdap_base_url: ${sdap_base_url}"
echo "variable: ${variable}"
echo "========================================"

# Build arguments for the generic pipeline
args=(
    --s3-bucket "."  # Reused parser where this is marked as required but it's not used here
    --mmgis-host "${mmgis_host}"
    --titiler-token-secret-name "${titiler_token_secret_name}"
    --job-queue "${job_queue}"
    --zarr-config-url "${zarr_config_url}"
    --maap-host "${maap_host}"
    --stac-collection "${stac_collection}"
    --sdap-collection-name "${sdap_collection}"
    --sdap-base-url "${sdap_base_url}"
    --variable "${variable}"
)

if [[ -z "${days_back}" ]]; then
  args+=(
    --start-date "${start_date}"
    --end-date "${end_date}"
  )
else
  args+=(--days-back "${days_back}")
fi

echo "Running STAC concatenation pipeline with parameters: ${args[@]}"

# Execute pipeline
python -u "${root_dir}"/src/stac_cat.py "${args[@]}"

