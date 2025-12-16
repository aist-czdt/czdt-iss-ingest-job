#!/usr/bin/env bash

# CZDT ISS Catalog Job - Pipeline Runner
# Reads job parameters from _job.json file instead of command line arguments

# Get current location of script
basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

echo "Running catalog job..."
source activate ingest

# Check if _job.json exists
if [[ ! -f "_job.json" ]]; then
    echo "ERROR: _job.json file not found"
    exit 1
fi

# Read parameters from _job.json using jq
parent_job_id=$(jq -r '.params.parent_job_id // empty' _job.json)
mmgis_host=$(jq -r '.params.mmgis_host // empty' _job.json)
titiler_token_secret_name=$(jq -r '.params.titiler_token_secret_name // empty' _job.json)
cmss_logger_host=$(jq -r '.params.cmss_logger_host // empty' _job.json)
collection_id=$(jq -r '.params.collection_id // empty' _job.json)
maap_host=$(jq -r '.params.maap_host // "api.maap-project.org"' _job.json)
max_wait_time=$(jq -r '.params.max_wait_time // "172800"' _job.json)
max_backoff=$(jq -r '.params.max_backoff // "64"' _job.json)
upsert=$(jq -r '.params.upsert // "false"' _job.json)

# Debug: Show parsed parameters
echo "=== Parsed Parameters from _job.json ==="
echo "parent_job_id: ${parent_job_id}"
echo "mmgis_host: ${mmgis_host}"
echo "titiler_token_secret_name: ${titiler_token_secret_name}"
echo "cmss_logger_host: ${cmss_logger_host}"
echo "collection_id: ${collection_id}"
echo "maap_host: ${maap_host}"
echo "max_wait_time: ${max_wait_time}"
echo "max_backoff: ${max_backoff}"
echo "upsert: ${upsert}"
echo "========================================"

# Build arguments for the catalog job
args=()

# Add required parameters
if [[ -n "${parent_job_id}" ]]; then
    args+=(--parent-job-id "${parent_job_id}")
fi
if [[ -n "${mmgis_host}" ]]; then
    args+=(--mmgis-host "${mmgis_host}")
fi
if [[ -n "${titiler_token_secret_name}" ]]; then
    args+=(--titiler-token-secret-name "${titiler_token_secret_name}")
fi
if [[ -n "${cmss_logger_host}" ]]; then
    args+=(--cmss-logger-host "${cmss_logger_host}")
fi
if [[ -n "${collection_id}" ]]; then
    args+=(--collection-id "${collection_id}")
fi

# Add optional parameters with defaults
if [[ -n "${maap_host}" && "${maap_host}" != "api.maap-project.org" ]]; then
    args+=(--maap-host "${maap_host}")
fi
if [[ -n "${max_wait_time}" && "${max_wait_time}" != "172800" ]]; then
    args+=(--max-wait-time "${max_wait_time}")
fi
if [[ -n "${max_backoff}" && "${max_backoff}" != "64" ]]; then
    args+=(--max-backoff "${max_backoff}")
fi
if [[ "${upsert}" == "true" ]]; then
    args+=(--upsert)
fi

echo "Executing: python ${root_dir}/src/catalog_job.py ${args[@]}"
conda run -n ingest --live-stream python "${root_dir}/src/catalog_job.py" "${args[@]}"