#!/usr/bin/env bash

# Get current location of build script
basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname "${basedir}")
granule_id="$1"
collection_id="$2"
s3_bucket="$3"
s3_prefix="$4"
role_arn="$5"
cmss_logger_host="$6"
mmgis_host="$7"
titiler_token_secret_name="$8"
job_queue="$9"
czdt_role_arn="$10"
zarr_config_url="$11"

mkdir -p output
source activate ingest

python "${root_dir}"/src/pipeline_daac.py \
    --granule-id "${granule_id}" \
    --collection-id "${collection_id}" \
    --s3-bucket "${s3_bucket}" \
    --s3-prefix "${s3_prefix}" \
    --role-arn "${role_arn}" \
    --cmss-logger-host "${cmss_logger_host}" \
    --mmgis-host "${mmgis_host}" \
    --titiler-token-secret-name "${titiler_token_secret_name}" \
    --job-queue "${job_queue}" \
    --czdt-role-arn "${czdt_role_arn}" \
    --zarr-config-url "${zarr_config_url}"