#!/usr/bin/env bash

# Get current location of build script
basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname "${basedir}")
ftp_server="$1"
area_of_interest="$2"
s3_bucket="$3"
s3_prefix="$4"
role_arn="$5"
cmss_logger_host="$6"
mmgis_host="$7"
titiler_token_secret_name="$8"
overwrite_existing="$9"

mkdir -p output
source activate ingest

python "${root_dir}"/src/stage_from_ftp.py \
    --ftp-server "${ftp_server}" \
    --area-of-interest "${area_of_interest}" \
    --s3-bucket "${s3_bucket}" \
    --s3-prefix "${s3_prefix}" \
    --role-arn "${role_arn}" \
    --cmss-logger-host "${cmss_logger_host}" \
    --mmgis-host "${mmgis_host}" \
    --titiler-token-secret-name "${titiler_token_secret_name}" \
    --overwrite-existing "${overwrite_existing}" 