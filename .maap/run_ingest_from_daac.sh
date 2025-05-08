#!/usr/bin/env bash

# Get current location of build script
basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname "${basedir}")
granule_id="$1"
collection_id="$2"
s3_bucket="$3"
s3_prefix="$4"
role_arn="$5"

mkdir -p output
source activate ingest

python "${root_dir}"/src/stage_from_daac.py \
    --granule-id "${granule_id}" \
    --collection-id "${collection_id}" \
    --s3-bucket "${s3_bucket}" \
    --s3-prefix "${s3_prefix}" \
    --role-arn "${role_arn}"