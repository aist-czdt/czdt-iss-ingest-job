#!/usr/bin/env bash

# Get current location of build script
basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname "${basedir}")
ftp_server="$1"
search_keyword="$2"
s3_bucket="$3"
s3_prefix="$4"
role_arn="$5"

mkdir -p output
source activate ingest

python "${root_dir}"/src/stage_from_ftp.py \
    --ftp-server "${ftp_server}" \
    --search-keyword "${search_keyword}" \
    --s3-bucket "${s3_bucket}" \
    --s3-prefix "${s3_prefix}" \
    --role-arn "${role_arn}"