#!/usr/bin/env bash

# Get current location of build script
basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname "${basedir}")
ftp_server="$1"
area_of_interest="$2"

mkdir -p output
source activate ingest

python "${root_dir}"/src/stage_from_ftp.py \
    --ftp-server "${ftp_server}" \
    --area-of-interest "${area_of_interest}"