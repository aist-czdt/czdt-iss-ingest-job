#!/usr/bin/env bash

# Get current location of build script
basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname "${basedir}")

INPUT_FILE=$(ls -d input/*)
mkdir -p output
source activate ingest
python "${root_dir}"/src/stage_from_daac.py -i "${INPUT_FILE}" --variables ${@:1} -o output
