#!/usr/bin/env bash

# Get current location of script
basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

echo "Running catalog job..."

# Activate conda environment
conda activate catalog-ingest

# Run the catalog job with provided arguments
python "${root_dir}/src/catalog_job.py" "$@"