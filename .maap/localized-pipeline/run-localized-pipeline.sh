#!/usr/bin/env bash

# Get current location of script
basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

echo "Running localized pipeline..."

# Activate conda environment
conda activate ingest

# Run the localized pipeline with provided arguments
python "${root_dir}/src/localized_pipeline.py" "$@"