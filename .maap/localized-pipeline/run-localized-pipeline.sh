#!/usr/bin/env bash

# Get current location of script
basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

echo "Running localized pipeline..."

# Activate conda environment
conda activate ingest

# Check if there's an input file (file parameter from MAAP)
input_netcdf_file=""
if [ -d "input" ] && [ "$(ls -A input 2>/dev/null)" ]; then
    input_netcdf_file=$(ls input/* | head -n 1)
    echo "Found input file: ${input_netcdf_file}"
fi

# Prepare arguments
args=("$@")
if [ -n "${input_netcdf_file}" ]; then
    args+=(--input-netcdf "${input_netcdf_file}")
    echo "Added --input-netcdf ${input_netcdf_file} to arguments"
fi

# Run the localized pipeline with provided arguments
python "${root_dir}/src/localized_pipeline.py" "${args[@]}"