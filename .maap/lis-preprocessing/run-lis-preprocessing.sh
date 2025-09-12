#!/usr/bin/env bash

# Get current location of script
basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

echo "Running LIS preprocessing pipeline..."

# Check if there's an input file (file parameter from MAAP)
input_file=""
if [ -d "input" ] && [ "$(ls -A input 2>/dev/null)" ]; then
    input_file=$(ls input/* | head -n 1)
    echo "Found input file: ${input_file}"
    echo "Note: LIS preprocessing pipeline requires --input-s3 parameter, input file will be ignored"
fi

# Run the LIS preprocessing pipeline with provided arguments
echo "Executing: python ${root_dir}/src/preprocess_lis_pipeline.py $@"
conda run -n ingest --live-stream python "${root_dir}/src/preprocess_lis_pipeline.py" "$@"