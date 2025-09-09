#!/usr/bin/env bash

# Get current location of build script
basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

echo "Building localized pipeline with transformers dependencies..."

# Clone czdt-iss-transformers repo if it doesn't exist
TRANSFORMERS_DIR="${root_dir}/czdt-iss-transformers"
if [ ! -d "${TRANSFORMERS_DIR}" ]; then
    echo "Cloning czdt-iss-transformers repository..."
    pushd "${root_dir}"
    git clone https://github.com/MAAP-Platform/czdt-iss-transformers.git
    popd
else
    echo "czdt-iss-transformers repository already exists, pulling latest changes..."
    pushd "${TRANSFORMERS_DIR}"
    git pull
    popd
fi

# Install base ingest job environment
echo "Installing base ingest job environment..."
pushd "${basedir}"
conda env update -f environment.yml
popd

# Install transformers dependencies
echo "Installing transformers dependencies..."
pushd "${TRANSFORMERS_DIR}"
conda env update -f environment.yaml
popd

echo "Build complete!"