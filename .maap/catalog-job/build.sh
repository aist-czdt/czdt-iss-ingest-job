#!/usr/bin/env bash

# Get current location of build script
basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

echo "Building catalog job environment..."

# Install catalog job environment
echo "Installing catalog job environment..."
pushd "${basedir}"
conda env update -f environment.yml
popd

echo "Catalog job build complete!"