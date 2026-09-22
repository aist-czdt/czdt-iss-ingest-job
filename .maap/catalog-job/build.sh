#!/usr/bin/env bash
# Fail the build on the first error so a broken environment can never be registered as a working algorithm.
set -eo pipefail

# Get current location of build script
basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

echo "Building catalog job environment..."

# Install catalog job environment
echo "Installing catalog job environment..."
pushd "${basedir}"
conda env update -f environment.yml
env_name=$(awk '/^name:/ {print $2}' environment.yml)
popd

# Smoke-test the environment so a broken install fails the build instead of every job
echo "Verifying the ${env_name} environment..."
conda run -n "${env_name}" python -c "import maap.maap, pystac, backoff, fsspec, requests; print('${env_name} environment OK')"

echo "Catalog job build complete!"