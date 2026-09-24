#!/usr/bin/env bash
# Fail the build on the first error. Without this a failed transformers install still produced a "successful"
# registration whose jobs all died with "No module named 'czdt_iss_transformers'" (v0.2.6, 2026-09-22).
set -eo pipefail

# Get current location of build script
basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

pushd "${root_dir}"
echo "Building localized pipeline with transformers dependencies from $(git rev-parse HEAD)..."
popd

# Clone czdt-iss-transformers repo if it doesn't exist
TRANSFORMERS_DIR="${root_dir}/czdt-iss-transformers"
TRANSFORMERS_BRANCH="master"

if [ ! -d "${TRANSFORMERS_DIR}" ]; then
    echo "Cloning czdt-iss-transformers repository..."
    pushd "${root_dir}"
    git clone --depth 1 --single-branch --branch "${TRANSFORMERS_BRANCH}" https://github.com/aist-czdt/czdt-iss-transformers.git
    pushd czdt-iss-transformers
    echo "$(git log -1)"
    popd
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
conda env update -n ingest --file environment.yml
popd


# Install transformers dependencies
echo "Installing transformers dependencies..."
pushd "${TRANSFORMERS_DIR}"
conda env update -n ingest --file environment.yaml
conda run -n ingest pip install -e .
popd


echo "Ensuring MAAP-py is installed (it has been observed to be uninstalled somehow by the above conda step)"
conda run -n ingest pip install 'maap-py<5'

# For input parsing
conda run -n ingest pip install jq

# Smoke-test the environment so a broken install fails the build instead of every job
echo "Verifying the ingest environment..."
conda run -n ingest python -c "import czdt_iss_transformers.cf2zarr, czdt_iss_transformers.zarr2cog, czdt_iss_transformers.zarr_concat, czdt_iss_transformers.preprocessors.lis.lis_preprocessor, maap.maap, pystac, backoff, jq; print('ingest environment OK')"

# Prove the installed stack writes COGs with the right numbers (see verify_cog_roundtrip.py)
echo "Verifying Zarr -> COG value roundtrip..."
conda run -n ingest --live-stream python "${basedir}/verify_cog_roundtrip.py"
echo "Build complete!"
