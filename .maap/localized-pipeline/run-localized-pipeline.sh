#!/usr/bin/env bash

# CZDT ISS Localized Pipeline - Pipeline Runner
# Reads job parameters from _job.json file instead of command line arguments

# Get current location of script
basedir=$( cd "$(dirname "$0")" ; pwd -P )
root_dir=$(dirname $(dirname "${basedir}"))

echo "Running localized pipeline..."

# Activate conda environment
source activate ingest

# Check if _job.json exists
if [[ ! -f "_job.json" ]]; then
    echo "ERROR: _job.json file not found"
    exit 1
fi

# Read parameters from _job.json using jq
granule_id=$(jq -r '.params.granule_id // empty' _job.json)
input_s3=$(jq -r '.params.input_s3 // empty' _job.json)
# Mapped from input_s3
input_url=$(jq -r '.params.input_s3 // empty' _job.json)
input_netcdf=$(jq -r '.params.input_netcdf // empty' _job.json)
collection_id=$(jq -r '.params.collection_id // empty' _job.json)
s3_bucket=$(jq -r '.params.s3_bucket // empty' _job.json)
s3_prefix=$(jq -r '.params.s3_prefix // ""' _job.json)
role_arn=$(jq -r '.params.role_arn // empty' _job.json)
cmss_logger_host=$(jq -r '.params.cmss_logger_host // empty' _job.json)
mmgis_host=$(jq -r '.params.mmgis_host // empty' _job.json)
titiler_token_secret_name=$(jq -r '.params.titiler_token_secret_name // empty' _job.json)
zarr_config_url=$(jq -r '.params.zarr_config_url // empty' _job.json)
gridding_config_url=$(jq -r '.params.gridding_config_url // empty' _job.json)
# Mapped from gridding_config_url
config=$(jq -r '.params.gridding_config_url // empty' _job.json)
variables=$(jq -r '.params.variables // "*"' _job.json)
enable_concat=$(jq -r '.params.enable_concat // "false"' _job.json)
local_download_path=$(jq -r '.params.local_download_path // "output"' _job.json)
maap_host=$(jq -r '.params.maap_host // "api.maap-project.org"' _job.json)
steps=$(jq -r '.params.steps // "all"' _job.json)
geoserver_host=$(jq -r '.params.geoserver_host // empty' _job.json)
upsert=$(jq -r '.params.upsert // "false"' _job.json)
concept_id=$(jq -r '.params.concept_id // empty' _job.json)
time_coord=$(jq -r '.params.time_coord // "time"' _job.json)
lat_coord=$(jq -r '.params.lat_coord // "lat"' _job.json)
lon_coord=$(jq -r '.params.lon_coord // "lon"' _job.json)
output_extent=$(jq -r '.params.output_extent // empty' _job.json)
# Replace commas with spaces to avoid arg.parse pythone errors
output_extent="${output_extent//,/ }"
grid_resolution=$(jq -r '.params.grid_resolution // empty' _job.json)
grid_size_lon=$(jq -r '.params.grid_size_lon // empty' _job.json)
grid_size_lat=$(jq -r '.params.grid_size_lat // empty' _job.json)
format=$(jq -r '.params.format // "netcdf"' _job.json)
zarr_version=$(jq -r '.params.zarr_version // 3' _job.json)
pattern=$(jq -r '.params.pattern // empty' _job.json)
combine=$(jq -r '.params.combine // "mean"' _job.json)

default_queue=$(jq -r '.job_info.job_queue // empty' _job.json)
job_queue=$(jq -r '.params.job_queue // empty' _job.json)

# Use default queue if job_queue not specified
if [[ -z "${job_queue}" ]]; then
    job_queue="${default_queue}"
fi

# Check if there's an input file (file parameter from MAAP) as fallback
if [[ -z "${input_netcdf}" ]] && [ -d "input" ] && [ "$(ls -A input 2>/dev/null)" ]; then
    input_netcdf=$(ls input/* | head -n 1)
    echo "Found input file: ${input_netcdf}"
fi

# Debug: Show parsed parameters
echo "=== Parsed Parameters from _job.json ==="
echo "granule_id: ${granule_id}"
echo "input_s3: ${input_s3}"
echo "input_netcdf: ${input_netcdf}"
echo "collection_id: ${collection_id}"
echo "s3_bucket: ${s3_bucket}"
echo "s3_prefix: ${s3_prefix}"
echo "role_arn: ${role_arn}"
echo "variables: ${variables}"
echo "enable_concat: ${enable_concat}"
echo "steps: ${steps}"
echo "========================================"

# Build arguments for the localized pipeline
args=()

# Add parameters if present
if [[ -n "${granule_id}" ]]; then
    args+=(--granule-id "${granule_id}")
fi
if [[ -n "${input_s3}" ]]; then
    args+=(--input-s3 "${input_s3}")
fi
if [[ -n "${input_url}" ]]; then
    args+=(--input-url "${input_url}")
fi
if [[ -n "${input_netcdf}" ]]; then
    args+=(--input-netcdf "${input_netcdf}")
fi
if [[ -n "${collection_id}" ]]; then
    args+=(--collection-id "${collection_id}")
fi
if [[ -n "${s3_bucket}" ]]; then
    args+=(--s3-bucket "${s3_bucket}")
fi
if [[ -n "${s3_prefix}" ]]; then
    args+=(--s3-prefix "${s3_prefix}")
fi
if [[ -n "${role_arn}" ]]; then
    args+=(--role-arn "${role_arn}")
fi
if [[ -n "${cmss_logger_host}" ]]; then
    args+=(--cmss-logger-host "${cmss_logger_host}")
fi
if [[ -n "${mmgis_host}" ]]; then
    args+=(--mmgis-host "${mmgis_host}")
fi
if [[ -n "${titiler_token_secret_name}" ]]; then
    args+=(--titiler-token-secret-name "${titiler_token_secret_name}")
fi
if [[ -n "${zarr_config_url}" ]]; then
    args+=(--zarr-config-url "${zarr_config_url}")
fi
if [[ -n "${gridding_config_url}" ]]; then
    args+=(--gridding-config-url "${gridding_config_url}")
fi
if [[ -n "${config}" ]]; then
    args+=(--config "${config}")
fi
if [[ -n "${variables}" && "${variables}" != "*" ]]; then
    args+=(--variables "${variables}")
fi
if [[ "${enable_concat}" == "true" ]]; then
    args+=(--enable-concat)
fi
if [[ -n "${local_download_path}" && "${local_download_path}" != "output" ]]; then
    args+=(--local-download-path "${local_download_path}")
fi

# Mapped from local_download_path
args+=(--output "${local_download_path}")

if [[ -n "${maap_host}" && "${maap_host}" != "api.maap-project.org" ]]; then
    args+=(--maap-host "${maap_host}")
fi
if [[ -n "${steps}" && "${steps}" != "all" ]]; then
    args+=(--steps "${steps}")
fi
if [[ -n "${geoserver_host}" ]]; then
    args+=(--geoserver-host "${geoserver_host}")
fi
if [[ "${upsert}" == "true" ]]; then
    args+=(--upsert)
fi
if [[ -n "${concept_id}" ]]; then
    args+=(--concept-id "${concept_id}")
fi
if [[ -n "${time_coord}" && "${time_coord}" != "time" ]]; then
    args+=(--time-coord "${time_coord}")
fi
if [[ -n "${lat_coord}" && "${lat_coord}" != "lat" ]]; then
    args+=(--lat-coord "${lat_coord}")
fi
if [[ -n "${lon_coord}" && "${lon_coord}" != "lon" ]]; then
    args+=(--lon-coord "${lon_coord}")
fi
if [[ -n "${output_extent}" ]]; then
    args+=(--output-extent "${output_extent}")
fi
if [[ -n "${grid_resolution}" ]]; then
    args+=(--grid-resolution "${grid_resolution}")
fi
if [[ -n "${grid_size_lon}" || -n "${grid_size_lat}" ]]; then
  if [[ -n "${grid_resolution}" ]]; then
    echo "ERROR: grid_resolution cannot be defined with grid_size_lat/grid_size_lon"
    exit 1
  fi

  if [[ -z "${grid_size_lon}" || -z "${grid_size_lat}" ]]; then
    echo "ERROR: both grid_size_lon and grid_size_lat must be defined together, not just one"
    exit 1
  fi
fi
if [[ -n "${grid_resolution}" ]]; then
  args+=(
    --grid-resolution "${grid_resolution}"
  )
elif [[ -n "${grid_size_lon}" || -n "${grid_size_lat}" ]]; then
  args+=(
    --grid-size "${grid_size_lon}" "${grid_size_lat}"
  )
fi
if [[ -n "${format}" ]]; then
    args+=(--format "${format}")
fi
if [[ -n "${zarr_version}" && "${zarr_version}" != "zarr_version" ]]; then
    args+=(--zarr-version "${zarr_version}")
fi
if [[ -n "${pattern}" ]]; then
    args+=(--pattern "${pattern}")
fi
if [[ -n "${combine}" ]]; then
    args+=(--combine "${combine}")
fi
if [[ -n "${job_queue}" ]]; then
    args+=(--job-queue "${job_queue}")
fi

# Determine which pipeline script to use based on zarr_config_url
pipeline_script="${root_dir}/src/localized_pipeline.py"

if [[ "${zarr_config_url}" == *"pace_cfg.yaml" ]] || [[ "${zarr_config_url}" == *"swot_cfg.yaml" ]]; then
    pipeline_script="${root_dir}/src/preprocess_gridding_pipeline.py"
fi

echo "Executing: python ${pipeline_script} ${args[@]}"
conda run -n ingest --live-stream python "${pipeline_script}" "${args[@]}"
