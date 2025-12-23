import asyncio
import json
import logging
import os
import tempfile
import warnings
from datetime import datetime, timedelta, date
from urllib.parse import urlparse
from uuid import uuid4

import backoff
import boto3
import requests
import yaml
from dateutil.parser import isoparse
from pystac_client import Client
from urllib3.exceptions import InsecureRequestWarning

from common_utils import ConfigUtils, MaapUtils, BackoffUtils
from pipeline_generic import wait_for_completion

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

warnings.filterwarnings("ignore", category=InsecureRequestWarning)


def _try_secure_get(*pargs, **kwargs):
    verify = kwargs.get('verify', True)
    url = pargs[0]
    url = urlparse(url)

    try:
        if not verify:
            logger.warning(f'Making GET request to {url.netloc} without SSL verification')
        return requests.get(*pargs, verify=verify, **kwargs)
    except requests.exceptions.SSLError:
        logger.warning(f'SSL verification failed for GET {url.netloc}, retrying without verification, but this should '
                       f'be addressed soon')
        return requests.get(*pargs, verify=False, **kwargs)


def _try_secure_post(*pargs, **kwargs):
    verify = kwargs.get('verify', True)
    url = pargs[0]
    url = urlparse(url)

    try:
        if not verify:
            logger.warning(f'Making GET request to {url.netloc} without SSL verification')
        return requests.post(*pargs, verify=verify, **kwargs)
    except requests.exceptions.SSLError:
        logger.warning(f'SSL verification failed for GET {url.netloc}, retrying without verification, but this should '
                       f'be addressed soon')
        return requests.post(*pargs, verify=False, **kwargs)


def _try_delete(bucket, key, s3):
    logger.info(f'Trying to delete s3://{bucket}/{key}')
    try:
        s3.delete_object(Bucket=bucket, Key=key)
        logger.info(f'Deleted s3://{bucket}/{key}')
    except Exception as e:
        logger.error(f'Failed to delete s3://{bucket}/{key}: {e}')


def _check_collection_in_sdap(args):
    url = f'{args.sdap_base_url.rstrip("/")}/list'

    logger.info('Listing SDAP collections')

    try:
        response = _try_secure_get(url, params={'nocached': True})
        response.raise_for_status()
    except Exception as e:
        logger.error(f'Failed to list collections: {e}')
        raise e

    collections = response.json()

    logger.info(f'There are {len(collections):,} collections in SDAP')

    return args.sdap_collection_name in [c['shortName'] for c in collections]


def _get_coords_from_config(args, s3_client):
    logger.info(f'Trying to get coordinates from config at {args.zarr_config_url}')

    parsed_url = urlparse(args.zarr_config_url)

    fd, local_file = tempfile.mkstemp(prefix='config', suffix='_temp.yaml')

    with os.fdopen(fd, 'wb') as f:
        s3_client.download_fileobj(parsed_url.netloc, parsed_url.path.lstrip('/'), f)

    with open(local_file, 'r') as f:
        config = yaml.safe_load(f)

    logger.info(f'Downloaded config data: {config}')

    if 'coordinates' not in config:
        if 'dimensions' in config:
            config['coordinates'] = config['dimensions']
        else:
            logger.info('No coordinate data provided, using defaults')

            return {
                'time': 'time',
                'latitude': 'latitude',
                'longitude': 'longitude'
            }

    return config['coordinates']


@backoff.on_exception(
    backoff.expo,
    (RuntimeError, requests.exceptions.RequestException),
    max_value=64,
    max_time=172800,
    on_backoff=BackoffUtils.backoff_logger,
    giveup=BackoffUtils.fatal_code
)
def _submit_job_with_maap(maap, **params):
    return maap.submitJob(**params)


async def main(args):
    start_time = datetime.now()
    s3_client = boto3.client('s3')

    maap_host_to_use = os.environ.get('MAAP_API_HOST', args.maap_host)
    logger.debug(f"Using MAAP host: {maap_host_to_use}")
    maap = MaapUtils.get_maap_instance(maap_host_to_use)

    czdt_token = maap.secrets.get_secret(args.titiler_token_secret_name)

    stac_catalog = Client.open(
        f'{args.mmgis_host}/stac/',
        headers={'Authorization': czdt_token},
    )

    logger.info(f'Opened STAC catalog at {args.mmgis_host}/stac/')

    collection = stac_catalog.get_collection(args.stac_collection)

    start = args.start_date.strftime('%Y-%m-%d' if isinstance(args.start_date, date) else '%Y-%m-%dT%H:%M:%SZ')
    end = args.end_date.strftime('%Y-%m-%d' if isinstance(args.end_date, date) else '%Y-%m-%dT%H:%M:%SZ')

    logger.info(f'Querying STAC collection {args.stac_collection} between {start} and {end}')

    search = stac_catalog.search(
        method='GET',
        collections=collection,
        datetime=f'{start}/{end}'
    )

    stac_items = list(search.items())

    logger.info(f'STAC search returned {len(stac_items):,} items')

    zarr_s3_urls = []

    for item in stac_items:
        assets = item.get_assets()

        if 'zarr' not in assets:
            logger.warning(f'No zarr URL found in item {item.id} in collection {item.collection_id}')
            continue

        zarr_url = urlparse(assets['zarr'].href)

        if zarr_url.scheme == 's3':
            zarr_s3_urls.append(assets['zarr'].href)
        elif zarr_url.scheme in {'http', 'https'}:
            zarr_s3_urls.append(f's3://{zarr_url.netloc.split(".")[0]}/{zarr_url.path.lstrip("/")}')
        else:
            logger.warning(f'Unsupported Zarr URL found for item {item.id} in collection '
                           f'{item.collection_id}: {assets["zarr"].href}')

    if len(zarr_s3_urls) == 0:
        logger.info('No zarr URLs found in STAC over desired window')
        return
    elif len(zarr_s3_urls) == 1 and not args.force_concat:
        logger.info('Only one matching zarr URL found in STAC over desired window. Skipping concat step')
        final_zarr_url = zarr_s3_urls[0]
    else:
        logger.info(f'Creating zarr concatenation manifest from {len(zarr_s3_urls):,} zarr URLs')

        maap_username = maap.profile.account_info()['username']

        # Create manifest
        os.makedirs("output", exist_ok=True)
        manifest_id = str(uuid4())
        local_zarr_path = f"output/{manifest_id}.json"

        with open(local_zarr_path, 'w') as fp:
            json.dump(zarr_s3_urls, fp, indent=2)

        manifest_key = f"{maap_username}/zarr_concat_manifests/{manifest_id}.json"
        s3_client.upload_file(
            local_zarr_path,
            "maap-ops-workspace",
            manifest_key
        )

        logger.info(f"Uploaded manifest to s3://maap-ops-workspace/{manifest_key}")

        job_params = {
            "identifier": f"STAC-Concat-Pipeline_zarr_concat_{args.stac_collection}_{manifest_id[-7:]}",
            "algo_id": "CZDT_ZARR_CONCAT",
            "version": "concat-cb",  # TODO: Temp using PGE version to subset to Chesapeake
            "queue": args.job_queue,
            "config": args.zarr_config_url,
            "config_path": os.path.join('input', os.path.basename(args.zarr_config_url)),
            "zarr_manifest": f"s3://maap-ops-workspace/{manifest_key}",
            "zarr_access": "mount",
            "duration": "none",  # No duration needed, should be managed by start & end
            "zarr_version": str(args.zarr_version),
            "output": f"concat.{manifest_id}.zarr"
        }

        logger.debug(f"Submitting Zarr concatenation job with parameters: {job_params}")
        # job = maap.submitJob(**job_params)
        job = _submit_job_with_maap(maap, **job_params)

        if not job.id:
            error_msg = MaapUtils.job_error_message(job)
            logger.error(f"Zarr concatenation job submission failed: {error_msg}")
            _try_delete("maap-ops-workspace", manifest_key, s3_client)
            raise RuntimeError(f"Failed to submit Zarr concatenation job: {error_msg}")

        logger.info(f"Zarr concatenation job submitted successfully with ID: {job.id}")
        logger.info("Waiting for Zarr concatenation job to complete")
        await wait_for_completion(job)
        logger.info("Zarr concatenation job completed")

        # _try_delete("maap-ops-workspace", manifest_key, s3_client)

        final_zarr_urls = MaapUtils.get_dps_output([job], ".zarr", True)

        if len(final_zarr_urls) == 0:
            logger.error(f'MAAP job {job.id} did not complete successfully')
            raise RuntimeError(f"MAAP job {job.id} did not complete successfully")

        final_zarr_url = final_zarr_urls[0]

        _try_delete("maap-ops-workspace", manifest_key, s3_client)

    logger.info(f'Final Zarr URL: {final_zarr_url}')

    if _check_collection_in_sdap(args):
        logger.info(f'Collection {args.sdap_collection_name} already exists and will need to be deleted')

        try:
            response = _try_secure_get(  # Yes, I know this should be a DELETE, it's TBD for SDAP
                f'{args.sdap_base_url.rstrip("/")}/datasets/remove',
                params={'name': args.sdap_collection_name},
            )
            response.raise_for_status()
        except Exception as e:
            logger.error(f'Failed to delete collection {args.sdap_collection_name} from SDAP: {e}')
            raise RuntimeError('Could not clear existing collection')

    add_url = f'{args.sdap_base_url.rstrip("/")}/datasets/add'
    add_params = {'name': args.sdap_collection_name, 'path': final_zarr_url}
    add_headers = {'Content-Type': 'application/yaml'}

    add_body = {
        'variable': args.variable,
        'coords': _get_coords_from_config(args, s3_client),
        'aws': {
            'region': 'us-west-2',  # We can leave this hardcoded for CZDT
            'public': False
        }
    }

    logger.info(f'Issuing collection registration POST request to {add_url}')
    logger.info(f'params: {add_params}')
    logger.info(f'headers: {add_headers}')
    logger.info(f'body: {add_body}')

    add_response = _try_secure_post(
        add_url,
        params=add_params,
        headers=add_headers,
        data=yaml.dump(add_body).encode('utf-8'),
    )

    if not add_response.ok:
        logger.error(f'Failed to add collection to SDAP: {add_response.text}')
        raise RuntimeError('Failed to add collection to SDAP')

    if not add_response.json()['success']:
        logger.error(f'SDAP Add collection API response unsuccessful: {add_response.json()["message"]}')
        raise RuntimeError('Failed to add collection to SDAP')

    if not _check_collection_in_sdap(args):
        logger.error(f'Add API call for collection {args.sdap_collection_name} succeeded, but collection is still '
                     f'not present. This requires investigation\n{add_url=}\n{add_params=}\n{add_headers=}\n'
                     f'request body: \n{yaml.dump(add_body)}')
        raise RuntimeError('Failed to add collection to SDAP')

    logger.info(f'Collection {args.sdap_collection_name} added to SDAP')

    logger.info(f'Pipeline completed in {datetime.now() - start_time}')


if __name__ == '__main__':
    parser = ConfigUtils.get_common_argument_parser()

    parser.add_argument(
        '--sdap-collection-name',
        required=True,
        help='The name of the SDAP collection to create or update',
    )

    parser.add_argument(
        '--sdap-base-url',
        required=True,
        help='The base URL for the SDAP API',
        type=lambda s: s.rstrip('/')
    )

    parser.add_argument(
        '--stac-collection',
        required=True,
        help='The name of the STAC collection to query',
    )

    parser.add_argument(
        '--start-date',
        help='The start date for the query in ISO 8601 format',
        type=isoparse,
    )

    parser.add_argument(
        '--end-date',
        help='The end date for the query in ISO 8601 format',
        type=isoparse,
    )

    parser.add_argument(
        '--days-back',
        help='The number of days back to query the STAC API',
        type=int,
    )

    # TODO: Should this be able to register a list of vars?
    parser.add_argument(
        '--variable',
        help='The name of the variable in the Zarr data to register in SDAP',
        required=True,
    )

    parser.add_argument(
        '--zarr-version',
        type=int,
        choices=[2, 3],
        default=3,
        help='Version of zarr standard to output'
    )

    parser.add_argument(
        '--force-concat',
        action='store_true',
        help='Force a concatenation PGE invocation. Useful if the source data zarr version differs from the desired '
             'zarr version, as STAC queries would otherwise short-circuit the concat PGE if only one item matches, '
             'registering an undesired format with SDAP'
    )

    args = parser.parse_args()

    print(args)

    if (args.start_date is None or args.end_date is None) and args.days_back is None:
        raise ValueError('Either start date and end date or a number of days back must be specified')
    elif args.start_date is not None and args.end_date is not None and args.days_back is not None:
        raise ValueError('Cannot specify days back with absolute date range')

    if args.days_back is not None:
        if args.days_back < 0:
            raise ValueError(f'days-back must not be negative')

        args.end_date = date.today()
        args.start_date = args.end_date - timedelta(days=args.days_back)

    if args.end_date < args.start_date:
        raise ValueError('Start date must be before end date')

    print(args)

    asyncio.run(main(args))
