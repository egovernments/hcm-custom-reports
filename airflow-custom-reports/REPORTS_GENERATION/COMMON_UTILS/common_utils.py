"""Shared Elasticsearch helpers used by every report script."""
import base64
import json
import os
import time

import requests

ELASTIC_PASSWORD = os.getenv('ELASTIC_PASSWORD')
ELASTIC_USERNAME = os.getenv('ELASTIC_USERNAME')

credentials = f"{ELASTIC_USERNAME}:{ELASTIC_PASSWORD}"
encoded = base64.b64encode(credentials.encode()).decode()
encoded_es_password = f"Basic {encoded}"

ES_HOST = os.getenv('ES_HOST', 'https://elasticsearch-data.es-cluster-v8.svc.cluster.local:9200')
TENANT_ID = os.getenv('TENANT_ID', '')
IS_CENTRAL_INSTANCE_ENABLED = os.getenv('IS_CENTRAL_INSTANCE_ENABLED', 'false').lower() == 'true'

max_retries = 30
retry_delay = 5


def es_index_url(index_name, suffix="/_search"):
    """Build an Elasticsearch index URL, prefixing with tenantId when central instance is enabled."""
    if IS_CENTRAL_INSTANCE_ENABLED and TENANT_ID:
        index_name = f"{TENANT_ID}-{index_name}"
    return f"{ES_HOST}/{index_name}{suffix}"


def es_scroll_url():
    """Return the Elasticsearch scroll API URL."""
    return f"{ES_HOST}/_search/scroll"


def get_resp(url, data, es=False):
    """POST `data` as JSON to `url`, retrying up to max_retries times on non-200 or network errors."""
    failed = False
    for attempt in range(max_retries):
        if failed:
            print(f"Retry {attempt}/{max_retries}")
        try:
            print(url, data)
            if es:
                response = requests.post(
                    url,
                    data=json.dumps(data),
                    headers={"Content-Type": "application/json", "Authorization": encoded_es_password},
                    verify=False,
                    timeout=30
                )
            else:
                response = requests.post(
                    url,
                    data=json.dumps(data),
                    headers={"Content-Type": "application/json"},
                    timeout=30
                )

            print(response)
            if response.status_code == 200:
                return response
            else:
                print(f"HTTP {response.status_code} received. Retrying in {retry_delay} seconds...")
                failed = True
        except requests.exceptions.RequestException as e:
            print(f"Network error ({type(e).__name__}): {str(e)}")
            print(f"Retrying in {retry_delay} seconds...")
            print(f"Retrying connecting to {url}")
            failed = True

        if failed:
            time.sleep(retry_delay)

    # If all retries failed, raise an exception
    raise Exception(f"Failed to connect to {url} after {max_retries} attempts")
