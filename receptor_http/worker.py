import json
import logging
import asyncio
import requests

import receptor

logger = logging.getLogger(__name__)


def configure_logger():
    receptor_logger = logging.getLogger("receptor")
    logger.setLevel(receptor_logger.level)
    for handler in receptor_logger.handlers:
        logger.addHandler(handler)


def get_url(method, url, **extra_data):
    with requests.Session() as session:
        with session.request(method, url, **extra_data) as response:
            response_text = dict(status=response.status_code, body=response.text)
    return response_text


@receptor.plugin_export(payload_type=receptor.BYTES_PAYLOAD)
def execute(message, config, result_queue):
    configure_logger()

    try:
        payload = json.loads(message)
    except json.JSONDecodeError as err:
        logger.exception(err)
        raise
    logger.debug(f"Parsed payload: {payload}")

    method = payload.pop("method")
    url = payload.pop("url")

    logger.debug(f"Making {method} request for {url}")
    try:
        response = get_url(method, url, **payload)
    except Exception as err:
        logger.exception(err)
        raise
    logger.debug(f"Got response status {response['status']}")

    result_queue.put(json.dumps(response))
