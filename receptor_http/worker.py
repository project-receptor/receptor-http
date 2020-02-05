import json
import logging
import requests

logger = logging.getLogger(__name__)


def configure_logger():
    receptor_logger = logging.getLogger('receptor')
    logger.setLevel(receptor_logger.level)
    for handler in receptor_logger.handlers:
        logger.addHandler(handler)


def execute(message, config, result_queue):
    configure_logger()

    try:
        payload = json.loads(message.raw_payload)
    except json.JSONDecodeError as err:
        logger.exception(err)
        raise
    logger.debug(f"Parsed payload: {payload}")

    method = payload.pop("method")
    url = payload.pop("url")
    
    logger.debug(f"Making {method} request for {url}")
    try:
        response = requests.request(method, url, **payload)
    except requests.RequestException as err:
        logger.excception(err)
        raise
    logger.debug(f"Got response status {response.status_code}")

    result_queue.put(response.text)
