import json
import logging
import asyncio
import aiohttp

logger = logging.getLogger(__name__)


def configure_logger():
    receptor_logger = logging.getLogger('receptor')
    logger.setLevel(receptor_logger.level)
    for handler in receptor_logger.handlers:
        logger.addHandler(handler)


async def get_url(method, url, **extra_data):
    async with aiohttp.ClientSession() as session:
        async with session.request(method, url, **extra_data) as response:
            response_text = dict(status=response.status,
                                 body=await response.text())
    return response_text


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
        response = asyncio.run(get_url(method, url, **payload))
    except Exception as err:
        logger.exception(err)
        raise
    logger.debug(f"Got response status {response['status']}")

    result_queue.put(response)
