import json
import logging
import itertools
import time
from urllib.request import urlopen
from typing import Union, List
from api.api_schemas import UrlDetailsSchema
from pydantic import ValidationError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def call_api(url_details: dict) -> Union[List[dict], None]:

    try:
        UrlDetailsSchema(**url_details)
    except ValidationError as ve:
        logger.error("Error validating url details", ve)
        return None

    responses = []

    if "session_keys_range" in url_details:
        if "driver_numbers_range" in url_details:
            for session_key in range(
                url_details["session_keys_range"]["start"],
                url_details["session_keys_range"]["end"],
            ):
                for driver_number in range(
                    url_details["driver_numbers_range"]["start"],
                    url_details["driver_numbers_range"]["end"],
                ):
                    try:
                        url_to_call = f"{url_details['base_url']}?session_key={session_key}&driver_number={driver_number}"
                        response_call = urlopen(url=url_to_call)
                        response = json.loads(response_call.read().decode("utf-8"))
                        if len(response) > 0:
                            responses = list(
                                itertools.chain(responses.copy(), response)
                            )
                        time.sleep(1.5)
                    except Exception as e:
                        logger.error(f"Error calling the API in block 1: {e}")
        else:
            for session_key in range(
                url_details["session_keys_range"]["start"],
                url_details["session_keys_range"]["end"],
            ):
                try:
                    url_to_call = f"{url_details['base_url']}?session_key={session_key}"
                    response_call = urlopen(url=url_to_call)
                    response = json.loads(response_call.read().decode("utf-8"))
                    if len(response) > 0:
                        responses = list(itertools.chain(responses.copy(), response))
                    time.sleep(1.5)
                except Exception as e:
                    logger.error(f"Error calling the API in block 2: {e}")

    elif "driver_numbers_range" in url_details:
        for driver_number in range(
            url_details["driver_numbers_range"]["start"],
            url_details["driver_numbers_range"]["end"],
        ):
            try:
                url_to_call = f"{url_details['base_url']}?driver_number={driver_number}"
                response_call = urlopen(url=url_to_call)
                response = json.loads(response_call.read().decode("utf-8"))
                if len(response) > 0:
                    responses = list(itertools.chain(responses.copy(), response))

                time.sleep(1.5)
            except Exception as e:
                logger.error(f"Error calling the API in block 3: {e}")
    else:
        try:
            url_to_call = url_details["base_url"]
            response_call = urlopen(url=url_to_call)
            response = json.loads(response_call.read().decode("utf-8"))
            if len(response) > 0:
                responses = list(itertools.chain(responses.copy(), response))

        except Exception as e:
            logger.error(f"Error calling the API in block 4: {e}")

    return responses if responses else None
