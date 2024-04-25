import json
import logging
import itertools
from functools import lru_cache
from urllib.request import urlopen
from typing import Union, List
from api.api_schemas import UrlDetailsSchema
from pydantic import ValidationError

logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)


@lru_cache(maxsize=None)
def call_api(url_details: dict) -> Union[List[dict], None]:

    try:
        validate_url_details = UrlDetailsSchema(**url_details).model_dump()
    except ValidationError as ve:
        logger.error("Error validating url details", ve)
        return None

    responses = []

    if "session_keys_range" in validate_url_details:
        if "driver_numbers_range" in validate_url_details:
            for session_key in range(
                validate_url_details["session_keys_range"][0],
                validate_url_details["session_keys_range"][1],
            ):
                for driver_number in range(
                    validate_url_details["driver_numbers_range"][0],
                    validate_url_details["driver_numbers_range"][1],
                ):
                    url_to_call = f"{validate_url_details['base_url']}?session_key={session_key}&driver_number={driver_number}"
                    response = urlopen(url=url_to_call)
                    if len(response) > 0:
                        response_json = json.loads(response.read().decode("utf-8"))
                        responses = list(
                            itertools.chain(responses.copy(), response_json)
                        )
        else:
            for session_key in range(
                validate_url_details["session_keys_range"][0],
                validate_url_details["session_keys_range"][1],
            ):
                url_to_call = (
                    f"{validate_url_details['base_url']}?session_key={session_key}"
                )
                response = urlopen(url=url_to_call)
                if len(response) > 0:
                    response_json = json.loads(response.read().decode("utf-8"))
                    responses = list(itertools.chain(responses.copy(), response_json))

    elif "driver_numbers_range" in validate_url_details:
        for driver_number in range(
            validate_url_details["driver_numbers_range"][0],
            validate_url_details["driver_numbers_range"][1],
        ):
            url_to_call = (
                f"{validate_url_details['base_url']}?driver_number={driver_number}"
            )
            response = urlopen(url=url_to_call)
            if len(response) > 0:
                response_json = json.loads(response.read().decode("utf-8"))
                responses = list(itertools.chain(responses.copy(), response_json))
    else:
        response = urlopen(url=validate_url_details["base_url"])
        if len(response) > 0:
            response_json = json.loads(response.read().decode("utf-8"))
            responses = list(itertools.chain(responses.copy(), response_json))

    return responses


# res = urlopen(
#     url="https://api.openf1.org/v1/car_data?driver_number=55&session_key=9159&speed>=315"
# )
# res_json = json.loads(res.read().decode("utf-8"))
# print(pd.DataFrame(res_json))
