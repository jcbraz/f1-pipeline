import json
import logging
import itertools
import time
import pandas as pd
from urllib.request import urlopen
from typing import Union, List
from api.api_schemas import UrlDetailsSchema
from pydantic import ValidationError

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

DRIVERS_PARQUET_PATH = "./dags/data/drivers.parquet"
SESSIONS_PARQUET_PATH = "./dags/data/sessions.parquet"


def map_parquet_file(file_path: str, type: str) -> List[int] | None:

    assert type in ["drivers", "sessions"], "Invalid type parameter"

    try:
        drivers_df = pd.read_parquet(file_path)
        if type == "sessions":
            return list(set(drivers_df["session_key"].tolist()))
        else:
            return list(set(drivers_df["driver_number"].tolist()))
    except Exception as e:
        logger.error(f"Error reading drivers file: {e}")
        return None


def call_api(url_details: dict) -> Union[List[dict], None]:

    try:
        UrlDetailsSchema(**url_details)
    except ValidationError as ve:
        logger.error("Error validating url details", ve)
        return None

    responses = []

    if "session_keys_range" in url_details:
        if "driver_numbers_range" in url_details:
            driver_numbers = map_parquet_file(
                file_path=DRIVERS_PARQUET_PATH, type="drivers"
            )
            if driver_numbers:
                for driver_number in driver_numbers:
                    sessions_keys = map_parquet_file(
                        file_path=SESSIONS_PARQUET_PATH, type="sessions"
                    )
                    if sessions_keys:
                        try:
                            for session_key in sessions_keys:
                                url_to_call = f"{url_details['base_url']}?session_key={session_key}&driver_number={driver_number}"
                                response_call = urlopen(url=url_to_call)
                                if response_call.getcode() != 200:
                                    raise Exception(
                                        f"Error calling API url: {url_to_call}. Status code: {response_call.getcode()}"
                                    )
                                else:
                                    response = json.loads(
                                        response_call.read().decode("utf-8")
                                    )
                                    if len(response) > 0:
                                        try:
                                            responses = list(
                                                itertools.chain(
                                                    responses.copy(), response
                                                )
                                            )
                                            logger.info(
                                                f"{url_to_call} response appended to responses. Responses current length: {len(responses)}"
                                            )
                                        except Exception as e:
                                            logger.error(
                                                f"Error appending response to responses: {e}"
                                            )
                                        time.sleep(5.5)
                                    else:
                                        time.sleep(1.5)

                        except Exception as e:
                            logger.error(f"Error calling the API in block 1: {e}")

        else:
            sessions_keys = map_parquet_file(
                file_path=SESSIONS_PARQUET_PATH, type="sessions"
            )
            if sessions_keys:
                for session_key in sessions_keys:
                    try:
                        url_to_call = (
                            f"{url_details['base_url']}?session_key={session_key}"
                        )
                        response_call = urlopen(url=url_to_call)
                        if response_call.getcode() != 200:
                            raise Exception(
                                f"Error calling API url: {url_to_call}. Status code: {response_call.getcode()}"
                            )
                        else:
                            response = json.loads(response_call.read().decode("utf-8"))
                            if len(response) > 0:
                                responses = list(
                                    itertools.chain(responses.copy(), response)
                                )
                                logger.info(
                                    f"{url_to_call} response appended to responses."
                                )
                        time.sleep(1.5)
                    except Exception as e:
                        logger.error(f"Error calling the API in block 2: {e}")

    elif "driver_numbers_range" in url_details:
        driver_numbers = map_parquet_file(
            file_path=DRIVERS_PARQUET_PATH, type="drivers"
        )
        for driver_number in driver_numbers:
            try:
                url_to_call = f"{url_details['base_url']}?driver_number={driver_number}"
                response_call = urlopen(url=url_to_call)
                if response_call.getcode() != 200:
                    raise Exception(
                        f"Error calling API url: {url_to_call}. Status code: {response_call.getcode()}"
                    )
                else:
                    response = json.loads(response_call.read().decode("utf-8"))
                    if len(response) > 0:
                        responses = list(itertools.chain(responses.copy(), response))
                        logger.info(f"{url_to_call} response appended to responses.")

                time.sleep(2)
            except Exception as e:
                logger.error(f"Error calling the API in block 3: {e}")
    else:
        try:
            url_to_call = url_details["base_url"]
            response_call = urlopen(url=url_to_call)
            if response_call.getcode() != 200:
                raise Exception(
                    f"Error calling API url: {url_to_call}. Status code: {response_call.getcode()}"
                )
            else:
                response = json.loads(response_call.read().decode("utf-8"))
                if len(response) > 0:
                    responses = list(itertools.chain(responses.copy(), response))
                    logger.info(f"{url_to_call} response appended to responses.")

        except Exception as e:
            logger.error(f"Error calling the API in block 4: {e}")

    return responses if responses else None


def normalize_df(df: pd.DataFrame, url_details: dict):

    if "intervals" in url_details["base_url"]:
        df_copy = df.copy()
        cols_to_convert = ["gap_to_leader", "interval"]
        df_copy[cols_to_convert] = df_copy[cols_to_convert].astype(str)
        return df_copy
    else:
        return df
