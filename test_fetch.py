import logging
from airflow.exceptions import AirflowBadRequest
from pydantic import BaseModel, ValidationError
from utils.utils import call_api
from api.api_schemas import CarInfoSchema
import boto3
import os
import pickle


logger = logging.getLogger(__name__)
logger.setLevel(logging.WARNING)

session = boto3.Session(
    aws_access_key_id=os.environ["CUBBIT_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["CUBBIT_SECRET_ACCESS_KEY"],
    region_name="eu-west-1",
)

s3 = session.client("s3", endpoint_url="https://s3.cubbit.eu")


def fetch_and_validate_data(
    url: str, schema: BaseModel, attributes_to_keep: list[str]
) -> dict:

    try:
        response_list = call_api(url)
        logger.info(f"Data fetched successfully from {url}")
        print(response_list)
    except AirflowBadRequest as e:
        logger.error(f"Error on the API call: {e}")

    validated_data = []
    for dict_item in response_list:
        try:
            validated_item = schema(**dict_item)
            logger.info(f"Data validated successfully for item: {dict_item}")
            validated_data.append(validated_item)
        except ValidationError as e:
            logger.error(f"Validation error for item: {dict_item}\n{e}")

    data_dict = [item.dict() for item in validated_data]

    for item in data_dict:
        entries_to_remove = [x for x in item.keys() if x not in attributes_to_keep]
        for k in entries_to_remove:
            try:
                item.pop(k, None)
            except KeyError as e:
                logger.error("Key not found!" + str(e))

    return data_dict


url = "https://api.openf1.org/v1/car_data?driver_number=55&session_key=9159&speed>=315" 

res = fetch_and_validate_data(
    url=url,
    schema=CarInfoSchema,
    attributes_to_keep=[
        "brake",
        "date",
        "driver_number",
        "meeting_key",
        "n_gear",
        "rpm",
        "session_key",
        "speed",
        "throttle",
    ],
)

try:
    serialized_data = pickle.dumps(res)
    s3.put_object(
        Bucket="f1-bucket",
        Key=f"{url.split("?")[0].split("/")[-1]}.pkl",
        Body=serialized_data,
    )
    logger.info('S3 insertion done!')
except Exception as e:
    logger.error(f"Error on the S3 insertion: {e}")
