import os
import logging
import boto3
import json
import pandas as pd
import pendulum
from pydantic import BaseModel, ValidationError
from io import BytesIO
from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from airflow.exceptions import AirflowBadRequest
from datetime import timedelta
from utils.utils import call_api
from api import api_schemas

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

session = boto3.Session(
    aws_access_key_id=os.environ["CUBBIT_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["CUBBIT_SECRET_ACCESS_KEY"],
    region_name="eu-west-1",
)

s3 = session.client("s3", endpoint_url="https://s3.cubbit.eu")


def fetch_and_validate_data(
    url_details: dict,
    schema: BaseModel,
    attributes_to_remove: list[str],
) -> dict:

    response_list = []
    try:
        response_list = call_api(url_details)
        if not response_list:
            raise AirflowBadRequest("No data fetched from API!")
        logger.info(
            f"Data fetched successfully from the base url {url_details['base_url']}"
        )
    except AirflowBadRequest as e:
        logger.error(f"Error on the API call: {e}")

    validated_data = []
    for i, dict_item in enumerate(response_list):
        try:
            validated_item = schema(**dict_item)
            logger.info(f"Data validated successfully for item {i}")
            validated_data.append(validated_item)
        except ValidationError as e:
            logger.error(f"Validation error for item: {dict_item}\n{e}")

    data_dict = [item.model_dump() for item in validated_data]

    if len(attributes_to_remove) > 0:
        for item in data_dict:
            entries_to_remove = [x for x in item.keys() if x in attributes_to_remove]
            for k in entries_to_remove:
                try:
                    item.pop(k, None)
                except KeyError as e:
                    logger.error("Key not found!" + str(e))

    return data_dict


def fetch_and_store_dag(
    urls_details: list[dict],
    schemas: list[BaseModel],
    bucket_name: str,
) -> None:

    for url_details, schema in zip(urls_details, schemas):
        logger.info(f"Fetching data from {url_details['base_url']}...")
        data_dict = fetch_and_validate_data(
            url_details, schema, url_details["attributes_to_remove"]
        )

        if not data_dict:
            raise AirflowBadRequest("No data fetched or properly validated from API!")
        df = pd.DataFrame(data_dict)
        serialized_data_buffer = BytesIO()
        df.to_parquet(serialized_data_buffer, compression="snappy")

        base_url_without_query = url_details["base_url"].split("?")[0]
        try:
            s3.put_object(
                Bucket=bucket_name,
                Key=f"{base_url_without_query.split('/')[-1]}.parquet",
                Body=serialized_data_buffer.getvalue(),
            )
            logger.info(
                f"Data from {base_url_without_query} stored successfully in S3!"
            )
        except Exception as e:
            logger.error(f"Error on the S3 insertion: {e}")


default_args = {
    "owner": "jcbraz",
    "depends_on_past": False,
    "start_date": pendulum.now("Europe/Rome"),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="API-to-S3",
    start_date=pendulum.now("Europe/Rome"),
    default_args=default_args,
    description="OpenF1 API to S3 bucket DAG",
    catchup=False,
    doc_md=__doc__,
    tags=["api", "s3"],
) as dag:

    try:
        url_details = []
        with open("./dags/api/api_details.json", "r") as f:
            url_details = json.load(f)
            logger.info("API details loaded successfully!")

        if len(url_details) == 0:
            raise FileNotFoundError("No API details found!")

        schema_list = [
            api_schemas.DriverInfoSchema,
            api_schemas.GapInfoSchema,
            api_schemas.LapInfoSchema,
            api_schemas.GapInfoSchema,
            api_schemas.PitStopInfoSchema,
            api_schemas.RaceControlInfoSchema,
            api_schemas.TyreInfoSchema,
            api_schemas.WeatherInfoSchema,
            api_schemas.LapInfoSchema,
            api_schemas.CarInfoSchema,
            api_schemas.PositionInfoSchema,
        ]

        assert (
            len(url_details) == len(schema_list) - 2
        ), "Different number of URLs and schemas!"

        fetch_and_store_task_venv = PythonVirtualenvOperator(
            task_id="fetch_and_store_task",
            python_callable=fetch_and_store_dag,
            op_args=[url_details, schema_list, "f1-bucket"],
            requirements=[
                "boto3==1.18.63",
                "pydantic==2.7.0",
                "pyarrow==16.0.0",
                "pendulum==3.0.0",
                "pandas==2.2.2",
            ],
            system_site_packages=False,
        )

    except Exception as e:
        logger.error(f"Error loading API details: {e}")
