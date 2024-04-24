import os
import logging
import boto3

from pydantic import BaseModel, ValidationError
import schemas as schemas
import pickle
from airflow import DAG
from airflow.operators.python import PythonVirtualenvOperator
from airflow.exceptions import AirflowBadRequest
from pendulum import datetime, time
from datetime import timedelta
from utils import call_api
from typing import Dict, List, Union

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

def fetch_and_store_dag(
    # urls: list[str], 
    urls_details: List[Dict[str, Union[str, int]]], # TODO: dict with url, range to iterate (avoid API to timeout)
    schemas: list[Dict[Union[str, Dict[str]]]],
    attributes_to_keep: list[list[str]],
    bucket_name: str
) -> None:
   
    for url, schema, attributes in zip(urls_details, schemas, attributes_to_keep):
        try:
            data_dict = fetch_and_validate_data(url, schema, attributes) # TODO: change logic to enrich url in function
            serialized_data = pickle.dumps(data_dict)
            s3.put_object(
                Bucket=bucket_name,
                Key=f"{url.split("?")[0].split("/")[-1]}.pkl",
                Body=serialized_data,
            )
        except Exception as e:
            logger.error(f"Error on the S3 insertion: {e}")


default_args = {
    "owner": "jcbraz",
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 23),  # TODO: change this afterwards
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="API-to-S3",
    start_date=datetime(2024, 4, 22),  # change this afterwards
    default_args=default_args,
    description="OpenF1 API to S3 bucket DAG",
    catchup=False,
    doc_md=__doc__,
    tags=["api", "s3"],
) as dag:
    
    fetch_and_store_task_venv = PythonVirtualenvOperator(
        task_id="fetch_and_store_task",
        python_callable=fetch_and_store_dag,
        op_args=[
            [], [], [], "f1-bucket" # TODO: fill this with the correct values
        ],
        requirements=["boto3==1.18.63", "pydantic==2.7.0"],
        system_site_packages=False
    )
