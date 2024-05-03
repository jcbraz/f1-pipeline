from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import pendulum
import logging
import os
import boto3


session = boto3.Session(
    aws_access_key_id=os.environ["CUBBIT_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["CUBBIT_SECRET_ACCESS_KEY"],
    region_name="eu-west-1",
)

s3 = session.client("s3", endpoint_url="https://s3.cubbit.eu")


with DAG("spark_s3_to_dw_job", start_date=pendulum.now()) as dag:

    submit_job = SparkSubmitOperator(
        task_id="submit_job",
        application="/path-to-pyspark-script",
        conn_id="spark_default",
        total_executor_cores="1",
        executor_cores="1",
        executor_memory="2g",
        num_executors="1",
        driver_memory="2g",
        verbose=True,
    )
