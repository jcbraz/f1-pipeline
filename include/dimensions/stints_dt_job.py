import logging
import os
import clickhouse_connect
import sys

sys.path.append("/Users/jcbraz/Projects/f1-pipeline/include")
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_timestamp, monotonically_increasing_id
from job_utils import write_to_clickhouse


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def elaborate_stints_dt(
    spark: SparkSession, stints_parquet_file_path: str
) -> DataFrame | None:

    try:
        stints_df = spark.read.parquet(stints_parquet_file_path)
        if not stints_df:
            raise Exception("Error reading stints parquet file!")

        stints_df = stints_df.withColumnRenamed(
            "driver_number", "driver_id"
        ).withColumnRenamed("session_key", "race_id")

        id_included_stints_df = stints_df.withColumn(
            "stint_id", monotonically_increasing_id()
        )

        if id_included_stints_df:
            cols_to_drop = ["meeting_key"]
            id_included_stints_df = id_included_stints_df.drop(*cols_to_drop)
            id_included_stints_df = id_included_stints_df.withColumnRenamed(
                "session_key", "race_id"
            ).withColumnRenamed("driver_number", "driver_id")
            id_included_stints_df = id_included_stints_df.fillna("Unknown")

            return id_included_stints_df.dropDuplicates()
    except Exception as e:
        logger.error("Error elaborating the StintsDT table!", e)
        return None


def execute_stints_dt_job() -> None:

    SparkContext.setSystemProperty("spark.executor.memory", "2g")

    spark = (
        SparkSession.builder.master("local")
        .appName("Colab")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.driver.maxResultSize", "1g")
        .getOrCreate()
    )
    sc = spark.sparkContext

    clickhouse_client = clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        user=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        secure=True,
    )

    stints_df = elaborate_stints_dt(
        spark=spark, stints_parquet_file_path="./include/data/stints.parquet"
    )

    if stints_df:
        query_summary = write_to_clickhouse(
            clickhouse_client=clickhouse_client,
            df=stints_df,
            table_name="StintsDT",
        )

        if query_summary:
            logger.info("StintsDT job executed successfully!")
            print(query_summary)
        else:
            logger.error("Error writing StintsDT to ClickHouse!")

    else:
        logger.error("Error executing StintsDT job!")

    sc.stop()


execute_stints_dt_job()
