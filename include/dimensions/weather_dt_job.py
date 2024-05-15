import logging
import os
import clickhouse_connect
import sys

sys.path.append("/Users/jcbraz/Projects/f1-pipeline/include")
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from job_utils import write_to_clickhouse
from pyspark.sql.functions import monotonically_increasing_id, to_timestamp


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def elaborate_weather_dt(
    spark: SparkSession, weather_parquet_file_path: str
) -> DataFrame | None:

    try:
        weather_df = spark.read.parquet(weather_parquet_file_path)
        if not weather_df:
            raise Exception("Error reading weather parquet file!")

        id_included_weather_df = weather_df.withColumn(
            "weather_id", monotonically_increasing_id()
        )

        if id_included_weather_df:
            cols_to_drop = ["session_key", "meeting_key"]
            id_included_weather_df = id_included_weather_df.drop(*cols_to_drop)
            id_included_weather_df = id_included_weather_df.withColumn(
                "date", to_timestamp(id_included_weather_df.date)
            ).withColumnRenamed("date", "timestamp")

            return id_included_weather_df.dropDuplicates()
        else:
            raise Exception("Error elaborating weather_df!")

    except Exception as e:
        logger.error("Error elaborating the WeatherDT table!", e)
        return None


def execute_weather_dt_job() -> None:

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

    weather_df = elaborate_weather_dt(
        spark=spark,
        weather_parquet_file_path="./include/data/weather.parquet",
    )

    if weather_df:
        query_summary = write_to_clickhouse(
            clickhouse_client=clickhouse_client,
            df=weather_df,
            table_name="WeatherDT",
        )
        if query_summary:
            logger.info(f"Query summary: {query_summary}")
            print(query_summary)
        else:
            logger.error("Error writing weather_df to Clickhouse!")
    else:
        logger.error("Error elaborating weather_df!")

    sc.stop()


execute_weather_dt_job()
