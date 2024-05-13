import logging
import os
import clickhouse_connect
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_timestamp
from job_utils import write_to_clickhouse

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def elaborate_pit_stops_ft(
    spark: SparkSession,
    pit_stops_parquet_file_path: str,
) -> DataFrame | None:

    try:
        pit_stops_df = spark.read.parquet(pit_stops_parquet_file_path)
        if not pit_stops_df:
            raise Exception("Error reading pit_stops parquet file!")

        pit_stops_df = pit_stops_df.drop(pit_stops_df.meeting_key)
        pit_stops_df = pit_stops_df.withColumnRenamed(
            "driver_number", "driver_id"
        ).withColumnRenamed("session_key", "race_id")

        pit_stops_df = pit_stops_df.withColumn("date", to_timestamp(pit_stops_df.date))

        if pit_stops_df:
            return pit_stops_df.dropDuplicates()
        else:
            raise Exception("Error elaborating pit_stops_df!")

    except Exception as e:
        logger.error("Error elaborating the PitStopsFT table!", e)


def execute_pit_stops_ft_job() -> None:

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

    pit_stops_df = elaborate_pit_stops_ft(
        spark=spark, pit_stops_parquet_file_path="./include/data/pit.parquet"
    )

    res = write_to_clickhouse(
        clickhouse_client=clickhouse_client,
        df=pit_stops_df,
        table_name="PitStopsFT",
    )

    print(res)

    sc.stop()


execute_pit_stops_ft_job()
