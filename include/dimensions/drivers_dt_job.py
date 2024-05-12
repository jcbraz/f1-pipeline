import logging
import os
import clickhouse_connect
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from job_utils import write_to_clickhouse

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def elaborate_drivers_dt(
    spark: SparkSession, drivers_parquet_file_path: str, teams_parquet_file_path: str
) -> DataFrame | None:

    try:
        drivers_df = spark.read.parquet(drivers_parquet_file_path)
        if not drivers_df:
            raise Exception("Error reading drivers parquet file!")

        drivers_df = (
            drivers_df.withColumnRenamed(
                "driver_number", "driver_id"
            ).withColumnRenamed("name_acronym", "acronym")
        ).drop(drivers_df.session_key, drivers_df.meeting_key)

        teams_df = spark.read.parquet(teams_parquet_file_path)
        if not teams_df:
            raise Exception("Error reading teams parquet file!")
        teams_df = teams_df.withColumnRenamed("name", "team_name")

        joined_drivers_df = (
            drivers_df.join(
                teams_df,
                drivers_df.team_name == teams_df.team_name,
                how="left",
            )
            .drop(teams_df.acronym, teams_df.country_code, teams_df.team_name)
            .select(drivers_df.columns + ["team_id"])
        )

        if joined_drivers_df:
            joined_drivers_df = joined_drivers_df.drop(
                joined_drivers_df.team_name
            ).dropDuplicates()
            return joined_drivers_df
        else:
            raise Exception("Error joining drivers_df and teams_df!")

    except Exception as e:
        logger.error("Error elaborating the DriversDT table!", e)


def execute_drivers_dt_job() -> None:

    SparkContext.setSystemProperty("spark.executor.memory", "2g")

    spark = (
        SparkSession.builder.master("local")
        .appName("Colab")
        .config("spark.ui.port", "4051")
        .getOrCreate()
    )

    sc = spark.sparkContext

    clickhouse_client = clickhouse_connect.get_client(
        host=os.environ["CLICKHOUSE_HOST"],
        user=os.environ["CLICKHOUSE_USER"],
        password=os.environ["CLICKHOUSE_PASSWORD"],
        secure=True,
    )

    drivers_df = elaborate_drivers_dt(
        spark=spark,
        drivers_parquet_file_path="./include/data/drivers.parquet",
        teams_parquet_file_path="./include/data/teams.parquet",
    )

    res = write_to_clickhouse(
        clickhouse_client=clickhouse_client, df=drivers_df, table_name="DriversDT"
    )

    print(res)

    sc.stop()


execute_drivers_dt_job()
