import logging
import os
import clickhouse_connect
from dataclasses import dataclass
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from job_utils import write_to_clickhouse
from abstractions.Job import Job

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclass
class DriversDTJob(Job):
    spark: SparkSession
    parquet_file_paths: dict[str, str]

    def elaborate(self) -> DataFrame | None:

        try:
            drivers_df = self.spark.read.parquet(self.parquet_file_paths["drivers"])
            if not drivers_df:
                raise Exception("Error reading drivers parquet file!")

            drivers_df = (
                drivers_df.withColumnRenamed(
                    "driver_number", "driver_id"
                ).withColumnRenamed("name_acronym", "acronym")
            ).drop(drivers_df.session_key, drivers_df.meeting_key)

            teams_df = self.spark.read.parquet(self.parquet_file_paths["teams"])
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

    def execute(self) -> None:

        clickhouse_client = clickhouse_connect.get_client(
            host=os.environ["CLICKHOUSE_HOST"],
            user=os.environ["CLICKHOUSE_USER"],
            password=os.environ["CLICKHOUSE_PASSWORD"],
            secure=True,
        )

        drivers_df = self.elaborate()

        res = write_to_clickhouse(
            clickhouse_client=clickhouse_client, df=drivers_df, table_name="DriversDT"
        )

        print(res)


SparkContext.setSystemProperty("spark.executor.memory", "2g")

spark = (
    SparkSession.builder.master("local")
    .appName("F1")
    .config("spark.driver.memory", "2g")
    .config("spark.executor.memory", "2g")
    .config("spark.driver.maxResultSize", "1g")
    .getOrCreate()
)
sc = spark.sparkContext

parquet_file_paths = {
    "drivers": "./jobs/data/drivers.parquet",
    "teams": "./jobs/data/teams.parquet",
}

job = DriversDTJob(spark=spark, parquet_file_paths=parquet_file_paths)
job.execute()

sc.stop()
