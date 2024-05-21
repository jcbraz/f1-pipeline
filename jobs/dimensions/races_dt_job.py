import logging
import os
import clickhouse_connect
from dataclasses import dataclass
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import to_timestamp
from job_utils import write_to_clickhouse
from abstractions.Job import Job


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclass
class RacesDTJob(Job):
    spark: SparkSession
    parquet_file_paths: dict[str, str]

    def elaborate(self) -> DataFrame | None:

        try:
            races_df = self.spark.read.parquet(self.parquet_file_paths["sessions"])
            if not races_df:
                raise Exception("Error reading sessions parquet file!")

            # drop unwanted columns
            cols_to_drop = [
                "circuit_key",
                "country_key",
                "country_code",
                "meeting_key",
                "gmt_offset",
                "year",
            ]
            races_df = races_df.drop(*cols_to_drop)

            # format columns
            races_df = races_df.withColumn(
                "date_start", to_timestamp(races_df.date_start)
            ).withColumn("date_end", to_timestamp(races_df.date_end))

            # rename columns
            races_df = (
                races_df.withColumnRenamed("session_key", "race_id")
                .withColumnRenamed("circuit_short_name", "circuit_name")
                .withColumnRenamed("session_type", "type")
                .withColumnRenamed("session_name", "race_name")
                .withColumnRenamed("date_start", "timestamp_start")
                .withColumnRenamed("date_end", "timestamp_end")
            )

            if races_df:
                races_df = races_df.dropDuplicates()
                return races_df

        except Exception as e:
            logger.error("Error elaborating the DriversDT table!", e)
            return None

    def execute(self) -> None:

        clickhouse_client = clickhouse_connect.get_client(
            host=os.environ["CLICKHOUSE_HOST"],
            user=os.environ["CLICKHOUSE_USER"],
            password=os.environ["CLICKHOUSE_PASSWORD"],
            secure=True,
        )

        teams_df = self.elaborate()

        res = write_to_clickhouse(
            clickhouse_client=clickhouse_client, df=teams_df, table_name="RacesDT"
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
    "sessions": "./jobs/data/sessions.parquet",
}

job = RacesDTJob(spark=spark, parquet_file_paths=parquet_file_paths)
job.execute()

sc.stop()
