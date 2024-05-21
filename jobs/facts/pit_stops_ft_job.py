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
class PitStopsFTJob(Job):
    spark: SparkSession
    parquet_file_paths: dict[str, str]

    def elaborate(self) -> DataFrame | None:

        try:
            pit_stops_df = spark.read.parquet(self.parquet_file_paths["pit_stops"])
            if not pit_stops_df:
                raise Exception("Error reading pit_stops parquet file!")

            pit_stops_df = pit_stops_df.drop(pit_stops_df.meeting_key)
            pit_stops_df = pit_stops_df.withColumnRenamed(
                "driver_number", "driver_id"
            ).withColumnRenamed("session_key", "race_id")

            pit_stops_df = pit_stops_df.withColumn(
                "date", to_timestamp(pit_stops_df.date)
            )

            if pit_stops_df:
                return pit_stops_df.dropDuplicates()
            else:
                raise Exception("Error elaborating pit_stops_df!")

        except Exception as e:
            logger.error("Error elaborating the PitStopsFT table!", e)

    def execute(self) -> None:

        clickhouse_client = clickhouse_connect.get_client(
            host=os.environ["CLICKHOUSE_HOST"],
            user=os.environ["CLICKHOUSE_USER"],
            password=os.environ["CLICKHOUSE_PASSWORD"],
            secure=True,
        )

        pit_stops_df = self.elaborate()

        res = write_to_clickhouse(
            clickhouse_client=clickhouse_client,
            df=pit_stops_df,
            table_name="PitStopsFT",
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
    "pit_stops": "./jobs/data/pit.parquet",
}

job = PitStopsFTJob(spark=spark, parquet_file_paths=parquet_file_paths)
job.execute()

sc.stop()
