import logging
import os
import clickhouse_connect
from dataclasses import dataclass
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import monotonically_increasing_id
from job_utils import write_to_clickhouse
from abstractions.Job import Job


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclass
class StintsDTJob(Job):
    spark: SparkSession
    parquet_file_paths: dict[str, str]

    def elaborate(self) -> DataFrame | None:

        try:
            stints_df = self.spark.read.parquet(self.parquet_file_paths["stints"])
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

    def execute(self) -> None:

        clickhouse_client = clickhouse_connect.get_client(
            host=os.environ["CLICKHOUSE_HOST"],
            user=os.environ["CLICKHOUSE_USER"],
            password=os.environ["CLICKHOUSE_PASSWORD"],
            secure=True,
        )

        stints_df = self.elaborate()

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
    "stints": "./jobs/data/stints.parquet",
}

job = StintsDTJob(spark=spark, parquet_file_paths=parquet_file_paths)
job.execute()

sc.stop()
