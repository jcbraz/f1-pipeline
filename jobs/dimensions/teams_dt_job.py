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
class TeamsDTJob(Job):
    spark: SparkSession
    parquet_file_paths: dict[str, str]

    def elaborate(self) -> DataFrame | None:

        try:
            teams_df = self.spark.read.parquet(self.parquet_file_paths["teams"])
            if not teams_df:
                raise Exception("Error reading teams parquet file!")

            teams_df = teams_df.dropDuplicates()
            return teams_df

        except Exception as e:
            logger.error("Error elaborating Teams DT!", e)

    def execute(self) -> None:

        clickhouse_client = clickhouse_connect.get_client(
            host=os.environ["CLICKHOUSE_HOST"],
            user=os.environ["CLICKHOUSE_USER"],
            password=os.environ["CLICKHOUSE_PASSWORD"],
            secure=True,
        )

        teams_df = self.elaborate()

        res = write_to_clickhouse(
            clickhouse_client=clickhouse_client, df=teams_df, table_name="TeamsDT"
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
    "teams": "./jobs/data/teams.parquet",
}

job = TeamsDTJob(spark=spark, parquet_file_paths=parquet_file_paths)
job.execute()

sc.stop()
