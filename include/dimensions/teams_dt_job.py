import logging
import os
import sys

sys.path.append("/Users/jcbraz/Projects/f1-pipeline/include")
import clickhouse_connect
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from job_utils import write_to_clickhouse


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def elaborate_teams_dt(
    spark: SparkSession, teams_parquet_file_path: str
) -> DataFrame | None:

    try:
        teams_df = spark.read.parquet(teams_parquet_file_path)
        if not teams_df:
            raise Exception("Error reading teams parquet file!")

        teams_df = teams_df.dropDuplicates()
        return teams_df

    except Exception as e:
        logger.error("Error elaborating Teams DT!", e)


def execute_teams_dt_job() -> None:

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

    teams_df = elaborate_teams_dt(
        spark=spark, teams_parquet_file_path="./include/data/teams.parquet"
    )

    res = write_to_clickhouse(
        clickhouse_client=clickhouse_client, df=teams_df, table_name="TeamsDT"
    )

    print(res)

    sc.stop()


execute_teams_dt_job()
