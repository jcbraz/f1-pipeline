import boto3
import logging
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
import clickhouse_connect

# if __name__ == '__main__':
#     client = clickhouse_connect.get_client(
#         host='jxcinbpb1q.europe-west4.gcp.clickhouse.cloud',
#         user='default',
#         password='_T8Yy.l03aiaj',
#         secure=True
#     )
#     print("Result:", client.query("SELECT 1").result_set[0][0])

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def drop_unwanted_cols(
    spark: SparkSession, parquet_file_path: str, cols_to_drop: list[str]
) -> DataFrame | None:
    try:
        df = spark.read.parquet(parquet_file_path)
        try:
            filtered_df = df.drop(*cols_to_drop)
            return filtered_df
        except Exception as e:
            logger.error("Error droping unwanted cols", e)

    except Exception as e:
        logger.error("Error reading pit.parquet with Spark", e)
        return None


def elaborate_race_results_ft(
    spark: SparkSession,
    table_name: str,
    parquet_file_paths: list[str],
    transformation_details: dict,
) -> DataFrame:
    return DataFrame()
