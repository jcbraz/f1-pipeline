import logging
from clickhouse_driver import Client
from pyspark.sql import SparkSession, DataFrame


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


def write_to_clickhouse(
    clickhouse_client: Client,
    df: DataFrame,
    table_name: str,
    column_types: dict | None = None,
) -> str | None:

    try:
        df_pandas = df.toPandas()
        query_summary = clickhouse_client.insert_df(
            df=df_pandas, table=table_name, column_types=column_types
        )
        if not query_summary:
            raise Exception("Error inserting pandas df in DW!")

        return query_summary.query_id()

    except Exception as e:
        logger.error("Error writing to Clickhouse!", e)
        return None
