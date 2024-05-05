import boto3
import logging
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import create_map, col, lit, monotonically_increasing_id
from itertools import chain
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


def calculate_positions(spark: SparkSession, df: DataFrame) -> DataFrame | None:

    query = """
        WITH ranked_positions AS (
            SELECT 
                driver_number, 
                session_key, 
                date, 
                position,
                ROW_NUMBER() OVER (PARTITION BY driver_number, session_key ORDER BY date DESC) as rn
            FROM positions_table
        )
        SELECT 
            driver_number, 
            session_key,
            date,
            position
        FROM ranked_positions
        WHERE rn = 1
    """

    try:
        if not spark.catalog.tableExists("positions"):
            _ = spark.sql("DROP TABLE IF EXISTS positions")
            df.select("*").write.saveAsTable("positons")

        sessions_final_positions_df = spark.sql(query)
        if sessions_final_positions_df:
            # Cast the date column to date type (convert into daily granularity)
            sessions_final_positions_df = sessions_final_positions_df.withColumn(
                "date", sessions_final_positions_df["date"].cast("date")
            )
            return sessions_final_positions_df
        else:
            raise Exception(
                "Error getting the sessions_final_positions_df from the query execution"
            )
    except Exception as e:
        logger.error("Error calculating positions!", e)
        return None


def calculate_race_points(
    latest_positions_df: DataFrame,
    points_mapping: dict[int, int],
) -> DataFrame | None:

    try:
        points_mapping_exp = create_map(
            [lit(x) for x in chain(*points_mapping.items())]
        )
        return latest_positions_df.select("*").withColumn(
            "points", points_mapping_exp[col("position")]
        )
    except Exception as e:
        logger.error("Error mapping and calculating race points!", e)
        return None


def get_weather_df(spark: SparkSession, parquet_file_path: str) -> DataFrame | None:
    try:
        weather_df = spark.read.parquet(parquet_file_path)
        weather_df_with_monotonically_increasing_id = weather_df.withColumn(
            "weather_id", monotonically_increasing_id()
        )
        return weather_df_with_monotonically_increasing_id
    except Exception as e:
        logger.error("Error reading weather.parquet with Spark", e)
        return None


def add_weather_reference(
    current_race_df: DataFrame, weather_df: DataFrame
) -> DataFrame:

    filtered_weather_df = weather_df.drop("session_key").drop("meeting_key")

    cols_to_drop = [
        x
        for x in filtered_weather_df.columns
        if x not in current_race_df.columns and x != "id"
    ]

    try:
        weather_referenced_df = current_race_df.select("*").join(
            filtered_weather_df, how="left", on=["date"]
        )
        return weather_referenced_df.drop(*cols_to_drop)
    except Exception as e:
        print(e)


def elaborate_race_results_ft(
    spark: SparkSession,
    parquet_file_paths: dict[str, str],
) -> DataFrame:

    points_dict = {
        1: 25,
        2: 18,
        3: 15,
        4: 12,
        5: 10,
        6: 8,
        7: 6,
        8: 4,
        9: 2,
        10: 1,
        11: 0,
        12: 0,
        13: 0,
        14: 0,
        15: 0,
        16: 0,
        17: 0,
        18: 0,
        19: 0,
        20: 0,
    }

    try:
        positions_df = spark.read.parquet(parquet_file_paths.get("positions"))
        if not positions_df:
            raise Exception("Error reading positions parquet file")

        latest_positions_df = calculate_positions(spark=spark, df=positions_df)
        if not latest_positions_df:
            raise Exception("Error calculating race's final positions")

        scored_df = calculate_race_points(
            latest_positions_df=latest_positions_df,
            points_mapping=points_dict,
        )
        if not scored_df:
            raise Exception("Error calculating race points in the scored_df")

        weather_referenced_scored_df = add_weather_reference(
            weather_df=get_weather_df(
                spark=spark, parquet_file_path=parquet_file_paths.get("weather")
            ),
            current_race_df=scored_df,
        )
        if not weather_referenced_scored_df:
            raise Exception("Error adding weather reference to the scored_df")

        return weather_referenced_scored_df

    except Exception as e:
        logger.error("Error elaborating the RaceResultsFT table", e)
