import os
import logging
import clickhouse_connect
from clickhouse_driver import Client
from itertools import chain
from dataclasses import dataclass
from pyspark import SparkContext
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import create_map, col, lit, monotonically_increasing_id
from job_utils import write_to_clickhouse
from abstractions.Job import Job

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


@dataclass
class ResultsFactsFTJob(Job):
    spark: SparkSession
    parquet_file_paths: dict[str, str]

    def __calculate_positions(self, df: DataFrame) -> DataFrame | None:

        query = """
            WITH ranked_positions AS (
                SELECT 
                    driver_number, 
                    session_key, 
                    date, 
                    position,
                    ROW_NUMBER() OVER (PARTITION BY driver_number, session_key ORDER BY date DESC) as rn
                FROM temp_positions_table
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
            if not self.spark.catalog.tableExists("temp_positions_table"):
                _ = self.spark.sql("DROP TABLE IF EXISTS temp_positions_table")
                df.select("*").write.saveAsTable("temp_positions_table")

            sessions_final_positions_df = self.spark.sql(query)
            if sessions_final_positions_df:
                # Cast the date column to date type (convert into daily granularity)
                sessions_final_positions_df = sessions_final_positions_df.withColumn(
                    "date", sessions_final_positions_df["date"].cast("date")
                )
                sessions_final_positions_df = (
                    sessions_final_positions_df.withColumnRenamed(
                        "position", "final_position"
                    )
                )
                return sessions_final_positions_df
            else:
                raise Exception(
                    "Error getting the sessions_final_positions_df from the query execution"
                )
        except Exception as e:
            logger.error("Error calculating positions!", e)
            return None

    def __calculate_race_points(
        self,  # Added self parameter here
        latest_positions_df: DataFrame,
        points_mapping: dict[int, int],
    ) -> DataFrame | None:

        try:
            points_mapping_exp = create_map(
                [lit(x) for x in chain(*points_mapping.items())]
            )
            return latest_positions_df.select("*").withColumn(
                "points_earned", points_mapping_exp[col("final_position")]
            )
        except Exception as e:
            logger.error("Error mapping and calculating race points!", e)
            return None

    def __get_weather_df(self, file_name: str) -> DataFrame | None:
        try:
            weather_df = self.spark.read.parquet(self.parquet_file_paths.get(file_name))
            weather_df_with_monotonically_increasing_id = weather_df.withColumn(
                "weather_id", monotonically_increasing_id()
            )
            return weather_df_with_monotonically_increasing_id
        except Exception as e:
            logger.error("Error reading weather.parquet with Spark", e)
            return None

    def __add_weather_reference(
        self,  # Added self parameter here
        current_race_df: DataFrame,
        weather_df: DataFrame,
    ) -> DataFrame:

        agg_weather_df = (
            weather_df.withColumn("date", weather_df["date"].cast("date"))
            .groupBy("date")
            .agg(
                {
                    "humidity": "avg",
                    "pressure": "avg",
                    "rainfall": "avg",
                    "air_temperature": "avg",
                    "track_temperature": "avg",
                    "wind_speed": "avg",
                    "wind_direction": "avg",
                }
            )
        ).withColumn("weather_id", monotonically_increasing_id())

        filtered_weather_df = agg_weather_df.drop("session_key").drop("meeting_key")

        cols_to_drop = [
            x for x in filtered_weather_df.columns if x not in ["date", "weather_id"]
        ]

        try:
            weather_referenced_df = current_race_df.select("*").join(
                filtered_weather_df, how="left", on=["date"]
            )
            return weather_referenced_df.drop(*cols_to_drop)
        except Exception as e:
            print(e)

    def elaborate(self) -> DataFrame | None:

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
            positions_df = self.spark.read.parquet(
                self.parquet_file_paths.get("positions")
            )
            if not positions_df:
                raise Exception("Error reading positions parquet file")

            latest_positions_df = self.__calculate_positions(df=positions_df)
            if not latest_positions_df:
                raise Exception("Error calculating race's final positions")

            scored_df = self.__calculate_race_points(
                latest_positions_df=latest_positions_df,
                points_mapping=points_dict,
            )
            if not scored_df:
                raise Exception("Error calculating race points in the scored_df")

            weather_referenced_scored_df = self.__add_weather_reference(
                weather_df=self.__get_weather_df(file_name="weather"),
                current_race_df=scored_df,
            )
            if not weather_referenced_scored_df:
                raise Exception("Error adding weather reference to the scored_df")

            try:
                weather_referenced_scored_df = (
                    weather_referenced_scored_df.withColumnRenamed(
                        "session_key", "race_id"
                    ).withColumnRenamed("driver_number", "driver_id")
                )
            except Exception as e:
                logger.error(
                    "Error renaming columns of the final dataframe. Check if the keys mentioned are available!"
                )
                return None

            return weather_referenced_scored_df

        except Exception as e:
            logger.error("Error elaborating the RaceResultsFT table", e)
            return None

    def __populate_results_facts_ft(self, clickhouse_client: Client) -> str | None:
        try:
            final_df = self.elaborate().dropDuplicates()

            if not final_df:
                raise Exception("Error elaborating df for ResultsFactsFT!")
            return write_to_clickhouse(
                clickhouse_client=clickhouse_client,
                df=final_df,
                table_name="ResultsFactsFT",
            )
        except Exception as e:
            logger.error("Something went wrong populating the ResultsFactsFT!", e)
            return None

    def execute(self) -> None:

        clickhouse_client = clickhouse_connect.get_client(
            host=os.environ["CLICKHOUSE_HOST"],
            user=os.environ["CLICKHOUSE_USER"],
            password=os.environ["CLICKHOUSE_PASSWORD"],
            secure=True,
        )

        res = self.__populate_results_facts_ft(
            clickhouse_client=clickhouse_client,
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

parquet_file_paths_dict = {
    "positions": "./jobs/data/position.parquet",
    "drivers": "./jobs/data/drivers.parquet",
    "pits": "./jobs/data/pit.parquet",
    "sessions": "./jobs/data/sessions.parquet",
    "weather": "./jobs/data/weather.parquet",
}

job = ResultsFactsFTJob(spark=spark, parquet_file_paths=parquet_file_paths_dict)
job.execute()

sc.stop()
