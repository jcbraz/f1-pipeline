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
class DailyWeatherDTJob(Job):
    spark: SparkSession
    parquet_file_paths: dict[str, str]

    def elaborate(self) -> DataFrame | None:
        try:
            weather_df = self.spark.read.parquet(self.parquet_file_paths.get("weather"))
            if not weather_df:
                raise Exception("Error reading weather parquet file!")

            weather_df = weather_df.withColumn("date", weather_df["date"].cast("date"))

            agg_weather_df = (
                weather_df.groupBy("date")
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
                .withColumnRenamed("avg(humidity)", "humidity")
                .withColumnRenamed("avg(pressure)", "pressure")
                .withColumnRenamed("avg(rainfall)", "rainfall")
                .withColumnRenamed("avg(air_temperature)", "air_temperature")
                .withColumnRenamed("avg(track_temperature)", "track_temperature")
                .withColumnRenamed("avg(wind_speed)", "wind_speed")
                .withColumnRenamed("avg(wind_direction)", "wind_direction")
            ).withColumn("weather_id", monotonically_increasing_id())

            if agg_weather_df:
                return agg_weather_df.dropDuplicates()
        except Exception as e:
            logger.error("Error elaborating the DailyWeatherDT table!", e)
            return None

    def execute(self) -> None:

        clickhouse_client = clickhouse_connect.get_client(
            host=os.environ["CLICKHOUSE_HOST"],
            user=os.environ["CLICKHOUSE_USER"],
            password=os.environ["CLICKHOUSE_PASSWORD"],
            secure=True,
        )

        weather_df = self.elaborate()

        if weather_df:
            query_summary = write_to_clickhouse(
                clickhouse_client=clickhouse_client,
                df=weather_df,
                table_name="DailyWeatherDT",
            )
            if query_summary:
                logger.info(f"Query summary: {query_summary}")
                print(query_summary)
            else:
                logger.error("Error writing weather_df to Clickhouse!")
        else:
            logger.error("Error elaborating weather_df!")


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
    "weather": "./jobs/data/weather.parquet",
}

job = DailyWeatherDTJob(spark=spark, parquet_file_paths=parquet_file_paths)
job.execute()

sc.stop()
