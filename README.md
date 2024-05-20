# f1-pipeline: Data infrastructure for Formula 1 analytics

![f1-pipeline](/docs/assets/f1.png)

**f1-pipeline** establish the infrastructure for reliable, flexible and performant Formula 1 analytics.

## Initial Approach
Based on the [OpenF1 API](https://openf1.org/) and the [Ergast API](http://ergast.com/mrd/), the initial approach is to build a data pipeline that collects, processes and stores Formula 1 data in a structured way to enable perfomannt analytics and Machine Learning pipelines.

Both batch (e.g. historical data (already available)) and streaming data (e.g. live event data (work in progress)) will be considered.

---

## Architecture: Simple Lambda Architecture
![architecture](/docs/assets/architecture.png)

#### Batch Layer (ETL)
- Extract: Airflow DAGs to fetch data from the OpenF1 API and store it in a raw data lake.
- Transform: Spark jobs to clean and transform raw data into schema-complient data.
- Load: ingestion into a Clickhouse Data-Warehouse.
#### Speed Layer (Real-time) (Work in Progress)


## Data Warehouse ([Clickhouse](https://clickhouse.tech/))
- Why Clickhouse?
  - Columnar Database
  - Fast
  - Scalable
  - SQL
  - Open Source
  - Performs for both OLAP and OLTP workloads

- Kimball Methodology: Schema
  
    ![schema](/docs/assets/batch_layer_schema.png)

    - Extra tables not mentioned in the schema:

        - RealTimeFact: Real-time data from the speed layer. Given Clickhouse's OLTP optimizations, there's no need to resort to a different storage engine (e.g. Redis or NoSQL in general) for real-time capabilities.
  
            ![RealTimeFactFT](/docs/assets/real_time_fact_schema.png)

        - WeatherDT: Extra table to store weather data by the minute. Given the high cardinality of the data, in order to reference weather data though foreign key, data should be aggregated by the day (DailyWeatherDT). However, for some specific use cases, it might be useful to have the data by the minute.

            ![WeatherDT](/docs/assets/real_time_weather_schema.png)



## Outcomes
In order to play with the data and run analytics yourself, the folder `outputs` contains a sample of the data stored in the Clickhouse Data-Warehouse. If you want to run queries directly in Clickhouse, send me a message and I can provide you with the credentials.

---


By **José Braz**