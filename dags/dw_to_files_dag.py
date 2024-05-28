import os
import clickhouse_connect
import pandas as pd
from clickhouse_driver import Client

clickhouse_client = clickhouse_connect.get_client(
    host=os.environ["CLICKHOUSE_HOST"],
    user=os.environ["CLICKHOUSE_USER"],
    password=os.environ["CLICKHOUSE_PASSWORD"],
    secure=True,
)


def pull_data_from_clickhouse(
    clickhouse_client: Client, table_name: str
) -> pd.DataFrame | None:

    query = f"SELECT * FROM {table_name}"
    try:
        query_result = clickhouse_client.query(query)
        return pd.DataFrame(query_result.result_rows, columns=query_result.column_names)
    except Exception as e:
        print(f"Error: {e}")
        return None


def transform_drivers_data(drivers_df: pd.DataFrame) -> pd.DataFrame:
    drivers_df_copy = drivers_df.copy()
    drivers_df_copy = drivers_df_copy.drop_duplicates(subset=["driver_id"])
    drivers_df_copy["driver_id"] = drivers_df_copy["driver_id"].astype(int)
    drivers_df_copy["team_id"] = drivers_df_copy["team_id"].fillna(-1).astype(int)
    return drivers_df_copy


def transform_results_facts_data(results_facts_df: pd.DataFrame) -> pd.DataFrame:
    results_facts_df["weather_id"] = (
        results_facts_df["weather_id"].fillna(-1).astype(int)
    )
    return results_facts_df.copy()


def etl_clickhouse_to_pandas(table_name: str) -> None:
    data = pull_data_from_clickhouse(clickhouse_client, table_name)
    if table_name == "DriversDT":
        data = transform_drivers_data(data)
    elif table_name == "ResultsFactsDT":
        data = transform_results_facts_data(data)

    data.to_csv(f"./outputs/{table_name}.csv", index=False)
    print(f"Data saved to ./outputs/{table_name}.csv")


def exec_dw_to_files_dag() -> None:

    tables = [
        "DriversDT",
        "PitStopsFT",
        "RacesDT",
        "ResultsFactsFT",
        "StintsDT",
        "TeamsDT",
        "WeatherDT",
        "DailyWeatherDT",
    ]

    if not os.path.exists("./outputs"):
        os.makedirs("./outputs")

    for table in tables:
        etl_clickhouse_to_pandas(table)


exec_dw_to_files_dag()
