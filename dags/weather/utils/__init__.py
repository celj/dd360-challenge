"""
Utils package for weather DAG.

Constains utility functions for web scraping weather data.
"""

from airflow.contrib.hooks.snowflake_hook import SnowflakeHook
from typing import List
import pandas as pd
import re
import requests


def get_span_value(
    html: str,
    span_id: str,
):
    """
    Get value from a span tag by its id.
    """
    match = re.search(
        r'<span id="{}">(.*?)</span>'.format(span_id),
        html,
    )

    return match.group(1) if match else None


def get_weather_data(
    cities: List[str],
    span_id_tags: dict,
):
    """
    Get weather data from the internet.
    """
    info = {}

    for city in cities:
        info[city] = {}
        response = requests.get(f"https://www.meteored.mx/{city}/historico")
        for tag, span_id in span_id_tags.items():
            info[city][tag] = (
                get_span_value(
                    html=response.text,
                    span_id=span_id,
                )
                if response.status_code == 200
                else None
            )
            info[city]["request_status"] = response.status_code

    return info


def upload_dataframe_sf(
    df: pd.DataFrame,
    db: str,
    schema: str,
    table_name: str,
    chunk_size: int = 10_000,
    snowflake_conn_id: str = "sf",
):
    """
    Upload table to Snowflake.
    """
    snowflake_hook = SnowflakeHook(snowflake_conn_id=snowflake_conn_id)
    snowflake_engine = snowflake_hook.get_sqlalchemy_engine()

    with snowflake_engine.begin() as connection:
        df.to_sql(
            chunksize=chunk_size,
            con=connection,
            index=False,
            method="multi",
            name=table_name,
            schema=f"{db}.{schema}",
        )
        snowflake_engine.dispose()


def process_json(
    db: str,
    schema: str,
    table_name: str,
    task_ids,
    ti,
    snowflake_conn_id: str = "sf",
):
    """
    Process JSON data and upload to Snowflake.
    """
    data = ti.xcom_pull(task_ids=task_ids)
    df = pd.DataFrame.from_dict(data, orient="index")

    df["record_id"] = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    df.reset_index(inplace=True)
    df.rename(columns={"index": "city_raw"}, inplace=True)

    upload_dataframe_sf(
        df=df,
        db=db,
        schema=schema,
        table_name=table_name,
        snowflake_conn_id=snowflake_conn_id,
    )
