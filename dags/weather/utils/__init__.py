"""
Utils package for weather DAG.

Constains utility functions for web scraping weather data.
"""

from tempfile import TemporaryDirectory
from typing import List
from utils import pandas_to_snowflake, snowflake_to_pandas
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

    pandas_to_snowflake(
        df=df,
        db=db,
        schema=schema,
        table_name=table_name,
        snowflake_conn_id=snowflake_conn_id,
    )


def to_parquet(
    table_name,
    schema,
    db,
    snowflake_conn_id="sf",
):
    """
    Convert snowflake table to parquet.
    """
    df = snowflake_to_pandas(
        query=f"SELECT * FROM {db}.{schema}.{table_name}",
        snowflake_conn_id=snowflake_conn_id,
    )

    with TemporaryDirectory() as tmp_dir:
        df.to_parquet(
            f"{tmp_dir}/{table_name}.parquet",
            compression=None,
            engine="pyarrow",
            index=False,
            partition_cols=["UPDATED_AT"],
        )

        print(
            pd.read_parquet(
                f"{tmp_dir}/{table_name}.parquet",
                engine="pyarrow",
            )
        )
