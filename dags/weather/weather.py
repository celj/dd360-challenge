"""
Web scraping weather data.

--------
* DAG Name:
    weather
* Owner:
    Carlos Lezama
* Output:
"""

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
from settings import LOCAL_TZ
from weather.utils import get_weather_data, process_json
import os

# -------------------- Globals -------------------- #
DAG_ID = "weather"
QUERIES_BASE_PATH = os.path.join(os.path.dirname(__file__), "queries")

CITIES = [
    "ciudad-de-mexico",
    "merida",
    "monterrey",
    "wakanda",
]

SPAN_ID_TAGS = {
    "distance": "dist_cant",
    "humidity": "ult_dato_hum",
    "temperature": "ult_dato_temp",
    "updated_at": "fecha_act_dato",
}

# -------------------- DAG -------------------- #
with DAG(
    dag_id=DAG_ID,
    catchup=False,
    max_active_runs=1,
    schedule_interval="0 * * * *",
    start_date=datetime(2023, 6, 15, tzinfo=LOCAL_TZ),
    template_searchpath=QUERIES_BASE_PATH,
    tags=[
        "data-science",
        "weather",
    ],
):
    # -------------------- Tasks -------------------- #
    create_schemas = SnowflakeOperator(
        task_id="create_schemas",
        sql="create_schemas.sql",
        snowflake_conn_id="sf",
    )

    web_scraping = PythonOperator(
        task_id="web_scraping",
        python_callable=get_weather_data,
        provide_context=True,
        op_kwargs={
            "cities": CITIES,
            "span_id_tags": SPAN_ID_TAGS,
        },
    )

    upload_to_sf = PythonOperator(
        task_id="upload_to_sf",
        python_callable=process_json,
        provide_context=True,
        op_kwargs={
            "db": "weather",
            "schema": "raw",
            "snowflake_conn_id": "sf",
            "table_name": "records_stg",
            "task_ids": "web_scraping",
        },
    )

    hist_insert = SnowflakeOperator(
        task_id="hist_insert",
        sql="hist_insert.sql",
        snowflake_conn_id="sf",
    )

    create_schemas >> web_scraping >> upload_to_sf
