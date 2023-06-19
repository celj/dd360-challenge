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
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from settings import LOCAL_TZ
from weather.utils import get_weather_data, process_json
import os

# -------------------- Globals -------------------- #
DAG_ID = "weather"
QUERIES_BASE_PATH = os.path.join(os.path.dirname(__file__), "queries")
SNOWFLAKE_CONN_ID = "sf"

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
    with TaskGroup(group_id="create") as create:
        create_schemas = SnowflakeOperator(
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            sql="create_schemas.sql",
            task_id="create_schemas",
        )

        create_cities = SnowflakeOperator(
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            sql="create_cities.sql",
            task_id="create_cities",
        )

        create_schemas >> create_cities

    with TaskGroup(group_id="upload_data") as upload_data:
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
                "snowflake_conn_id": SNOWFLAKE_CONN_ID,
                "table_name": "records_tmp",
                "task_ids": "upload_data.web_scraping",
            },
        )

        hist_insert = SnowflakeOperator(
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            sql="hist_insert.sql",
            task_id="hist_insert",
        )

        drop_tmp = SnowflakeOperator(
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            sql="drop_tmp.sql",
            task_id="drop_tmp",
        )

        web_scraping >> upload_to_sf >> hist_insert >> drop_tmp

    with TaskGroup(group_id="process_data") as process_data:
        fetch_requests = SnowflakeOperator(
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            sql="fetch_requests.sql",
            task_id="fetch_requests",
        )

        successful_fetches = SnowflakeOperator(
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            sql="successful_fetches.sql",
            task_id="successful_fetches",
        )

        summary = SnowflakeOperator(
            snowflake_conn_id=SNOWFLAKE_CONN_ID,
            sql="summary.sql",
            task_id="summary",
        )

        fetch_requests >> successful_fetches >> summary

    # -------------------- Dependencies -------------------- #
    create >> upload_data >> process_data
