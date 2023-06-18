"""
Web scraping weather data.

--------
* DAG Name:
    weather
* Owner:
    Carlos Lezama
* Output:
"""

import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from dags.weather.utils import get_weather_data
from datetime import datetime
from pendulum import timezone

# -------------------- Globals -------------------- #
DAG_ID = "weather"
QUERIES_BASE_PATH = os.path.join(os.path.dirname(__file__), "queries")
TIMEZONE = "UTC"

with DAG(
    dag_id=DAG_ID,
    catchup=False,
    max_active_runs=1,
    schedule_interval="0 * * * *",
    start_date=datetime(2023, 6, 15, tzinfo=timezone(TIMEZONE)),
    template_searchpath=QUERIES_BASE_PATH,
    tags=[
        "data-science",
        "weather",
    ],
):
    # web_scraping = PythonOperator(
    #     task_id="web_scraping",
    #     python_callable=get_weather_data,
    #     provide_context=True,
    #     op_kwargs={},
    # )

    pass
