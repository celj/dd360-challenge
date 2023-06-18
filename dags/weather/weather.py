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
from dags.settings import LOCAL_TZ
from dags.weather.utils import get_weather_data
from datetime import datetime

# -------------------- Globals -------------------- #
DAG_ID = "weather"
QUERIES_BASE_PATH = os.path.join(os.path.dirname(__file__), "queries")

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
    web_scraping = PythonOperator(
        task_id="web_scraping",
        python_callable=get_weather_data,
        provide_context=True,
        op_kwargs={
            "cities": [
                "ciudad-de-mexico",
                "merida",
                "monterrey",
                "wakanda",
            ],
        },
    )

    pass
