import os
from datetime import datetime

import requests

from airflow import DAG
from airflow.decorators import task

DEFAULT_ARGS = {
    "depends_on_past": False,
    "email": ["admin@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": 30,
}


JOB1_PORT = 8081
JOB2_PORT = 8082


with DAG(
    dag_id="process_sales",
    start_date=datetime(2022, 8, 9),
    end_date=datetime(2022, 8, 11),
    schedule_interval="@daily",
    catchup=True,
    default_args=DEFAULT_ARGS,
) as dag:

    @task(task_id="extract_data_from_api")
    def extract_data_from_api(ds=None, **kwargs):
        print(kwargs)
        print(ds)
        date = str(ds)
        raw_dir = os.path.join(os.path.dirname(__file__), "temp", "raw", date)
        response = requests.post(
            url=f"http://localhost:{JOB1_PORT}/",
            json={"date": date, "raw_dir": raw_dir},
        )
        response.raise_for_status()
        return response.json()

    @task(task_id="convert_to_avro")
    def convert_to_avro(ds=None, **kwargs):
        print(kwargs)
        print(ds)
        date = str(ds)
        raw_dir = os.path.join(os.path.dirname(__file__), "temp", "raw", date)
        stg_dir = os.path.join(os.path.dirname(__file__), "temp", "stg", date)
        response = requests.post(
            url=f"http://localhost:{JOB2_PORT}/",
            json={"raw_dir": raw_dir, "stg_dir": stg_dir},
        )
        response.raise_for_status()
        return response.json()

    extract_data_from_api().set_downstream(convert_to_avro())
