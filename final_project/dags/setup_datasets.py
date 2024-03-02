from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
)

raw_gcs_bucket = "de07-hw-bucket-oleksandrsan"
location = "EU"
project_id = "de07-oleksandr-anosov"


DEFAULT_ARGS = {
    "depends_on_past": False,
    "email": ["oleksandr.v.anosov@gmail.com"],
    "owner": "oleksandr-san",
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": 30,
    "start_date": datetime(2022, 8, 31),
    "end_date": datetime(2022, 10, 1),
}


with DAG(
    dag_id="setup_datasets",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    catchup=True,
) as dag:
    create_bronze_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bronze_dataset",
        dataset_id="bronze",
        location=location,
        exists_ok=True,
        dag=dag,
    )

    create_silver_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_silver_dataset",
        dataset_id="silver",
        location=location,
        exists_ok=True,
        dag=dag,
    )

    create_gold_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_gold_dataset",
        dataset_id="gold",
        location=location,
        exists_ok=True,
        dag=dag,
    )

    _ = (
           create_bronze_dataset
        >> create_silver_dataset
        >> create_gold_dataset
    )
