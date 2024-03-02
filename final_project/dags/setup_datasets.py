from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
)
from common import DEFAULT_DAG_ARGS, DS_BRONZE, DS_GOLD, DS_LOCATION, DS_SILVER

DEFAULT_ARGS = {
    **DEFAULT_DAG_ARGS,
    "start_date": datetime(2022, 8, 31),
    "end_date": datetime(2022, 9, 2),
}


with DAG(
    dag_id="setup_datasets",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    catchup=True,
) as dag:
    create_bronze_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_bronze_dataset",
        dataset_id=DS_BRONZE,
        location=DS_LOCATION,
        exists_ok=True,
        dag=dag,
    )

    create_silver_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_silver_dataset",
        dataset_id=DS_SILVER,
        location=DS_LOCATION,
        exists_ok=True,
        dag=dag,
    )

    create_gold_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_gold_dataset",
        dataset_id=DS_GOLD,
        location=DS_LOCATION,
        exists_ok=True,
        dag=dag,
    )

    _ = create_bronze_dataset >> create_silver_dataset >> create_gold_dataset
