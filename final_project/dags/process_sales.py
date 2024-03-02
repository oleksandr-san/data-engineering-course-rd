from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from common import DEFAULT_DAG_ARGS, DS_LOCATION, PROJECT_ID, RAW_BUCKET
from table_defs.sales_csv import sales_csv

DEFAULT_ARGS = {
    **DEFAULT_DAG_ARGS,
    "start_date": datetime(2022, 8, 31),
    "end_date": datetime(2022, 10, 1),
}


with DAG(
    dag_id="process_sales",
    description="Process raw sales data in GCS into bronze and silver datasets in BigQuery",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    catchup=True,
    max_active_runs=1,
) as dag:
    transfer_sales_raw_to_bronze = BigQueryInsertJobOperator(
        task_id="transfer_sales_raw_to_bronze",
        dag=dag,
        location=DS_LOCATION,
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": "{% include 'sql/transfer_sales_raw_to_bronze.sql' %}",
                "useLegacySql": False,
                "tableDefinitions": {
                    "sales_csv": sales_csv,
                },
            },
        },
        params={"data_lake_raw_bucket": RAW_BUCKET, "project_id": PROJECT_ID},
    )

    transfer_sales_bronze_to_silver = BigQueryInsertJobOperator(
        task_id="transfer_sales_bronze_to_silver",
        dag=dag,
        location=DS_LOCATION,
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": "{% include 'sql/transfer_sales_bronze_to_silver.sql' %}",
                "useLegacySql": False,
            }
        },
        params={"project_id": PROJECT_ID},
    )

    _ = transfer_sales_raw_to_bronze >> transfer_sales_bronze_to_silver
