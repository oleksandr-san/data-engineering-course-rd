from datetime import datetime

from airflow import DAG

from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor


PROJECT_ID = 'de-07-andrii-chuhunkin'
RAW_DATA_BUCKET = 'raw-data-bucket-ach'

BRONZE_DS = 'bronze'
SILVER_DS = 'silver'

SALES_TABLE = 'sales'
TEMP_TABLE_NAME = 'temp_sales'

date = "{{execution_date.strftime('%Y-%m-%-d')}}"
filename_tmplt = f"sales/{date}/{date}__sales.csv"

process_sales_on_cloud = DAG(
    dag_id='process_sales_on_cloud',
    start_date=datetime(2022, 9, 1),
    end_date=datetime(2022, 10, 1),
    schedule_interval='0 1 * * *',
    max_active_runs=1,
    catchup=True
)

get_bronze_dataset = BigQueryGetDatasetOperator(
    task_id="get_bronze_dataset",
    dataset_id=BRONZE_DS,
    dag=process_sales_on_cloud
)

get_silver_dataset = BigQueryGetDatasetOperator(
    task_id="get_silver_dataset",
    dataset_id=SILVER_DS,
    dag=process_sales_on_cloud
)

check_file_exists = GCSObjectExistenceSensor(
    task_id='check_file_exists',
    bucket=RAW_DATA_BUCKET,
    object=filename_tmplt,
    dag=process_sales_on_cloud
)

create_bronze_table_if_not_exists = BigQueryCreateEmptyTableOperator(
    task_id='create_bronze_table_if_not_exists',
    dataset_id=BRONZE_DS,
    table_id=SALES_TABLE,
    schema_fields=[
        {'name': 'CustomerId', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'PurchaseDate', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'Product', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'Price', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': '_exec_date', 'type': 'DATE', 'mode': 'NULLABLE'},
    ],
    time_partitioning={'type': 'DAY', 'field': '_exec_date'},
    dag=process_sales_on_cloud
)

load_csv_to_temp_table = GCSToBigQueryOperator(
    task_id='load_csv_to_temp_table',
    bucket=RAW_DATA_BUCKET,
    source_objects=[filename_tmplt],
    destination_project_dataset_table=f'{PROJECT_ID}:{BRONZE_DS}.{TEMP_TABLE_NAME}',
    schema_fields=[
        {'name': 'CustomerId', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'PurchaseDate', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'Product', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'Price', 'type': 'STRING', 'mode': 'REQUIRED'},
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=process_sales_on_cloud,
)

load_csv_from_temp_to_bronze = BigQueryInsertJobOperator(
    task_id='load_csv_from_temp_to_bronze',
    dag=process_sales_on_cloud,
    configuration={
        "query": {
            "query": "{% include 'sql/load_csv_to_bronze.sql' %}",
            "useLegacySql": False,
        }
    },
    params={
        'project_id': PROJECT_ID,
        'dest_ds': BRONZE_DS,
        'dest_table': SALES_TABLE,
        'temp_table': TEMP_TABLE_NAME,
    }

)

create_silver_table_if_not_exists = BigQueryCreateEmptyTableOperator(
    task_id='create_silver_table_if_not_exists',
    dataset_id=SILVER_DS,
    table_id=SALES_TABLE,
    schema_fields=[
        {'name': 'client_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'purchase_date', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'product_name', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'price', 'type': 'DECIMAL', 'mode': 'REQUIRED'},
        {'name': '_exec_date', 'type': 'DATE', 'mode': 'REQUIRED'},
    ],
    time_partitioning={'type': 'DAY', 'field': '_exec_date'},
    dag=process_sales_on_cloud
)

transform_data = BigQueryInsertJobOperator(
    task_id='transform_to_silver',
    dag=process_sales_on_cloud,
    configuration={
        "query": {
            "query": "{% include 'sql/transfer_sales_to_silver.sql' %}",
            "useLegacySql": False,
        }
    },
    params={
        'project_id': PROJECT_ID,
        'dest_ds': SILVER_DS,
        'dest_table': SALES_TABLE,
        'src_ds': BRONZE_DS,
        'src_table': SALES_TABLE,
    }
)

# Define the sequence of tasks
checks = [get_bronze_dataset, get_silver_dataset, check_file_exists]
checks >> create_bronze_table_if_not_exists
create_bronze_table_if_not_exists >> load_csv_to_temp_table >> load_csv_from_temp_to_bronze
load_csv_from_temp_to_bronze >> create_silver_table_if_not_exists >> transform_data

