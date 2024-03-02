from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator

from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator

PROJECT_ID = 'de-07-andrii-chuhunkin'
RAW_DATA_BUCKET = 'raw-data-bucket-ach'

BRONZE_DS = 'bronze'
SILVER_DS = 'silver'
GOLD_DS = 'gold'

CUSTOMER_TABLE = 'customers'

date = "{{execution_date.strftime('%Y-%m-%-d')}}"
filaname_tmplt = f"customers/{date}/"

def load_csv_func(**kwargs):
    """Workaround to pass listed files to the next task."""
    ti = kwargs['ti']
    source_objects = ti.xcom_pull(task_ids='get_available_files')
    load_csv = GCSToBigQueryOperator(
        task_id='gcs_to_bq_bronze',
        bucket=RAW_DATA_BUCKET,
        source_objects=source_objects,
        destination_project_dataset_table=f'{PROJECT_ID}:{BRONZE_DS}.{CUSTOMER_TABLE}',
        schema_fields=[
            {'name': 'Id', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'FirstName', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'LastName', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'Email', 'type': 'STRING', 'mode': 'REQUIRED'},
            {'name': 'RegistrationDate', 'type': 'STRING', 'mode': 'NULLABLE'},
            {'name': 'State', 'type': 'STRING', 'mode': 'NULLABLE'},
        ],
        write_disposition='WRITE_TRUNCATE',
    )
    load_csv.execute(context=kwargs)

process_customers_on_cloud = DAG(
    dag_id='process_customers_on_cloud',
    start_date=datetime(2022, 8, 1),
    end_date=datetime(2022, 8, 6),
    schedule_interval='0 1 * * *',
    max_active_runs=1,
    catchup=True
)

get_bronze_dataset = BigQueryGetDatasetOperator(
    task_id="get_bronze_dataset",
    dataset_id=BRONZE_DS,
    dag=process_customers_on_cloud
)

get_silver_dataset = BigQueryGetDatasetOperator(
    task_id="get_silver_dataset",
    dataset_id=SILVER_DS,
    dag=process_customers_on_cloud
)

get_available_files = GCSListObjectsOperator(
    task_id='get_available_files',
    bucket=RAW_DATA_BUCKET,
    prefix=filaname_tmplt,
    delimiter='/',
    dag=process_customers_on_cloud
)

load_csv_to_bronze = PythonOperator(
    task_id='load_csv_to_bronze',
    python_callable=load_csv_func,
    provide_context=True,
    dag=process_customers_on_cloud
)

create_silver_table_if_not_exists = BigQueryCreateEmptyTableOperator(
    task_id='create_silver_table_if_not_exists',
    dataset_id=SILVER_DS,
    table_id=CUSTOMER_TABLE,
    schema_fields=[
        {'name': 'client_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'email', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'registration_date', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': '_exec_date', 'type': 'DATE', 'mode': 'REQUIRED'},
    ],
    dag=process_customers_on_cloud
)

transform_data = BigQueryInsertJobOperator(
    task_id='transform_to_silver',
    dag=process_customers_on_cloud,
    configuration={
        "query": {
            "query": "{% include 'sql/transfer_customers_to_silver.sql' %}",
            "useLegacySql": False,
        }
    },
    params={
        'project_id': PROJECT_ID,
        'dest_ds': SILVER_DS,
        'dest_table': CUSTOMER_TABLE,
        'src_ds': BRONZE_DS,
        'src_table': CUSTOMER_TABLE,
    }
)

[get_bronze_dataset, get_silver_dataset] >> get_available_files
get_available_files >> load_csv_to_bronze
load_csv_to_bronze >> create_silver_table_if_not_exists
create_silver_table_if_not_exists >> transform_data


