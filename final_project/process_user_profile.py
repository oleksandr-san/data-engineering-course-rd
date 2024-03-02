from airflow import DAG

from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

PROJECT_ID = 'de-07-andrii-chuhunkin'
RAW_DATA_BUCKET = 'raw-data-bucket-ach'

SILVER_DS = 'silver'
USER_TABLE = 'user_profiles'

filename = 'user_profiles/user_profiles.json'

process_user_profile = DAG(
    dag_id='process_user_profiles',
    schedule_interval=None,
    max_active_runs=1,
    catchup=False
)

get_silver_dataset = BigQueryGetDatasetOperator(
    task_id="get_silver_dataset",
    dataset_id=SILVER_DS,
    dag=process_user_profile
)

create_silver_table_if_not_exists = BigQueryCreateEmptyTableOperator(
    task_id='create_silver_table_if_not_exists',
    dataset_id=SILVER_DS,
    table_id=USER_TABLE,
    schema_fields = [
        {'name': 'email', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'full_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'birth_date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'phone_number', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    dag=process_user_profile
)

load_data_to_silver = GCSToBigQueryOperator(
    task_id='load_data_to_silver',
    bucket=RAW_DATA_BUCKET,
    source_objects=[filename],
    destination_project_dataset_table=f'{PROJECT_ID}.{SILVER_DS}.{USER_TABLE}',
    source_format='NEWLINE_DELIMITED_JSON',
    create_disposition='CREATE_IF_NEEDED',
    write_disposition='WRITE_TRUNCATE',
    schema_fields=[
        {'name': 'email', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'full_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'birth_date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'phone_number', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    dag=process_user_profile
)

get_silver_dataset >> create_silver_table_if_not_exists >> load_data_to_silver