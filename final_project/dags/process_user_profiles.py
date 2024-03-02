from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from common import DEFAULT_DAG_ARGS, DS_LOCATION, DS_SILVER, PROJECT_ID, RAW_BUCKET
from table_defs.user_profiles import SCHEMA_FIELDS, FILE_PATH


with DAG(
    dag_id="process_user_profiles",
    description="Process JSONLine user profiles into silver dataset in BigQuery ",
    default_args=DEFAULT_DAG_ARGS,
    schedule_interval=None,
    catchup=True,
    start_date=datetime(2024, 1, 1),
) as dag:
    transfer_gcs_profiles_silver = GCSToBigQueryOperator(
        task_id="transfer_gcs_profiles_silver",
        bucket=RAW_BUCKET,
        source_objects=[FILE_PATH],
        destination_project_dataset_table=f"{PROJECT_ID}.{DS_SILVER}.user_profiles",
        schema_fields=SCHEMA_FIELDS,
        source_format="NEWLINE_DELIMITED_JSON",
    )
