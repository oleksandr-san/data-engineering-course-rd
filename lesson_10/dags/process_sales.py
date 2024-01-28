import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator


DEFAULT_ARGS = {
    "depends_on_past": False,
    "email": ["admin@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": 30,
}


with DAG(
    dag_id="process_sales",
    start_date=datetime(2022, 8, 9),
    end_date=datetime(2022, 8, 11),
    schedule_interval="@daily",
    catchup=True,
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
) as dag:

    @task(task_id="extract_data_from_api")
    def extract_data_from_api(ds=None, **kwargs):
        print(kwargs)
        print(ds)
        date = str(ds)
        raw_dir = os.path.join(os.path.dirname(__file__), "temp", "raw", date)

        path = lesson_02.job1.bll.save_sales_to_local_disk(date, raw_dir)
        return {'path': path}

    @task(task_id="convert_to_avro")
    def convert_to_avro(ds=None, **kwargs):
        print(kwargs)
        print(ds)
        date = str(ds)
        raw_dir = os.path.join(os.path.dirname(__file__), "temp", "raw", date)
        stg_dir = os.path.join(os.path.dirname(__file__), "temp", "stg", date)
        
        lesson_02.job2.bll.stg.process_stg(raw_dir=raw_dir, stg_dir=stg_dir)
        return {'raw_dir': raw_dir, 'stg_dir': stg_dir}

    BUCKET = 'de07-hw-bucket-oleksandrsan'
    dag_run_date = '{{ ds }}'
    upload_to_gcs = LocalFilesystemToGCSOperator(
        src=f'{os.path.dirname(__file__)}/temp/stg/{dag_run_date}/sales_{dag_run_date}.avro',
        dst=f'sales/avro/{dag_run_date}/sales_{dag_run_date}.avro',
        bucket=BUCKET,
        task_id='upload_to_gcs',
        dag=dag,
    )

    list_objects = GCSListObjectsOperator(
        task_id='list_bucket_objects',
        dag=dag,
        bucket=BUCKET,
    )

    _ = extract_data_from_api() >> convert_to_avro() >> upload_to_gcs >> list_objects
