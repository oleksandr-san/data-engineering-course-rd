import os
import requests
import json
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Dict, List

import fastavro

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


JOB1_API_URL = "https://fake-api-vycpfa6oca-uc.a.run.app/"
JOB1_API_TOKEN = "2b8d97ce57d401abd89f45b0079d8790edd940e6"


def get_sales(date: str) -> List[Dict[str, Any]]:
    """
    Get data from sales API for specified date.

    :param date: data retrieve the data from
    :return: list of records
    """
    PAGES_COUNT = 4
    with requests.Session() as session:
        session.headers.update({"Authorization": JOB1_API_TOKEN})
        futures = {}
        with ThreadPoolExecutor() as executor:
            for page in range(1, PAGES_COUNT + 1):
                f = executor.submit(
                    session.get,
                    url=JOB1_API_URL + "sales",
                    params={"date": date, "page": page},
                )
                futures[f] = page

        results = []
        for future in futures:
            page = futures[future]
            response = future.result()
            try:
                response.raise_for_status()
                results.extend(response.json())
            except requests.RequestException as ex:
                logging.error("Failed to get data for page %s: %s", page, ex)
        return results


def save_json_to_disk(json_content: List[Dict[str, Any]], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(json_content, f)


def save_sales_to_local_disk(date: str, raw_dir: str) -> str:
    data = get_sales(date)
    path = os.path.join(raw_dir, f"sales_{date}.json")
    save_json_to_disk(data, path)
    return path


def list_files(dir: str) -> List[str]:
    return [os.path.join(dir, file) for file in os.listdir(dir)]


def load_json(path: str) -> List[Dict[str, Any]]:
    with open(path, "r") as f:
        return json.load(f)


schema = {
    "doc": "Sales data reading.",
    "name": "Sales",
    "namespace": "sales",
    "type": "record",
    "fields": [
        {"name": "client", "type": "string"},
        {"name": "purchase_date", "type": {"type": "string", "logicalType": "date"}},
        {"name": "product", "type": "string"},
        {"name": "price", "type": "long"},
    ],
}


parsed_schema = fastavro.parse_schema(schema)


def save_avro(content: List[Dict[str, Any]], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as f:
        logging.info(f"Writing Avro file {path}...")
        fastavro.writer(f, parsed_schema, content)


def load_avro(path: str) -> List[Dict[str, Any]]:
    logging.info(f"Reading Avro file {path}...")
    result = []
    with open(path, "rb") as f:
        for line in fastavro.reader(f, parsed_schema):
            result.append(line)
    return result


def process_stg(stg_dir: str, raw_dir: str) -> None:
    logging.info("Processing %s into %s", raw_dir, stg_dir)
    for path in list_files(raw_dir):
        data = load_json(path)
        logging.info("Read %s rows from %s", len(data), path)

        stg_file = os.path.basename(path).replace(".json", ".avro")
        stg_path = os.path.join(stg_dir, stg_file)
        save_avro(data, path=stg_path)


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

        path = save_sales_to_local_disk(date, raw_dir)
        return {'path': path}

    @task(task_id="convert_to_avro")
    def convert_to_avro(ds=None, **kwargs):
        print(kwargs)
        print(ds)
        date = str(ds)
        raw_dir = os.path.join(os.path.dirname(__file__), "temp", "raw", date)
        stg_dir = os.path.join(os.path.dirname(__file__), "temp", "stg", date)
        
        process_stg(raw_dir=raw_dir, stg_dir=stg_dir)
        return {'raw_dir': raw_dir, 'stg_dir': stg_dir}

    BUCKET = 'de07-hw-bucket-oleksandrsan'
    dag_run_date = '{{ ds }}'
    dag_run_date_path = "year={{ execution_date.strftime('%Y') }}/month={{ execution_date.strftime('%m') }}/day={{ execution_date.strftime('%d') }}"
    upload_to_gcs = LocalFilesystemToGCSOperator(
        src=f'{os.path.dirname(__file__)}/temp/stg/{dag_run_date}/sales_{dag_run_date}.avro',
        dst=f'src1/sales/v1/{dag_run_date_path}/sales_{dag_run_date}.avro',
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
