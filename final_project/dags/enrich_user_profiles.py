from datetime import datetime

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from common import DEFAULT_DAG_ARGS, PROJECT_ID

with DAG(
    dag_id="enrich_user_profiles",
    description="Pipeline to enrich user profiles and create gold table",
    default_args=DEFAULT_DAG_ARGS,
    schedule_interval=None,
    catchup=True,
    start_date=datetime(2024, 1, 1),
) as dag:
    enrich_user_profiles = BigQueryInsertJobOperator(
        task_id="enrich_user_profiles",
        dag=dag,
        project_id=PROJECT_ID,
        configuration={
            "query": {
                "query": "{% include 'sql/enrich_user_profiles.sql' %}",
                "useLegacySql": False,
            }
        },
        params={
            "project_id": PROJECT_ID,
        },
    )
