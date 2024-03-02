from airflow import DAG

from airflow.providers.google.cloud.operators.bigquery import BigQueryGetDatasetOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = 'de-07-andrii-chuhunkin'

SILVER_DS = 'silver'
GOLD_DS = 'gold'
USER_TABLE = 'user_profiles'
CUSTOMER_TABLE = 'customers'

ENRICHED_PROFILES_TABLE = 'user_profiles_enriched'


enrich_user_profile = DAG(
    dag_id='enrich_user_profile',
    schedule_interval=None,
    max_active_runs=1,
    catchup=False
)

get_silver_dataset = BigQueryGetDatasetOperator(
    task_id="get_silver_dataset",
    dataset_id=SILVER_DS,
    dag=enrich_user_profile
)

get_gold_dataset = BigQueryGetDatasetOperator(
    task_id="get_gold_dataset",
    dataset_id=GOLD_DS,
    dag=enrich_user_profile
)

create_gold_table = BigQueryCreateEmptyTableOperator(
    task_id='create_gold_table_if_not_exists',
    dataset_id=GOLD_DS,
    table_id=ENRICHED_PROFILES_TABLE,
    schema_fields = [
        {'name': 'client_id', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'first_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'last_name', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'email', 'type': 'STRING', 'mode': 'REQUIRED'},
        {'name': 'registration_date', 'type': 'DATE', 'mode': 'REQUIRED'},
        {'name': 'state', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'birth_date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'phone_number', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'age', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    ],
    dag=enrich_user_profile
)

enrich_customers_table = BigQueryInsertJobOperator(
    task_id='enrich_customers_table',
    dag=enrich_user_profile,
    configuration={
        "query": {
            "query": "{% include 'sql/enrich_customer_table.sql' %}",
            "useLegacySql": False,
        }
    },
    params={
        'project_id': PROJECT_ID,
        'dest_ds': GOLD_DS,
        'dest_table': ENRICHED_PROFILES_TABLE,
        'src_ds': SILVER_DS,
        'src_table_customers': CUSTOMER_TABLE,
        'src_table_user_profiles': USER_TABLE,
    }
)

[get_silver_dataset, get_gold_dataset] >> create_gold_table >> enrich_customers_table