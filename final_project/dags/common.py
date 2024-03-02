RAW_BUCKET = "de07-hw-bucket-oleksandrsan"
PROJECT_ID = "de07-oleksandr-anosov"
DS_LOCATION = "EU"
DS_BRONZE = "bronze"
DS_SILVER = "silver"
DS_GOLD = "gold"

DEFAULT_DAG_ARGS = {
    "depends_on_past": False,
    "email": ["oleksandr.v.anosov@gmail.com"],
    "owner": "oleksandr-san",
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": 30,
}
