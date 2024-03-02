customers_csv = {
    "autodetect": False,
    "schema": {
        "fields": [
            {
                "name": "Id",
                "type": "STRING",
                "mode": "REQUIRED"
            },
            {
                "name": "FirstName",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "LastName",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "Email",
                "type": "STRING",
                "mode": "NULLABLE"
            },
            {
                "name": "RegistrationDate",
                "type": "STRING",
                "mode": "REQUIRED"
            },
            {
                "name": "State",
                "type": "STRING",
                "mode": "NULLABLE"
            },
        ]
    },
    "csvOptions": {
        "allowJaggedRows": False,
        "allowQuotedNewlines": False,
        "maxBadRecords": 0,
        "encoding": "UTF-8",
        "quote": "\"",
        "fieldDelimiter": ",",
        "skipLeadingRows": 1
    },
    "sourceFormat": "CSV",
    "sourceUris": [
        (
            "gs://{{ params.data_lake_raw_bucket }}"
            "/final/data/customers"
            "/{{ dag_run.logical_date.strftime('%Y-%m-%-d') }}"
            "/*.csv"
        )
    ]
}