DELETE FROM `{{ params.project_id }}.{{params.dest_ds}}.{{params.dest_table}}`
WHERE DATE(_exec_date) = "{{ ds }}"
;

INSERT `{{ params.project_id }}.{{params.dest_ds}}.{{params.dest_table}}` (
    CustomerId,
    PurchaseDate,
    Product,
    Price,
    _exec_date
)
SELECT
    CustomerId,
    PurchaseDate,
    Product,
    Price,
    DATE('{{ dag_run.logical_date }}') AS _exec_date
    
FROM `{{ params.project_id }}.{{params.dest_ds}}.{{params.temp_table}}`
;