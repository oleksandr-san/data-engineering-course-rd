CREATE OR REPLACE TABLE `{{ params.project_id }}.bronze.sales` (
    CustomerId STRING,
    PurchaseDate STRING,
    Product STRING,
    Price STRING,

    _id STRING,
    _logical_dt TIMESTAMP,
    _job_start_dt TIMESTAMP
);

DELETE FROM `{{ params.project_id }}.bronze.sales`
WHERE DATE(_logical_dt) = "{{ ds }}";

INSERT INTO `{{ params.project_id }}.bronze.sales` (
    CustomerId,
    PurchaseDate,
    Product,
    Price,
    _id,
    _logical_dt,
    _job_start_dt
)
SELECT
    CustomerId,
    PurchaseDate,
    Product,
    Price,
    GENERATE_UUID() AS _id,
    CAST('{{ dag_run.logical_date }}' AS TIMESTAMP) AS _logical_dt,
    CAST('{{ dag_run.start_date }}' AS TIMESTAMP) AS _job_start_dt
FROM sales_csv;