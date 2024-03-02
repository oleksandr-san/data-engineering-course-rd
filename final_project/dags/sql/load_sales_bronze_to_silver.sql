CREATE TABLE IF NOT EXISTS `{{ params.project_id }}.silver.sales` (
    client_id INT,
    price INTEGER,
    product_name STRING,
    purchase_date DATE,

    _id STRING,
    _logical_dt TIMESTAMP,
    _job_start_dt TIMESTAMP
);

DELETE FROM `{{ params.project_id }}.silver.sales`
WHERE DATE(purchase_date) = "{{ ds }}";

INSERT `{{ params.project_id }}.silver.sales` (
    client_id,
    purchase_date,
    product_name,
    price,

    _id,
    _logical_dt,
    _job_start_dt
)
SELECT
    CAST(CustomerId as INT) as client_id,
    COALESCE(
        SAFE.PARSE_DATE('%Y-%m-%d', PurchaseDate),
        SAFE.PARSE_DATE('%Y/%m/%d', PurchaseDate),
        SAFE.PARSE_DATE('%Y-%h-%d', PurchaseDate)
    ) AS purchase_date,
    Product AS product_name,
    CAST(REGEXP_REPLACE(Price, r'USD|\$', '') AS INTEGER) AS price,

    _id,
    _logical_dt,
    _job_start_dt
FROM `{{ params.project_id }}.bronze.sales`
WHERE DATE(_logical_dt) = "{{ ds }}";