DELETE FROM `{{ params.project_id }}.{{params.dest_ds}}.{{params.dest_table}}`
WHERE DATE(_exec_date) = "{{ ds }}";

INSERT INTO `{{ params.project_id }}.{{params.dest_ds}}.{{params.dest_table}}` (
        client_id,
        purchase_date,
        product_name,
        price,
        _exec_date
    )
SELECT 
    CustomerId,
    CASE
        WHEN REGEXP_CONTAINS(PurchaseDate, r'^\d{4}/\d{2}/\d{2}$') THEN PARSE_DATE('%Y/%m/%d', PurchaseDate)
        WHEN REGEXP_CONTAINS(PurchaseDate, r'^\d{4}-[A-Za-z]{3}-\d{2}$') THEN PARSE_DATE('%Y-%b-%d', PurchaseDate)
        ELSE DATE(PurchaseDate)
    END AS purchase_date,
    Product,
    CAST(REGEXP_REPLACE(Price, '[^0-9.]', '') AS DECIMAL) AS Price,
    DATE("{{ ds }}") AS _exec_date
FROM `{{ params.project_id }}.{{params.src_ds}}.{{params.src_table}}`
WHERE _exec_date = DATE("{{ ds }}")
;