CREATE TABLE IF NOT EXISTS `{{ params.project_id }}.bronze.customers` (
    Id STRING,
    FirstName STRING,
    LastName STRING,
    Email STRING,
    RegistrationDate STRING,
    State STRING,

    _id STRING,
    _logical_dt TIMESTAMP,
    _job_start_dt TIMESTAMP
);

DELETE FROM `{{ params.project_id }}.bronze.customers`
WHERE DATE(_logical_dt) = "{{ ds }}";

MERGE INTO `{{ params.project_id }}.bronze.customers` AS customers
USING customers_csv
ON customers.Id = customers_csv.Id
WHEN NOT MATCHED THEN
INSERT (
    Id,
    FirstName,
    LastName,
    Email,
    RegistrationDate,
    State,
    _id,
    _logical_dt,
    _job_start_dt
)
VALUES (
    Id,
    FirstName,
    LastName,
    Email,
    RegistrationDate,
    State,
    GENERATE_UUID(),
    CAST('{{ dag_run.logical_date }}' AS TIMESTAMP),
    CAST('{{ dag_run.start_date }}' AS TIMESTAMP)
);