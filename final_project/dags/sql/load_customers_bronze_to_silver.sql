CREATE TABLE IF NOT EXISTS `{{ params.project_id }}.silver.customers` (
    client_id INTEGER,
    first_name STRING,
    last_name STRING,
    email STRING,
    registration_date DATE,
    state STRING,

    _id STRING,
    _logical_dt TIMESTAMP,
    _job_start_dt TIMESTAMP
);

DELETE FROM `{{ params.project_id }}.silver.customers`
WHERE DATE(registration_date) = "{{ ds }}";

INSERT `{{ params.project_id }}.silver.customers` (
    client_id,
    first_name,
    last_name,
    email,
    registration_date,
    state,
    _id,
    _logical_dt,
    _job_start_dt
)
SELECT
    CAST(Id as INT) as client_id,
    FirstName AS first_name,
    LastName AS last_name,
    Email AS email,

    COALESCE(
        SAFE.PARSE_DATE('%Y-%m-%d', RegistrationDate),
        SAFE.PARSE_DATE('%Y/%m/%d', RegistrationDate),
        SAFE.PARSE_DATE('%Y-%h-%d', RegistrationDate),
        SAFE.PARSE_DATE('%Y.%m.%d', RegistrationDate)
    ) AS registration_date,
    State AS state,

    _id,
    _logical_dt,
    _job_start_dt
FROM `{{ params.project_id }}.bronze.customers`
WHERE DATE(_logical_dt) = "{{ ds }}";