CREATE OR REPLACE TABLE `{{ params.project_id }}.gold.user_profiles_enriched` (
    client_id INT,
    first_name STRING,
    last_name STRING,
    email STRING,
    registration_date DATE,
    state STRING,
    birth_date DATE,
    phone_number STRING,

    _id STRING,
    _logical_dt TIMESTAMP,
    _job_start_dt TIMESTAMP
);

INSERT INTO `{{ params.project_id }}.gold.user_profiles_enriched`
SELECT
    c.client_id,
    CASE WHEN c.first_name IS NULL OR c.first_name = '' THEN SPLIT(up.full_name, ' ')[OFFSET(0)] ELSE c.first_name END AS first_name,
    CASE WHEN c.last_name IS NULL OR c.last_name = '' THEN SPLIT(up.full_name, ' ')[OFFSET(1)] ELSE c.last_name END AS last_name,
    c.email,
    c.registration_date,
    CASE WHEN c.state IS NULL OR c.state = '' THEN up.state ELSE c.state END as state,
    SAFE.PARSE_DATE('%Y-%m-%d', up.birth_date),
    up.phone_number,
    GENERATE_UUID() AS _id,
    CURRENT_TIMESTAMP() AS _logical_dt,
    CAST('{{ ds }}' AS TIMESTAMP) AS _job_start_dt
FROM
    `{{ params.project_id }}.silver.customers` AS c
LEFT JOIN
    `{{ params.project_id }}.silver.user_profiles` AS up
ON
    c.email = up.email;