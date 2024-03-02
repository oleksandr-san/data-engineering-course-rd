TRUNCATE TABLE `{{ params.project_id }}.{{params.dest_ds}}.{{params.dest_table}}`;

INSERT INTO `{{ params.project_id }}.{{params.dest_ds}}.{{params.dest_table}}` (
        client_id,
        first_name,
        last_name,
        email,
        registration_date,
        state,
        _exec_date
    )
SELECT 
    Id,
    FirstName,
    LastName,
    Email,
    CASE
        WHEN REGEXP_CONTAINS(RegistrationDate, r'^\d{4}/\d{2}/\d{2}$') THEN PARSE_DATE('%Y/%m/%d', RegistrationDate)
        WHEN REGEXP_CONTAINS(RegistrationDate, r'^\d{4}-[A-Za-z]{3}-\d{2}$') THEN PARSE_DATE('%Y-%b-%d', RegistrationDate)
        ELSE DATE(RegistrationDate)
    END AS registration_date,
    State,
    DATE("{{ ds }}") AS _exec_date
FROM `{{ params.project_id }}.{{params.src_ds}}.{{params.src_table}}`
;