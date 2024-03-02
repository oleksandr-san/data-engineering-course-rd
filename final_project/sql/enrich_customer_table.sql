TRUNCATE TABLE `{{ params.project_id }}.{{params.dest_ds}}.{{params.dest_table}}`;
INSERT INTO `{{ params.project_id }}.{{params.dest_ds}}.{{params.dest_table}}` 
(
  client_id,
  email,
  first_name,
  last_name,
  registration_date,
  state,
  birth_date,
  phone_number,
  age
)
SELECT 
  c.client_id,
  c.email,
  IFNULL(c.first_name, SPLIT(u.full_name, ' ')[OFFSET(0)]),
  IFNULL(c.last_name, SPLIT(u.full_name, ' ')[OFFSET(1)]),
  c.registration_date,
  IFNULL(c.state, u.state) AS state,
  u.birth_date,
  u.phone_number,
  DATE_DIFF(CURRENT_DATE, u.birth_date, YEAR)

FROM `{{ params.project_id }}.{{params.src_ds}}.{{params.src_table_customers}}` AS c
LEFT JOIN `{{ params.project_id }}.{{params.src_ds}}.{{params.src_table_user_profiles}}` AS u
ON c.email = u.email;