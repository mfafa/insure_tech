{{ config(materialized='incremental', unique_key='merge_key') }}

WITH flattened AS (
  SELECT
    s.application_id AS application_id,
    f.key::VARCHAR AS json_key,
    f.value::VARCHAR AS json_value,
    TRY_PARSE_JSON(f.value) AS nested_1,
    concat(s.application_id, '|', f.key, '|', f.value) AS merge_key,
    ROW_NUMBER() OVER (
      PARTITION BY concat(s.application_id, '|', f.key, '|', f.value)
      ORDER BY s.application_id
    ) AS row_num
  FROM {{ ref('stg_submissions') }} s,
  LATERAL FLATTEN(input => TRY_PARSE_JSON(s.sample_json)) f
  WHERE f.value IS NOT NULL
)

SELECT *
FROM flattened
WHERE row_num = 1
