{{ config(materialized='incremental', unique_key='merge_key') }}

WITH flattened AS (
  SELECT
    f.application_id,  
    f.json_key AS parent_key,
    nested.key::VARCHAR AS nested_key,
    nested.value::VARCHAR AS nested_value,
    concat(f.application_id, '|', f.json_key, '|', nested.key) AS merge_key,
    ROW_NUMBER() OVER (
      PARTITION BY concat(f.application_id, '|', f.json_key, '|', nested.key)
      ORDER BY f.application_id
    ) AS row_num
  FROM {{ ref('int_submissions__sample_json_flattened') }} f,
  LATERAL FLATTEN(input => f.nested_1) nested
  WHERE IS_OBJECT(f.nested_1)
)

SELECT *
FROM flattened
WHERE row_num = 1
