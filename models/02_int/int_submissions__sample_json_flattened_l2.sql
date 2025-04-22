/* This currently results in a blank table, but will be used for nested json as required */

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
  FROM {{ ref('int_submissions__sample_json_flattened_l1') }} f,
  LATERAL FLATTEN(input => f.nested_json_value) nested
  WHERE --IS_OBJECT(f.nested_json_value)
  f.nested_json_value IS NOT NULL
)

SELECT *
FROM flattened
WHERE row_num = 1
