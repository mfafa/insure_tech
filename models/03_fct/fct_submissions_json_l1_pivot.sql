{{ config(materialized='view') }}

{% set keys = get_unique_json_keys(source_model='int_submissions__unique_sample_json_keys') %}

SELECT
  application_id
  {% for key in keys %}
    , MAX(CASE WHEN json_key = '{{ key }}' THEN TRY_TO_NUMBER(json_value) END) AS {{ key | lower }}
  {% endfor %}
FROM {{ ref('int_submissions__sample_json_flattened_l1') }}
GROUP BY application_id


