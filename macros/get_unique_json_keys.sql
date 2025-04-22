/* returns a unique list of json keys to later be used in the  pivot  model */

{% macro get_unique_json_keys(source_model, key_col='json_key') %}
    {% set query %}
        SELECT DISTINCT {{ key_col }}
        FROM {{ source_model }}
        WHERE {{ key_col }} IS NOT NULL
    {% endset %}

    {% set results = run_query(query) %}
    {% if execute %}
        {% set keys = results.columns[0].values() %}
        {{ return(keys) }}
    {% else %}
        {{ return([]) }}
    {% endif %}
{% endmacro %}


