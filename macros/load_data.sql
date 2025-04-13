-- macros/load_data.sql

-- load quotes
{% macro load_quotes() %}
USE SCHEMA insurance;
    COPY INTO insure_tech.insurance.quotes
    FROM (
      SELECT
        $1, $2, $3, $4, $5,
        CURRENT_TIMESTAMP(),
        METADATA$FILENAME
      FROM @insure_tech_s3/quotes/
    )
    FILE_FORMAT = (
      TYPE = CSV
      SKIP_HEADER = 1
      FIELD_DELIMITER = ','
      FIELD_OPTIONALLY_ENCLOSED_BY = '"'
      NULL_IF = (' ')
      ESCAPE = '\\'
    );
{% endmacro %}


-- load policies
{% macro load_policies() %}
USE SCHEMA insurance;
    COPY INTO insure_tech.insurance.policies
    FROM (
      SELECT
        $1, $2, $3, $4, $5,
        CURRENT_TIMESTAMP(),
        METADATA$FILENAME
      FROM @insure_tech_s3/policies/
    )
    FILE_FORMAT = (
      TYPE = CSV
      SKIP_HEADER = 1
      FIELD_DELIMITER = ','
      FIELD_OPTIONALLY_ENCLOSED_BY = '"'
      NULL_IF = (' ')
      ESCAPE = '\\'
    );
{% endmacro %}

-- load submissions
{% macro load_submissions() %}
USE SCHEMA insurance;
    COPY INTO insure_tech.insurance.submissions
    FROM (
      SELECT
        $1, $2, $3, $4, $5, $6,
        CURRENT_TIMESTAMP(),
        METADATA$FILENAME
      FROM @insure_tech_s3/submissions/
    )
    FILE_FORMAT = (
      TYPE = CSV
      SKIP_HEADER = 1
      FIELD_DELIMITER = ','
      FIELD_OPTIONALLY_ENCLOSED_BY = '"'
      NULL_IF = (' ')
      ESCAPE = '\\'
    );
{% endmacro %}


-- load all 
{% macro load_all() %}
    {% do run_query(load_quotes()) %}
    {% do run_query(load_policies()) %}
    {% do run_query(load_submissions()) %}
{% endmacro %}
