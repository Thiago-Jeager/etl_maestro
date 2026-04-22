{% macro mask_email(email_column) %}
    CASE
        WHEN {{ email_column }} IS NULL THEN NULL
        WHEN LENGTH({{ email_column }}) <= 5 THEN '***'
        ELSE
            CONCAT(
                LEFT({{ email_column }}, 2),
                '***',
                RIGHT({{ email_column }}, LENGTH({{ email_column }}) - POSITION('@' IN {{ email_column }}) + 1)
            )
    END
{% endmacro %}


{% macro mask_id(id_column) %}
    CASE
        WHEN {{ id_column }} IS NULL THEN NULL
        WHEN LENGTH({{ id_column }}) <= 8 THEN '****'
        ELSE
            CONCAT(
                LEFT({{ id_column }}, 2),
                '****',
                RIGHT({{ id_column }}, 4)
            )
    END
{% endmacro %}


{% macro mask_name(name_column) %}
    CASE
        WHEN {{ name_column }} IS NULL THEN NULL
        ELSE CONCAT(LEFT({{ name_column }}, 1), '***')
    END
{% endmacro %}
