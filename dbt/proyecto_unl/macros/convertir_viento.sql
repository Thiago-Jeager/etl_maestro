{% macro ms_a_kmh(columna_viento) %}
    ROUND(CAST({{ columna_viento }} * 3.6 AS NUMERIC), 2)
{% endmacro %}