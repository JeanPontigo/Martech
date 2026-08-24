{% macro ga4_param(key, value_type='string') %}
    (
        SELECT value.{{ value_type }}_value
        FROM UNNEST(event_params)
        WHERE key = '{{ key }}'
    )
{% endmacro %}