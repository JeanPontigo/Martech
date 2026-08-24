-- macros/normalize_string.sql
{% macro normalize_string(column) %}
    LOWER(
        REGEXP_REPLACE(
        REGEXP_REPLACE(
        REGEXP_REPLACE(
        REGEXP_REPLACE(
        REGEXP_REPLACE(
        REGEXP_REPLACE(
            TRIM({{ column }}),
        '[áàäâãÁÀÄÂÃ]', 'a'),
        '[éèëêÉÈËÊ]', 'e'),
        '[íìïîÍÌÏÎ]', 'i'),
        '[óòöôõÓÒÖÔÕ]', 'o'),
        '[úùüûÚÙÜÛ]', 'u'),
        '[ñÑ]', 'n')
    )
{% endmacro %}
