{# turns a date or timestamp into an integer key like 20230101 #}
{% macro date_key(column) %}cast(date_format({{ column }}, 'yyyyMMdd') as int){% endmacro %}
