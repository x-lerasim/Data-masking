{% macro pseudo_key(column) %}
    {%- if env_var('MASKING_SALT', '') | length < 16 -%}
        {{ exceptions.raise_compiler_error("MASKING_SALT не задана или короче 16 символов") }}
    {%- endif -%}
    lower(hex(substring(SHA256(concat(getSetting('SQL_masking_salt'), ':', toString({{ column }}))), 1, 8)))
{% endmacro %}
