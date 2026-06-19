{# Trino does not support ILIKE. Override dbt_utils default with lower() LIKE lower(). #}
{% macro trino__get_tables_by_pattern_sql(schema_pattern, table_pattern, exclude='', database=target.database) %}

    select distinct
        table_schema as {{ adapter.quote('table_schema') }},
        table_name   as {{ adapter.quote('table_name') }},
        table_type
    from {{ database }}.information_schema.tables
    where lower(table_schema) like lower('{{ schema_pattern }}')
      and lower(table_name)   like lower('{{ table_pattern }}')
      {% if exclude %}
      and lower(table_name) not like lower('{{ exclude }}')
      {% endif %}

{% endmacro %}
