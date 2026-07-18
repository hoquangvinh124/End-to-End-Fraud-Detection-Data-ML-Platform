FROM apache/airflow:3.1.0

RUN pip install --no-cache-dir \
    'apache-airflow[otel]==3.1.0' \
    'apache-airflow-providers-docker>=3.9.0' \
    'apache-airflow-providers-standard>=1.0.0' \
    'astronomer-cosmos==1.14.1' \
    'dbt-core~=1.10.0' \
    'dbt-trino==1.10.1' \
    'protobuf<6.32'
