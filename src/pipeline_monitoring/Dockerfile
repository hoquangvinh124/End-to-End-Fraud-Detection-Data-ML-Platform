FROM python:3.11-slim

WORKDIR /opt/app

RUN pip install --no-cache-dir \
    boto3==1.34.0 \
    clickhouse-connect==0.15.1 \
    confluent-kafka==2.12.2 \
    opentelemetry-sdk==1.41.0 \
    opentelemetry-exporter-otlp-proto-http==1.41.0 \
    redis==5.2.1 \
    trino==0.337.0

COPY src/pipeline_monitoring/ /opt/app/pipeline_monitoring/

ENV PYTHONPATH=/opt/app

CMD ["python", "-m", "pipeline_monitoring.observer_app"]
