FROM apache/spark:4.0.2-python3

USER root

RUN pip install --no-cache-dir \
    confluent-kafka==2.15.0 \
    opentelemetry-sdk==1.41.0 \
    opentelemetry-exporter-otlp-proto-http==1.41.0

# Pre-fetch Kafka + Avro + S3A + Delta JARs via Ivy at build time so the container
# never needs Maven Central at runtime. Resolved JARs land in $SPARK_HOME/jars/.
RUN PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2,org.apache.spark:spark-avro_2.13:4.0.2,org.apache.hadoop:hadoop-aws:3.4.1,io.delta:delta-spark_2.13:4.0.0" && \
    printf 'from pyspark.sql import SparkSession\nSparkSession.builder.getOrCreate().stop()\n' > /tmp/noop.py && \
    /opt/spark/bin/spark-submit \
        --packages "${PACKAGES}" \
        --conf spark.jars.ivy=/tmp/ivy \
        /tmp/noop.py && \
    find /tmp/ivy/jars -name "*.jar" -exec cp {} /opt/spark/jars/ \; && \
    rm -rf /tmp/ivy /tmp/noop.py

# Bake job configuration into the image
COPY src/cdc_ingestion/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Bake CDC ingestion job scripts into the image
COPY src/cdc_ingestion/cdc_transactions_to_bronze.py /opt/cdc_ingestion/cdc_transactions_to_bronze.py
COPY src/cdc_ingestion/cdc_fraud_cases_to_bronze.py /opt/cdc_ingestion/cdc_fraud_cases_to_bronze.py
COPY src/cdc_ingestion/utils/schema_registry_helpers.py /opt/cdc_ingestion/utils/schema_registry_helpers.py
COPY src/pipeline_monitoring/ /opt/pipeline_monitoring/

ENV PYTHONPATH=/opt

USER spark
