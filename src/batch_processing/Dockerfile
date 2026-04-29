FROM apache/spark:4.0.2-python3

USER root

# Pre-fetch Kafka + Avro + S3A JARs via Ivy at build time so the container
# never needs Maven Central at runtime. Resolved JARs land in $SPARK_HOME/jars/.
RUN PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.2,org.apache.spark:spark-avro_2.13:4.0.2,org.apache.hadoop:hadoop-aws:3.4.1" && \
    printf 'from pyspark.sql import SparkSession\nSparkSession.builder.getOrCreate().stop()\n' > /tmp/noop.py && \
    /opt/spark/bin/spark-submit \
        --packages "${PACKAGES}" \
        --conf spark.jars.ivy=/tmp/ivy \
        /tmp/noop.py && \
    find /tmp/ivy/jars -name "*.jar" -exec cp {} /opt/spark/jars/ \; && \
    rm -rf /tmp/ivy /tmp/noop.py

# Bake job configuration into the image — Spark auto-loads this on startup.
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf

USER spark
