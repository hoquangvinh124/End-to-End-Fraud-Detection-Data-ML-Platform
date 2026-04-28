FROM bitnami/spark:3.5.1

USER root

# Pre-fetch Kafka + S3A JARs via Ivy at build time so the container
# never needs Maven Central at runtime. Resolved JARs land in $SPARK_HOME/jars/.
RUN PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.hadoop:hadoop-aws:3.3.4" && \
    printf 'from pyspark.sql import SparkSession\nSparkSession.builder.getOrCreate().stop()\n' > /tmp/noop.py && \
    /opt/bitnami/spark/bin/spark-submit \
        --packages "${PACKAGES}" \
        --conf spark.jars.ivy=/tmp/ivy \
        /tmp/noop.py && \
    find /tmp/ivy/jars -name "*.jar" -exec cp {} /opt/bitnami/spark/jars/ \; && \
    rm -rf /tmp/ivy /tmp/noop.py

# Bake static Spark properties into the image.
COPY spark-defaults.conf /opt/bitnami/spark/conf/spark-defaults.conf

USER 1001
