import json
import struct
import time
import urllib.error
import urllib.request
import uuid

from confluent_kafka import Consumer, KafkaException, TopicPartition


def _schema_id_from_value(value: bytes | None) -> int:
    if value is None or len(value) < 5 or value[0] != 0:
        raise ValueError("message is not Confluent Avro wire format")
    return struct.unpack(">I", value[1:5])[0]


def _latest_schema_id(bootstrap_servers: str, topic: str) -> int:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"schema-recovery-{uuid.uuid4()}",
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = consumer.list_topics(topic, timeout=10)
        for partition_id in metadata.topics[topic].partitions:
            partition = TopicPartition(topic, partition_id)
            _, high = consumer.get_watermark_offsets(partition, timeout=10)
            if high == 0:
                continue
            consumer.assign([TopicPartition(topic, partition_id, high - 1)])
            message = consumer.poll(10)
            if message is not None and not message.error():
                return _schema_id_from_value(message.value())
    finally:
        consumer.close()
    raise RuntimeError(f"no Avro message is available in topic {topic!r}")


def _restore_subject(
    sr_url: str,
    subject: str,
    kafka_bootstrap_servers: str,
) -> str:
    topic = subject.removesuffix("-value")
    schema_id = _latest_schema_id(kafka_bootstrap_servers, topic)
    with urllib.request.urlopen(f"{sr_url}/schemas/ids/{schema_id}") as response:
        schema = json.loads(response.read())["schema"]

    request = urllib.request.Request(
        f"{sr_url}/subjects/{subject}/versions",
        data=json.dumps({"schema": schema}).encode(),
        headers={"Content-Type": "application/vnd.schemaregistry.v1+json"},
        method="POST",
    )
    with urllib.request.urlopen(request):
        pass
    return schema


def fetch_avro_schema(
    sr_url: str,
    subject: str,
    retries: int = 30,
    delay: int = 5,
    kafka_bootstrap_servers: str = "kafka:9092",
) -> str:
    """Fetch latest Avro schema string from Confluent Schema Registry.

    Retries to handle the window between connector registration and
    Debezium's first snapshot message (which triggers schema registration).
    """
    for attempt in range(retries):
        try:
            with urllib.request.urlopen(f"{sr_url}/subjects?deleted=true") as response:
                subjects = json.loads(response.read())
            if subject in subjects:
                url = f"{sr_url}/subjects/{subject}/versions/latest"
                with urllib.request.urlopen(url) as response:
                    return json.loads(response.read())["schema"]

            return _restore_subject(sr_url, subject, kafka_bootstrap_servers)
        except (urllib.error.URLError, KafkaException, RuntimeError) as exc:
            print(
                f"Schema unavailable for {subject!r}: {exc}, "
                f"retrying ({attempt + 1}/{retries})…"
            )
            time.sleep(delay)
    raise RuntimeError(
        f"Could not fetch Avro schema for subject {subject!r} after {retries} attempts"
    )
