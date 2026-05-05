import json
import time
import uuid
from typing import Any

from kafka import KafkaAdminClient, KafkaConsumer, KafkaProducer
from kafka.admin import NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError


DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092"


def json_serializer(value: dict[str, Any]) -> bytes:
    return json.dumps(value).encode("utf-8")


def json_deserializer(value: bytes | None) -> dict[str, Any] | None:
    if value is None:
        return None
    return json.loads(value.decode("utf-8"))


def new_run_id() -> str:
    return uuid.uuid4().hex[:8]


def unique_topic_name(prefix: str) -> str:
    return f"{prefix}-{new_run_id()}"


def unique_group_id(prefix: str) -> str:
    return f"{prefix}-{new_run_id()}"


def wait_for_broker(bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS, timeout_s: int = 60) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        consumer = None
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=bootstrap_servers,
                request_timeout_ms=3000,
                api_version_auto_timeout_ms=3000,
            )
            consumer.topics()
            return
        except NoBrokersAvailable:
            time.sleep(1)
        finally:
            if consumer is not None:
                consumer.close()
    raise TimeoutError(f"Kafka broker at {bootstrap_servers} was not ready within {timeout_s} seconds")


def ensure_topics(
    topic_specs: list[tuple[str, int, int]],
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
) -> None:
    admin = KafkaAdminClient(bootstrap_servers=bootstrap_servers, client_id="kafka-zero-to-hero")
    try:
        new_topics = [
            NewTopic(name=name, num_partitions=partitions, replication_factor=replication)
            for name, partitions, replication in topic_specs
        ]
        try:
            admin.create_topics(new_topics=new_topics, validate_only=False)
        except TopicAlreadyExistsError:
            pass
    finally:
        admin.close()


def build_producer(bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS) -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        acks="all",
        retries=5,
        linger_ms=10,
        value_serializer=json_serializer,
    )


def build_consumer(
    topic: str,
    group_id: str,
    bootstrap_servers: str = DEFAULT_BOOTSTRAP_SERVERS,
) -> KafkaConsumer:
    return KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=json_deserializer,
        consumer_timeout_ms=1000,
    )


def drain_messages(consumer: KafkaConsumer, expected_count: int, timeout_s: int = 30) -> list[dict[str, Any]]:
    deadline = time.time() + timeout_s
    drained: list[dict[str, Any]] = []

    while time.time() < deadline and len(drained) < expected_count:
        batches = consumer.poll(timeout_ms=500, max_records=expected_count - len(drained))
        for topic_partition, messages in batches.items():
            for message in messages:
                drained.append(
                    {
                        "topic": topic_partition.topic,
                        "partition": topic_partition.partition,
                        "offset": message.offset,
                        "key": message.key.decode("utf-8") if message.key else None,
                        "value": message.value,
                    }
                )
                if len(drained) == expected_count:
                    return drained

    raise TimeoutError(f"Expected {expected_count} messages but received {len(drained)}") 