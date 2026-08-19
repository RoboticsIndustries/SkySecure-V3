"""Partition-safe Kafka offset commit helpers."""

from aiokafka.structs import OffsetAndMetadata, TopicPartition


async def commit_record(consumer, record) -> None:
    """Commit exactly the supplied completed record, never another partition."""
    await consumer.commit({
        TopicPartition(record.topic, record.partition):
            OffsetAndMetadata(record.offset + 1, ""),
    })
