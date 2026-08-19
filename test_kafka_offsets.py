import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from aiokafka.structs import OffsetAndMetadata, TopicPartition


class KafkaOffsetCommitTests(unittest.IsolatedAsyncioTestCase):
    async def test_commit_record_advances_only_completed_partition(self):
        from kafka_offsets import commit_record

        consumer = AsyncMock()
        record = SimpleNamespace(topic="events", partition=3, offset=41)
        await commit_record(consumer, record)

        consumer.commit.assert_awaited_once_with({
            TopicPartition("events", 3): OffsetAndMetadata(42, ""),
        })


if __name__ == "__main__":
    unittest.main()
