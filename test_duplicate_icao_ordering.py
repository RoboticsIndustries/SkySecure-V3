import unittest
from unittest.mock import AsyncMock

import orjson

from processing.fusion_engine import DuplicateICAODetector


class DuplicateIcaoOrderingTests(unittest.IsolatedAsyncioTestCase):
    async def test_delayed_position_does_not_replace_newer_sample(self):
        redis = AsyncMock()
        redis.get.return_value = orjson.dumps({
            "lat": 40.0,
            "lon": -75.0,
            "timestamp": 200.0,
        })
        detector = DuplicateICAODetector(redis)

        result = await detector.check("ABC123", 40.01, -75.01, 100.0)

        self.assertIsNone(result)
        redis.setex.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
