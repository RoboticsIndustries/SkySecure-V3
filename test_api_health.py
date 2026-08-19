import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import HTTPException

import api.main as api_main
from api.main import healthz


class ApiHealthTests(unittest.IsolatedAsyncioTestCase):
    async def test_mlat_readiness_requires_recent_minimum_receiver_set(self):
        handler = getattr(api_main, "mlat_readiness", None)
        self.assertTrue(callable(handler))
        redis = AsyncMock()
        redis.mget.return_value = [None, None, None, None]
        credentials = {
            receiver_id: f"test-secret-{index}"
            for index, receiver_id in enumerate(api_main.settings.MLAT_RECEIVER_LOCATIONS)
        }
        with (
            patch("api.main.redis_client", redis),
            patch("api.main.mlat_reception_producer", object()),
            patch.object(api_main.settings, "MLAT_RECEIVER_API_KEYS", credentials),
        ):
            with self.assertRaises(HTTPException) as unavailable:
                await handler()
        self.assertEqual(unavailable.exception.status_code, 503)

        redis.mget.return_value = [b"1000", b"1000", b"1000", b"1000"]
        with (
            patch("api.main.redis_client", redis),
            patch("api.main.mlat_reception_producer", object()),
            patch("api.main.time.time", return_value=1001.0),
            patch.object(api_main.settings, "MLAT_RECEIVER_API_KEYS", credentials),
        ):
            ready = await handler()
        self.assertEqual(ready["status"], "ready")
        self.assertEqual(ready["active_receivers"], 4)

    async def test_health_reports_redis_dependency(self):
        redis = AsyncMock()
        redis.ping.return_value = True
        postgres = AsyncMock()
        postgres.fetchval.return_value = 1
        writer = MagicMock()
        writer.wait_closed = AsyncMock()

        with (
            patch("api.main.redis_client", redis),
            patch("api.main.asyncpg.connect", AsyncMock(return_value=postgres)),
            patch("api.main.asyncio.open_connection", AsyncMock(return_value=(object(), writer))),
        ):
            result = await healthz()

        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["dependencies"]["redis"], "ok")
        self.assertEqual(result["dependencies"]["postgres"], "ok")
        self.assertEqual(result["dependencies"]["kafka"], "ok")

    async def test_health_fails_when_redis_is_unavailable(self):
        redis = AsyncMock()
        redis.ping.side_effect = ConnectionError("offline")

        with patch("api.main.redis_client", redis):
            with self.assertRaises(HTTPException) as raised:
                await healthz()

        self.assertEqual(raised.exception.status_code, 503)
        self.assertEqual(raised.exception.detail["dependencies"]["redis"], "error")


if __name__ == "__main__":
    unittest.main()
