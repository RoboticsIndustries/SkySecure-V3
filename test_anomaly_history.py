import unittest
import asyncio
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import orjson

from models import AnomalyFlag, AnomalyType, DetectionLayer, StateVector
import api.main as api_main


class AnomalyHistoryPersistenceTests(unittest.IsolatedAsyncioTestCase):
    def test_history_migration_adds_replay_identity_and_map_fields(self):
        sql = Path("scripts/migrations/002_anomaly_history.sql").read_text()
        for required in (
            "event_id", "callsign", "layer", "detector", "risk_score",
            "UNIQUE", "idx_anomaly_geo", "idx_anomaly_time",
        ):
            self.assertIn(required, sql)
        self.assertIn("NOT VALID", sql)
        self.assertIn("VALIDATE CONSTRAINT", sql)
        self.assertIn("pg_index", sql)
        self.assertIn("NOT i.indisvalid", sql)
        self.assertIn("DROP INDEX CONCURRENTLY", sql)
        self.assertNotIn("CREATE UNIQUE INDEX IF NOT EXISTS idx_anomaly_event_id", sql)

    def test_api_waits_for_successful_history_migration(self):
        compose = Path("docker-compose.yml").read_text()
        api_block = compose.split("  api:", 1)[1].split("  frontend:", 1)[0]
        self.assertIn("postgres-migrate:", api_block)
        self.assertIn("condition: service_completed_successfully", api_block)

    async def test_history_schema_verification_fails_startup_when_columns_are_missing(self):
        pool = AsyncMock()
        pool.fetchval.return_value = False

        with self.assertRaisesRegex(RuntimeError, "anomaly history schema"):
            await api_main.verify_anomaly_history_schema(pool)

        sql = pool.fetchval.await_args.args[0]
        for required in ("event_id", "callsign", "layer", "detector", "risk_score"):
            self.assertIn(required, sql)
        self.assertIn("indisunique", sql)
        self.assertIn("'public.anomaly_events'::regclass", sql)
        self.assertIn("is_nullable", sql)
        self.assertIn("a.attnum = i.indkey[0]", sql)
        self.assertIn("i.indnatts = 1", sql)

    async def test_persists_each_trigger_as_an_idempotent_geolocated_snapshot(self):
        pool = AsyncMock()
        state = StateVector(
            icao24="ABC123",
            callsign="TEST1",
            lat=40.25,
            lon=-75.5,
            risk_score=72,
            last_seen=1_720_000_000.0,
            anomalies=[
                AnomalyFlag(
                    anomaly_type=AnomalyType.TELEPORTATION,
                    layer=DetectionLayer.L2,
                    detector="teleportation",
                    score_delta=45,
                    description="Impossible position jump",
                    timestamp=1_720_000_000.0,
                    meta={"distance_nm": 900},
                )
            ],
        )

        count = await api_main.persist_anomaly_snapshot(pool, state, "kafka-event-7")

        self.assertEqual(count, 1)
        pool.executemany.assert_awaited_once()
        sql, rows = pool.executemany.await_args.args
        self.assertIn("ON CONFLICT (event_id) DO NOTHING", sql)
        self.assertEqual(len(rows), 1)
        row = rows[0]
        self.assertEqual(row[0], "kafka-event-7:0:teleportation")
        self.assertEqual(row[1], datetime.fromtimestamp(1_720_000_000.0, tz=timezone.utc))
        self.assertEqual(row[2], "ABC123")
        self.assertEqual(row[3], "TEST1")
        self.assertEqual(row[4:8], ("TELEPORTATION", "L2", "teleportation", 72))
        self.assertEqual(row[11:13], (40.25, -75.5))
        self.assertEqual(orjson.loads(row[13])["evidence"], {"distance_nm": 900})

    async def test_does_not_store_unlocated_or_non_anomalous_tracks(self):
        pool = AsyncMock()
        self.assertEqual(
            await api_main.persist_anomaly_snapshot(pool, StateVector(icao24="ABC123"), "event"),
            0,
        )
        pool.executemany.assert_not_awaited()

    async def test_history_endpoint_returns_durable_map_markers_for_time_window(self):
        pool = AsyncMock()
        pool.fetch.return_value = [{
            "event_id": "event:0:teleportation",
            "time": datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc),
            "icao24": "ABC123",
            "callsign": "TEST1",
            "anomaly_type": "TELEPORTATION",
            "layer": "L2",
            "detector": "teleportation",
            "risk_score": 72,
            "description": "Impossible position jump",
            "score_delta": 45,
            "lat": 40.25,
            "lon": -75.5,
            "meta": {"evidence": {"distance_nm": 900}},
        }]

        with patch("api.main.postgres_pool", pool):
            result = await api_main.get_anomaly_history(hours=24, limit=500)

        self.assertEqual(result["count"], 1)
        self.assertEqual(result["events"][0]["event_id"], "event:0:teleportation")
        self.assertEqual(result["events"][0]["time"], "2026-08-20T12:00:00+00:00")
        self.assertEqual(result["events"][0]["lat"], 40.25)
        sql, hours, limit = pool.fetch.await_args.args
        self.assertIn("time >= NOW() - ($1 * INTERVAL '1 hour')", sql)
        self.assertEqual((hours, limit), (24, 500))

    async def test_hotspot_endpoint_aggregates_repeated_events_into_map_cells(self):
        pool = AsyncMock()
        pool.fetch.return_value = [{
            "lat": 40.25,
            "lon": -75.55,
            "event_count": 7,
            "max_risk": 91,
            "aircraft_count": 3,
            "last_seen": datetime(2026, 8, 20, 12, 0, tzinfo=timezone.utc),
            "anomaly_types": ["TELEPORTATION", "DUPLICATE_ICAO"],
        }]

        with patch("api.main.postgres_pool", pool):
            result = await api_main.get_anomaly_hotspots(hours=168, precision=1)

        self.assertEqual(result["count"], 1)
        self.assertEqual(result["hotspots"][0]["event_count"], 7)
        self.assertEqual(result["hotspots"][0]["max_risk"], 91)
        self.assertEqual(result["hotspots"][0]["last_seen"], "2026-08-20T12:00:00+00:00")
        sql, hours, precision = pool.fetch.await_args.args
        self.assertIn("GROUP BY", sql)
        self.assertEqual((hours, precision), (24, 1))

    async def test_hotspots_require_multi_aircraft_sustained_recent_clusters(self):
        pool = AsyncMock()
        pool.fetch.return_value = []
        api_main._hotspot_cache.clear()

        with patch("api.main.postgres_pool", pool):
            result = await api_main.get_anomaly_hotspots(hours=168, precision=1)

        sql, effective_hours, precision = pool.fetch.await_args.args
        self.assertIn("date_bin(INTERVAL '10 minutes'", sql)
        self.assertIn("COUNT(DISTINCT icao24) >= 3", sql)
        self.assertIn("COUNT(*) >= 10", sql)
        self.assertIn("INTERVAL '15 minutes'", sql)
        self.assertIn("INTERVAL '2 hours'", sql)
        self.assertEqual(effective_hours, 24)
        self.assertEqual(precision, 1)
        self.assertEqual(result["hotspot_hours"], 24)
        self.assertEqual(result["recency_hours"], 2)

    async def test_all_retained_events_only_build_hotspots_from_last_day(self):
        pool = AsyncMock()
        pool.fetch.return_value = []
        api_main._hotspot_cache.clear()

        with patch("api.main.postgres_pool", pool):
            result = await api_main.get_anomaly_hotspots(hours=0, precision=1)

        _sql, effective_hours, precision = pool.fetch.await_args.args
        self.assertEqual((effective_hours, precision), (24, 1))
        self.assertEqual(result["hours"], 0)
        self.assertEqual(result["hotspot_hours"], 24)


    async def test_hotspot_endpoint_rejects_arbitrary_expensive_windows(self):
        with self.assertRaises(api_main.HTTPException) as raised:
            await api_main.get_anomaly_hotspots(hours=12345, precision=1)
        self.assertEqual(raised.exception.status_code, 422)

    async def test_hotspot_endpoint_caches_bounded_public_queries(self):
        pool = AsyncMock()
        pool.fetch.return_value = []
        api_main._hotspot_cache.clear()
        with patch("api.main.postgres_pool", pool):
            first = await api_main.get_anomaly_hotspots(hours=720, precision=2)
            second = await api_main.get_anomaly_hotspots(hours=720, precision=2)
        self.assertEqual(first, second)
        self.assertEqual(pool.fetch.await_count, 1)

    async def test_hotspot_endpoint_cancels_queries_over_budget(self):
        async def hang(*_args):
            await asyncio.Event().wait()

        pool = AsyncMock()
        pool.fetch.side_effect = hang
        api_main._hotspot_cache.clear()
        with (
            patch("api.main.postgres_pool", pool),
            patch("api.main.HOTSPOT_QUERY_TIMEOUT_SECONDS", 0.01),
        ):
            with self.assertRaises(api_main.HTTPException) as raised:
                await api_main.get_anomaly_hotspots(hours=24, precision=1)
        self.assertEqual(raised.exception.status_code, 503)

    async def test_alert_consumer_persists_history_without_connected_browser(self):
        state = StateVector(
            icao24="ABC123", lat=40.25, lon=-75.5, risk_score=72,
            anomalies=[AnomalyFlag(
                anomaly_type=AnomalyType.TELEPORTATION,
                layer=DetectionLayer.L2,
                detector="teleportation",
                score_delta=45,
                description="Impossible jump",
            )],
        )
        message = SimpleNamespace(
            value=state.to_bytes(), headers=[("event_id", b"kafka-event-7")]
        )

        class Consumer:
            start = AsyncMock()
            stop = AsyncMock()
            commit = AsyncMock()

            def __aiter__(self):
                async def records():
                    yield message
                return records()

        consumer = Consumer()
        persist = AsyncMock(return_value=1)
        api_main._ws_clients.clear()
        with (
            patch("api.main.AIOKafkaConsumer", return_value=consumer),
            patch("api.main.postgres_pool", AsyncMock()),
            patch("api.main.persist_anomaly_snapshot", persist),
            patch("api.main._reserve_alert_effect", AsyncMock(return_value="lease")),
            patch("api.main._complete_alert_effect", AsyncMock()),
            patch("api.main._release_alert_effect", AsyncMock()),
            patch("api.main._publish_alert", AsyncMock()) as publish,
            patch("api.main.commit_record", AsyncMock()) as commit,
            patch("api.main.load_coverage_area", AsyncMock(return_value=object())),
            patch("api.main._track_in_coverage", return_value=True),
            patch("api.main.TDOA_AVAILABLE", False),
        ):
            await api_main.alert_consumer_loop()

        persist.assert_awaited_once()
        publish.assert_not_awaited()
        commit.assert_awaited_once_with(consumer, message)

    async def test_alert_consumer_does_not_ack_when_history_store_is_unavailable(self):
        state = StateVector(icao24="ABC123", lat=40.25, lon=-75.5)
        message = SimpleNamespace(value=state.to_bytes(), headers=[])

        class Consumer:
            start = AsyncMock()
            stop = AsyncMock()
            commit = AsyncMock()
            def __aiter__(self):
                async def records():
                    yield message
                return records()

        consumer = Consumer()
        reserve = AsyncMock()
        with (
            patch("api.main.AIOKafkaConsumer", return_value=consumer),
            patch("api.main.postgres_pool", None),
            patch("api.main._reserve_alert_effect", reserve),
        ):
            with self.assertRaisesRegex(RuntimeError, "history storage unavailable"):
                await api_main.alert_consumer_loop()

        reserve.assert_not_awaited()
        consumer.commit.assert_not_awaited()

    async def test_api_lifespan_opens_and_closes_history_pool(self):
        async def block():
            await asyncio.Event().wait()

        redis = AsyncMock()
        producer = AsyncMock()
        pool = AsyncMock()
        pool.fetchval.return_value = True
        with (
            patch("api.main.aioredis.from_url", return_value=redis),
            patch("api.main.asyncpg.create_pool", AsyncMock(return_value=pool)) as create_pool,
            patch("api.main.AIOKafkaProducer", return_value=producer),
            patch("api.main.broadcast_loop", new=block),
            patch("api.main.alert_consumer_loop", new=block),
            patch("api.main.TDOA_AVAILABLE", False),
        ):
            async with api_main.lifespan(object()):
                self.assertIs(api_main.postgres_pool, pool)

        create_pool.assert_awaited_once()
        pool.fetchval.assert_awaited_once()
        pool.close.assert_awaited_once()
        self.assertIsNone(api_main.postgres_pool)

    async def test_postgres_shutdown_is_bounded_and_preserves_other_cleanup_errors(self):
        async def block():
            await asyncio.Event().wait()

        redis = AsyncMock()
        redis.close.side_effect = RuntimeError("redis close failed")
        producer = AsyncMock()
        producer.stop.side_effect = RuntimeError("producer stop failed")
        pool = AsyncMock()
        pool.fetchval.return_value = True

        async def hanging_close():
            await asyncio.Event().wait()
        pool.close.side_effect = hanging_close

        with (
            patch("api.main.aioredis.from_url", return_value=redis),
            patch("api.main.asyncpg.create_pool", AsyncMock(return_value=pool)),
            patch("api.main.AIOKafkaProducer", return_value=producer),
            patch("api.main.broadcast_loop", new=block),
            patch("api.main.alert_consumer_loop", new=block),
            patch("api.main.TDOA_AVAILABLE", False),
            patch("api.main.POSTGRES_CLOSE_TIMEOUT_SECONDS", 0.01, create=True),
        ):
            with self.assertRaises(BaseExceptionGroup) as raised:
                async with api_main.lifespan(object()):
                    pass

        messages = [str(error) for error in raised.exception.exceptions]
        self.assertTrue(any("producer stop failed" in message for message in messages))
        self.assertTrue(any("PostgreSQL pool shutdown timed out" in message for message in messages))
        self.assertTrue(any("redis close failed" in message for message in messages))


if __name__ == "__main__":
    unittest.main()
