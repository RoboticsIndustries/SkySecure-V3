import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from models import DataSource, RawADSBMessage, RawMLATReport, StateVector
from processing.fusion_engine import FusionEngine, drain_fusion_outbox_periodically


class FusionPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_periodic_drainer_recovers_pending_row_without_source_replay(self):
        postgres = SimpleNamespace(fetch=AsyncMock(side_effect=[
            [{"event_id": "event-1"}], asyncio.CancelledError(),
        ]))
        engine = FusionEngine(AsyncMock(), postgres_pool=postgres)
        engine.deliver_outbox_event = AsyncMock(return_value=b"payload")
        producer = AsyncMock()
        with self.assertRaises(asyncio.CancelledError):
            await drain_fusion_outbox_periodically(
                engine, producer, batch_size=100, interval=0,
            )
        engine.deliver_outbox_event.assert_awaited_once_with("event-1", producer)

    def test_event_ledger_schema_retains_raw_event_and_hash(self):
        from pathlib import Path

        for path in (
            "scripts/init.sql",
            "scripts/migrations/001_fusion_event_commits.sql",
        ):
            source = Path(path).read_text()
            self.assertRegex(source, r"raw_event\s+BYTEA")
            self.assertRegex(source, r"raw_event_sha256\s+TEXT")

    async def test_duplicate_detector_check_is_read_only(self):
        from processing.fusion_engine import DuplicateICAODetector

        redis = AsyncMock()
        redis.get.return_value = None
        detector = DuplicateICAODetector(redis)

        self.assertIsNone(await detector.check("ABC123", 40.0, -75.0, 100.0))
        redis.setex.assert_not_awaited()
        await detector.record("ABC123", 40.0, -75.0, 100.0)
        redis.setex.assert_awaited_once()

    async def test_source_event_and_fused_state_identity_mismatch_has_no_effects(self):
        events = [
            RawADSBMessage(
                receiver_id="feed", recv_time=100.0, icao24="DEF456",
                raw_message="8DDEF456", msg_type=17,
            ),
            RawMLATReport(
                session_id="mlat", solve_time=100.0, icao24="DEF456",
                lat=40.0, lon=-75.0, altitude_baro=10000,
                num_receivers=4, tdoa_residual=10.0, cep90=10.0,
                receiver_ids=["receiver-1", "receiver-2", "receiver-3", "receiver-4"],
                source_event_ids=[f"{i:064x}" for i in range(4)],
            ),
        ]
        for event in events:
            with self.subTest(event=type(event).__name__):
                redis = AsyncMock()
                producer = AsyncMock()
                postgres = AsyncMock()
                with self.assertRaises(ValueError):
                    await FusionEngine(redis, postgres_pool=postgres).save(
                        StateVector(icao24="ABC123"), producer, event=event,
                    )
                postgres.execute.assert_not_awaited()
                producer.send_and_wait.assert_not_awaited()
                redis.setex.assert_not_awaited()

    async def test_postgres_save_without_immutable_source_event_fails_closed(self):
        engine = FusionEngine(AsyncMock(), postgres_pool=AsyncMock())
        with self.assertRaises(ValueError):
            await engine.save(StateVector(icao24="ABC123"), AsyncMock())

    async def test_delayed_source_event_persists_its_own_measurement_not_fused_state(self):
        redis = AsyncMock()
        producer = AsyncMock()
        pool = AsyncMock()
        pool.fetchrow.return_value = {
            "topic": "fused.tracks", "message_key": b"ABC123", "payload": b"{}"
        }
        pool.execute.return_value = "UPDATE 1"
        aggregate = StateVector(
            icao24="ABC123", lat=49.0, lon=-10.0, altitude_baro=40000,
            velocity=500, last_seen=200.0,
        )
        event = RawADSBMessage(
            receiver_id="feed", recv_time=100.0, icao24="ABC123",
            raw_message="8DABC123", msg_type=17, lat=39.9, lon=-75.1,
            altitude_baro=10000, velocity=250, heading=90,
        )

        await FusionEngine(redis, postgres_pool=pool).save(
            aggregate, producer, event=event,
        )

        _sql, *values = pool.execute.await_args_list[0].args
        self.assertEqual(values[1], 100.0)
        self.assertEqual(values[5:9], [39.9, -75.1, 10000, None])
        self.assertEqual(values[9:11], [250.0, 90.0])

    async def test_save_persists_track_to_postgres(self):
        redis = AsyncMock()
        producer = AsyncMock()
        pool = AsyncMock()
        pool.fetchrow.return_value = {
            "topic": "fused.tracks", "message_key": b"ABC123", "payload": b"{}"
        }
        pool.execute.return_value = "UPDATE 1"
        engine = FusionEngine(redis, postgres_pool=pool)
        sv = StateVector(
            icao24="ABC123",
            callsign="TEST1",
            lat=39.9,
            lon=-75.1,
            altitude_baro=10000,
            velocity=250,
            heading=90,
            vertical_rate=500,
            primary_source=DataSource.ADSB,
            confidence=0.8,
            last_seen=1_700_000_000,
        )

        event = RawADSBMessage(
            receiver_id="feed", recv_time=sv.last_seen, icao24=sv.icao24,
            raw_message="8DABC123", msg_type=17, callsign=sv.callsign,
            lat=sv.lat, lon=sv.lon, altitude_baro=sv.altitude_baro,
            velocity=sv.velocity, heading=sv.heading,
            vertical_rate=sv.vertical_rate,
        )
        await engine.save(sv, producer, event=event)

        redis.setex.assert_awaited_once()
        self.assertEqual(redis.setex.await_args.args[0], "fusion:sv:ABC123")
        producer.send_and_wait.assert_awaited_once()
        producer.send.assert_not_called()
        pool.execute.assert_awaited()
        sql, *values = pool.execute.await_args_list[0].args
        self.assertIn("INSERT INTO track_points", sql)
        self.assertEqual(values[2], "ABC123")
        self.assertEqual(values[4], "TEST1")
        self.assertEqual(values[5], 39.9)
        self.assertEqual(values[6], -75.1)

    async def test_durable_outbox_payload_carries_source_event_id(self):
        redis = AsyncMock()
        producer = AsyncMock()
        pool = AsyncMock()

        async def fetch_outbox(*_args):
            return {
                "topic": "fused.tracks",
                "message_key": b"ABC123",
                "payload": pool.execute.await_args_list[0].args[-1],
            }

        pool.fetchrow.side_effect = fetch_outbox
        pool.execute.return_value = "UPDATE 1"
        event = RawADSBMessage(
            receiver_id="feed", recv_time=100.0, icao24="ABC123",
            raw_message="8DABC123", msg_type=17,
        )
        await FusionEngine(redis, postgres_pool=pool).save(
            StateVector(icao24="ABC123", last_seen=100.0), producer, event=event
        )
        claim_sql = pool.execute.await_args_list[0].args[0]
        self.assertIn("INSERT INTO fusion_outbox", claim_sql)
        sent = StateVector.from_bytes(producer.send_and_wait.await_args.kwargs["value"])
        self.assertIsNotNone(sent.source_event_id)
        self.assertEqual(sent.source_event_id, pool.execute.await_args_list[0].args[1])

    async def test_fusion_load_uses_service_owned_state_namespace(self):
        redis = AsyncMock()
        redis.get.return_value = None
        engine = FusionEngine(redis)

        await engine._load_or_create("abc123")

        redis.get.assert_awaited_once_with("fusion:sv:ABC123")

    async def test_failed_fused_delivery_does_not_advance_fusion_state(self):
        redis = AsyncMock()
        producer = AsyncMock()
        producer.send_and_wait.side_effect = RuntimeError("broker unavailable")
        engine = FusionEngine(redis)

        with self.assertRaisesRegex(RuntimeError, "broker unavailable"):
            await engine.save(StateVector(icao24="ABC123"), producer)

        redis.setex.assert_not_awaited()


if __name__ == "__main__":
    unittest.main()
