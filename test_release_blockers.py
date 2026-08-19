import unittest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

from pydantic import ValidationError

from models import RawADSBMessage, RawMLATReport, StateVector
from receiver_auth import validate_receiver_credentials
from processing.mlat_solver import sign_mlat_report, verify_mlat_report


class FinalReleaseBlockerTests(unittest.IsolatedAsyncioTestCase):
    def test_receiver_credentials_are_complete_nonempty_and_distinct(self):
        locations = {"a": (0, 0, 0), "b": (1, 1, 1)}
        with self.assertRaises(ValueError):
            validate_receiver_credentials(locations, {"a": "", "b": "x"})
        with self.assertRaises(ValueError):
            validate_receiver_credentials(locations, {"a": "x", "b": "x"})
        self.assertEqual(
            validate_receiver_credentials(locations, {"a": "x", "b": "y"}),
            {"a": "x", "b": "y"},
        )

    def test_solver_report_signature_rejects_wrong_secret(self):
        report = RawMLATReport(
            session_id="s", solve_time=1, icao24="ABC123", lat=1, lon=2,
            altitude_baro=0, num_receivers=4, tdoa_residual=1, cep90=1,
            receiver_ids=["a", "b", "c", "d"],
            source_event_ids=[f"{i:064x}" for i in range(4)],
        )
        report.auth_tag = sign_mlat_report(report, "correct")
        self.assertTrue(verify_mlat_report(report, "correct"))
        self.assertFalse(verify_mlat_report(report, "wrong"))

    def _reception(self):
        return RawADSBMessage(
            receiver_id="receiver-london", recv_time=1000.0,
            icao24="ABC123", raw_message="8DABC12358C382D690C8AC2863A7",
            msg_type=17,
        )

    def test_physical_reception_key_is_secret_authenticated(self):
        from processing.mlat_solver import physical_reception_key
        reception = self._reception()
        self.assertNotEqual(
            physical_reception_key(reception, "receiver-secret-a"),
            physical_reception_key(reception, "receiver-secret-b"),
        )

    def test_kafka_and_zookeeper_have_named_persistent_volumes(self):
        compose = Path("docker-compose.yml").read_text()
        self.assertIn("kafkadata:/var/lib/kafka/data", compose)
        self.assertIn("zookeeperdata:/var/lib/zookeeper/data", compose)
        self.assertIn("zookeeperlog:/var/lib/zookeeper/log", compose)
        self.assertIn("kafkadata:", compose)
        self.assertIn("zookeeperdata:", compose)
        self.assertIn("zookeeperlog:", compose)

    def test_mlat_provenance_ids_are_exact_distinct_canonical(self):
        base = dict(
            session_id="s", solve_time=1, icao24="ABC123", lat=1, lon=2,
            altitude_baro=0, num_receivers=4, tdoa_residual=1, cep90=1,
            receiver_ids=["a", "b", "c", "d"],
        )
        for ids in (
            [], ["0" * 64] * 4, ["not-hex"] * 4,
            [f"{i:064x}" for i in range(3)],
        ):
            with self.subTest(ids=ids), self.assertRaises(ValidationError):
                RawMLATReport(**base, source_event_ids=ids)
        RawMLATReport(**base, source_event_ids=[f"{i:064x}" for i in range(4)])

    def test_frozen_frontend_and_docs_match_secured_runtime(self):
        dist_path = Path("frontend/dist/app.js")
        if dist_path.exists():
            dist = dist_path.read_text()
            dist_html = Path("frontend/dist/index.html").read_text()
            self.assertIn("X-SkySecure-Operator-Key", dist)
            for claim in ("94.3%", "2.1%", "97%"):
                self.assertNotIn(claim, dist_html)
        running = Path("RUNNING.md").read_text()
        guide = Path("INTEGRATION_GUIDE.md").read_text()
        readme = Path("README.md").read_text()
        self.assertNotIn("L2/L3 aren't fused", running)
        self.assertNotIn("L4/L5 have no code path", running)
        self.assertNotIn("docker compose up -d", guide)
        six = "fusion-engine adsb-ingestor api mlat-solver anomaly-detector frontend"
        self.assertIn(f"docker compose build {six}", readme)
        self.assertIn(f"docker compose up -d --no-deps --force-recreate {six}", readme)
        env_path = Path(".env.example")
        if env_path.exists():
            env_example = env_path.read_text()
            self.assertIn("MLAT_RECEIVER_API_KEYS=", env_example)
            self.assertIn("MLAT_SOLVER_SIGNING_KEY=", env_example)
            self.assertEqual(env_example.count("replace-with-distinct-secret-"), 4)
        self.assertNotIn("95%", guide)

    def test_no_unbounded_redis_keys_or_unsupported_metrics(self):
        api = Path("api/main.py").read_text()
        frontend = Path("frontend/index.html").read_text()
        self.assertNotIn('redis_client.keys("sv:*', api)
        self.assertNotIn('redis_client.keys("ac:*', api)
        self.assertNotIn("scan_iter", api)
        ingestor = Path("ingestion/adsb_receiver.py").read_text()
        military = Path("military/classifier.py").read_text()
        fusion = Path("processing/fusion_engine.py").read_text()
        self.assertIn("MAX_PROVIDER_RECORDS", ingestor)
        self.assertIn("REDIS_PIPELINE_CHUNK", ingestor)
        self.assertNotIn('self.redis.keys("sv:*', military)
        self.assertIn("delivered_at IS NULL", fusion)
        self.assertIn("drain_fusion_outbox_periodically", fusion)
        self.assertNotIn("94.3%", frontend)
        self.assertNotIn("2.1%", frontend)

    async def test_scan_publishes_complete_bounded_cycle_snapshot(self):
        import api.main as api_main
        previous = api_main.redis_client
        fake = AsyncMock()
        fake.scan.side_effect = [(1, [b"a"]), (0, [b"b"]), (0, [b"b"])]
        api_main.redis_client = fake
        api_main._SCAN_CURSORS.clear()
        api_main._SCAN_SNAPSHOTS.clear()
        api_main._SCAN_BUILDING.clear()
        api_main._SCAN_LOCKS.clear()
        try:
            self.assertEqual(
                await api_main.scan_key_batch("sv:*", max_batches=1), [b"a"]
            )
            self.assertEqual(
                set(await api_main.scan_key_batch("sv:*", max_batches=1)),
                {b"a", b"b"},
            )
            self.assertEqual(
                await api_main.scan_key_batch("sv:*", max_batches=1), [b"b"]
            )
        finally:
            api_main.redis_client = previous

    async def test_fused_and_duplicate_redis_state_are_one_atomic_transaction(self):
        from processing.fusion_engine import FusionEngine
        redis = MagicMock()
        redis.setex = AsyncMock()
        pipeline = MagicMock()
        pipeline.setex.return_value = pipeline
        pipeline.execute = AsyncMock()
        redis.pipeline.return_value = pipeline
        engine = FusionEngine(redis)
        state = StateVector(icao24="ABC123", lat=40.0, lon=-75.0, last_seen=1000.0)
        await engine.save(
            state, AsyncMock(), event=self._reception(),
            duplicate_position=(40.0, -75.0, 1000.0),
        )
        redis.pipeline.assert_called_once_with(transaction=True)
        self.assertEqual(pipeline.setex.call_count, 2)
        pipeline.execute.assert_awaited_once()

    def test_covariance_scale_has_noise_floor_and_correct_sse_factor(self):
        from processing.mlat_solver import covariance_variance
        floor = covariance_variance(cost=0.0, residual_count=3, parameter_count=3, noise_ns=50.0)
        self.assertGreater(floor, 0.0)
        self.assertEqual(
            covariance_variance(cost=20.0, residual_count=5, parameter_count=3, noise_ns=0.0),
            20.0,
        )

    def test_database_bound_telemetry_is_bounded(self):
        base = dict(
            receiver_id="receiver-london", recv_time=1000.0,
            icao24="ABC123", raw_message="8DABC12358C382D690C8AC2863A7", msg_type=17,
        )
        for field, value in (
            ("callsign", "TOO-LONG-9"),
            ("altitude_baro", 2**31),
            ("altitude_geo", -(2**31)-1),
            ("vertical_rate", 2**31),
        ):
            with self.subTest(field=field), self.assertRaises(ValidationError):
                RawADSBMessage(**base, **{field: value})
        with self.assertRaises(ValidationError):
            RawMLATReport(
                session_id="s", solve_time=1000.0, icao24="ABC123",
                lat=40.0, lon=-75.0, altitude_baro=2**31,
                num_receivers=4, tdoa_residual=10.0, cep90=100.0,
                receiver_ids=["a", "b", "c", "d"],
            )


if __name__ == "__main__":
    unittest.main()
