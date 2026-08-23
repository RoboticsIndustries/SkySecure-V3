import asyncio
import math
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import orjson
from pydantic import ValidationError
from fastapi import HTTPException

import api.main as api_main
from anomaly.detector import AnomalyDetector, run
from api.main import (
    _L1_CACHE,
    _complete_alert_effect,
    _l1_cache_is_fresh,
    _reserve_alert_effect,
    _validate_raw_l1_claim,
    alert_consumer_loop,
    lifespan,
    run_l1_cross_validation,
    validate_position_l1,
)
from models import (
    DataSource,
    DetectionLayer,
    LayerEvaluation,
    RawADSBMessage,
    RawMLATReport,
    RiskBand,
    SourceReport,
    StateVector,
)
from processing.cross_source_validator import (
    CrossValidationResult,
    DISAGREEMENT_SPOOFED_M,
    DISAGREEMENT_UNCERTAIN_M,
    SourceReport as ValidationSourceReport,
)
from processing.fusion_engine import FusionEngine
from processing.cross_source_validator import CrossSourceValidator


class L1CacheMalformedEntryRegressionTests(unittest.TestCase):
    def setUp(self):
        _L1_CACHE.clear()
        self.aircraft = {
            "icao": "ABC123",
            "src": "adsb_lol",
            "lat": 39.95,
            "lon": -75.16,
        }
        self.key = ("ABC123", "adsb_lol")
        self.result = {"max_disagreement_m": 100.0}

    def tearDown(self):
        _L1_CACHE.clear()

    def test_bad_timestamp_invalidates_without_throwing(self):
        for timestamp in ("not-a-timestamp", None, math.nan, math.inf):
            with self.subTest(timestamp=timestamp):
                _L1_CACHE[self.key] = (
                    timestamp, self.result, self.aircraft["lat"], self.aircraft["lon"]
                )
                self.assertFalse(_l1_cache_is_fresh(self.aircraft, 101.0))

    def test_short_tuple_invalidates_without_throwing(self):
        for cached in ((), (100.0,), (100.0, self.result), (100.0, self.result, 39.95)):
            with self.subTest(cached=cached):
                _L1_CACHE[self.key] = cached
                self.assertFalse(_l1_cache_is_fresh(self.aircraft, 101.0))

    def test_malformed_current_coordinates_invalidate_without_throwing(self):
        cases = (
            {"lat": None},
            {"lon": None},
            {"lat": "bad"},
            {"lon": "bad"},
            {"lat": math.nan},
            {"lon": math.inf},
        )
        _L1_CACHE[self.key] = (100.0, self.result, 39.95, -75.16)
        for replacement in cases:
            with self.subTest(replacement=replacement):
                aircraft = dict(self.aircraft)
                aircraft.update(replacement)
                self.assertFalse(_l1_cache_is_fresh(aircraft, 101.0))

    def test_missing_current_coordinates_invalidate_without_throwing(self):
        _L1_CACHE[self.key] = (100.0, self.result, 39.95, -75.16)
        for field in ("lat", "lon"):
            with self.subTest(field=field):
                aircraft = dict(self.aircraft)
                aircraft.pop(field)
                self.assertFalse(_l1_cache_is_fresh(aircraft, 101.0))

    def test_malformed_cached_coordinates_invalidate_without_throwing(self):
        for cached_lat, cached_lon in (
            (None, -75.16),
            (39.95, None),
            ("bad", -75.16),
            (39.95, "bad"),
            (math.nan, -75.16),
            (39.95, math.inf),
        ):
            with self.subTest(cached_lat=cached_lat, cached_lon=cached_lon):
                _L1_CACHE[self.key] = (
                    100.0, self.result, cached_lat, cached_lon
                )
                self.assertFalse(_l1_cache_is_fresh(self.aircraft, 101.0))


class RawL1ClaimTrustBoundaryRegressionTests(unittest.IsolatedAsyncioTestCase):
    def test_operator_only_endpoints_have_server_side_dependencies(self):
        protected = {
            "/api/coverage", "/api/l1/validate", "/api/mlat/receptions",
        }
        routes = {
            route.path: route for route in api_main.app.routes
            if getattr(route, "path", None) in protected
            and ({"PUT", "POST"} & set(getattr(route, "methods", set())))
        }
        self.assertEqual(set(routes), protected)
        for path in ("/api/coverage", "/api/l1/validate"):
            self.assertIn(
                api_main.require_operator,
                [dependency.call for dependency in routes[path].dependant.dependencies],
            )
        receiver_auth = getattr(api_main, "require_mlat_receiver", None)
        self.assertTrue(callable(receiver_auth))
        self.assertIn(
            receiver_auth,
            [dependency.call for dependency in routes["/api/mlat/receptions"].dependant.dependencies],
        )
        self.assertTrue(hasattr(api_main.settings, "OPERATOR_API_KEY"))
        self.assertTrue(hasattr(api_main.settings, "MLAT_RECEIVER_API_KEYS"))
        self.assertIn("l1:manual:rate", Path(api_main.__file__).read_text())

    def test_deployed_mlat_uses_dedicated_physical_reception_topic(self):
        solver = (Path(__file__).parent / "processing" / "mlat_solver.py").read_text()
        kafka_init = (Path(__file__).parent / "scripts" / "init-kafka.sh").read_text()
        compose = (Path(__file__).parent / "docker-compose.yml").read_text()
        self.assertTrue(hasattr(api_main.settings, "TOPIC_MLAT_RECEPTIONS"))
        topic = api_main.settings.TOPIC_MLAT_RECEPTIONS
        self.assertIn("settings.TOPIC_MLAT_RECEPTIONS", solver)
        self.assertIn(topic, kafka_init)
        self.assertGreaterEqual(compose.count("MLAT_RECEIVER_LOCATIONS"), 3)
        self.assertIn("kafka_msg.key != physical_reception_key(", solver)

    def test_existing_postgres_volume_has_idempotent_event_ledger_migration(self):
        migration = Path("scripts/migrations/001_fusion_event_commits.sql")
        self.assertTrue(migration.is_file())
        source = migration.read_text()
        self.assertIn("BEGIN;", source)
        self.assertIn("SET LOCAL lock_timeout", source)
        self.assertIn("CREATE TABLE IF NOT EXISTS fusion_event_commits", source)
        self.assertIn("event_id", source)
        self.assertIn("PRIMARY KEY", source)
        self.assertIn("COMMIT;", source)
        compose = Path("docker-compose.yml").read_text()
        fusion = Path("processing/fusion_engine.py").read_text()
        self.assertIn("postgres-migrate:", compose)
        self.assertIn("condition: service_completed_successfully", compose)
        self.assertIn("001_fusion_event_commits.sql", compose)
        self.assertNotIn("CREATE TABLE IF NOT EXISTS fusion_event_commits", fusion)

    async def test_physical_reception_intake_validates_and_acknowledges_before_readiness(self):
        producer = AsyncMock()
        redis = AsyncMock()
        effects = []

        async def kafka_ack(**kwargs):
            effects.append(("kafka", kwargs))

        async def heartbeat(*args, **kwargs):
            effects.append(("redis", args, kwargs))

        producer.send_and_wait.side_effect = kafka_ack
        redis.setex.side_effect = heartbeat
        previous_producer = getattr(api_main, "mlat_reception_producer", None)
        previous_redis = api_main.redis_client
        previous_keys = api_main.settings.MLAT_RECEIVER_API_KEYS
        api_main.settings.MLAT_RECEIVER_API_KEYS = {
            receiver_id: f"test-secret-{index}"
            for index, receiver_id in enumerate(
                api_main.settings.MLAT_RECEIVER_LOCATIONS, start=1
            )
        }
        api_main.mlat_reception_producer = producer
        api_main.redis_client = redis
        canonical = "8DABC12358C382D690C8AC2863A7"
        try:
            for receiver_id, recv_time in (
                ("unknown", time.time()),
                ("receiver-london", time.time() - 301.0),
            ):
                with self.subTest(receiver_id=receiver_id, recv_time=recv_time):
                    with self.assertRaises(HTTPException) as rejected:
                        await api_main.ingest_mlat_reception(
                            RawADSBMessage(
                                receiver_id=receiver_id, recv_time=recv_time,
                                icao24="ABC123", raw_message=canonical, msg_type=17,
                            ),
                            authenticated_receiver_id="receiver-london",
                        )
                    self.assertIn(rejected.exception.status_code, (403, 422))
            for raw_message, msg_type in (
                ("", 17), ("8DABC123", 17), (canonical, 5),
            ):
                with self.subTest(raw_message=raw_message, msg_type=msg_type):
                    with self.assertRaises(HTTPException) as rejected:
                        await api_main.ingest_mlat_reception(
                            RawADSBMessage(
                                receiver_id="receiver-london", recv_time=time.time(),
                                icao24="ABC123", raw_message=raw_message,
                                msg_type=msg_type,
                            ),
                            authenticated_receiver_id="receiver-london",
                        )
                    self.assertEqual(rejected.exception.status_code, 422)
            producer.send_and_wait.assert_not_awaited()
            redis.setex.assert_not_awaited()

            reception = RawADSBMessage(
                receiver_id="receiver-london", recv_time=time.time(),
                icao24="ABC123", raw_message=canonical, msg_type=17,
            )
            with self.assertRaises(HTTPException) as impersonation:
                await api_main.ingest_mlat_reception(
                    reception, authenticated_receiver_id="receiver-paris"
                )
            self.assertEqual(impersonation.exception.status_code, 403)
            result = await api_main.ingest_mlat_reception(
                reception, authenticated_receiver_id="receiver-london"
            )
            self.assertEqual(result, {"status": "accepted"})
            self.assertEqual([effect[0] for effect in effects], ["kafka", "redis"])
            self.assertEqual(
                producer.send_and_wait.await_args.kwargs["topic"],
                api_main.settings.TOPIC_MLAT_RECEPTIONS,
            )
            sent = producer.send_and_wait.await_args.kwargs
            self.assertEqual(
                sent["key"],
                api_main.physical_reception_key(
                    reception,
                    api_main.settings.MLAT_RECEIVER_API_KEYS["receiver-london"],
                ),
            )
        finally:
            api_main.mlat_reception_producer = previous_producer
            api_main.redis_client = previous_redis
            api_main.settings.MLAT_RECEIVER_API_KEYS = previous_keys

    async def test_operator_authorization_fails_closed_and_compares_secret(self):
        previous = api_main.settings.OPERATOR_API_KEY
        try:
            api_main.settings.OPERATOR_API_KEY = ""
            with self.assertRaises(HTTPException) as unconfigured:
                await api_main.require_operator(None)
            self.assertEqual(unconfigured.exception.status_code, 503)
            api_main.settings.OPERATOR_API_KEY = "expected-secret"
            with self.assertRaises(HTTPException) as denied:
                await api_main.require_operator("wrong-secret")
            self.assertEqual(denied.exception.status_code, 403)
            self.assertIsNone(await api_main.require_operator("expected-secret"))
        finally:
            api_main.settings.OPERATOR_API_KEY = previous

    async def test_receiver_credentials_are_complete_distinct_and_identity_bound(self):
        previous = api_main.settings.MLAT_RECEIVER_API_KEYS
        receiver_ids = list(api_main.settings.MLAT_RECEIVER_LOCATIONS)
        try:
            api_main.settings.MLAT_RECEIVER_API_KEYS = {}
            with self.assertRaises(HTTPException) as absent:
                await api_main.require_mlat_receiver("key")
            self.assertEqual(absent.exception.status_code, 503)

            api_main.settings.MLAT_RECEIVER_API_KEYS = {
                receiver_id: "shared" for receiver_id in receiver_ids
            }
            with self.assertRaises(HTTPException) as duplicate:
                await api_main.require_mlat_receiver("shared")
            self.assertEqual(duplicate.exception.status_code, 503)

            api_main.settings.MLAT_RECEIVER_API_KEYS = {
                receiver_id: f"secret-{index}"
                for index, receiver_id in enumerate(receiver_ids)
            }
            self.assertEqual(
                await api_main.require_mlat_receiver("secret-0"), receiver_ids[0]
            )
        finally:
            api_main.settings.MLAT_RECEIVER_API_KEYS = previous

    def test_redis_track_decoder_rejects_namespace_fallback_and_key_mismatch(self):
        decoder = getattr(api_main, "_decode_redis_track", None)
        self.assertTrue(callable(decoder))
        assert decoder is not None
        state = StateVector(
            icao24="ABC123", lat=39.95, lon=-75.16,
            altitude_baro=12000, velocity=400, heading=90, last_seen=100.0,
        )
        self.assertIsNone(decoder("ac", "ac:ABC123", state.to_bytes()))
        self.assertIsNone(decoder("sv", "sv:DEF456", state.to_bytes()))
        self.assertIsNone(decoder("sv", "ac:ABC123", state.to_bytes()))
        decoded = decoder("sv", "sv:ABC123", state.to_bytes())
        self.assertEqual(decoded[1]["icao"], "ABC123")

    def test_raw_claim_decoder_normalizes_untrusted_risk_and_timestamp(self):
        decoder = getattr(api_main, "_decode_redis_track", None)
        self.assertTrue(callable(decoder))
        assert decoder is not None
        raw = orjson.dumps({
            "icao": "ABC123", "src": "adsb_lol", "lat": 39.95,
            "lon": -75.16, "risk": {"not": "sortable"}, "ts": math.nan,
            "anoms": 42,
        })
        decoded = decoder("ac", "ac:ABC123", raw)
        self.assertEqual(decoded[1]["risk"], 0.0)
        self.assertNotIn("ts", decoded[1])
        self.assertEqual(decoded[1]["anoms"], [])

        raw_with_anomalies = orjson.dumps({
            "icao": "ABC123", "src": "adsb_lol", "lat": 39.95,
            "lon": -75.16,
            "anoms": [
                "L1_POSITION_DISAGREEMENT",
                {"type": "L2_SPEED_ANOMALY"},
                "<img src=x onerror=alert(1)>",
                {"type": "<svg/onload=alert(1)>"},
                "X" * 200,
            ],
        })
        decoded = decoder("ac", "ac:ABC123", raw_with_anomalies)
        self.assertEqual(
            decoded[1]["anoms"],
            ["L1_POSITION_DISAGREEMENT", "L2_SPEED_ANOMALY"],
        )

        hostile_public_fields = orjson.dumps({
            "icao": "ABC123", "src": "adsb_lol", "lat": 39.95,
            "lon": -75.16, "cs": "<img onerror=alert(1)>",
            "cls": "<svg/onload=alert(1)>", "alt": "<script>x</script>",
            "vel": "fast", "hdg": float("nan"), "vr": {},
            "conf": 9, "mil": -4, "band": "<b>bad</b>", "trail": ["bad"],
        })
        decoded = decoder("ac", "ac:ABC123", hostile_public_fields)
        self.assertIsNone(decoded[1]["cs"])
        self.assertEqual(decoded[1]["cls"], "UNKNOWN")
        self.assertIsNone(decoded[1]["alt"])
        self.assertIsNone(decoded[1]["vel"])
        self.assertIsNone(decoded[1]["hdg"])
        self.assertIsNone(decoded[1]["vr"])
        self.assertEqual(decoded[1]["conf"], 1.0)
        self.assertEqual(decoded[1]["mil"], 0.0)
        self.assertEqual(decoded[1]["band"], "NORMAL")
        self.assertEqual(decoded[1]["trail"], [])

    def test_canonical_models_reject_non_hex_or_namespace_icao(self):
        cases = (
            (StateVector, {"icao24": "BAD:*:ID"}),
            (RawADSBMessage, {
                "receiver_id": "r", "recv_time": 1.0, "icao24": "ZZZZZZ",
                "raw_message": "", "msg_type": 17,
            }),
            (RawMLATReport, {
                "session_id": "s", "solve_time": 1.0, "icao24": "ABC*23",
                "lat": 0.0, "lon": 0.0, "altitude_baro": 1,
                "num_receivers": 2, "tdoa_residual": 1.0, "cep90": 1.0,
            }),
        )
        for model, kwargs in cases:
            with self.subTest(model=model.__name__), self.assertRaises(ValidationError):
                model(**kwargs)

    async def test_manual_l1_rejects_namespace_icao_before_validation(self):
        validator = AsyncMock()
        with (
            patch("api.main.TDOA_AVAILABLE", True),
            patch("api.main.cross_validator", validator),
            self.assertRaises(HTTPException) as raised,
        ):
            await validate_position_l1("BAD:*", 39.95, -75.16)
        self.assertEqual(raised.exception.status_code, 422)
        validator.validate_aircraft.assert_not_awaited()

    def test_timestamp_less_raw_api_observation_is_not_assessed(self):
        detector = MagicMock()
        with patch("api.main.anomaly_detector", detector):
            api_main.run_l2_l3_detection([{
                "icao": "ABC123", "lat": 39.95, "lon": -75.16,
                "src": "adsb_lol", "risk": 0, "anoms": [],
            }])
        detector.assess.assert_not_called()

    async def test_layer_loader_rejects_key_payload_identity_mismatch(self):
        class Pipe:
            def get(self, key): return self
            async def execute(self):
                return [StateVector(icao24="DEF456").to_bytes()]
        class Redis:
            async def scan(self, *args, **kwargs):
                return 0, [b"sv:ABC123"]
            def pipeline(self): return Pipe()
        api_main._layer_vector_cache.update(client_id=None, ts=0, vectors=[])
        with patch("api.main.redis_client", Redis()):
            self.assertEqual(await api_main._load_state_vectors(), [])

    async def test_fusion_rejects_key_payload_identity_mismatch(self):
        redis = SimpleNamespace(get=AsyncMock(
            return_value=StateVector(icao24="DEF456").to_bytes()
        ))
        sv = await FusionEngine(redis)._load_or_create("ABC123")
        self.assertEqual(sv.icao24, "ABC123")

    async def test_fusion_postgres_failure_propagates_for_source_retry(self):
        redis = SimpleNamespace(setex=AsyncMock())
        postgres = SimpleNamespace(execute=AsyncMock(side_effect=OSError("db down")))
        producer = SimpleNamespace(send_and_wait=AsyncMock())
        with self.assertRaises(OSError):
            await FusionEngine(redis, postgres).save(
                StateVector(icao24="ABC123", last_seen=100.0), producer,
                event=RawADSBMessage(
                    receiver_id="feed", recv_time=100.0, icao24="ABC123",
                    raw_message="8DABC123", msg_type=17,
                ),
            )
        producer.send_and_wait.assert_not_awaited()
        redis.setex.assert_not_awaited()

    async def test_distinct_delayed_events_use_distinct_postgres_identities(self):
        postgres = SimpleNamespace(
            execute=AsyncMock(return_value="UPDATE 1"),
            fetchrow=AsyncMock(return_value={
                "topic": "fused.tracks", "message_key": b"ABC123", "payload": b"{}"
            }),
        )
        redis = SimpleNamespace(setex=AsyncMock())
        producer = SimpleNamespace(send_and_wait=AsyncMock())
        engine = FusionEngine(redis, postgres)
        state = StateVector(icao24="ABC123", last_seen=200.0)

        event_a = RawADSBMessage(
            receiver_id="feed", recv_time=100.0, icao24="ABC123",
            raw_message="8DABC123A", msg_type=17, lat=40.0, lon=-75.0,
        )
        event_b = RawADSBMessage(
            receiver_id="feed", recv_time=101.0, icao24="ABC123",
            raw_message="8DABC123B", msg_type=17, lat=40.1, lon=-75.1,
        )
        await engine.save(state, producer, event=event_a)
        await engine.save(state, producer, event=event_b)

        claims = [
            call for call in postgres.execute.await_args_list
            if "fusion_event_commits" in call.args[0]
        ]
        first, second = claims
        self.assertIn("fusion_event_commits", first.args[0])
        self.assertIn("ON CONFLICT", first.args[0])
        self.assertNotEqual(first.args[1], second.args[1])
        self.assertEqual(first.args[2:5], (100.0, "ABC123", "ADSB"))
        self.assertEqual(second.args[2:5], (101.0, "ABC123", "ADSB"))

    async def test_detector_hydration_rejects_key_payload_identity_mismatch(self):
        detector = AnomalyDetector()
        prior = StateVector(icao24="DEF456")
        prior.layer_evaluations[DetectionLayer.L2.value] = LayerEvaluation(
            layer=DetectionLayer.L2, detectors_evaluated=["velocity_baseline"]
        )
        redis = SimpleNamespace(get=AsyncMock(return_value=prior.to_bytes()))
        await detector.hydrate_last_result(redis, "ABC123")
        self.assertNotIn("ABC123", detector._last_results)

    def test_canonical_models_reject_nonfinite_numbers(self):
        cases = [
            lambda: RawADSBMessage(receiver_id="r", recv_time=math.inf,
                icao24="ABC123", raw_message="00" * 7, msg_type=17),
            lambda: RawMLATReport(session_id="s", solve_time=math.nan,
                icao24="ABC123", lat=1, lon=1, altitude_baro=1,
                num_receivers=2, tdoa_residual=1, cep90=1),
            lambda: SourceReport(source=DataSource.ADSB, timestamp=math.inf),
            lambda: StateVector(icao24="ABC123", last_seen=math.inf),
            lambda: StateVector(icao24="ABC123", lat=math.nan),
            lambda: StateVector(icao24="ABC123", last_update_timestamp=math.inf),
            lambda: StateVector(icao24="ABC123", position_history=[{"lat": math.nan}]),
        ]
        for build in cases:
            with self.subTest(build=build):
                with self.assertRaises(Exception):
                    build()

    def test_l1_nonfinite_source_coordinates_never_become_legitimate(self):
        validator = CrossSourceValidator()
        result = validator.validate_reports(
            "ABC123",
            [ValidationSourceReport(
                source="opensky", icao="ABC123", lat=math.nan, lon=-75.16,
                alt_ft=None, velocity_kts=None, heading_deg=None, observed_at=100.0,
            )],
            claimed_lat=39.95, claimed_lon=-75.16, claimed_observed_at=100.0,
        )
        self.assertNotEqual(result.verdict, "LEGITIMATE")

    def test_fusion_persistence_is_db_first_and_replay_idempotent(self):
        source = (Path(__file__).parent / "processing" / "fusion_engine.py").read_text()
        save = source[source.index("    async def save("):source.index(
            "    async def _lookup_icao_by_registration", source.index("    async def save(")
        )]
        self.assertLess(save.index("postgres.execute"), save.index("producer.send_and_wait"))
        self.assertIn("fusion_event_commits", save)
        self.assertIn("ON CONFLICT (event_id) DO NOTHING", save)

    def test_ingestion_waits_for_kafka_ack_before_redis_visibility(self):
        source = (Path(__file__).parent / "ingestion" / "adsb_receiver.py").read_text()
        self.assertIn("producer.send_and_wait(", source)
        self.assertNotIn("producer.send(\n", source)

    async def test_cross_source_validator_rejects_missing_event_time(self):
        class Response:
            status = 200
            def __init__(self, payload): self.payload = payload
            async def __aenter__(self): return self
            async def __aexit__(self, *args): return False
            async def json(self, **kwargs): return self.payload
        class Session:
            def __init__(self, payload): self.payload = payload
            def get(self, *args, **kwargs): return Response(self.payload)
        opensky_state = ["ABC123", None, None, None, None, -75.16, 39.95] + [None] * 10
        validator = CrossSourceValidator(Session({"time": 999, "states": [opensky_state]}))
        self.assertIsNone(await validator._fetch_opensky("ABC123"))
        validator = CrossSourceValidator(Session({"ac": [{
            "hex": "ABC123", "lat": 39.95, "lon": -75.16,
        }]}))
        self.assertIsNone(await validator._fetch_point_source(
            "adsb_lol", "ABC123", 39.95, -75.16
        ))

    def test_mlat_loop_has_explicit_poison_record_commit_path(self):
        source = (Path(__file__).parent / "processing" / "mlat_solver.py").read_text()
        self.assertIn("Discarding malformed MLAT input record", source)
        self.assertIn("await commit_record(consumer, kafka_msg)\n                    continue", source)

    async def test_alert_publish_raises_when_all_connected_clients_fail(self):
        class Lock:
            async def __aenter__(self): return self
            async def __aexit__(self, *args): return False
            async def extend(self, *args, **kwargs): return True
        redis = SimpleNamespace(lock=lambda *args, **kwargs: Lock())
        ws = MagicMock()
        ws.send_bytes = AsyncMock(side_effect=OSError("closed"))
        api_main._ws_clients.clear()
        api_main._ws_clients.add(ws)
        try:
            with (
                patch("api.main.redis_client", redis),
                patch("api.main.load_coverage_area", AsyncMock(return_value={})),
                patch("api.main._track_in_coverage", return_value=True),
            ):
                with self.assertRaises(RuntimeError):
                    await api_main._publish_alert({"icao": "ABC123"}, [])
        finally:
            api_main._ws_clients.clear()

    async def test_l1_cache_is_bound_to_projection_inputs(self):
        for changed_field, changed_value in (
            ("ts", 101.0), ("vel", 450.0), ("hdg", 91.0),
        ):
            with self.subTest(changed_field=changed_field):
                validator = AsyncMock()
                validator.validate_aircraft.return_value = CrossValidationResult(
                    icao="ABC123", is_valid=True, max_disagreement_m=0.0,
                    confidence=0.9, sources_used=["adsb_fi"], verdict="LEGITIMATE",
                )
                first = {
                    "icao": "ABC123", "src": "adsb_lol", "lat": 39.95,
                    "lon": -75.16, "ts": 100.0, "vel": 400.0, "hdg": 90.0,
                }
                changed = dict(first)
                changed[changed_field] = changed_value
                _L1_CACHE.clear()
                with patch("api.main.cross_validator", validator):
                    await run_l1_cross_validation([first])
                    await run_l1_cross_validation([changed])
                self.assertEqual(validator.validate_aircraft.await_count, 2)
    def test_broadcast_raw_claim_must_match_full_redis_key_and_trust_boundary(self):
        validator = getattr(api_main, "_raw_claim_matches_redis_key", None)
        self.assertTrue(callable(validator))
        assert validator is not None
        valid = {"icao": "ABC123", "src": "adsb_lol", "lat": 39.95, "lon": -75.16}
        self.assertTrue(validator("ac:ABC123", valid))
        for key, replacement in (
            ("ac:DEF456", {}),
            ("ac:ABC12", {}),
            ("ac:ABC123", {"src": "ADSB"}),
            ("ac:ABC123", {"lat": math.nan}),
            ("ac:ABC123", {"lon": 181.0}),
        ):
            with self.subTest(key=key, replacement=replacement):
                claim = dict(valid)
                claim.update(replacement)
                self.assertFalse(validator(key, claim))

    async def _assert_rejected(self, claim):
        redis = AsyncMock()
        redis.get.return_value = orjson.dumps(claim)
        validator = AsyncMock()
        cross_validate = AsyncMock()

        with (
            patch("api.main.redis_client", redis),
            patch("api.main.cross_validator", validator),
            patch("api.main.run_l1_cross_validation", cross_validate),
        ):
            result = await _validate_raw_l1_claim("ABC123")

        self.assertIsNone(result)
        self.assertNotIn("l1", claim)
        cross_validate.assert_not_awaited()
        validator.validate_aircraft.assert_not_awaited()

    async def test_raw_claim_icao_must_normalize_to_requested_cache_key(self):
        await self._assert_rejected({
            "icao": "DEF456", "src": "adsb_lol", "lat": 39.95, "lon": -75.16,
        })

    async def test_raw_claim_icao_must_be_exactly_six_hex_characters(self):
        for icao in ("ABC12", "ABC1234", "GHI789", "ABC-12", ""):
            with self.subTest(icao=icao):
                await self._assert_rejected({
                    "icao": icao, "src": "adsb_lol", "lat": 39.95, "lon": -75.16,
                })

    async def test_raw_claim_source_must_be_supported(self):
        await self._assert_rejected({
            "icao": "abc123", "src": "ADSB", "lat": 39.95, "lon": -75.16,
        })

    async def test_raw_claim_coordinates_must_be_present_finite_and_in_range(self):
        cases = (
            {"missing": "lat"}, {"missing": "lon"},
            {"lat": None}, {"lon": None}, {"lat": math.nan},
            {"lon": math.inf}, {"lat": 90.01}, {"lat": -90.01},
            {"lon": 180.01}, {"lon": -180.01},
        )
        for replacement in cases:
            with self.subTest(replacement=replacement):
                claim = {
                    "icao": "ABC123", "src": "adsb_lol", "lat": 39.95, "lon": -75.16,
                }
                missing = replacement.get("missing")
                if missing:
                    claim.pop(missing)
                else:
                    claim.update(replacement)
                await self._assert_rejected(claim)


class L1CacheRawDisagreementRegressionTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _cache_entry(*, icao="ABC123", raw=100.04, display=None,
                     verdict="LEGITIMATE", is_valid=True):
        if display is None:
            display = round(raw, 1)
        return (
            100.0,
            {
                "icao": icao,
                "max_disagreement_m": display,
                "confidence": 0.9,
                "is_valid": is_valid,
                "verdict": verdict,
                "sources_used": ["adsb_fi"],
            },
            0.0,
            0.0,
            raw,
        )

    def tearDown(self):
        _L1_CACHE.clear()

    def test_semantically_corrupt_cache_entries_are_invalidated(self):
        aircraft = {"icao": "ABC123", "src": "adsb_lol", "lat": 0.0, "lon": 0.0}
        corrupt_entries = {
            "result ICAO differs from cache key": self._cache_entry(icao="DEF456"),
            "unsupported verdict": self._cache_entry(verdict="VALIDATED"),
            "is_valid disagrees with verdict": self._cache_entry(is_valid=False),
            "display is not rounded raw value": self._cache_entry(display=100.2),
            "verdict disagrees with 1500m threshold": self._cache_entry(
                raw=1500.0, display=1500.0, verdict="LEGITIMATE", is_valid=True,
            ),
            "verdict disagrees with 5000m threshold": self._cache_entry(
                raw=5000.0, display=5000.0, verdict="UNCERTAIN", is_valid=False,
            ),
        }
        timestamp, result, lat, lon, raw = self._cache_entry()
        self_corroborating_result = dict(result)
        self_corroborating_result["sources_used"] = ["adsb_lol"]
        corrupt_entries["claim source corroborates itself"] = (
            timestamp, self_corroborating_result, lat, lon, raw,
        )
        for label, entry in corrupt_entries.items():
            with self.subTest(label=label):
                key = ("ABC123", "adsb_lol")
                _L1_CACHE[key] = entry
                self.assertFalse(_l1_cache_is_fresh(aircraft, 101.0))
                self.assertNotIn(key, _L1_CACHE)

    async def test_fresh_semantically_inconsistent_validator_results_are_not_applied_or_cached(self):
        aircraft = [{
            "icao": "ABC123", "src": "adsb_lol", "lat": 0.0, "lon": 0.0,
        }]
        corrupt_results = {
            "wrong ICAO": CrossValidationResult(
                icao="DEF456", is_valid=True, max_disagreement_m=100.0,
                confidence=0.9, sources_used=["adsb_fi"], verdict="LEGITIMATE",
            ),
            "unknown verdict": CrossValidationResult(
                icao="ABC123", is_valid=False, max_disagreement_m=100.0,
                confidence=0.9, sources_used=["adsb_fi"], verdict="VALIDATED",
            ),
            "is_valid mismatch": CrossValidationResult(
                icao="ABC123", is_valid=False, max_disagreement_m=100.0,
                confidence=0.9, sources_used=["adsb_fi"], verdict="LEGITIMATE",
            ),
            "threshold mismatch": CrossValidationResult(
                icao="ABC123", is_valid=False, max_disagreement_m=5000.0,
                confidence=0.9, sources_used=["adsb_fi"], verdict="UNCERTAIN",
            ),
            "display/raw mismatch": SimpleNamespace(
                icao="ABC123", max_disagreement_m=100.04,
                to_dict=lambda: {
                    "icao": "ABC123", "is_valid": True,
                    "max_disagreement_m": 100.2, "confidence": 0.9,
                    "sources_used": ["adsb_fi"], "verdict": "LEGITIMATE",
                },
            ),
        }
        validator = AsyncMock()
        for label, result in corrupt_results.items():
            with self.subTest(label=label):
                _L1_CACHE.clear()
                aircraft[0].pop("l1", None)
                validator.validate_aircraft.reset_mock()
                validator.validate_aircraft.return_value = result
                with patch("api.main.cross_validator", validator), patch(
                    "api.main.time.time", return_value=100.0
                ):
                    await run_l1_cross_validation(aircraft)
                self.assertNotIn("l1", aircraft[0])
                self.assertNotIn(("ABC123", "adsb_lol"), _L1_CACHE)

    async def test_cache_sweep_discards_bad_timestamp_without_throwing(self):
        aircraft = [{
            "icao": "ABC123", "src": "adsb_lol", "lat": 0.0, "lon": 0.0
        }]
        _L1_CACHE[("ABC123", "adsb_lol")] = (
            "bad-timestamp", {"max_disagreement_m": 100.0}, 0.0, 0.0
        )
        validator = AsyncMock()
        validator.validate_aircraft.return_value = CrossValidationResult(
            icao="ABC123",
            is_valid=True,
            max_disagreement_m=100.0,
            confidence=0.9,
            sources_used=["adsb_fi"],
            verdict="LEGITIMATE",
        )

        with patch("api.main.cross_validator", validator), patch("api.main.time.time", return_value=100.0):
            await run_l1_cross_validation(aircraft)

        validator.validate_aircraft.assert_awaited_once()
        self.assertIn("l1", aircraft[0])

    async def test_cache_entry_preserves_unrounded_disagreement_for_safety(self):
        raw_disagreement = DISAGREEMENT_UNCERTAIN_M - 0.04
        validator = AsyncMock()
        validator.validate_aircraft.return_value = CrossValidationResult(
            icao="ABC123",
            is_valid=True,
            max_disagreement_m=raw_disagreement,
            confidence=0.9,
            sources_used=["adsb_fi"],
            verdict="LEGITIMATE",
        )
        aircraft = [{
            "icao": "ABC123", "src": "adsb_lol", "lat": 0.0, "lon": 0.0
        }]
        _L1_CACHE.clear()

        with patch("api.main.cross_validator", validator), patch("api.main.time.time", return_value=100.0):
            await run_l1_cross_validation(aircraft)

        cached = _L1_CACHE[("ABC123", "adsb_lol")]
        self.assertEqual(cached[1]["max_disagreement_m"], DISAGREEMENT_UNCERTAIN_M)
        self.assertEqual(cached[4], raw_disagreement)

    def test_cache_safety_uses_raw_not_display_disagreement_at_both_boundaries(self):
        for boundary in (DISAGREEMENT_UNCERTAIN_M, DISAGREEMENT_SPOOFED_M):
            with self.subTest(boundary=boundary):
                raw_disagreement = boundary - 0.04
                movement_m = 0.01
                moved_lat = math.degrees(movement_m / 6_371_000)
                aircraft = {
                    "icao": "ABC123",
                    "src": "adsb_lol",
                    "lat": moved_lat,
                    "lon": 0.0,
                }
                # Wished-for entry API: preserve the exact disagreement as the
                # fifth field while retaining the rounded result for display.
                _L1_CACHE.clear()
                _L1_CACHE[("ABC123", "adsb_lol")] = (
                    100.0,
                    {
                        "icao": "ABC123",
                        "max_disagreement_m": round(raw_disagreement, 1),
                        "confidence": 0.9,
                        "is_valid": boundary == DISAGREEMENT_UNCERTAIN_M,
                        "verdict": (
                            "LEGITIMATE"
                            if boundary == DISAGREEMENT_UNCERTAIN_M
                            else "UNCERTAIN"
                        ),
                        "sources_used": ["adsb_fi"],
                    },
                    0.0,
                    0.0,
                    raw_disagreement,
                )
                self.assertTrue(_l1_cache_is_fresh(aircraft, 101.0))


class _MemoryOutboxRedis:
    def __init__(self, store=None, scan_pages=None):
        self.store = dict(store or {})
        self.scan_pages = dict(scan_pages or {})
        self.scan_cursors = []
        self.now = 0.0
        self.expiries = {}

    def _expire(self, key):
        deadline = self.expiries.get(key)
        if deadline is not None and deadline <= self.now:
            self.store.pop(key, None)
            self.expiries.pop(key, None)

    def advance(self, seconds):
        self.now += seconds
        for key in list(self.expiries):
            self._expire(key)

    async def scan(self, *, cursor, match, count):
        self.scan_cursors.append(cursor)
        return self.scan_pages.get(cursor, (0, []))

    async def set(self, key, value, *, nx=False, ex=None):
        self._expire(key)
        if nx and key in self.store:
            return False
        self.store[key] = value
        if ex is not None:
            self.expiries[key] = self.now + float(ex)
        else:
            self.expiries.pop(key, None)
        return True

    async def get(self, key):
        self._expire(key)
        return self.store.get(key)

    async def eval(self, script, numkeys, *args):
        if numkeys == 1:
            if "EXPIRE" in script:
                claim_key, token, ttl = args
                if await self.get(claim_key) == token:
                    self.expiries[claim_key] = self.now + float(ttl)
                    return 1
                return 0
            claim_key, token = args
            if await self.get(claim_key) == token:
                self.store.pop(claim_key, None)
                self.expiries.pop(claim_key, None)
                return 1
            return 0
        key, claim_key, first_arg, second_arg = args
        key_text = key.decode() if isinstance(key, bytes) else str(key)
        if key_text.startswith("completed:api-alert:"):
            if "NX" in script:
                token, ttl = first_arg, second_arg
                if await self.get(key) is not None:
                    return 2
                if await self.get(claim_key) is not None:
                    return 0
                await self.set(claim_key, token, nx=True, ex=ttl)
                return 1
            token, ttl = first_arg, second_arg
            if await self.get(claim_key) != token:
                return 0
            await self.set(key, b"1", ex=ttl)
            self.store.pop(claim_key, None)
            self.expiries.pop(claim_key, None)
            return 1
        payload, token = first_arg, second_arg
        if await self.get(claim_key) != token:
            return 0
        if await self.get(key) != payload:
            self.store.pop(claim_key, None)
            self.expiries.pop(claim_key, None)
            return 0
        self.store.pop(key, None)
        self.store.pop(claim_key, None)
        self.expiries.pop(key, None)
        self.expiries.pop(claim_key, None)
        return 1

    def expire_claim(self, key):
        self.store.pop(AnomalyDetector._claim_key(key), None)
        self.expiries.pop(AnomalyDetector._claim_key(key), None)


class OutboxRegressionTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def _state(t):
        return StateVector(
            icao24="ABC123",
            lat=39.95,
            lon=-75.16,
            altitude_baro=12_000,
            velocity=1_300,
            heading=0,
            last_seen=float(t),
        )

    async def test_payload_stored_under_mismatched_outbox_key_is_not_published(self):
        state = self._state(9)
        wrong_key = AnomalyDetector.alert_outbox_key(self._state(8))
        redis = _MemoryOutboxRedis({wrong_key: state.to_bytes()})
        producer = AsyncMock()

        self.assertFalse(await AnomalyDetector().publish_pending_alert_key(
            redis, producer, wrong_key
        ))

        producer.send_and_wait.assert_not_awaited()
        self.assertNotIn(wrong_key, redis.store)
        self.assertNotIn(AnomalyDetector._claim_key(wrong_key), redis.store)

    async def test_scan_cursor_advances_across_empty_nonterminal_page(self):
        state = self._state(1)
        key = AnomalyDetector.alert_outbox_key(state)
        redis = _MemoryOutboxRedis(
            {key: state.to_bytes()},
            {0: (7, []), 7: (0, [key])},
        )
        producer = AsyncMock()
        detector = AnomalyDetector()

        first = await detector.drain_pending_alerts(redis, producer, max_items=1)
        second = await detector.drain_pending_alerts(redis, producer, max_items=1)

        self.assertEqual((first, second), (0, 1))
        self.assertEqual(redis.scan_cursors, [0, 7])
        producer.send_and_wait.assert_awaited_once()

    async def test_scan_cursor_eventually_reaches_entries_after_overlapping_pages(self):
        states = [self._state(t) for t in (1, 2, 3)]
        keys = [AnomalyDetector.alert_outbox_key(state) for state in states]
        redis = _MemoryOutboxRedis(
            dict(zip(keys, (state.to_bytes() for state in states))),
            {
                0: (7, [keys[0]]),
                7: (11, [keys[0], keys[1]]),
                11: (0, [keys[1], keys[2]]),
            },
        )
        producer = AsyncMock()
        detector = AnomalyDetector()

        published = [
            await detector.drain_pending_alerts(redis, producer, max_items=10)
            for _ in range(3)
        ]

        self.assertEqual(redis.scan_cursors, [0, 7, 11])
        self.assertEqual(sum(published), 3)
        self.assertEqual(producer.send_and_wait.await_count, 3)
        self.assertFalse(any(key in redis.store for key in keys))

    async def test_global_scan_cursor_is_not_evicted_by_per_icao_replay_cursors(self):
        redis = _MemoryOutboxRedis(scan_pages={0: (7, []), 7: (0, [])})
        detector = AnomalyDetector()
        producer = AsyncMock()

        await detector.drain_pending_alerts(redis, producer)
        for index in range(detector._MAX_SCAN_CURSORS + 1):
            await detector.drain_pending_alerts(
                redis,
                producer,
                match=f"outbox:anomaly-alert:{index:06X}:*",
            )
        await detector.drain_pending_alerts(redis, producer)

        self.assertEqual(redis.scan_cursors[-1], 7)

    async def test_renewal_fake_models_expiry_and_ownership_loss(self):
        redis = _MemoryOutboxRedis()
        claim_key = "claim:test"
        stopped = asyncio.Event()
        lease_lost = asyncio.Event()
        await redis.set(claim_key, "owner-a", nx=True, ex=1)
        redis.advance(0.75)

        renew_task = asyncio.create_task(AnomalyDetector._renew_claim_loop(
            redis, claim_key, "owner-a", 1, stopped, lease_lost, 1.0,
        ))
        await asyncio.sleep(0.4)
        redis.advance(0.5)
        self.assertEqual(await redis.get(claim_key), "owner-a")

        await redis.set(claim_key, "owner-b", ex=1)
        await asyncio.sleep(0.4)
        self.assertTrue(lease_lost.is_set())
        self.assertEqual(await redis.get(claim_key), "owner-b")
        stopped.set()
        await asyncio.gather(renew_task, return_exceptions=True)

    async def test_byte_key_claim_publish_and_ack_matches_redis_asyncio(self):
        state = self._state(5)
        key = AnomalyDetector.alert_outbox_key(state).encode()
        redis = _MemoryOutboxRedis({key: state.to_bytes()})
        producer = AsyncMock()
        detector = AnomalyDetector()

        self.assertTrue(await detector.publish_pending_alert_key(redis, producer, key))
        self.assertNotIn(key, redis.store)
        self.assertNotIn(AnomalyDetector._claim_key(key), redis.store)

    async def test_retry_has_stable_identity_and_duplicate_downstream_effect_is_suppressed(self):
        state = self._state(1)
        key = AnomalyDetector.alert_outbox_key(state)
        producers = [AsyncMock(), AsyncMock()]
        for producer in producers:
            # Redis-to-Kafka cannot atomically couple acknowledgement with
            # deletion. Model the unavoidable retry in another process.
            redis = _MemoryOutboxRedis({key: state.to_bytes()})
            self.assertTrue(await AnomalyDetector().publish_pending_alert_key(
                redis, producer, key
            ))

        headers = [
            producer.send_and_wait.await_args.kwargs["headers"]
            for producer in producers
        ]
        self.assertEqual(headers[0], headers[1])
        event_id = headers[0][0][1]

        dedupe_redis = _MemoryOutboxRedis()
        messages = [
            SimpleNamespace(value=state.to_bytes(), headers=[("event_id", event_id)])
            for _ in range(2)
        ]
        with patch("api.main.redis_client", dedupe_redis):
            token = await _reserve_alert_effect(messages[0])
            self.assertIsNotNone(token)
            assert token is not None
            await _complete_alert_effect(messages[0], token)
            self.assertIsNone(await _reserve_alert_effect(messages[1]))

    async def test_empty_payload_is_atomically_removed_instead_of_left_queued(self):
        key = "outbox:anomaly-alert:ABC123:empty"
        redis = _MemoryOutboxRedis({key: b""})
        producer = AsyncMock()

        published = await AnomalyDetector().publish_pending_alert_key(
            redis, producer, key
        )

        self.assertFalse(published)
        self.assertNotIn(key, redis.store)
        self.assertNotIn(AnomalyDetector._claim_key(key), redis.store)
        producer.send_and_wait.assert_not_awaited()


class _FiniteConsumer:
    def __init__(self, messages=()):
        self.messages = list(messages)
        for offset, message in enumerate(self.messages):
            if not hasattr(message, "topic"):
                message.topic = "test-topic"
            if not hasattr(message, "partition"):
                message.partition = 0
            if not hasattr(message, "offset"):
                message.offset = offset
        self.start = AsyncMock()
        self.stop = AsyncMock()
        self.commit = AsyncMock()

    def __aiter__(self):
        async def iterate():
            for message in self.messages:
                yield message
        return iterate()


class AlertDeliveryStateMachineRegressionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        api_main._ws_clients.add(MagicMock())

    def tearDown(self):
        api_main._ws_clients.clear()

    @staticmethod
    def _message():
        state = StateVector(
            icao24="ABC123", lat=39.95, lon=-75.16,
            altitude_baro=12_000, velocity=400, heading=90, last_seen=100.0,
        )
        return SimpleNamespace(value=state.to_bytes(), headers=[("event_id", b"event-1")])

    async def test_completion_race_cannot_acquire_a_new_processing_lease(self):
        msg = self._message()
        completed_key, lease_key = api_main._alert_effect_keys(msg)

        class CompletionRaceRedis(_MemoryOutboxRedis):
            async def set(self, key, value, *, nx=False, ex=None):
                if key == lease_key:
                    self.store[completed_key] = b"1"
                return await super().set(key, value, nx=nx, ex=ex)

            async def eval(self, script, numkeys, *args):
                if numkeys == 2 and "NX" in script:
                    self.store[completed_key] = b"1"
                return await super().eval(script, numkeys, *args)

        with patch("api.main.redis_client", CompletionRaceRedis()):
            self.assertIsNone(await _reserve_alert_effect(msg))

    def test_event_identity_is_payload_bound_not_header_bound(self):
        first = self._message()
        second = self._message()
        second.value = StateVector(
            icao24="DEF456", lat=40.0, lon=-75.0, altitude_baro=13000,
            velocity=410, heading=91, last_seen=101.0,
        ).to_bytes()
        first.headers = second.headers = [("event_id", b"forged-shared-id")]
        self.assertNotEqual(
            api_main._alert_event_identity(first), api_main._alert_event_identity(second)
        )

    async def test_cancelled_publish_releases_processing_reservation_for_replay(self):
        msg = self._message()
        consumer = _FiniteConsumer([msg])
        reserve = AsyncMock(return_value="lease-token")
        release = AsyncMock()
        complete = AsyncMock()
        publish = AsyncMock(side_effect=asyncio.CancelledError())

        with (
            patch("api.main.AIOKafkaConsumer", return_value=consumer),
            patch("api.main.postgres_pool", AsyncMock()),
            patch("api.main.persist_anomaly_snapshot", new=AsyncMock()),
            patch("api.main.load_coverage_area", new=AsyncMock(return_value=object())),
            patch("api.main._track_in_coverage", return_value=True),
            patch("api.main._reserve_alert_effect", reserve),
            patch("api.main._release_alert_effect", release, create=True),
            patch("api.main._complete_alert_effect", complete, create=True),
            patch("api.main._publish_alert", publish),
            patch("api.main.TDOA_AVAILABLE", False),
        ):
            with self.assertRaises(asyncio.CancelledError):
                await alert_consumer_loop()

        release.assert_awaited_once_with(msg, "lease-token")
        complete.assert_not_awaited()
        consumer.commit.assert_not_awaited()

    async def test_durable_alert_commits_when_optional_live_push_is_contended(self):
        msg = self._message()
        consumer = _FiniteConsumer([msg])
        persist = AsyncMock()
        complete = AsyncMock()
        release = AsyncMock()
        publish = AsyncMock(side_effect=RuntimeError(
            "Unable to acquire lock within the time specified"
        ))

        with (
            patch("api.main.AIOKafkaConsumer", return_value=consumer),
            patch("api.main.postgres_pool", AsyncMock()),
            patch("api.main.persist_anomaly_snapshot", persist),
            patch("api.main.load_coverage_area", new=AsyncMock(return_value=object())),
            patch("api.main._track_in_coverage", return_value=True),
            patch("api.main._reserve_alert_effect", new=AsyncMock(return_value="lease-token")),
            patch("api.main._release_alert_effect", release, create=True),
            patch("api.main._complete_alert_effect", complete, create=True),
            patch("api.main._publish_alert", publish),
            patch("api.main.TDOA_AVAILABLE", False),
        ):
            await alert_consumer_loop()

        persist.assert_awaited_once()
        publish.assert_awaited_once()
        complete.assert_awaited_once_with(msg, "lease-token")
        release.assert_not_awaited()
        consumer.commit.assert_awaited_once()

    async def test_completed_effect_is_suppressed_and_offsets_commit_after_completion(self):
        first = self._message()
        replay = self._message()
        consumer = _FiniteConsumer([first, replay])
        events = []

        async def reserve(msg):
            events.append(("reserve", msg))
            return "lease-token" if msg is first else None

        async def publish(*_args):
            events.append(("publish", first))

        async def complete(msg, token):
            events.append(("complete", msg, token))

        async def commit(*_args, **_kwargs):
            events.append(("commit", None))

        consumer.commit.side_effect = commit
        with (
            patch("api.main.AIOKafkaConsumer", return_value=consumer),
            patch("api.main.postgres_pool", AsyncMock()),
            patch("api.main.persist_anomaly_snapshot", new=AsyncMock()),
            patch("api.main.load_coverage_area", new=AsyncMock(return_value=object())),
            patch("api.main._track_in_coverage", return_value=True),
            patch("api.main._reserve_alert_effect", new=reserve),
            patch("api.main._complete_alert_effect", new=complete, create=True),
            patch("api.main._release_alert_effect", new=AsyncMock(), create=True),
            patch("api.main._publish_alert", new=publish),
            patch("api.main.TDOA_AVAILABLE", False),
        ):
            await alert_consumer_loop()

        self.assertEqual(
            [event[0] for event in events],
            ["reserve", "publish", "complete", "commit", "reserve", "commit"],
        )

    async def test_transient_reservation_failure_does_not_kill_subsequent_delivery(self):
        first = self._message()
        second = self._message()
        second.headers = [("event_id", b"event-2")]
        consumer = _FiniteConsumer([first, second])
        reserve = AsyncMock(
            side_effect=[RuntimeError("redis transient"), "lease-1", "lease-2"]
        )
        publish = AsyncMock()

        with (
            patch("api.main.AIOKafkaConsumer", return_value=consumer),
            patch("api.main.postgres_pool", AsyncMock()),
            patch("api.main.persist_anomaly_snapshot", new=AsyncMock()),
            patch("api.main.load_coverage_area", new=AsyncMock(return_value=object())),
            patch("api.main._track_in_coverage", return_value=True),
            patch("api.main._reserve_alert_effect", reserve),
            patch("api.main._complete_alert_effect", new=AsyncMock(), create=True),
            patch("api.main._release_alert_effect", new=AsyncMock(), create=True),
            patch("api.main._publish_alert", publish),
            patch("api.main.TDOA_AVAILABLE", False),
        ):
            await alert_consumer_loop()

        self.assertEqual(reserve.await_count, 3)
        self.assertEqual(publish.await_count, 2)
        self.assertEqual(consumer.commit.await_count, 2)


class ApiLifespanTaskSupervisionRegressionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.history_pool = AsyncMock()
        self.history_pool.close = AsyncMock()
        self.history_pool_patch = patch(
            "api.main.asyncpg.create_pool", AsyncMock(return_value=self.history_pool)
        )
        self.history_pool_patch.start()
        self.addCleanup(self.history_pool_patch.stop)

    async def test_lifespan_starts_and_stops_physical_reception_publisher(self):
        async def block():
            await asyncio.Event().wait()

        redis = AsyncMock()
        producer = AsyncMock()
        factory = MagicMock(return_value=producer)
        with (
            patch("api.main.aioredis.from_url", return_value=redis),
            patch("api.main.AIOKafkaProducer", factory),
            patch("api.main.broadcast_loop", new=block),
            patch("api.main.alert_consumer_loop", new=block),
            patch("api.main.TDOA_AVAILABLE", False),
        ):
            async with lifespan(MagicMock()):
                factory.assert_called_once()
                producer.start.assert_awaited_once()
                self.assertIs(api_main.mlat_reception_producer, producer)
        producer.stop.assert_awaited_once()
        self.assertIsNone(api_main.mlat_reception_producer)

    async def test_shutdown_cancels_and_awaits_both_tasks_before_closing_resources(self):
        events = []
        started = {"broadcast": asyncio.Event(), "alert": asyncio.Event()}

        async def background(name):
            started[name].set()
            try:
                await asyncio.Event().wait()
            finally:
                events.append(f"{name}:cancelled-and-awaited")

        redis = AsyncMock()
        redis.close.side_effect = lambda: events.append("redis:closed")
        producer = AsyncMock()
        validator = MagicMock()
        validator.__aenter__ = AsyncMock(return_value=validator)
        validator.__aexit__ = AsyncMock(
            side_effect=lambda *_args: events.append("validator:closed")
        )
        spawned = []
        real_create_task = asyncio.create_task

        def capture_task(coro):
            task = real_create_task(coro)
            spawned.append(task)
            return task

        try:
            with (
                patch("api.main.aioredis.from_url", return_value=redis),
                patch("api.main.AIOKafkaProducer", return_value=producer),
                patch("api.main.CrossSourceValidator", return_value=validator),
                patch("api.main.EnhancedAnomalyDetector", return_value=MagicMock()),
                patch("api.main.broadcast_loop", new=lambda: background("broadcast")),
                patch("api.main.alert_consumer_loop", new=lambda: background("alert")),
                patch("api.main.asyncio.create_task", side_effect=capture_task),
                patch("api.main.TDOA_AVAILABLE", True),
            ):
                async with lifespan(MagicMock()):
                    await started["broadcast"].wait()
                    await started["alert"].wait()
        finally:
            for task in spawned:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*spawned, return_exceptions=True)

        self.assertCountEqual(
            events,
            [
                "broadcast:cancelled-and-awaited", "alert:cancelled-and-awaited",
                "redis:closed", "validator:closed",
            ],
        )
        last_task_cleanup = max(
            events.index("broadcast:cancelled-and-awaited"),
            events.index("alert:cancelled-and-awaited"),
        )
        first_resource_close = min(
            events.index("redis:closed"), events.index("validator:closed")
        )
        self.assertLess(last_task_cleanup, first_resource_close)

    async def test_unexpected_critical_task_exit_is_surfaced_by_lifespan(self):
        failed = asyncio.Event()

        async def fail_critically():
            await asyncio.sleep(0)
            failed.set()
            raise RuntimeError("critical background task exited")

        async def block():
            await asyncio.Event().wait()

        redis = AsyncMock()
        producer = AsyncMock()
        spawned = []
        real_create_task = asyncio.create_task

        def capture_task(coro):
            task = real_create_task(coro)
            spawned.append(task)
            return task

        caught = None
        continued_after_failure = False
        try:
            with (
                patch("api.main.aioredis.from_url", return_value=redis),
                patch("api.main.AIOKafkaProducer", return_value=producer),
                patch("api.main.broadcast_loop", new=fail_critically),
                patch("api.main.alert_consumer_loop", new=block),
                patch("api.main.asyncio.create_task", side_effect=capture_task),
                patch("api.main.TDOA_AVAILABLE", False),
            ):
                try:
                    async with lifespan(MagicMock()):
                        await failed.wait()
                        await asyncio.sleep(0)
                        await asyncio.sleep(0)
                        continued_after_failure = True
                except BaseException as exc:
                    caught = exc
        finally:
            for task in spawned:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*spawned, return_exceptions=True)

        self.assertIsNotNone(caught)
        self.assertIn("critical background task exited", repr(caught))
        self.assertFalse(continued_after_failure)


class DetectorLifecycleRegressionTests(unittest.IsolatedAsyncioTestCase):
    async def test_outbox_drain_failure_is_supervised_and_fatal(self):
        from anomaly.detector import supervise_detector_tasks

        consumer_cancelled = asyncio.Event()

        async def consume_forever():
            try:
                await asyncio.Event().wait()
            finally:
                consumer_cancelled.set()

        async def failed_drain():
            raise RuntimeError("outbox recovery failed")

        with self.assertRaisesRegex(RuntimeError, "outbox recovery failed"):
            await supervise_detector_tasks(consume_forever(), failed_drain())
        self.assertTrue(consumer_cancelled.is_set())

    async def test_redis_constructor_failure_does_not_construct_kafka_resources(self):
        consumer_factory = MagicMock()
        producer_factory = MagicMock()
        with (
            patch("anomaly.detector.aioredis.from_url", side_effect=RuntimeError("redis ctor")),
            patch("anomaly.detector.AIOKafkaConsumer", consumer_factory),
            patch("anomaly.detector.AIOKafkaProducer", producer_factory),
        ):
            with self.assertRaisesRegex(RuntimeError, "redis ctor"):
                await run()

        consumer_factory.assert_not_called()
        producer_factory.assert_not_called()

    async def test_consumer_constructor_failure_closes_previously_created_redis(self):
        redis = AsyncMock()
        producer_factory = MagicMock()
        with (
            patch("anomaly.detector.aioredis.from_url", return_value=redis),
            patch("anomaly.detector.AIOKafkaConsumer", side_effect=RuntimeError("consumer ctor")),
            patch("anomaly.detector.AIOKafkaProducer", producer_factory),
        ):
            with self.assertRaisesRegex(RuntimeError, "consumer ctor"):
                await run()

        redis.close.assert_awaited_once()
        producer_factory.assert_not_called()

    async def test_producer_constructor_failure_closes_consumer_and_redis(self):
        redis = AsyncMock()
        consumer = _FiniteConsumer()
        with (
            patch("anomaly.detector.aioredis.from_url", return_value=redis),
            patch("anomaly.detector.AIOKafkaConsumer", return_value=consumer),
            patch("anomaly.detector.AIOKafkaProducer", side_effect=RuntimeError("producer ctor")),
        ):
            with self.assertRaisesRegex(RuntimeError, "producer ctor"):
                await run()

        consumer.stop.assert_awaited_once()
        redis.close.assert_awaited_once()

    async def test_malformed_fused_record_is_committed_once_and_next_record_is_processed(self):
        valid = StateVector(
            icao24="ABC123",
            lat=39.95,
            lon=-75.16,
            altitude_baro=12_000,
            velocity=400,
            heading=90,
            last_seen=100.0,
        )
        consumer = _FiniteConsumer([
            SimpleNamespace(value=b"not-json"),
            SimpleNamespace(value=valid.to_bytes()),
        ])
        producer = AsyncMock()
        redis = AsyncMock()
        detector = MagicMock()
        detector.drain_pending_alerts = AsyncMock(return_value=0)
        detector.hydrate_last_result = AsyncMock()
        detector.hydrate_l2_baseline = AsyncMock()
        detector.is_replay.return_value = False
        detector.process.side_effect = lambda state: state
        detector.persist_event_state = AsyncMock()
        detector.prune_l2_state.return_value = 0

        with (
            patch("anomaly.detector.aioredis.from_url", return_value=redis),
            patch("anomaly.detector.AIOKafkaConsumer", return_value=consumer),
            patch("anomaly.detector.AIOKafkaProducer", return_value=producer),
            patch("anomaly.detector.AnomalyDetector", return_value=detector),
        ):
            await run()

        self.assertEqual(consumer.commit.await_count, 2)
        detector.process.assert_called_once()
        detector.persist_event_state.assert_awaited_once()
        persisted = detector.persist_event_state.await_args.args[1]
        self.assertEqual(persisted.icao24, "ABC123")
        self.assertEqual(persisted.risk_band, RiskBand.NORMAL)


if __name__ == "__main__":
    unittest.main()
