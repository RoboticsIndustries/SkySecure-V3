import unittest
from unittest.mock import AsyncMock, MagicMock, patch
from types import SimpleNamespace
import orjson

from api.main import (
    _L1_CACHE,
    _l1_cache_is_fresh,
    _deduplicate_track_records,
    _merge_l1_results,
    _select_l1_claim_records,
    _validate_raw_l1_claim,
    _fetch_live_aircraft,
    _select_l1_candidates,
    _parse_adsb_lol_aircraft,
    run_l1_cross_validation,
    run_l2_l3_detection,
)
from processing.cross_source_validator import (
    CrossValidationResult,
    DISAGREEMENT_SPOOFED_M,
    DISAGREEMENT_UNCERTAIN_M,
)
from ingestion.adsb_receiver import (
    _parse_adsb_lol_states, _parse_feed_state, adsb_lol_fallback_url,
)


class AdsbLolFallbackTests(unittest.TestCase):
    def test_ingestion_parser_isolates_bad_neighbors_and_preserves_event_time(self):
        states = _parse_adsb_lol_states({"ac": [
            {"hex": "a1b2c3", "lat": 39.95, "lon": -75.16, "seen_pos": 2.5},
            {"hex": "d4e5f6", "lat": 40.0, "lon": -74.0},
            None,
            {"hex": "BAD:*", "lat": 39.95, "lon": -75.16},
        ]}, received_at=100.0)
        self.assertEqual(len(states), 2)
        self.assertEqual(states[0][0], "A1B2C3")
        self.assertEqual(states[0][4], 97.5)
        self.assertIsNone(states[1][3])
        self.assertIsNone(states[1][4])

        parsed = _parse_feed_state(states[0], "adsb_lol", 100.0)
        self.assertIsNotNone(parsed)
        aircraft, message = parsed
        self.assertEqual(aircraft["ts"], 97.5)
        self.assertEqual(message.recv_time, 97.5)
        self.assertIsNone(_parse_feed_state(states[1], "adsb_lol", 100.0))
        self.assertIsNone(_parse_feed_state(["short"], "opensky", 100.0))

    def test_ingestion_rejects_stale_and_future_feed_event_times(self):
        def row(event_time):
            return [
                "abc123", "CALL", None, event_time, event_time,
                -75.0, 40.0, 1000.0, False, 100.0, 90.0, 0.0,
            ]

        self.assertIsNone(_parse_feed_state(row(699.0), "opensky", 1000.0))
        self.assertIsNone(_parse_feed_state(row(1006.0), "opensky", 1000.0))

    def _cache_is_fresh(self, disagreement_m, movement_m):
        moved_lat = movement_m / 6_371_000 * 180 / 3.141592653589793
        ac = {"icao": "ABC123", "src": "adsb_lol", "lat": moved_lat, "lon": 0.0}
        verdict = (
            "LEGITIMATE"
            if disagreement_m < DISAGREEMENT_UNCERTAIN_M
            else "UNCERTAIN"
            if disagreement_m < DISAGREEMENT_SPOOFED_M
            else "SPOOFED"
        )
        _L1_CACHE.clear()
        _L1_CACHE[("ABC123", "adsb_lol")] = (
            100.0,
            {
                "icao": "ABC123",
                "max_disagreement_m": round(disagreement_m, 1),
                "confidence": 0.9,
                "is_valid": verdict == "LEGITIMATE",
                "verdict": verdict,
                "sources_used": ["adsb_fi"],
            },
            0.0,
            0.0,
            disagreement_m,
        )
        return _l1_cache_is_fresh(ac, 101.0)

    def test_cache_near_1500m_boundary_uses_strict_movement_margin(self):
        disagreement = DISAGREEMENT_UNCERTAIN_M - 1.0
        self.assertTrue(self._cache_is_fresh(disagreement, 0.9))
        self.assertFalse(self._cache_is_fresh(disagreement, 1.0))

    def test_cache_at_1500m_boundary_always_revalidates(self):
        self.assertFalse(self._cache_is_fresh(DISAGREEMENT_UNCERTAIN_M, 0.0))

    def test_cache_near_and_at_5000m_boundary_revalidates_safely(self):
        self.assertTrue(self._cache_is_fresh(DISAGREEMENT_SPOOFED_M - 1.0, 0.9))
        self.assertFalse(self._cache_is_fresh(DISAGREEMENT_SPOOFED_M - 1.0, 1.0))
        self.assertFalse(self._cache_is_fresh(DISAGREEMENT_SPOOFED_M, 0.0))

    def test_fallback_area_is_configurable_and_radius_is_bounded(self):
        self.assertEqual(
            adsb_lol_fallback_url(40.7128, -74.0060, 300),
            "https://api.adsb.lol/v2/point/40.7128/-74.006/250",
        )

    def test_parses_adsb_lol_aircraft_into_api_shape(self):
        payload = {
            "ac": [
                {
                    "hex": "~a1b2c3",
                    "flight": " TEST123 ",
                    "lat": 39.95,
                    "lon": -75.16,
                    "alt_baro": 12000,
                    "gs": 430.5,
                    "track": 90,
                    "baro_rate": 640,
                    "seen_pos": 2.0,
                },
                None,
                {"hex": "bad", "lat": None, "lon": -75.0},
                {"hex": "ZZZZZZ", "lat": 39.95, "lon": -75.16},
                {
                    "hex": "d4e5f6", "lat": 40.0, "lon": -74.0,
                    "alt_baro": float("inf"), "gs": float("nan"),
                    "track": "not-a-number", "baro_rate": {},
                },
            ]
        }

        with patch("api.main.time.time", return_value=100.0):
            aircraft = _parse_adsb_lol_aircraft(payload)

        self.assertEqual(len(aircraft), 2)
        self.assertEqual(aircraft[0]["icao"], "A1B2C3")
        self.assertEqual(aircraft[0]["cs"], "TEST123")
        self.assertEqual(aircraft[0]["alt"], 12000)
        self.assertEqual(aircraft[0]["vel"], 430)
        self.assertEqual(aircraft[0]["src"], "adsb_lol")
        self.assertEqual(aircraft[0]["band"], "NORMAL")
        self.assertEqual(aircraft[0]["ts"], 98.0)
        self.assertNotIn("ts", aircraft[1])
        self.assertIsNone(aircraft[1]["alt"])
        self.assertIsNone(aircraft[1]["vel"])
        self.assertIsNone(aircraft[1]["hdg"])
        self.assertIsNone(aircraft[1]["vr"])

    def test_generic_fused_adsb_track_is_not_live_source_candidate(self):
        _L1_CACHE.clear()
        tracks = [{
            "icao": "A1B2C3", "lat": 39.95, "lon": -75.16,
            "src": "ADSB", "risk": 0,
        }]

        self.assertEqual(_select_l1_candidates(tracks), [])

    def test_websocket_track_dedup_prefers_canonical_state_vector(self):
        records = [
            ("ac", {"icao": "A1B2C3", "risk": 0, "src": "adsb_lol"}),
            ("sv", {"icao": "A1B2C3", "risk": 80, "src": "ADSB"}),
            ("ac", {"icao": "D4E5F6", "risk": 0, "src": "adsb_lol"}),
        ]

        tracks = _deduplicate_track_records(records)

        self.assertEqual({track["icao"] for track in tracks}, {"A1B2C3", "D4E5F6"})
        selected = next(track for track in tracks if track["icao"] == "A1B2C3")
        self.assertEqual(selected["risk"], 80)


class _FakeResponse:
    def __init__(self, status, payload):
        self.status = status
        self._payload = payload

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def json(self, **_kwargs):
        return self._payload


class _FakeSession:
    def __init__(self, responses):
        self._responses = iter(responses)

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    def get(self, *_args, **_kwargs):
        return next(self._responses)


class AdsbLolFallbackIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_opensky_skips_malformed_records_without_blanking_valid_neighbors(self):
        valid = ["a1b2c3", "TEST123", None, 100.0, 100.0,
                 -75.16, 39.95, 1000.0, False, 200.0, 90.0, 0.0]
        malformed_optional = ["d4e5f6", None, None, "bad-ts", "bad-ts",
                              -74.0, 40.0, None, False, None, {}, []]
        session = _FakeSession([_FakeResponse(200, {
            "states": [valid, None, ["short"], malformed_optional, valid],
        })])
        with (
            patch("api.main.aiohttp.ClientSession", return_value=session),
            patch("api.main.TDOA_AVAILABLE", False),
        ):
            aircraft = await _fetch_live_aircraft()

        self.assertEqual(
            [item["icao"] for item in aircraft],
            ["A1B2C3", "D4E5F6", "A1B2C3"],
        )
        self.assertIsNone(aircraft[1]["hdg"])
        self.assertIsNone(aircraft[1]["vr"])

    async def test_exact_boundary_result_is_applied_but_never_reused(self):
        validator = AsyncMock()
        validator.validate_aircraft.return_value = CrossValidationResult(
            icao="ABC123", is_valid=False,
            max_disagreement_m=DISAGREEMENT_UNCERTAIN_M,
            confidence=0.7, sources_used=["adsb_fi"], verdict="UNCERTAIN",
        )
        aircraft = [{
            "icao": "ABC123", "lat": 39.95, "lon": -75.16,
            "src": "adsb_lol",
        }]
        _L1_CACHE.clear()

        with patch("api.main.cross_validator", validator):
            await run_l1_cross_validation(aircraft)
            self.assertEqual(aircraft[0]["l1"]["verdict"], "UNCERTAIN")
            await run_l1_cross_validation(aircraft)

        self.assertEqual(validator.validate_aircraft.await_count, 2)
        self.assertEqual(aircraft[0]["l1"]["verdict"], "UNCERTAIN")

    async def test_five_to_twenty_km_displacement_invalidates_cached_l1(self):
        validator = AsyncMock()
        validator.validate_aircraft.side_effect = [
            CrossValidationResult(
                icao="ABC123", is_valid=True, max_disagreement_m=0,
                confidence=0.9, sources_used=["adsb_fi"], verdict="LEGITIMATE",
            ),
            RuntimeError("validator unavailable"),
        ]
        detector = MagicMock()
        assessment = SimpleNamespace(
            threat_level="LOW", fused_score=0.0, layers={}, notes=[],
            corroborated=False, to_dict=lambda: {},
        )
        detector.assess.return_value = assessment
        _L1_CACHE.clear()
        original = [{"icao": "ABC123", "lat": 39.95, "lon": -75.16, "src": "adsb_lol", "ts": 100.0}]
        # About 11 km: beyond the 5 km SPOOFED threshold but below the old
        # 20 km cache break distance.
        displaced = [{"icao": "ABC123", "lat": 40.05, "lon": -75.16, "src": "adsb_lol", "ts": 101.0}]

        with patch("api.main.cross_validator", validator):
            await run_l1_cross_validation(original)
            await run_l1_cross_validation(displaced)
        with patch("api.main.anomaly_detector", detector):
            run_l2_l3_detection(displaced)

        self.assertEqual(validator.validate_aircraft.await_count, 2)
        self.assertIsNone(detector.assess.call_args.kwargs["l1_result"])

    async def test_alert_l1_uses_raw_claim_and_excludes_its_source(self):
        validator = AsyncMock()
        validator.validate_aircraft.return_value = CrossValidationResult(
            icao="ABC123", is_valid=False, max_disagreement_m=0,
            confidence=0, sources_used=[], verdict="INSUFFICIENT_SOURCES",
        )
        redis = AsyncMock()
        redis.get.return_value = orjson.dumps({
            "icao": "ABC123", "lat": 39.95, "lon": -75.16,
            "src": "adsb_lol", "vel": 400.0, "hdg": 90.0, "ts": 123.0,
        })
        _L1_CACHE.clear()

        with (
            patch("api.main.redis_client", redis),
            patch("api.main.cross_validator", validator),
        ):
            result = await _validate_raw_l1_claim("ABC123")

        self.assertEqual(result["verdict"], "INSUFFICIENT_SOURCES")
        validator.validate_aircraft.assert_awaited_once_with(
            "ABC123", 39.95, -75.16,
            claimed_velocity_kts=400.0,
            claimed_heading_deg=90.0,
            claimed_observed_at=123.0,
            claimed_source="adsb_lol",
        )
    async def test_l1_cache_survives_small_movement_and_minute_boundary(self):
        validator = AsyncMock()
        validator.validate_aircraft.return_value = CrossValidationResult(
            icao="ABC123", is_valid=True, max_disagreement_m=0,
            confidence=0.9, sources_used=["adsb_fi"], verdict="LEGITIMATE",
        )
        _L1_CACHE.clear()
        first = [{"icao": "ABC123", "lat": 39.95000, "lon": -75.16000, "src": "adsb_lol"}]
        moved_one_metre = [{"icao": "ABC123", "lat": 39.950009, "lon": -75.16000, "src": "adsb_lol"}]

        with (
            patch("api.main.cross_validator", validator),
            patch("api.main.time.time", side_effect=[59.0, 61.0]),
        ):
            await run_l1_cross_validation(first)
            await run_l1_cross_validation(moved_one_metre)

        self.assertEqual(validator.validate_aircraft.await_count, 1)
        self.assertEqual(moved_one_metre[0]["l1"]["verdict"], "LEGITIMATE")

    async def test_raw_claim_l1_result_is_merged_into_canonical_track(self):
        validator = AsyncMock()
        validator.validate_aircraft.return_value = CrossValidationResult(
            icao="ABC123", is_valid=False, max_disagreement_m=9000,
            confidence=0.95, sources_used=["adsb_fi"], verdict="SPOOFED",
        )
        _L1_CACHE.clear()
        records = [
            ("ac", {"icao": "ABC123", "lat": 39.95, "lon": -75.16,
                    "src": "adsb_lol", "risk": 0, "anoms": [],
                    "vel": 420.0, "hdg": 90.0, "ts": 1234.5}),
            ("sv", {"icao": "ABC123", "lat": 39.95, "lon": -75.16,
                    "src": "ADSB", "risk": 20, "band": "NORMAL", "anoms": [],
                    "layer_evaluations": {"L3": {"status": "EVALUATED"}}}),
        ]
        tracks = _deduplicate_track_records(records)
        claims = _select_l1_claim_records(records)

        with patch("api.main.cross_validator", validator):
            await run_l1_cross_validation(claims)
        _merge_l1_results(tracks, claims)

        self.assertEqual(validator.validate_aircraft.await_count, 1)
        self.assertEqual(tracks[0]["l1"]["verdict"], "SPOOFED")
        self.assertEqual(tracks[0]["risk"], 80)
        self.assertEqual(tracks[0]["anoms"], ["L1_POSITION_DISAGREEMENT"])
        validator.validate_aircraft.assert_awaited_once_with(
            "ABC123", 39.95, -75.16,
            claimed_velocity_kts=420.0,
            claimed_heading_deg=90.0,
            claimed_observed_at=1234.5,
            claimed_source="adsb_lol",
        )

    async def test_http_429_fallback_receives_l1_enrichment(self):
        fallback_payload = {
            "ac": [{"hex": "a1b2c3", "lat": 39.95, "lon": -75.16}]
        }
        session = _FakeSession([
            _FakeResponse(429, {}),
            _FakeResponse(200, fallback_payload),
        ])

        async def mark_validated(aircraft):
            aircraft[0]["l1"] = {"verdict": "INSUFFICIENT_SOURCES"}

        with (
            patch("api.main.aiohttp.ClientSession", return_value=session),
            patch("api.main.TDOA_AVAILABLE", True),
            patch("api.main.cross_validator", object()),
            patch(
                "api.main.run_l1_cross_validation",
                new=AsyncMock(side_effect=mark_validated),
            ) as validate,
        ):
            aircraft = await _fetch_live_aircraft()

        validate.assert_awaited_once_with(aircraft)
        self.assertEqual(aircraft[0]["l1"]["verdict"], "INSUFFICIENT_SOURCES")

    async def test_l1_cache_is_scoped_to_claim_source(self):
        validator = AsyncMock()
        validator.validate_aircraft.return_value = CrossValidationResult(
            icao="ABC123", is_valid=False, max_disagreement_m=0,
            confidence=0, sources_used=[], verdict="INSUFFICIENT_SOURCES",
        )
        _L1_CACHE.clear()
        opensky = [{"icao": "ABC123", "lat": 1.0, "lon": 2.0, "src": "opensky"}]
        adsb_lol = [{"icao": "ABC123", "lat": 1.0, "lon": 2.0, "src": "adsb_lol"}]

        with patch("api.main.cross_validator", validator):
            await run_l1_cross_validation(opensky)
            await run_l1_cross_validation(adsb_lol)

        self.assertEqual(validator.validate_aircraft.await_count, 2)

    async def test_l1_cache_is_scoped_to_claim_position(self):
        validator = AsyncMock()
        validator.validate_aircraft.return_value = CrossValidationResult(
            icao="ABC123", is_valid=True, max_disagreement_m=0,
            confidence=0.9, sources_used=["adsb_lol"], verdict="LEGITIMATE",
        )
        _L1_CACHE.clear()
        first = [{"icao": "ABC123", "lat": 39.9500, "lon": -75.1600, "src": "opensky"}]
        moved = [{"icao": "ABC123", "lat": 40.9600, "lon": -76.1700, "src": "opensky"}]

        with patch("api.main.cross_validator", validator):
            await run_l1_cross_validation(first)
            await run_l1_cross_validation(moved)

        self.assertEqual(validator.validate_aircraft.await_count, 2)

    async def test_cached_spoof_verdict_does_not_duplicate_anomaly(self):
        validator = AsyncMock()
        validator.validate_aircraft.return_value = CrossValidationResult(
            icao="ABC123", is_valid=False, max_disagreement_m=9000,
            confidence=0.95, sources_used=["adsb_lol", "adsb_fi"], verdict="SPOOFED",
        )
        _L1_CACHE.clear()
        aircraft = [{
            "icao": "ABC123", "lat": 39.95, "lon": -75.16,
            "src": "opensky", "risk": 0, "anoms": [],
        }]

        with patch("api.main.cross_validator", validator):
            await run_l1_cross_validation(aircraft)
            await run_l1_cross_validation(aircraft)

        anomalies = [a for a in aircraft[0]["anoms"] if a["type"] == "L1_POSITION_DISAGREEMENT"]
        self.assertEqual(len(anomalies), 1)


if __name__ == "__main__":
    unittest.main()
