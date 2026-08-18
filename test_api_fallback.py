import unittest
from unittest.mock import AsyncMock, patch

from api.main import (
    _L1_CACHE,
    _fetch_live_aircraft,
    _select_l1_candidates,
    _parse_adsb_lol_aircraft,
    run_l1_cross_validation,
)
from processing.cross_source_validator import CrossValidationResult
from ingestion.adsb_receiver import adsb_lol_fallback_url


class AdsbLolFallbackTests(unittest.TestCase):
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
                },
                {"hex": "bad", "lat": None, "lon": -75.0},
            ]
        }

        aircraft = _parse_adsb_lol_aircraft(payload)

        self.assertEqual(len(aircraft), 1)
        self.assertEqual(aircraft[0]["icao"], "A1B2C3")
        self.assertEqual(aircraft[0]["cs"], "TEST123")
        self.assertEqual(aircraft[0]["alt"], 12000)
        self.assertEqual(aircraft[0]["vel"], 430)
        self.assertEqual(aircraft[0]["src"], "adsb_lol")
        self.assertEqual(aircraft[0]["band"], "NORMAL")

    def test_generic_fused_adsb_track_is_not_live_source_candidate(self):
        _L1_CACHE.clear()
        tracks = [{
            "icao": "A1B2C3", "lat": 39.95, "lon": -75.16,
            "src": "ADSB", "risk": 0,
        }]

        self.assertEqual(_select_l1_candidates(tracks), [])


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


if __name__ == "__main__":
    unittest.main()
