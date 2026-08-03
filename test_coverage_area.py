import unittest
from unittest.mock import patch

from pydantic import ValidationError

from coverage_area import (
    CoverageArea,
    coverage_url,
    load_coverage_area,
    load_coverage_area_record,
    save_coverage_area,
    within_coverage_area,
)
import api.main as api_main
from api.main import _track_in_coverage, _websocket_origin_allowed
from ingestion.adsb_receiver import current_coverage_url


class FakeRedis:
    def __init__(self):
        self.values = {}

    async def get(self, key):
        return self.values.get(key)

    async def set(self, key, value, **kwargs):
        self.values[key] = value

    def lock(self, *args, **kwargs):
        class Lock:
            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False
        return Lock()


class CoverageAreaTests(unittest.IsolatedAsyncioTestCase):
    def test_area_validates_coordinates_and_provider_radius(self):
        with self.assertRaises(ValidationError):
            CoverageArea(latitude=91, longitude=0, radius_nm=50)
        with self.assertRaises(ValidationError):
            CoverageArea(latitude=0, longitude=181, radius_nm=50)
        with self.assertRaises(ValidationError):
            CoverageArea(latitude=0, longitude=0, radius_nm=251)
        with self.assertRaises(ValidationError):
            CoverageArea(latitude=True, longitude=0, radius_nm=1)
        with self.assertRaises(ValidationError):
            CoverageArea(latitude=0, longitude=0, radius_nm=1, label="   ")

    def test_url_uses_selected_area(self):
        area = CoverageArea(
            latitude=40.7128,
            longitude=-74.006,
            radius_nm=125,
            label="New York",
        )
        self.assertEqual(
            coverage_url(area),
            "https://api.adsb.lol/v2/point/40.7128/-74.006/125",
        )

    def test_global_source_positions_are_limited_to_selected_area(self):
        area = CoverageArea(latitude=40.6413, longitude=-73.7781, radius_nm=100, label="JFK")
        self.assertTrue(within_coverage_area(40.7128, -74.0060, area))
        self.assertFalse(within_coverage_area(34.0522, -118.2437, area))
        self.assertTrue(_track_in_coverage({"lat": 40.7, "lon": -74.0}, area))
        self.assertFalse(_track_in_coverage({"lat": 34.05, "lon": -118.24}, area))

    def test_websocket_origin_is_restricted_to_configured_dashboard_origins(self):
        class Socket:
            def __init__(self, origin):
                self.headers = {"origin": origin} if origin is not None else {}

        self.assertFalse(_websocket_origin_allowed(Socket("https://evil.example")))
        self.assertTrue(_websocket_origin_allowed(Socket("http://localhost:3000")))
        self.assertTrue(_websocket_origin_allowed(Socket(None)))

    async def test_selected_area_is_shared_through_redis(self):
        redis = FakeRedis()
        selected = CoverageArea(
            latitude=33.9416,
            longitude=-118.4085,
            radius_nm=200,
            label="LAX",
        )

        await save_coverage_area(redis, selected)
        restored = await load_coverage_area(redis)

        self.assertEqual(restored, selected)
        _, token = await load_coverage_area_record(redis)
        self.assertEqual(token, selected.model_dump_json().encode())

    async def test_ingestor_reads_live_area_from_shared_redis(self):
        redis = FakeRedis()
        selected = CoverageArea(
            latitude=-33.9461,
            longitude=151.1772,
            radius_nm=175,
            label="SYD",
        )
        await save_coverage_area(redis, selected)

        self.assertEqual(
            await current_coverage_url(redis),
            "https://api.adsb.lol/v2/point/-33.9461/151.1772/175",
        )


class CoverageAreaApiTests(unittest.IsolatedAsyncioTestCase):
    async def test_live_fetch_discards_batch_when_area_changes_in_flight(self):
        redis = FakeRedis()
        first = CoverageArea(latitude=40.0, longitude=-75.0, radius_nm=100, label="First")
        second = CoverageArea(latitude=34.0, longitude=-118.0, radius_nm=100, label="Second")
        third = CoverageArea(latitude=51.5, longitude=-0.1, radius_nm=100, label="Third")
        await save_coverage_area(redis, first)
        previous_redis = api_main.redis_client
        previous_cache = api_main._live_cache
        api_main.redis_client = redis
        api_main._live_cache = {"ts": 0, "aircraft": [], "coverage_token": b""}
        calls = []

        async def fake_fetch(area):
            calls.append(area.label)
            if len(calls) == 1:
                await save_coverage_area(redis, second)
                return [{"icao": "OLD"}]
            if len(calls) == 2:
                await save_coverage_area(redis, third)
                return [{"icao": "ALSO_OLD"}]
            return [{"icao": "NEW"}]

        try:
            with patch.object(api_main, "_fetch_live_aircraft", side_effect=fake_fetch):
                response = await api_main.get_live_aircraft()
        finally:
            api_main.redis_client = previous_redis
            api_main._live_cache = previous_cache

        self.assertEqual(calls, ["First", "Second", "Third"])
        self.assertEqual(response["aircraft"], [{"icao": "NEW"}])

    async def test_api_updates_shared_area_and_invalidates_live_cache(self):
        redis = FakeRedis()
        previous_redis = api_main.redis_client
        api_main.redis_client = redis
        api_main._live_cache = {"ts": 123.0, "aircraft": [{"icao": "OLD"}]}
        selected = CoverageArea(
            latitude=51.4700,
            longitude=-0.4543,
            radius_nm=150,
            label="LHR — London Heathrow",
        )
        try:
            response = await api_main.update_coverage_area_config(selected)
            current = await api_main.get_coverage_area_config()
            with self.assertRaises(api_main.HTTPException) as rate_limited:
                await api_main.update_coverage_area_config(selected)
        finally:
            api_main.redis_client = previous_redis

        self.assertEqual(response["coverage"], selected.model_dump())
        self.assertEqual(current["coverage"], selected.model_dump())
        self.assertEqual(rate_limited.exception.status_code, 429)
        self.assertEqual(api_main._live_cache, {"ts": 0, "aircraft": [], "coverage_token": b""})
