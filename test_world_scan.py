import unittest
from unittest.mock import AsyncMock, patch

from coverage_area import COVERAGE_REDIS_KEY, CoverageArea, load_coverage_area
import api.main as api_main
import ingestion.adsb_receiver as adsb_receiver
import world_scan


class FakeRedis:
    def __init__(self):
        self.values = {}

    async def get(self, key):
        return self.values.get(key)

    async def set(self, key, value, **_kwargs):
        self.values[key] = value

    async def eval(self, _script, numkeys, *args):
        keys = args[:numkeys]
        values = args[numkeys:]
        self.values.update(dict(zip(keys, values)))
        return 1

    def lock(self, *_args, **_kwargs):
        class Lock:
            async def __aenter__(self): return self
            async def __aexit__(self, *_args): return False
        return Lock()


class WorldScanTests(unittest.TestCase):
    def test_scan_waits_for_trajectory_warmup_before_advancing(self):
        state = world_scan.WorldScanState(
            enabled=True,
            dwell_seconds=315,
            tile_index=0,
            switched_at=1000.0,
        )

        unchanged, area = world_scan.advance_world_scan_state(state, now=1314.0)

        self.assertEqual(unchanged.tile_index, 0)
        self.assertIsNone(area)

    def test_scan_advances_to_next_global_tile_after_dwell(self):
        state = world_scan.WorldScanState(
            enabled=True,
            dwell_seconds=315,
            tile_index=0,
            switched_at=1000.0,
        )

        advanced, area = world_scan.advance_world_scan_state(state, now=1315.0)

        self.assertEqual(advanced.tile_index, 1)
        self.assertEqual(advanced.switched_at, 1315.0)
        self.assertIsInstance(area, CoverageArea)
        self.assertEqual(area.radius_nm, 250)
        self.assertTrue(area.label.startswith("World scan"))

    def test_scan_wraps_and_has_broad_worldwide_distribution(self):
        self.assertGreaterEqual(len(world_scan.WORLD_SCAN_TILES), 20)
        latitudes = [tile[1] for tile in world_scan.WORLD_SCAN_TILES]
        longitudes = [tile[2] for tile in world_scan.WORLD_SCAN_TILES]
        self.assertLess(min(latitudes), -30)
        self.assertGreater(max(latitudes), 50)
        self.assertLess(min(longitudes), -100)
        self.assertGreater(max(longitudes), 120)

        state = world_scan.WorldScanState(
            enabled=True,
            dwell_seconds=315,
            tile_index=len(world_scan.WORLD_SCAN_TILES) - 1,
            switched_at=1000.0,
        )
        advanced, _ = world_scan.advance_world_scan_state(state, now=1315.0)
        self.assertEqual(advanced.tile_index, 0)


class WorldScanRuntimeTests(unittest.IsolatedAsyncioTestCase):
    async def test_world_scan_busy_lock_returns_retryable_service_unavailable(self):
        previous_redis = api_main.redis_client
        api_main.redis_client = object()
        try:
            with patch(
                "api.main.configure_world_scan",
                new=AsyncMock(side_effect=api_main.LockError("busy")),
            ):
                with self.assertRaises(api_main.HTTPException) as raised:
                    await api_main.set_world_scan(
                        api_main.WorldScanCommand(enabled=True, dwell_seconds=360)
                    )
        finally:
            api_main.redis_client = previous_redis

        self.assertEqual(raised.exception.status_code, 503)
        self.assertIn("retry", raised.exception.detail.lower())

    async def test_ingestor_releases_coverage_lock_between_bounded_publish_chunks(self):
        class Pipeline:
            def setex(self, *_args): pass
            async def execute(self): return []

        class ChunkRedis:
            def __init__(self):
                self.token = b"first"
                self.entries = 0

            def pipeline(self): return Pipeline()

            def lock(self, *_args, **_kwargs):
                owner = self
                class Lock:
                    async def __aenter__(self):
                        owner.entries += 1
                        return self
                    async def __aexit__(self, *_args):
                        if owner.entries == 1:
                            owner.token = b"changed"
                        return False
                    async def extend(self, *_args, **_kwargs): return True
                return Lock()

        redis = ChunkRedis()
        producer = AsyncMock()
        messages = [
            (f"A{index:05X}".encode(), b"payload", {"icao": f"A{index:05X}"})
            for index in range(30)
        ]

        async def load_token(client):
            return object(), client.token

        with (
            patch(
                "ingestion.adsb_receiver.load_coverage_area_record",
                side_effect=load_token,
            ),
            patch("ingestion.adsb_receiver.asyncio.sleep", new=AsyncMock()) as handoff,
        ):
            published, completed = await adsb_receiver.publish_coverage_batch(
                redis, producer, messages, b"first"
            )

        self.assertFalse(completed)
        self.assertEqual(redis.entries, 2)
        self.assertEqual(published, adsb_receiver.COVERAGE_PUBLISH_CHUNK)
        handoff.assert_awaited_once_with(adsb_receiver.COVERAGE_LOCK_HANDOFF_SECONDS)

    async def test_ingestor_ticks_scanner_before_loading_cycle_coverage(self):
        redis = FakeRedis()
        area = world_scan.tile_area(3)
        with (
            patch("ingestion.adsb_receiver.tick_world_scan", AsyncMock()) as tick,
            patch(
                "ingestion.adsb_receiver.load_coverage_area_record",
                AsyncMock(return_value=(area, b"token")),
            ) as load,
        ):
            result = await adsb_receiver.prepare_scan_cycle(redis, now=2000.0)

        tick.assert_awaited_once_with(redis, now=2000.0)
        load.assert_awaited_once_with(redis)
        self.assertEqual(result, (area, b"token"))

    async def test_enabling_scan_immediately_selects_first_tile(self):
        redis = FakeRedis()

        state = await world_scan.configure_world_scan(
            redis, enabled=True, dwell_seconds=315, now=1000.0
        )

        area = await load_coverage_area(redis)
        self.assertTrue(state.enabled)
        self.assertEqual(state.tile_index, 0)
        self.assertEqual(area, world_scan.tile_area(0))
        self.assertIn(world_scan.WORLD_SCAN_REDIS_KEY, redis.values)
        self.assertIn(COVERAGE_REDIS_KEY, redis.values)

    async def test_runtime_tick_persists_next_tile(self):
        redis = FakeRedis()
        await world_scan.configure_world_scan(
            redis, enabled=True, dwell_seconds=315, now=1000.0
        )

        state = await world_scan.tick_world_scan(redis, now=1315.0)

        self.assertEqual(state.tile_index, 1)
        self.assertEqual(await load_coverage_area(redis), world_scan.tile_area(1))

    async def test_atomic_world_scan_write_cannot_leave_partial_state(self):
        class FailingAtomicRedis(FakeRedis):
            async def eval(self, *_args):
                raise RuntimeError("simulated Redis transaction failure")

        redis = FailingAtomicRedis()
        with self.assertRaisesRegex(RuntimeError, "transaction failure"):
            await world_scan.configure_world_scan(
                redis, enabled=True, dwell_seconds=315, now=1000.0
            )

        self.assertNotIn(world_scan.WORLD_SCAN_REDIS_KEY, redis.values)
        self.assertNotIn(COVERAGE_REDIS_KEY, redis.values)

    async def test_operator_api_enables_and_reports_world_scan(self):
        redis = FakeRedis()
        command = api_main.WorldScanCommand(enabled=True, dwell_seconds=360)
        with (
            patch("api.main.redis_client", redis),
            patch("api.main.time.time", return_value=1000.0),
        ):
            updated = await api_main.set_world_scan(command)
            status = await api_main.get_world_scan()

        self.assertTrue(updated["scan"]["enabled"])
        self.assertEqual(status["scan"]["dwell_seconds"], 360)
        self.assertEqual(status["tile_count"], len(world_scan.WORLD_SCAN_TILES))
        self.assertEqual(status["current_tile"]["label"], world_scan.tile_area(0).label)

    async def test_manual_coverage_selection_stops_world_scan(self):
        redis = FakeRedis()
        await world_scan.configure_world_scan(
            redis, enabled=True, dwell_seconds=360, now=1000.0
        )
        manual = CoverageArea(
            latitude=35.0, longitude=-80.0, radius_nm=100, label="Manual"
        )

        with patch("api.main.redis_client", redis):
            await api_main.update_coverage_area_config(manual)

        self.assertFalse((await world_scan.load_world_scan_state(redis)).enabled)
        self.assertEqual(await load_coverage_area(redis), manual)


if __name__ == "__main__":
    unittest.main()
