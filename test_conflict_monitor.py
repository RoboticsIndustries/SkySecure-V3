import unittest
from pathlib import Path
from unittest.mock import patch

import orjson

from models import DataSource, SourceReport, StateVector

COMPOSE = Path("docker-compose.yml").read_text()


class LeaseEmu:
    """Emulates the monitor owner lease and fenced publish Lua scripts."""
    owner = None
    published = {}

    async def set(self, key, value, ex=None, nx=False):
        if nx:
            if self.owner is not None:
                return False
            self.owner = value
            return True
        self.published[key] = (value, ex)
        return True

    async def eval(self, script, numkeys, *args):
        if numkeys == 1:  # REFRESH_OWNER_SCRIPT
            return int(self.owner == args[1])
        # FENCED_PUBLISH_SCRIPT: keys owner/lifecycle/snapshot, then argv
        if self.owner != args[3]:
            return 0
        self.published[args[1]] = (args[4], args[6])
        self.published[args[2]] = (args[5], args[7])
        return 1


class ConflictMonitorInputTests(unittest.IsolatedAsyncioTestCase):
    def test_fresh_partial_report_does_not_refresh_old_complete_kinematics(self):
        try:
            from processing.conflict_monitor import coherent_adsb_track
        except ModuleNotFoundError:
            self.fail("processing.conflict_monitor is not implemented")

        state = StateVector(
            icao24="ABC123",
            lat=51.0,
            lon=-1.0,
            altitude_baro=10_000,
            velocity=300,
            heading=90,
            vertical_rate=0,
            last_seen=1_000.0,
            source_reports=[
                SourceReport(
                    source=DataSource.ADSB,
                    lat=51.0,
                    lon=-1.0,
                    altitude=10_000,
                    velocity=300,
                    heading=90,
                    vertical_rate=0,
                    on_ground=False,
                    timestamp=900.0,
                ),
                SourceReport(
                    source=DataSource.ADSB,
                    velocity=310,
                    heading=91,
                    timestamp=1_000.0,
                ),
            ],
        )

        track, reason = coherent_adsb_track(state, now=1_000.0)

        self.assertIsNone(track)
        self.assertEqual(reason, "stale_complete_source_report")

    def test_monitor_builds_snapshot_only_from_coherent_source_reports(self):
        try:
            from processing.conflict_monitor import build_conflict_snapshot
        except ImportError:
            self.fail("conflict monitor snapshot builder is not implemented")

        def state(icao, lat, lon, heading):
            return StateVector(
                icao24=icao,
                source_reports=[SourceReport(
                    source=DataSource.ADSB,
                    lat=lat,
                    lon=lon,
                    altitude=10_000,
                    velocity=300,
                    heading=heading,
                    vertical_rate=0,
                    on_ground=False,
                    timestamp=1_000.0,
                )],
            )

        snapshot = build_conflict_snapshot([
            state("AAA001", 0.0, -0.05, 90.0),
            state("BBB002", -0.05, 0.0, 0.0),
        ], now=1_000.0)

        self.assertTrue(snapshot.get("analysis_available"))
        self.assertEqual(snapshot["evidence_scope"], "coherent_adsb_source_reports")
        self.assertEqual(len(snapshot["conflicts"]), 1)
        self.assertEqual(snapshot["monitor_skipped_tracks"], {})

    def test_all_ineligible_source_reports_make_analysis_unavailable(self):
        from processing.conflict_monitor import build_conflict_snapshot

        stale = StateVector(
            icao24="AAA001",
            source_reports=[SourceReport(
                source=DataSource.ADSB,
                lat=0.0,
                lon=0.0,
                altitude=10_000,
                velocity=300,
                heading=90,
                vertical_rate=0,
                on_ground=False,
                timestamp=900.0,
            )],
        )

        snapshot = build_conflict_snapshot([stale], now=1_000.0)

        self.assertFalse(snapshot["analysis_available"])
        self.assertEqual(snapshot["unavailable_reason"], "no_eligible_conflict_tracks")
        self.assertEqual(snapshot["evaluated_aircraft"], [])

    async def test_monitor_publishes_shared_snapshot_with_ttl(self):
        try:
            from processing.conflict_monitor import publish_conflict_snapshot
        except ImportError:
            self.fail("conflict monitor publication is not implemented")

        class FakeRedis:
            call = None

            async def set(self, key, value, ex=None):
                self.call = (key, value, ex)

        redis = FakeRedis()
        snapshot = {
            "mode": "PASSIVE_NON_OPERATIONAL",
            "analysis_available": True,
            "conflicts": [],
        }

        await publish_conflict_snapshot(redis, snapshot)

        if redis.call is None:
            self.fail("monitor did not publish a shared snapshot")
        key, raw, ttl = redis.call
        self.assertEqual(key, "conflict:latest")
        self.assertEqual(orjson.loads(raw), snapshot)
        self.assertEqual(ttl, 30)

    async def test_monitor_cycle_reads_only_fusion_state_and_publishes_result(self):
        try:
            from processing.conflict_monitor import run_cycle
        except ImportError:
            self.fail("conflict monitor runtime cycle is not implemented")

        def state(icao, lat, lon, heading):
            return StateVector(
                icao24=icao,
                source_reports=[SourceReport(
                    source=DataSource.ADSB,
                    lat=lat,
                    lon=lon,
                    altitude=10_000,
                    velocity=300,
                    heading=heading,
                    vertical_rate=0,
                    on_ground=False,
                    timestamp=1_000.0,
                )],
            )

        records = {
            b"fusion:sv:AAA001": state("AAA001", 0.0, -0.05, 90.0).to_bytes(),
            b"fusion:sv:BBB002": state("BBB002", -0.05, 0.0, 0.0).to_bytes(),
        }

        class FakeRedis(LeaseEmu):
            published = {}
            scan_match = None

            async def scan(self, cursor, match=None, count=None):
                self.scan_match = match
                return 0, list(records)

            async def mget(self, keys):
                return [records[key] for key in keys]

            async def get(self, key):
                return None

        redis = FakeRedis()
        snapshot = await run_cycle(redis, now=1_000.0)

        self.assertEqual(redis.scan_match, "fusion:sv:*")
        self.assertTrue(snapshot["analysis_available"])
        self.assertEqual(len(snapshot["conflicts"]), 1)
        self.assertIn("conflict:latest", redis.published)
        self.assertIn("conflict:lifecycle", redis.published)
        lifecycle, ttl = redis.published["conflict:lifecycle"]
        self.assertLessEqual(len(orjson.loads(lifecycle)), 500)
        self.assertEqual(ttl, 120)

    async def test_monitor_reads_fusion_values_in_bounded_batches(self):
        from processing.conflict_monitor import run_cycle

        keys = [f"fusion:sv:{index:06X}".encode() for index in range(33)]

        class FakeRedis(LeaseEmu):
            mget_sizes = []

            async def scan(self, cursor, match=None, count=None):
                return 0, keys

            async def mget(self, batch):
                self.mget_sizes.append(len(batch))
                return [None] * len(batch)

            async def get(self, key):
                return None

        redis = FakeRedis()
        await run_cycle(redis, now=1_000.0)

        self.assertGreater(len(redis.mget_sizes), 1)
        self.assertLessEqual(max(redis.mget_sizes), 16)

    async def test_empty_fusion_keyspace_is_unavailable_and_holds_prior_conflict(self):
        from processing.conflict_monitor import run_cycle

        prior = {
            "pair": {
                "hits": 2,
                "misses": 0,
                "active": True,
                "conflict": {
                    "conflict_id": "pair",
                    "aircraft": ["AAA001", "BBB002"],
                    "operational_advisory": False,
                },
            },
        }

        class FakeRedis(LeaseEmu):
            async def scan(self, cursor, match=None, count=None):
                return 0, []

            async def get(self, key):
                return orjson.dumps(prior) if key == "conflict:lifecycle" else None

        result = await run_cycle(FakeRedis(), now=1_000.0)

        self.assertFalse(result["analysis_available"])
        self.assertEqual(result["unavailable_reason"], "no_fusion_states")
        self.assertEqual(result["conflicts"][0]["lifecycle"], "STALE_HOLD")

    async def test_malformed_fusion_state_fails_entire_cycle_closed(self):
        from processing.conflict_monitor import run_cycle

        class FakeRedis(LeaseEmu):
            async def scan(self, cursor, match=None, count=None):
                return 0, [b"fusion:sv:AAA001"]

            async def mget(self, keys):
                return [b"not-a-state-vector"]

            async def get(self, key):
                return None

        result = await run_cycle(FakeRedis(), now=1_000.0)

        self.assertFalse(result["analysis_available"])
        self.assertEqual(result["unavailable_reason"], "malformed_fusion_states")

    async def test_monitor_lease_allows_one_owner_and_same_token_refresh(self):
        from processing import conflict_monitor as monitor

        class FakeRedis:
            owner = None

            async def set(self, key, value, ex=None, nx=False):
                if nx and self.owner is not None:
                    return False
                self.owner = value
                return True

            async def eval(self, script, numkeys, *args):
                token = args[1]
                return int(self.owner == token)

        redis = FakeRedis()
        self.assertTrue(await monitor._acquire_monitor_lease(redis, "owner-a"))
        self.assertTrue(await monitor._acquire_monitor_lease(redis, "owner-a"))
        self.assertFalse(await monitor._acquire_monitor_lease(redis, "owner-b"))

    async def test_fenced_publish_rejects_non_owner_without_state_writes(self):
        from processing import conflict_monitor as monitor

        class FakeRedis:
            direct_sets = []
            eval_calls = []

            async def get(self, key):
                return None

            async def set(self, *args, **kwargs):
                self.direct_sets.append((args, kwargs))

            async def eval(self, script, numkeys, *args):
                self.eval_calls.append((script, numkeys, args))
                return 0

        redis = FakeRedis()
        snapshot = monitor._unavailable_snapshot(1_000.0, "test")
        result = await monitor._publish_with_lifecycle(
            redis, snapshot, now=1_000.0, owner_token="stale-owner",
        )

        self.assertFalse(result["analysis_available"])
        self.assertEqual(result["unavailable_reason"], "monitor_ownership_lost")
        self.assertEqual(redis.direct_sets, [])
        self.assertEqual(len(redis.eval_calls), 1)

    async def test_oversized_lifecycle_value_is_rejected_before_json_parse(self):
        from processing import conflict_monitor as monitor

        class FakeRedis:
            async def get(self, key):
                return b"x" * 600_000

            async def set(self, key, value, ex=None):
                return None

            async def eval(self, script, numkeys, *args):
                return 1

        snapshot = monitor._unavailable_snapshot(1_000.0, "test")
        with patch.object(monitor.orjson, "loads", wraps=monitor.orjson.loads) as loads:
            await monitor._publish_with_lifecycle(
                FakeRedis(), snapshot, now=1_000.0, owner_token="owner-a",
            )
        loads.assert_not_called()

    def test_compose_declares_dedicated_conflict_monitor_service(self):
        self.assertIn("  conflict-monitor:", COMPOSE)
        self.assertIn("command: python -m processing.conflict_monitor", COMPOSE)
        monitor = COMPOSE.split("  conflict-monitor:", 1)[1].split("\n  api:", 1)[0]
        self.assertIn("restart: unless-stopped", monitor)
        self.assertIn("redis:\n        condition: service_healthy", monitor)
        self.assertNotIn("ports:", monitor)

    def test_lifecycle_requires_confirmation_unless_entry_is_immediate(self):
        try:
            from processing.conflict_monitor import apply_conflict_lifecycle
        except ImportError:
            self.fail("conflict lifecycle is not implemented")

        def snapshot(entry_seconds):
            return {
                "analysis_available": True,
                "conflicts": [{
                    "conflict_id": "pair",
                    "severity": "TRAFFIC_CONFLICT",
                    "time_to_threshold_seconds": entry_seconds,
                }],
            }

        first, state = apply_conflict_lifecycle(snapshot(60.0), {}, now=1_000.0)
        second, state = apply_conflict_lifecycle(snapshot(55.0), state, now=1_005.0)
        immediate, _ = apply_conflict_lifecycle(snapshot(15.0), {}, now=1_000.0)

        self.assertEqual(first["conflicts"], [])
        self.assertEqual(second["conflicts"][0]["lifecycle"], "ACTIVE")
        self.assertEqual(immediate["conflicts"][0]["lifecycle"], "ACTIVE")

    def test_lifecycle_holds_pair_when_one_aircraft_was_not_evaluated(self):
        from processing.conflict_monitor import apply_conflict_lifecycle

        prior = {
            "pair": {
                "hits": 2,
                "misses": 0,
                "active": True,
                "conflict": {
                    "conflict_id": "pair",
                    "pair": ["AAA001", "BBB002"],
                    "operational_advisory": False,
                },
            },
        }
        partial_coverage = {
            "analysis_available": True,
            "evaluated_aircraft": ["AAA001"],
            "conflicts": [],
        }

        rendered, state = apply_conflict_lifecycle(
            partial_coverage, prior, now=1_005.0,
        )

        self.assertEqual(rendered["conflicts"][0]["lifecycle"], "STALE_HOLD")
        self.assertEqual(rendered["conflicts"][0]["data_status"], "INSUFFICIENT_DATA")
        self.assertEqual(state["pair"]["misses"], 0)

    def test_lifecycle_requires_three_clear_cycles_and_holds_on_outage(self):
        from processing.conflict_monitor import apply_conflict_lifecycle

        hit = {
            "analysis_available": True,
            "evaluated_aircraft": ["AAA001", "BBB002"],
            "conflicts": [{
                "conflict_id": "pair",
                "pair": ["AAA001", "BBB002"],
                "severity": "TRAFFIC_CONFLICT",
                "time_to_threshold_seconds": 10.0,
            }],
        }
        clear = {
            "analysis_available": True,
            "evaluated_aircraft": ["AAA001", "BBB002"],
            "conflicts": [],
        }
        unavailable = {"analysis_available": False, "conflicts": []}

        _, state = apply_conflict_lifecycle(hit, {}, now=1_000.0)
        held, same_state = apply_conflict_lifecycle(unavailable, state, now=1_005.0)
        first, state = apply_conflict_lifecycle(clear, same_state, now=1_010.0)
        second, state = apply_conflict_lifecycle(clear, state, now=1_015.0)
        third, state = apply_conflict_lifecycle(clear, state, now=1_020.0)

        self.assertEqual(held["conflicts"][0]["lifecycle"], "STALE_HOLD")
        self.assertEqual(first["conflicts"][0]["lifecycle"], "CLEARING")
        self.assertEqual(second["conflicts"][0]["lifecycle"], "CLEARING")
        self.assertEqual(third["conflicts"], [])
        self.assertEqual(state, {})


if __name__ == "__main__":
    unittest.main()
