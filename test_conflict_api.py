import unittest
from unittest.mock import patch

import orjson

import api.main as api_main


def _valid_shared_snapshot():
    snapshot = api_main._unavailable_conflict_snapshot("test")
    snapshot.update({
        "generated_at": 1_000.0,
        "analysis_available": True,
        "evaluated_tracks": 2,
        "evaluated_aircraft": ["AAA001", "BBB002"],
        "candidate_pairs": 1,
        "total_possible_pairs": 1,
        "spatial_comparisons": 1,
        "conflicts": [{
            "conflict_id": "0123456789abcdef",
            "aircraft": ["AAA001", "BBB002"],
            "pair": ["AAA001", "BBB002"],
            "observations": [
                {"icao": "AAA001", "source": "ADSB", "observed_at": 999.0,
                 "age_seconds": 1.0},
                {"icao": "BBB002", "source": "ADSB", "observed_at": 999.0,
                 "age_seconds": 1.0},
            ],
            "severity": "TRAFFIC_CONFLICT",
            "tcpa_seconds": 30.0,
            "current_horizontal_nm": 6.0,
            "current_vertical_ft": 500.0,
            "predicted_horizontal_nm": 1.0,
            "predicted_vertical_ft": 100.0,
            "time_to_threshold_seconds": 20.0,
            "evaluation_time": 1_000.0,
            "input_time_skew_seconds": 0.0,
            "operational_advisory": False,
            "basis": "public_state_vector_projection",
            "lifecycle": "ACTIVE",
        }],
    })
    snapshot.pop("unavailable_reason")
    return snapshot


class ConflictApiTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    def crossing_tracks():
        return [
            {
                "icao": "AAA001", "lat": 0.0, "lon": -0.05,
                "alt": 10_000, "vel": 300.0, "hdg": 90.0, "vr": 0,
                "ts": 1_000.0, "gnd": False,
            },
            {
                "icao": "BBB002", "lat": -0.05, "lon": 0.0,
                "alt": 10_000, "vel": 300.0, "hdg": 0.0, "vr": 0,
                "ts": 1_000.0, "gnd": False,
            },
        ]

    def test_snapshot_message_includes_passive_conflict_analysis(self):
        builder = getattr(api_main, "_build_snapshot_message", None)
        if builder is None:
            self.fail("snapshot conflict integration is not implemented")

        analysis = {
            "mode": "PASSIVE_NON_OPERATIONAL",
            "conflicts": [{"conflict_id": "pair", "operational_advisory": False}],
        }
        message = builder(
            self.crossing_tracks(), now=1_000.0, conflict_analysis=analysis,
        )

        self.assertEqual(message["type"], "snapshot")
        self.assertEqual(message["count"], 2)
        delivered = message["conflict_analysis"]
        self.assertEqual(delivered["mode"], "PASSIVE_NON_OPERATIONAL")
        self.assertEqual(len(delivered["conflicts"]), 1)
        self.assertFalse(delivered["conflicts"][0]["operational_advisory"])

    def test_snapshot_builder_can_reuse_conflict_analysis_without_recomputation(self):
        builder = api_main._build_snapshot_message
        cached = {"mode": "PASSIVE_NON_OPERATIONAL", "conflicts": [{"conflict_id": "cached"}]}

        message = builder(
            self.crossing_tracks(), now=1_000.0, conflict_analysis=cached
        )

        self.assertIs(message["conflict_analysis"], cached)

    async def test_conflict_endpoint_returns_latest_bounded_snapshot(self):
        endpoint = getattr(api_main, "get_conflicts", None)
        if endpoint is None:
            self.fail("conflict endpoint is not implemented")
        shared = _valid_shared_snapshot()
        class FakeRedis:
            key = None

            async def get(self, key):
                self.key = key
                return orjson.dumps(shared)

        previous_redis = api_main.redis_client
        fake = FakeRedis()
        api_main.redis_client = fake
        try:
            response = await endpoint()
        finally:
            api_main.redis_client = previous_redis

        self.assertEqual(fake.key, "conflict:latest")
        self.assertEqual(response["layer"], "L2")
        self.assertEqual(response["conflicts"], shared["conflicts"])
        self.assertFalse(response["truncated"])

    async def test_global_endpoint_does_not_replace_websocket_scoped_cache(self):
        shared = _valid_shared_snapshot()
        scoped = {
            "scope": "coverage_filtered",
            "conflicts": [],
            "analysis_available": True,
        }

        class FakeRedis:
            async def get(self, _key):
                return orjson.dumps(shared)

        with (
            patch.object(api_main, "redis_client", FakeRedis()),
            patch.object(api_main, "_conflict_snapshot", scoped),
        ):
            response = await api_main.get_conflicts()
            self.assertEqual(response, shared)
            self.assertIs(api_main._conflict_snapshot, scoped)

    def test_websocket_conflicts_are_filtered_to_published_track_snapshot(self):
        helper = getattr(api_main, "_conflicts_for_tracks", None)
        if helper is None:
            self.fail("coverage-scoped conflict projection is not implemented")
        analysis = {
            "mode": "PASSIVE_NON_OPERATIONAL",
            "analysis_available": True,
            "conflicts": [
                {"conflict_id": "inside", "pair": ["AAA001", "BBB002"]},
                {"conflict_id": "outside", "pair": ["AAA001", "CCC003"]},
            ],
        }

        scoped = helper(analysis, [{"icao": "AAA001"}, {"icao": "BBB002"}])

        self.assertEqual(
            [conflict["conflict_id"] for conflict in scoped["conflicts"]],
            ["inside"],
        )
        self.assertEqual(scoped["scope"], "coverage_filtered")

    def test_unavailable_snapshot_preserves_conflict_schema_contract(self):
        with patch.object(api_main, "_conflict_snapshot", {"conflicts": []}):
            snapshot = api_main._unavailable_conflict_snapshot(
                "monitor_snapshot_unavailable"
            )
        self.assertFalse(snapshot["analysis_available"])
        self.assertEqual(snapshot["unavailable_reason"], "monitor_snapshot_unavailable")
        for field in (
            "total_possible_pairs",
            "spatial_comparisons",
            "truncated",
            "limits",
            "skipped_tracks",
            "skipped_pairs",
        ):
            self.assertIn(field, snapshot)

    def test_monitor_and_api_unavailable_snapshots_share_contract(self):
        from processing.conflict_monitor import _unavailable_snapshot as monitor_unavailable

        api_snapshot = api_main._unavailable_conflict_snapshot("test")
        monitor_snapshot = monitor_unavailable(1_000.0, "test")

        self.assertEqual(set(api_snapshot), set(monitor_snapshot))
        self.assertEqual(api_snapshot["limits"], monitor_snapshot["limits"])
        self.assertEqual(api_snapshot["evidence_scope"], monitor_snapshot["evidence_scope"])

    async def test_shared_snapshot_redis_read_error_fails_closed(self):
        class FailingRedis:
            async def get(self, _key):
                raise ConnectionError("redis unavailable")

        with patch.object(api_main, "redis_client", FailingRedis()):
            snapshot = await api_main._load_shared_conflict_snapshot()
        self.assertFalse(snapshot["analysis_available"])
        self.assertEqual(snapshot["unavailable_reason"], "conflict_snapshot_read_failed")
        self.assertEqual(snapshot["conflicts"], [])

    async def test_shared_snapshot_rejects_incomplete_available_schema(self):
        class FakeRedis:
            async def get(self, _key):
                return orjson.dumps({
                    "mode": "PASSIVE_NON_OPERATIONAL",
                    "layer": "L2",
                    "detector": "aircraft_conflict_projection",
                    "analysis_available": True,
                    "conflicts": [],
                })

        with patch.object(api_main, "redis_client", FakeRedis()):
            snapshot = await api_main._load_shared_conflict_snapshot()
        self.assertFalse(snapshot["analysis_available"])
        self.assertEqual(snapshot["unavailable_reason"], "invalid_conflict_snapshot_schema")

    async def test_shared_snapshot_rejects_invalid_counter_types_and_ranges(self):
        base = api_main._unavailable_conflict_snapshot("test")
        base["analysis_available"] = True
        base.pop("unavailable_reason")
        invalid_values = (
            ("candidate_pairs", -1),
            ("evaluated_tracks", True),
            ("evaluated_aircraft", "AAA001"),
            ("limits", {"max_candidate_pairs": 100_000}),
        )

        for field, value in invalid_values:
            with self.subTest(field=field, value=value):
                payload = dict(base)
                payload[field] = value

                class FakeRedis:
                    async def get(self, _key):
                        return orjson.dumps(payload)

                with patch.object(api_main, "redis_client", FakeRedis()):
                    snapshot = await api_main._load_shared_conflict_snapshot()
                self.assertFalse(snapshot["analysis_available"])
                self.assertEqual(
                    snapshot["unavailable_reason"],
                    "invalid_conflict_snapshot_schema",
                )

    async def test_shared_snapshot_rejects_incomplete_conflict_record(self):
        shared = api_main._unavailable_conflict_snapshot("test")
        shared.update({
            "generated_at": 1_000.0,
            "analysis_available": True,
            "evaluated_tracks": 2,
            "evaluated_aircraft": ["AAA001", "BBB002"],
            "candidate_pairs": 1,
            "total_possible_pairs": 1,
            "spatial_comparisons": 1,
            "conflicts": [{
                "conflict_id": "incomplete",
                "operational_advisory": False,
            }],
        })
        shared.pop("unavailable_reason")

        class FakeRedis:
            async def get(self, _key):
                return orjson.dumps(shared)

        with patch.object(api_main, "redis_client", FakeRedis()):
            snapshot = await api_main._load_shared_conflict_snapshot()
        self.assertFalse(snapshot["analysis_available"])
        self.assertEqual(
            snapshot["unavailable_reason"],
            "invalid_conflict_snapshot_schema",
        )

    async def test_shared_snapshot_rejects_invalid_nested_conflict_fields(self):
        invalid_values = (
            ("observations", []),
            ("severity", "RESOLUTION_ADVISORY"),
            ("tcpa_seconds", -1.0),
            ("basis", "onboard_tcas"),
            ("lifecycle", "CLEARED"),
        )
        for field, value in invalid_values:
            with self.subTest(field=field, value=value):
                shared = _valid_shared_snapshot()
                shared["conflicts"][0][field] = value

                class FakeRedis:
                    async def get(self, _key):
                        return orjson.dumps(shared)

                with patch.object(api_main, "redis_client", FakeRedis()):
                    snapshot = await api_main._load_shared_conflict_snapshot()
                self.assertFalse(snapshot["analysis_available"])
                self.assertEqual(
                    snapshot["unavailable_reason"],
                    "invalid_conflict_snapshot_schema",
                )

    async def test_shared_snapshot_rejects_operational_advisory_semantics(self):
        shared = _valid_shared_snapshot()
        shared["conflicts"][0]["operational_advisory"] = True

        class FakeRedis:
            async def get(self, _key):
                return orjson.dumps(shared)

        previous = api_main.redis_client
        api_main.redis_client = FakeRedis()
        try:
            result = await api_main._load_shared_conflict_snapshot()
        finally:
            api_main.redis_client = previous
        self.assertFalse(result.get("analysis_available", True))
        self.assertEqual(
            result.get("unavailable_reason"),
            "invalid_conflict_snapshot_schema",
        )

    async def test_shared_snapshot_rejects_oversized_value_before_parsing(self):
        class FakeRedis:
            async def get(self, _key):
                return orjson.dumps({
                    "mode": "PASSIVE_NON_OPERATIONAL",
                    "conflicts": [],
                    "padding": "x" * 1_100_000,
                })

        previous = api_main.redis_client
        api_main.redis_client = FakeRedis()
        try:
            result = await api_main._load_shared_conflict_snapshot()
        finally:
            api_main.redis_client = previous
        self.assertFalse(result.get("analysis_available", True))
        self.assertEqual(result.get("unavailable_reason"), "monitor_snapshot_too_large")

    async def test_layer_two_summary_keeps_pair_analysis_separate(self):
        previous = api_main._conflict_snapshot
        api_main._conflict_snapshot = {
            "mode": "PASSIVE_NON_OPERATIONAL",
            "layer": "L2",
            "detector": "aircraft_conflict_projection",
            "evaluated_tracks": 4,
            "candidate_pairs": 3,
            "conflicts": [
                {"severity": "MONITOR"},
                {"severity": "TRAFFIC_CONFLICT"},
            ],
            "truncated": False,
        }
        try:
            from unittest.mock import patch
            with patch.object(api_main, "_load_state_vectors", return_value=[]):
                response = await api_main.get_layer_summary()
        finally:
            api_main._conflict_snapshot = previous

        relational = response["layers"]["L2"].get("relational_analysis")
        self.assertEqual(relational, {
            "detector": "aircraft_conflict_projection",
            "mode": "PASSIVE_NON_OPERATIONAL",
            "evaluated_tracks": 4,
            "candidate_pairs": 3,
            "conflict_count": 2,
            "severity_counts": {"MONITOR": 1, "TRAFFIC_CONFLICT": 1},
            "truncated": False,
        })


if __name__ == "__main__":
    unittest.main()
