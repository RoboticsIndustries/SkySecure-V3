"""Tests for processing/watch_monitor.py detection logic and squawk plumbing."""
import time
import unittest
from pathlib import Path

from models import StateVector, Classification
from config import settings

from processing.watch_monitor import (
    WatchEvent,
    build_concentration_event,
    build_high_performance_event,
    build_squawk_event,
    build_transponder_off_event,
    cluster_military,
    concentration_cell,
    count_live_peers,
    is_disappearance_candidate,
    snapshot_from_sv,
)
from ingestion.adsb_receiver import _squawk_from_state, _parse_adsb_lol_states


NOW = 1_700_000_000.0


def make_sv(**overrides):
    base = dict(
        icao24="ABC123",
        callsign="TEST1",
        lat=30.0,
        lon=-85.0,
        altitude_baro=25_000,
        velocity=420.0,
        heading=90.0,
        vertical_rate=0,
        on_ground=False,
        military_score=0.0,
        last_seen=NOW,
        update_count=10,
    )
    base.update(overrides)
    return StateVector(**base)


def ledger_entry(**overrides):
    entry = snapshot_from_sv(make_sv(update_count=10))
    entry.update(overrides)
    return entry


class DisappearanceCandidateTests(unittest.TestCase):
    def test_airborne_silent_track_is_candidate(self):
        entry = ledger_entry(last_seen=NOW - settings.WATCH_SILENCE_SEC - 5)
        self.assertTrue(is_disappearance_candidate(entry, NOW))

    def test_recent_track_is_not_candidate(self):
        entry = ledger_entry(last_seen=NOW - 10)
        self.assertFalse(is_disappearance_candidate(entry, NOW))

    def test_low_altitude_excluded_as_landing(self):
        entry = ledger_entry(
            alt=800, last_seen=NOW - settings.WATCH_SILENCE_SEC - 5,
        )
        self.assertFalse(is_disappearance_candidate(entry, NOW))

    def test_slow_track_excluded_as_taxi(self):
        entry = ledger_entry(
            vel=30.0, last_seen=NOW - settings.WATCH_SILENCE_SEC - 5,
        )
        self.assertFalse(is_disappearance_candidate(entry, NOW))

    def test_briefly_tracked_excluded(self):
        entry = ledger_entry(
            updates=1, last_seen=NOW - settings.WATCH_SILENCE_SEC - 5,
        )
        self.assertFalse(is_disappearance_candidate(entry, NOW))

    def test_missing_position_excluded(self):
        entry = ledger_entry(
            lat=None, last_seen=NOW - settings.WATCH_SILENCE_SEC - 5,
        )
        self.assertFalse(is_disappearance_candidate(entry, NOW))


class PeerLivenessTests(unittest.TestCase):
    def test_counts_only_nearby_aircraft(self):
        entry = ledger_entry()
        near = [make_sv(icao24=f"AEB0{i:02d}", lat=30.05, lon=-85.05) for i in range(3)]
        far = [make_sv(icao24="BE0001", lat=35.0, lon=-90.0)]
        self.assertEqual(count_live_peers(entry, near + far, NOW), 3)

    def test_excludes_the_vanished_aircraft_itself(self):
        entry = ledger_entry()
        same = [make_sv(icao24=entry["icao24"])]
        self.assertEqual(count_live_peers(entry, same, NOW), 0)


class TransponderOffEventTests(unittest.TestCase):
    def test_event_contains_evidence_and_caveats(self):
        entry = ledger_entry(last_seen=NOW - 300, mil=0.9, cls="CONFIRMED_MILITARY")
        event = build_transponder_off_event(entry, peers=5, now=NOW)
        self.assertEqual(event.kind, "TRANSPONDER_OFF")
        self.assertEqual(event.severity, 4)  # military raises severity
        self.assertEqual(event.meta["live_peers_within_nm"]["count"], 5)
        self.assertEqual(event.meta["confidence"], "SUSPECTED")
        self.assertTrue(event.meta["caveats"])
        self.assertIn("deliberate transponder shutoff", event.summary)

    def test_civilian_severity_lower(self):
        entry = ledger_entry(last_seen=NOW - 300, mil=0.0)
        event = build_transponder_off_event(entry, peers=3, now=NOW)
        self.assertEqual(event.severity, 3)

    def test_event_id_deterministic(self):
        entry = ledger_entry(last_seen=NOW - 300)
        a = build_transponder_off_event(entry, 3, NOW)
        b = build_transponder_off_event(entry, 3, NOW)
        self.assertEqual(a.event_id, b.event_id)


class SquawkEventTests(unittest.TestCase):
    def test_emergency_squawks_flagged(self):
        for squawk, severity in (("7500", 5), ("7600", 4), ("7700", 5)):
            event = build_squawk_event(make_sv(squawk=squawk), NOW)
            self.assertIsNotNone(event, squawk)
            self.assertEqual(event.kind, "EMERGENCY_SQUAWK")
            self.assertEqual(event.severity, severity)
            self.assertEqual(event.meta["squawk"], squawk)

    def test_normal_squawk_ignored(self):
        self.assertIsNone(build_squawk_event(make_sv(squawk="1200"), NOW))
        self.assertIsNone(build_squawk_event(make_sv(squawk=None), NOW))


class HighPerformanceTests(unittest.TestCase):
    def test_fast_and_low_military_flagged(self):
        sv = make_sv(military_score=0.85, velocity=620.0, altitude_baro=8_000)
        event = build_high_performance_event(sv, NOW)
        self.assertIsNotNone(event)
        self.assertEqual(event.kind, "MIL_HIGH_PERFORMANCE")

    def test_extreme_climb_flagged(self):
        sv = make_sv(military_score=0.85, vertical_rate=7_500, altitude_baro=10_000)
        self.assertIsNotNone(build_high_performance_event(sv, NOW))

    def test_civilian_never_flagged(self):
        sv = make_sv(military_score=0.2, velocity=620.0, altitude_baro=8_000)
        self.assertIsNone(build_high_performance_event(sv, NOW))

    def test_ordinary_military_flight_not_flagged(self):
        sv = make_sv(military_score=0.85, velocity=350.0, altitude_baro=30_000)
        self.assertIsNone(build_high_performance_event(sv, NOW))

    def test_on_ground_not_flagged(self):
        sv = make_sv(military_score=0.85, velocity=620.0, altitude_baro=8_000, on_ground=True)
        self.assertIsNone(build_high_performance_event(sv, NOW))


class ConcentrationTests(unittest.TestCase):
    def test_nearby_military_clustered(self):
        cluster_in = [
            make_sv(icao24=f"AE{i:04d}", military_score=0.9,
                    lat=30.0 + i * 0.05, lon=-85.0)
            for i in range(4)
        ]
        civilian = make_sv(icao24="CE0001", military_score=0.1, lat=30.0, lon=-85.0)
        clusters = cluster_military(cluster_in + [civilian], 60.0)
        self.assertEqual(len(clusters), 1)
        self.assertEqual(len(clusters[0]), 4)

    def test_distant_military_not_clustered(self):
        far_apart = [
            make_sv(icao24=f"AE{i:04d}", military_score=0.9,
                    lat=30.0 + i * 5.0, lon=-85.0)
            for i in range(3)
        ]
        clusters = cluster_military(far_apart, 60.0)
        self.assertEqual(len(clusters), 3)

    def test_chain_merging(self):
        # A-B within radius, B-C within radius, A-C not: single cluster.
        a = make_sv(icao24="AA0001", military_score=0.9, lat=30.0, lon=-85.0)
        b = make_sv(icao24="AA0002", military_score=0.9, lat=30.5, lon=-85.0)
        c = make_sv(icao24="AA0003", military_score=0.9, lat=31.0, lon=-85.0)
        clusters = cluster_military([a, b, c], 45.0)
        self.assertEqual(len(clusters), 1)
        self.assertEqual(len(clusters[0]), 3)

    def test_cell_stable_for_same_cluster(self):
        cluster = [
            make_sv(icao24=f"AE{i:04d}", military_score=0.9,
                    lat=30.0 + i * 0.05, lon=-85.0)
            for i in range(4)
        ]
        self.assertEqual(concentration_cell(cluster), concentration_cell(cluster))

    def test_event_lists_members(self):
        cluster = [
            make_sv(icao24=f"AE{i:04d}", military_score=0.9,
                    lat=30.0 + i * 0.05, lon=-85.0)
            for i in range(5)
        ]
        event = build_concentration_event(cluster, concentration_cell(cluster), NOW)
        self.assertEqual(event.kind, "MIL_CONCENTRATION")
        self.assertEqual(event.meta["aircraft_count"], 5)
        self.assertEqual(len(event.meta["members"]), 5)


class SquawkParsingTests(unittest.TestCase):
    def test_opensky_slot_14(self):
        state = [None] * 17
        state[14] = "7700"
        self.assertEqual(_squawk_from_state(state), "7700")

    def test_adsb_lol_extension_slot_19(self):
        state = [None] * 20
        state[19] = "7600"
        self.assertEqual(_squawk_from_state(state), "7600")

    def test_invalid_squawks_rejected(self):
        for bad in ("8888", "12345", "77", "abcd", "", None):
            state = [None] * 20
            state[14] = bad
            state[19] = bad
            self.assertIsNone(_squawk_from_state(state), bad)

    def test_adsb_lol_parse_carries_squawk(self):
        data = {"ac": [{
            "hex": "abc123", "flight": "RCH001", "lat": 30.0, "lon": -85.0,
            "alt_baro": 25000, "gs": 420, "track": 90, "baro_rate": 0,
            "seen_pos": 5, "squawk": "7700",
        }]}
        states = _parse_adsb_lol_states(data, received_at=NOW)
        self.assertEqual(len(states), 1)
        self.assertEqual(states[0][19], "7700")


class StateVectorSquawkTests(unittest.TestCase):
    def test_squawk_survives_serialization(self):
        sv = make_sv(squawk="7700")
        restored = StateVector.from_bytes(sv.to_bytes())
        self.assertEqual(restored.squawk, "7700")

    def test_squawk_in_api_dict(self):
        sv = make_sv(squawk="7500")
        self.assertEqual(sv.to_api_dict()["sqk"], "7500")

    def test_old_payloads_without_squawk_still_load(self):
        sv = make_sv(squawk="7700")
        import orjson
        payload = orjson.loads(sv.to_bytes())
        del payload["squawk"]
        restored = StateVector.from_bytes(orjson.dumps(payload))
        self.assertIsNone(restored.squawk)


class DeploymentWiringTests(unittest.TestCase):
    def test_compose_defines_watch_monitor(self):
        compose = Path("docker-compose.yml").read_text()
        self.assertIn("watch-monitor:", compose)
        self.assertIn("python -m processing.watch_monitor", compose)

    def test_migration_registered_and_idempotent(self):
        compose = Path("docker-compose.yml").read_text()
        self.assertIn("003_watch_events.sql", compose)
        sql = Path("scripts/migrations/003_watch_events.sql").read_text()
        self.assertIn("CREATE TABLE IF NOT EXISTS watch_events", sql)
        self.assertIn("CREATE UNIQUE INDEX IF NOT EXISTS", sql)


if __name__ == "__main__":
    unittest.main()
