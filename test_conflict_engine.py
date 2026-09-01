import unittest


class ConflictEngineTests(unittest.TestCase):
    def test_crossing_tracks_predict_loss_of_separation_without_advisory_command(self):
        try:
            from processing.conflict_engine import analyze_conflicts
        except ModuleNotFoundError:
            self.fail("processing.conflict_engine is not implemented")

        tracks = [
            {
                "icao": "AAA001", "lat": 0.0, "lon": -0.05,
                "alt": 10_000, "vel": 300.0, "hdg": 90.0, "vr": 0,
                "ts": 1_000.0, "gnd": False, "src": "opensky",
            },
            {
                "icao": "BBB002", "lat": -0.05, "lon": 0.0,
                "alt": 10_000, "vel": 300.0, "hdg": 0.0, "vr": 0,
                "ts": 1_000.0, "gnd": False, "src": "adsb_lol",
            },
        ]

        result = analyze_conflicts(tracks, now=1_000.0)

        self.assertEqual(result.get("layer"), "L2")
        self.assertEqual(result.get("detector"), "aircraft_conflict_projection")
        self.assertEqual(result.get("evidence_scope"), "public_state_vectors")
        self.assertEqual(result["evaluated_tracks"], 2)
        self.assertEqual(result["candidate_pairs"], 1)
        self.assertEqual(len(result["conflicts"]), 1)
        conflict = result["conflicts"][0]
        self.assertEqual(conflict["aircraft"], ["AAA001", "BBB002"])
        self.assertEqual(conflict.get("pair"), ["AAA001", "BBB002"])
        self.assertEqual(conflict.get("observations"), [
            {"icao": "AAA001", "source": "opensky", "observed_at": 1_000.0, "age_seconds": 0.0},
            {"icao": "BBB002", "source": "adsb_lol", "observed_at": 1_000.0, "age_seconds": 0.0},
        ])
        self.assertEqual(conflict["severity"], "PREDICTED_LOSS_OF_SEPARATION")
        self.assertLess(conflict["predicted_horizontal_nm"], 0.2)
        self.assertEqual(conflict["predicted_vertical_ft"], 0.0)
        self.assertGreater(conflict["tcpa_seconds"], 20.0)
        self.assertLess(conflict["tcpa_seconds"], 60.0)
        self.assertFalse(conflict["operational_advisory"])
        self.assertNotIn("command", conflict)

    def test_diverging_tracks_are_not_predicted_as_a_future_conflict(self):
        from processing.conflict_engine import analyze_conflicts

        tracks = [
            {
                "icao": "AAA001", "lat": 0.0, "lon": -0.1,
                "alt": 10_000, "vel": 300.0, "hdg": 270.0, "vr": 0,
                "ts": 1_000.0, "gnd": False,
            },
            {
                "icao": "BBB002", "lat": -0.05, "lon": 0.0,
                "alt": 10_000, "vel": 300.0, "hdg": 180.0, "vr": 0,
                "ts": 1_000.0, "gnd": False,
            },
        ]

        result = analyze_conflicts(tracks, now=1_000.0)

        self.assertEqual(result["candidate_pairs"], 1)
        self.assertEqual(result["conflicts"], [])

    def test_unsuitable_public_tracks_are_explicitly_skipped(self):
        from processing.conflict_engine import analyze_conflicts

        valid = {
            "icao": "AAA001", "lat": 10.0, "lon": 10.0,
            "alt": 10_000, "vel": 250.0, "hdg": 90.0, "vr": 0,
            "ts": 1_000.0, "gnd": False,
        }
        tracks = [
            valid,
            {**valid, "icao": "BBB002", "ts": 960.0},
            {**valid, "icao": "CCC003", "gnd": True},
            {**valid, "icao": "DDD004", "alt": None},
            {**valid, "icao": "NOT-ICAO"},
        ]

        result = analyze_conflicts(tracks, now=1_000.0)

        self.assertEqual(result["evaluated_tracks"], 1)
        self.assertEqual(result["candidate_pairs"], 0)
        self.assertEqual(result["skipped_tracks"], {
            "stale": 1,
            "on_ground": 1,
            "incomplete": 1,
            "invalid_identity": 1,
        })

    def test_missing_vertical_rate_is_insufficient_data_not_level_flight(self):
        from processing.conflict_engine import analyze_conflicts

        tracks = [
            {
                "icao": "AAA001", "lat": 0.0, "lon": -0.05,
                "alt": 10_000, "vel": 300.0, "hdg": 90.0, "vr": None,
                "ts": 1_000.0, "gnd": False,
            },
            {
                "icao": "BBB002", "lat": -0.05, "lon": 0.0,
                "alt": 10_000, "vel": 300.0, "hdg": 0.0, "vr": 0,
                "ts": 1_000.0, "gnd": False,
            },
        ]

        result = analyze_conflicts(tracks, now=1_000.0)

        self.assertEqual(result["evaluated_tracks"], 1)
        self.assertEqual(result["conflicts"], [])
        self.assertEqual(result["skipped_tracks"], {"incomplete": 1})

    def test_worldwide_tracks_do_not_create_quadratic_candidate_pairs(self):
        from processing.conflict_engine import analyze_conflicts

        tracks = [
            {
                "icao": f"{index:06X}", "lat": -70.0 + index * 5.0,
                "lon": -120.0 if index % 2 else 120.0,
                "alt": 30_000, "vel": 450.0, "hdg": 90.0, "vr": 0,
                "ts": 1_000.0, "gnd": False,
            }
            for index in range(30)
        ]

        result = analyze_conflicts(tracks, now=1_000.0)

        self.assertEqual(result["evaluated_tracks"], 30)
        self.assertEqual(result["candidate_pairs"], 0)
        self.assertEqual(result["total_possible_pairs"], 435)
        self.assertLess(result["spatial_comparisons"], 100)

    def test_tracks_are_projected_to_a_common_event_time_before_cpa(self):
        from processing.conflict_engine import analyze_conflicts

        tracks = [
            {
                "icao": "AAA001", "lat": 0.0, "lon": -0.05,
                "alt": 10_000, "vel": 300.0, "hdg": 90.0, "vr": 0,
                "ts": 995.0, "gnd": False,
            },
            {
                "icao": "BBB002", "lat": -0.05, "lon": 0.0,
                "alt": 10_000, "vel": 300.0, "hdg": 0.0, "vr": 0,
                "ts": 1_000.0, "gnd": False,
            },
        ]

        conflict = analyze_conflicts(tracks, now=1_000.0)["conflicts"][0]

        self.assertEqual(conflict.get("evaluation_time"), 1_000.0)
        self.assertEqual(conflict.get("input_time_skew_seconds"), 5.0)
        self.assertAlmostEqual(conflict["tcpa_seconds"], 33.5, delta=0.2)
        self.assertAlmostEqual(conflict["predicted_horizontal_nm"], 0.295, delta=0.01)

    def test_dense_snapshot_respects_explicit_pair_and_output_budgets(self):
        from processing.conflict_engine import analyze_conflicts

        tracks = [
            {
                "icao": f"{index + 1:06X}", "lat": 35.0,
                "lon": -80.0 + index * 0.001, "alt": 10_000,
                "vel": 250.0, "hdg": 90.0 if index % 2 else 270.0,
                "vr": 0, "ts": 1_000.0, "gnd": False,
            }
            for index in range(20)
        ]

        try:
            result = analyze_conflicts(
                tracks, now=1_000.0, max_candidate_pairs=10, max_conflicts=3
            )
        except TypeError:
            self.fail("analyze_conflicts does not expose bounded pair/output budgets")

        self.assertEqual(result["candidate_pairs"], 10)
        self.assertEqual(result["conflicts"], [])
        self.assertFalse(result.get("analysis_available", True))
        self.assertEqual(result.get("unavailable_reason"), "candidate_pair_limit_exceeded")
        self.assertTrue(result["truncated"])
        self.assertEqual(result["limits"], {
            "max_candidate_pairs": 10,
            "max_conflicts": 3,
        })

    def test_dateline_pair_uses_short_horizontal_arc(self):
        from processing.conflict_engine import analyze_conflicts

        tracks = [
            {
                "icao": "AAA001", "lat": 0.0, "lon": 179.95,
                "alt": 10_000, "vel": 300.0, "hdg": 90.0, "vr": 0,
                "ts": 1_000.0, "gnd": False,
            },
            {
                "icao": "BBB002", "lat": 0.0, "lon": -179.95,
                "alt": 10_000, "vel": 300.0, "hdg": 270.0, "vr": 0,
                "ts": 1_000.0, "gnd": False,
            },
        ]

        result = analyze_conflicts(tracks, now=1_000.0)

        self.assertEqual(result["candidate_pairs"], 1)
        self.assertEqual(len(result["conflicts"]), 1)
        self.assertLess(result["conflicts"][0]["predicted_horizontal_nm"], 0.1)

    def test_temporally_incompatible_tracks_are_not_compared(self):
        from processing.conflict_engine import analyze_conflicts

        tracks = [
            {
                "icao": "AAA001", "lat": 0.0, "lon": -0.05,
                "alt": 10_000, "vel": 300.0, "hdg": 90.0, "vr": 0,
                "ts": 994.0, "gnd": False,
            },
            {
                "icao": "BBB002", "lat": -0.05, "lon": 0.0,
                "alt": 10_000, "vel": 300.0, "hdg": 0.0, "vr": 0,
                "ts": 1_000.0, "gnd": False,
            },
        ]

        result = analyze_conflicts(tracks, now=1_000.0)

        self.assertEqual(result["candidate_pairs"], 1)
        self.assertEqual(result["conflicts"], [])
        self.assertEqual(result.get("skipped_pairs"), {"time_skew": 1})

    def test_output_budget_prioritizes_critical_conflicts(self):
        from processing.conflict_engine import analyze_conflicts

        tracks = [
            {
                "icao": "AAA001", "lat": 0.0, "lon": 0.0,
                "alt": 10_000, "vel": 10.0, "hdg": 90.0, "vr": 0,
                "ts": 1_000.0, "gnd": False,
            },
            {
                "icao": "BBB002", "lat": 0.0, "lon": 0.133,
                "alt": 10_000, "vel": 10.0, "hdg": 270.0, "vr": 0,
                "ts": 1_000.0, "gnd": False,
            },
            {
                "icao": "CCC003", "lat": 20.0, "lon": -0.05,
                "alt": 10_000, "vel": 300.0, "hdg": 90.0, "vr": 0,
                "ts": 1_000.0, "gnd": False,
            },
            {
                "icao": "DDD004", "lat": 19.95, "lon": 0.0,
                "alt": 10_000, "vel": 300.0, "hdg": 0.0, "vr": 0,
                "ts": 1_000.0, "gnd": False,
            },
        ]

        result = analyze_conflicts(tracks, now=1_000.0, max_conflicts=1)

        self.assertTrue(result["truncated"])
        self.assertEqual(len(result["conflicts"]), 1)
        self.assertEqual(result["conflicts"][0]["severity"], "PREDICTED_LOSS_OF_SEPARATION")

    def test_detects_simultaneous_threshold_entry_before_horizontal_cpa(self):
        from processing.conflict_engine import analyze_conflicts

        tracks = [
            {
                "icao": "AAA001", "lat": 0.0, "lon": -0.05,
                "alt": 10_000, "vel": 300.0, "hdg": 90.0, "vr": 0,
                "ts": 1_000.0, "gnd": False,
            },
            {
                "icao": "BBB002", "lat": 0.0, "lon": 0.05,
                "alt": 10_000, "vel": 300.0, "hdg": 270.0, "vr": 2_000,
                "ts": 1_000.0, "gnd": False,
            },
        ]

        result = analyze_conflicts(tracks, now=1_000.0)

        self.assertEqual(len(result["conflicts"]), 1)
        conflict = result["conflicts"][0]
        self.assertEqual(conflict["severity"], "PREDICTED_LOSS_OF_SEPARATION")
        self.assertAlmostEqual(conflict.get("time_to_threshold_seconds"), 18.0, delta=1.0)
        self.assertGreater(conflict["predicted_vertical_ft"], 1_000.0)

    def test_polar_tracks_use_one_shared_ecef_velocity_frame(self):
        from processing.conflict_engine import analyze_conflicts

        tracks = [
            {
                "icao": "AAA001", "lat": 89.9, "lon": 0.0,
                "alt": 10_000, "vel": 300.0, "hdg": 0.0, "vr": 0,
                "ts": 1_000.0, "gnd": False,
            },
            {
                "icao": "BBB002", "lat": 89.9, "lon": 180.0,
                "alt": 10_000, "vel": 300.0, "hdg": 0.0, "vr": 0,
                "ts": 1_000.0, "gnd": False,
            },
        ]

        result = analyze_conflicts(tracks, now=1_000.0)

        self.assertEqual(len(result["conflicts"]), 1)
        self.assertAlmostEqual(result["conflicts"][0]["tcpa_seconds"], 72.0, delta=2.0)
        self.assertLess(result["conflicts"][0]["predicted_horizontal_nm"], 0.1)

    def test_horizontal_spatial_index_does_not_mix_barometric_altitude_into_ecef(self):
        from processing.conflict_engine import _ecef

        ground_radius = _ecef({"lat": 45.0, "lon": 10.0, "alt": 0})
        high_baro = _ecef({"lat": 45.0, "lon": 10.0, "alt": 100_000})

        self.assertEqual(high_baro, ground_radius)


if __name__ == "__main__":
    unittest.main()
