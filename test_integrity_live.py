import unittest

import api.main as api
from anomaly.enhanced_detector import EnhancedAnomalyDetector


class LiveIntegrityWiringTests(unittest.TestCase):
    def test_forget_clears_integrity_result_cache_for_reacquisition(self):
        detector = EnhancedAnomalyDetector()
        for timestamp in (100.0, 110.0, 120.0):
            detector.check_integrity("ABC123", 8, 10, timestamp)
        degraded = detector.check_integrity("ABC123", 0, 0, 130.0)
        self.assertGreaterEqual(degraded.score, 0.5)

        detector.forget("ABC123")
        reacquired = detector.check_integrity("ABC123", 8, 10, 50.0)

        self.assertLess(reacquired.score, degraded.score)
        self.assertEqual(detector.integrity_history["ABC123"], [(8, 10)])

    def tearDown(self):
        api.anomaly_detector = None
        api._L1_CACHE.clear()

    def test_adsb_lol_api_parser_preserves_integrity_metadata(self):
        aircraft = api._parse_adsb_lol_aircraft({
            "ac": [{
                "hex": "abc123",
                "lat": 39.95,
                "lon": -75.16,
                "nic": 8,
                "nac_p": 10,
            }]
        })

        self.assertEqual(aircraft[0]["nic"], 8)
        self.assertEqual(aircraft[0]["nac_p"], 10)

    def test_repeated_snapshot_timestamp_does_not_grow_detector_history(self):
        api.anomaly_detector = EnhancedAnomalyDetector()
        aircraft = {
            "icao": "ABC123", "lat": 39.95, "lon": -75.16,
            "alt": 12000, "vel": 400, "hdg": 90,
            "nic": 8, "nac_p": 10, "ts": 100.0,
            "risk": 0, "band": "NORMAL", "anoms": [],
        }

        api.run_l2_l3_detection([aircraft])
        api.run_l2_l3_detection([aircraft])

        self.assertEqual(len(api.anomaly_detector.integrity_history["ABC123"]), 1)
        self.assertEqual(api.anomaly_detector.previous_states["ABC123"]["t"], 100.0)

    def test_canonical_snapshot_is_not_resampled_by_api_detector(self):
        api.anomaly_detector = EnhancedAnomalyDetector()
        aircraft = {
            "icao": "ABC123", "lat": 39.95, "lon": -75.16,
            "nic": 8, "nac_p": 10, "ts": 100.0,
            "layer_evaluations": {"L3": {"status": "EVALUATED"}},
            "risk": 0, "band": "NORMAL", "anoms": [],
        }

        api.run_l2_l3_detection([aircraft])

        self.assertNotIn("ABC123", api.anomaly_detector.integrity_history)
        self.assertNotIn("fused", aircraft)

    def test_layer_notes_match_canonical_l4_l5_taxonomy(self):
        detector = EnhancedAnomalyDetector()
        assessment = detector.assess(
            icao="ABC123", lat=39.95, lon=-75.16, velocity=400,
            observed_at=100.0,
        )

        notes = " ".join(assessment.notes)
        self.assertNotIn("RF fingerprinting", notes)
        self.assertNotIn("Galileo OSNMA", notes)
        self.assertIn("upstream canonical pipeline", notes)

    def test_live_detection_receives_and_flags_integrity_degradation(self):
        api.anomaly_detector = EnhancedAnomalyDetector()
        stable = {
            "icao": "ABC123", "lat": 39.95, "lon": -75.16,
            "alt": 12000, "vel": 400, "hdg": 90,
            "nic": 8, "nac_p": 10, "risk": 0, "band": "NORMAL", "anoms": [],
        }
        for observed_at in (100.0, 101.0, 102.0):
            api.run_l2_l3_detection([dict(stable, ts=observed_at)])

        degraded = dict(stable, nic=2, nac_p=2, anoms=[], ts=103.0)
        api.run_l2_l3_detection([degraded])

        self.assertIn("l3_integrity", degraded["fused"]["layers_available"])
        self.assertEqual(degraded["fused"]["layers"]["l3_integrity"]["score"], 1.0)
        self.assertTrue(any(a["type"] == "L3_INTEGRITY" for a in degraded["anoms"]))
        self.assertGreaterEqual(degraded["risk"], 45)


if __name__ == "__main__":
    unittest.main()
