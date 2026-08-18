import unittest

import api.main as api
from anomaly.enhanced_detector import EnhancedAnomalyDetector


class LiveIntegrityWiringTests(unittest.TestCase):
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

    def test_live_detection_receives_and_flags_integrity_degradation(self):
        api.anomaly_detector = EnhancedAnomalyDetector()
        stable = {
            "icao": "ABC123", "lat": 39.95, "lon": -75.16,
            "alt": 12000, "vel": 400, "hdg": 90,
            "nic": 8, "nac_p": 10, "risk": 0, "band": "NORMAL", "anoms": [],
        }
        for _ in range(3):
            api.run_l2_l3_detection([dict(stable)])

        degraded = dict(stable, nic=2, nac_p=2, anoms=[])
        api.run_l2_l3_detection([degraded])

        self.assertIn("l3_integrity", degraded["fused"]["layers_available"])
        self.assertEqual(degraded["fused"]["layers"]["l3_integrity"]["score"], 1.0)
        self.assertTrue(any(a["type"] == "L3_INTEGRITY" for a in degraded["anoms"]))
        self.assertGreaterEqual(degraded["risk"], 45)


if __name__ == "__main__":
    unittest.main()
