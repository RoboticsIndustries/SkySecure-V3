import unittest

from ingestion.adsb_receiver import _integrity_from_state, _parse_adsb_lol_states


class IngestionFallbackParserTests(unittest.TestCase):
    def test_converts_adsb_lol_record_to_opensky_shape(self):
        states = _parse_adsb_lol_states({
            "ac": [{
                "hex": "~abc123",
                "flight": " TEST1 ",
                "lat": 39.9,
                "lon": -75.1,
                "alt_baro": 10000,
                "gs": 250,
                "track": 90,
                "baro_rate": 500,
                "nic": 8,
                "nac_p": 10,
            }]
        })

        self.assertEqual(len(states), 1)
        state = states[0]
        self.assertEqual(state[0], "ABC123")
        self.assertEqual(state[1], "TEST1")
        self.assertAlmostEqual(state[5], -75.1)
        self.assertAlmostEqual(state[6], 39.9)
        self.assertAlmostEqual(state[7] * 3.28084, 10000, places=1)
        self.assertAlmostEqual(state[9] * 1.944, 250, places=1)
        self.assertAlmostEqual(state[11] * 196.85, 500, places=1)
        self.assertEqual(state[17], 8)
        self.assertEqual(state[18], 10)
        self.assertEqual(_integrity_from_state(state), (8, 10))

    def test_drops_invalid_records(self):
        states = _parse_adsb_lol_states({"ac": [
            {"hex": "bad", "lat": 1, "lon": 2},
            {"hex": "abc123", "lat": 100, "lon": 2},
            {"hex": "abc124", "lat": 1, "lon": None},
        ]})
        self.assertEqual(states, [])


if __name__ == "__main__":
    unittest.main()
