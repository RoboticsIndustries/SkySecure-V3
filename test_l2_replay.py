import unittest
import time

from anomaly.detector import AnomalyDetector
from models import StateVector


class L2ReplayTests(unittest.TestCase):
    def setUp(self):
        self.base_time = time.time()

    def _sample(self, index, velocity, heading, vertical_rate=0):
        return StateVector(
            icao24="REPLAY1",
            lat=40.0 + index * 0.001,
            lon=-75.0 + index * 0.001,
            altitude_baro=12000 + index * 10,
            velocity=velocity,
            heading=heading,
            vertical_rate=vertical_rate,
            last_seen=self.base_time + index,
        )

    def test_normal_then_spoofed_trajectory_replay(self):
        detector = AnomalyDetector()

        normal_trigger_count = 0
        for index in range(40):
            state = detector.process(self._sample(index, 420.0 + (index % 3), 90.0 + index * 0.2))
            normal_trigger_count += len([
                flag for flag in state.anomalies if flag.layer.value == "L2"
            ])

        spoofed = detector.process(self._sample(40, 900.0, 210.0, 15000))
        triggered = {flag.detector for flag in spoofed.anomalies if flag.layer.value == "L2"}

        self.assertEqual(normal_trigger_count, 0)
        self.assertIn("acceleration_rate", triggered)
        self.assertIn("turn_rate", triggered)
        self.assertIn("velocity_baseline", triggered)
        self.assertIn("vertical_rate_baseline", triggered)
        self.assertEqual(spoofed.layer_evaluations["L2"].status.value, "TRIGGERED")


if __name__ == "__main__":
    unittest.main()
