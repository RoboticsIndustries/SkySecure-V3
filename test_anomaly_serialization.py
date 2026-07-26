import unittest
from collections import deque

from anomaly.detector import AircraftBaseline


class AnomalySerializationTests(unittest.TestCase):
    def test_z_score_is_native_float(self):
        baseline = AircraftBaseline()
        values = deque([float(i) for i in range(30)], maxlen=120)

        score = baseline.z_score(100.0, values)

        self.assertIs(type(score), float)


if __name__ == "__main__":
    unittest.main()
