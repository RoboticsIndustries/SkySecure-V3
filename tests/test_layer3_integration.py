import unittest

from anomaly.detector import AnomalyDetector
from models import StateVector


class Layer3IntegrationTests(unittest.TestCase):
    def setUp(self):
        self.detector = AnomalyDetector()

    def process_integrity(self, nic, nac_p):
        return self.detector.process(StateVector(
            icao24="ABC123",
            nic=nic,
            nac_p=nac_p,
        ))

    def test_sudden_integrity_degradation_reaches_canonical_anomaly_pipeline(self):
        self.process_integrity(8, 10)
        self.process_integrity(8, 10)
        self.process_integrity(8, 10)

        result = self.process_integrity(2, 2)

        integrity_flags = [
            flag for flag in result.anomalies
            if flag.anomaly_type.value == "INTEGRITY_DEGRADATION"
        ]
        self.assertEqual(len(integrity_flags), 1)
        self.assertEqual(integrity_flags[0].score_delta, 20)
        self.assertEqual(integrity_flags[0].meta["integrity_score"], 1.0)
        self.assertEqual(result.risk_score, 20)

    def test_missing_integrity_metadata_is_not_treated_as_anomaly(self):
        result = self.process_integrity(None, None)

        self.assertFalse(any(
            flag.anomaly_type.value == "INTEGRITY_DEGRADATION"
            for flag in result.anomalies
        ))
        self.assertEqual(result.risk_score, 0)


if __name__ == "__main__":
    unittest.main()
