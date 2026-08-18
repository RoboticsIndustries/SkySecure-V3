import unittest

from anomaly.detector import AnomalyDetector
from models import DataSource, SourceReport, StateVector


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

    def test_mlat_update_does_not_resample_retained_adsb_integrity(self):
        adsb = StateVector(
            icao24="ABC123", nic=8, nac_p=10, last_seen=100.0, update_count=1,
            last_update_source=DataSource.ADSB,
            source_reports=[SourceReport(
                source=DataSource.ADSB, lat=40.0, lon=-75.0, timestamp=100.0
            )],
        )
        self.detector.process(adsb)
        mlat = StateVector(
            icao24="ABC123", nic=8, nac_p=10, last_seen=100.0, update_count=2,
            last_update_source=DataSource.MLAT,
            source_reports=adsb.source_reports + [SourceReport(
                source=DataSource.MLAT, lat=40.0, lon=-75.0, timestamp=99.0
            )],
        )

        result = self.detector.process(mlat)

        self.assertEqual(len(self.detector.integrity.integrity_history["ABC123"]), 1)
        self.assertEqual(result.layer_evaluations["L3"].status.value, "SKIPPED")

    def test_replayed_event_does_not_grow_canonical_detector_history(self):
        event = StateVector(
            icao24="ABC123",
            lat=39.95,
            lon=-75.16,
            nic=8,
            nac_p=10,
            last_seen=100.0,
        )

        first = self.detector.process(event)
        second = self.detector.process(StateVector.from_bytes(event.to_bytes()))

        self.assertEqual(len(self.detector.integrity.integrity_history["ABC123"]), 1)
        self.assertEqual(len(self.detector.lstm._sequences["ABC123"]), 1)
        self.assertEqual(second.to_bytes(), first.to_bytes())

    def test_same_global_timestamp_with_higher_update_count_is_new_event(self):
        first = StateVector(
            icao24="ABC123", lat=39.95, lon=-75.16,
            nic=8, nac_p=10, last_seen=200.0, update_count=1,
        )
        second = StateVector(
            icao24="ABC123", lat=39.95, lon=-75.16,
            nic=7, nac_p=9, last_seen=200.0, update_count=2,
        )

        self.detector.process(first)
        self.detector.process(second)

        self.assertEqual(len(self.detector.integrity.integrity_history["ABC123"]), 2)

    def test_missing_integrity_metadata_is_not_treated_as_anomaly(self):
        result = self.process_integrity(None, None)

        self.assertFalse(any(
            flag.anomaly_type.value == "INTEGRITY_DEGRADATION"
            for flag in result.anomalies
        ))
        self.assertEqual(result.risk_score, 0)


if __name__ == "__main__":
    unittest.main()
