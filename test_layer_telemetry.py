import unittest

from config import KAFKA_CONSUMER_STABILITY
from models import (
    AnomalyFlag,
    AnomalyType,
    DetectionLayer,
    LayerEvaluation,
    LayerStatus,
    StateVector,
)


class LayerTelemetryTests(unittest.TestCase):
    def test_anomaly_api_contract_includes_score_and_event_time(self):
        flag = AnomalyFlag(
            anomaly_type=AnomalyType.IMPOSSIBLE_SPEED,
            layer=DetectionLayer.L2,
            detector="impossible_speed",
            score_delta=20,
            description="too fast",
            timestamp=123.0,
        )

        payload = flag.to_api_dict()

        self.assertEqual(payload["score_delta"], 20)
        self.assertEqual(payload["timestamp"], 123.0)

    def test_kafka_consumers_require_manual_commit(self):
        self.assertIs(KAFKA_CONSUMER_STABILITY["enable_auto_commit"], False)

    def test_layer_metadata_survives_state_vector_round_trip(self):
        flag = AnomalyFlag(
            anomaly_type=AnomalyType.IMPOSSIBLE_SPEED,
            layer=DetectionLayer.L2,
            detector="velocity_baseline",
            score_delta=25,
            description="velocity outside baseline",
            meta={"z_score": 5.2},
        )
        evaluation = LayerEvaluation(
            layer=DetectionLayer.L2,
            status=LayerStatus.TRIGGERED,
            detectors_evaluated=["velocity_baseline"],
            triggered_detectors=["velocity_baseline"],
            score_delta=25,
        )
        state = StateVector(
            icao24="ABC123",
            anomalies=[flag],
            layer_evaluations={DetectionLayer.L2.value: evaluation},
        )

        restored = StateVector.from_bytes(state.to_bytes())
        payload = restored.to_api_dict()

        self.assertEqual(restored.anomalies[0].layer, DetectionLayer.L2)
        self.assertEqual(restored.anomalies[0].detector, "velocity_baseline")
        self.assertEqual(payload["layer_evaluations"]["L2"]["status"], "TRIGGERED")
        self.assertEqual(payload["layer_triggers"][0]["layer"], "L2")
        self.assertEqual(payload["layer_triggers"][0]["evidence"]["z_score"], 5.2)

    def test_legacy_anomaly_payload_remains_readable(self):
        flag = AnomalyFlag(
            anomaly_type=AnomalyType.TELEPORTATION,
            score_delta=45,
            description="legacy payload",
        )

        self.assertEqual(flag.layer, DetectionLayer.L2)
        self.assertEqual(flag.detector, "legacy")


if __name__ == "__main__":
    unittest.main()
