import unittest
from unittest.mock import AsyncMock

import orjson

from anomaly.detector import AircraftBaseline, AnomalyDetector, StatisticalDetector
from models import AnomalyType, DetectionLayer, LayerStatus, StateVector


class L2OperationalTests(unittest.IsolatedAsyncioTestCase):
    def _state(self, *, t, velocity, heading, vertical_rate=0):
        return StateVector(
            icao24="ABC123",
            lat=39.95,
            lon=-75.16,
            altitude_baro=12000,
            velocity=velocity,
            heading=heading,
            vertical_rate=vertical_rate,
            last_seen=t,
        )

    async def test_timestamp_aware_kinematics_trigger_with_evidence(self):
        detector = StatisticalDetector()
        detector.check(self._state(t=100.0, velocity=400.0, heading=0.0))

        flags = detector.check(self._state(t=102.0, velocity=600.0, heading=90.0))

        by_detector = {flag.detector: flag for flag in flags}
        self.assertIn("acceleration_rate", by_detector)
        self.assertIn("turn_rate", by_detector)
        self.assertEqual(by_detector["turn_rate"].layer, DetectionLayer.L2)
        self.assertAlmostEqual(by_detector["acceleration_rate"].meta["knots_per_second"], 100.0)
        self.assertAlmostEqual(by_detector["turn_rate"].meta["degrees_per_second"], 45.0)

    async def test_baseline_round_trip_preserves_observations(self):
        baseline = AircraftBaseline()
        baseline.update(self._state(t=100.0, velocity=410.0, heading=20.0))

        restored = AircraftBaseline.from_dict(baseline.to_dict())

        self.assertEqual(list(restored.velocities), [410.0])
        self.assertEqual(restored.last_observation["heading"], 20.0)
        self.assertEqual(restored.last_observation["timestamp"], 100.0)

    async def test_detector_hydrates_and_persists_l2_baseline_in_redis(self):
        stored = AircraftBaseline()
        stored.update(self._state(t=100.0, velocity=410.0, heading=20.0))
        redis = AsyncMock()
        redis.get.return_value = orjson.dumps(stored.to_dict())
        detector = AnomalyDetector()

        await detector.hydrate_l2_baseline(redis, "ABC123")
        await detector.persist_l2_baseline(redis, "ABC123", ttl=3600)
        await detector.persist_l2_baseline(redis, "ABC123", ttl=3600)

        self.assertEqual(detector.statistical.get_baseline("ABC123").last_observation["velocity"], 410.0)
        redis.setex.assert_awaited_once()
        self.assertEqual(redis.setex.await_args.args[0], "baseline:l2:ABC123")
        self.assertEqual(redis.setex.await_args.args[1], 3600)

    async def test_process_records_layer_evaluation(self):
        detector = AnomalyDetector()

        state = detector.process(self._state(t=100.0, velocity=1300.0, heading=0.0))

        evaluation = state.layer_evaluations["L2"]
        self.assertEqual(evaluation.layer, DetectionLayer.L2)
        self.assertEqual(evaluation.status, LayerStatus.TRIGGERED)
        self.assertEqual(evaluation.timestamp, 100.0)
        self.assertTrue(state.anomalies)
        self.assertTrue(all(flag.timestamp == 100.0 for flag in state.anomalies))
        self.assertIn("impossible_speed", evaluation.detectors_evaluated)
        self.assertIn("velocity_baseline", evaluation.detectors_evaluated)
        self.assertEqual(state.layer_evaluations["L3"].status, LayerStatus.SKIPPED)
        self.assertIn("warming up", state.layer_evaluations["L3"].skipped_reason or "")


if __name__ == "__main__":
    unittest.main()
