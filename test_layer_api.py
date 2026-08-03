import time
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from api.main import get_layer_summary, get_layer_triggers
from config import settings
from models import (
    AnomalyFlag,
    AnomalyType,
    DetectionLayer,
    LayerEvaluation,
    LayerStatus,
    StateVector,
)


class LayerApiTests(unittest.IsolatedAsyncioTestCase):
    def _redis(self, vectors):
        redis = AsyncMock()
        keys = [f"sv:{v.icao24}".encode() for v in vectors]

        async def scan_iter(**_kwargs):
            for key in keys:
                yield key

        redis.scan_iter = MagicMock(side_effect=scan_iter)
        pipeline = MagicMock()
        pipeline.get = MagicMock(return_value=pipeline)
        pipeline.execute = AsyncMock(return_value=[v.to_bytes() for v in vectors])
        redis.pipeline = MagicMock(return_value=pipeline)
        return redis

    def _vectors(self):
        triggered = StateVector(
            icao24="ABC123",
            anomalies=[AnomalyFlag(
                anomaly_type=AnomalyType.ABNORMAL_TURN_RATE,
                layer=DetectionLayer.L2,
                detector="turn_rate",
                score_delta=20,
                description="rapid turn",
                meta={"degrees_per_second": 14.0},
            )],
            layer_evaluations={"L2": LayerEvaluation(
                layer=DetectionLayer.L2,
                status=LayerStatus.TRIGGERED,
                detectors_evaluated=["turn_rate"],
                triggered_detectors=["turn_rate"],
                score_delta=20,
            )},
        )
        evaluated = StateVector(
            icao24="DEF456",
            layer_evaluations={"L2": LayerEvaluation(
                layer=DetectionLayer.L2,
                status=LayerStatus.EVALUATED,
                detectors_evaluated=["turn_rate"],
            )},
        )
        return [triggered, evaluated]

    async def test_layer_summary_counts_evaluations_and_triggers(self):
        with patch("api.main.redis_client", self._redis(self._vectors())):
            payload = await get_layer_summary()

        l2 = payload["layers"]["L2"]
        self.assertEqual(l2["evaluated"], 2)
        self.assertEqual(l2["triggered"], 1)
        self.assertEqual(l2["skipped"], 0)
        self.assertEqual(l2["trigger_count"], 1)
        self.assertEqual(l2["detectors"]["turn_rate"], 1)

    async def test_layer_trigger_endpoint_returns_evidence(self):
        with patch("api.main.redis_client", self._redis(self._vectors())):
            payload = await get_layer_triggers("L2", limit=10)

        self.assertEqual(payload["count"], 1)
        self.assertEqual(payload["triggers"][0]["aircraft_id"], "ABC123")
        self.assertEqual(payload["triggers"][0]["detector"], "turn_rate")
        self.assertEqual(payload["triggers"][0]["evidence"]["degrees_per_second"], 14.0)

    async def test_expired_l4_evidence_is_not_reported_as_active(self):
        event_time = time.time() - settings.FUSION_TRIGGER_TTL_SEC - 1
        vector = StateVector(
            icao24="ABC123",
            anomalies=[AnomalyFlag(
                anomaly_type=AnomalyType.GHOST_AIRCRAFT,
                layer=DetectionLayer.L4,
                detector="adsb_mlat_disagreement",
                score_delta=25,
                description="stale",
                timestamp=event_time,
            )],
            layer_evaluations={"L4": LayerEvaluation(
                layer=DetectionLayer.L4,
                status=LayerStatus.TRIGGERED,
                timestamp=event_time,
            )},
        )
        redis = self._redis([vector])

        with patch("api.main.redis_client", redis):
            summary = await get_layer_summary()
            triggers = await get_layer_triggers("L4", limit=10)

        self.assertEqual(summary["layers"]["L4"]["triggered"], 0)
        self.assertEqual(summary["layers"]["L4"]["trigger_count"], 0)
        self.assertEqual(summary["layers"]["L4"]["skipped"], 1)
        self.assertEqual(
            summary["layers"]["L4"]["skipped_reasons"]["L4 trigger evidence expired"],
            1,
        )
        self.assertEqual(triggers["count"], 0)

    async def test_unknown_layer_is_rejected(self):
        with self.assertRaises(Exception) as raised:
            await get_layer_triggers("L9", limit=10)
        self.assertEqual(getattr(raised.exception, "status_code", None), 422)


if __name__ == "__main__":
    unittest.main()
