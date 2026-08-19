import asyncio
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import orjson

from anomaly.detector import AircraftBaseline, AnomalyDetector, StatisticalDetector, run
from models import (
    AnomalyType, DataSource, DetectionLayer, LayerStatus, RiskBand,
    SourceReport, StateVector,
)


class L2OperationalTests(unittest.IsolatedAsyncioTestCase):
    @staticmethod
    async def _scan_keys(*keys):
        for key in keys:
            yield key

    class _ConditionalRedis:
        def __init__(self, key, payload):
            self.store = {key: payload}

        async def set(self, key, value, *, nx=False, ex=None):
            if nx and key in self.store:
                return False
            self.store[key] = value
            return True

        async def get(self, key):
            return self.store.get(key)

        async def eval(self, _script, numkeys, *args):
            if numkeys == 1:
                claim_key, token = args
                if self.store.get(claim_key) == token:
                    del self.store[claim_key]
                    return 1
                return 0
            key, claim_key, payload, token = args
            if self.store.get(claim_key) != token:
                return 0
            if self.store.get(key) != payload:
                del self.store[claim_key]
                return 0
            del self.store[key]
            del self.store[claim_key]
            return 1

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

    async def test_delayed_mlat_does_not_advance_l2_or_l3_history(self):
        detector = AnomalyDetector()
        adsb_report = SourceReport(
            source=DataSource.ADSB, lat=39.95, lon=-75.16,
            timestamp=200.0, velocity=410.0, heading=20.0,
            vertical_rate=300.0,
        )
        adsb = StateVector(
            icao24="ABC123", lat=39.95, lon=-75.16,
            altitude_baro=12000, velocity=410.0, heading=20.0,
            vertical_rate=300.0, last_seen=200.0, update_count=1,
            last_update_source=DataSource.ADSB,
            source_reports=[adsb_report],
        )
        detector.process(adsb)
        baseline = detector.statistical.get_baseline("ABC123")
        before = (
            list(baseline.velocities), list(baseline.altitudes),
            list(baseline.vrates), list(detector.lstm._sequences["ABC123"]),
        )

        delayed_mlat = StateVector(
            icao24="ABC123", lat=39.95, lon=-75.16,
            altitude_baro=12000, velocity=410.0, heading=20.0,
            vertical_rate=300.0, last_seen=200.0, update_count=2,
            last_update_source=DataSource.MLAT,
            source_reports=[
                SourceReport(
                    source=DataSource.MLAT, lat=39.95, lon=-75.16,
                    timestamp=199.0,
                ),
                adsb_report,
            ],
        )
        detector.process(delayed_mlat)

        self.assertEqual(list(baseline.velocities), before[0])
        self.assertEqual(list(baseline.altitudes), before[1])
        self.assertEqual(list(baseline.vrates), before[2])
        self.assertEqual(list(detector.lstm._sequences["ABC123"]), before[3])

    async def test_stale_adsb_report_does_not_advance_altitude_or_integrity(self):
        detector = AnomalyDetector()
        stale = StateVector(
            icao24="ABC123", lat=39.95, lon=-75.16,
            altitude_baro=12000, velocity=1300.0, nic=8, nac_p=10,
            last_seen=200.0, update_count=1,
            last_update_source=DataSource.ADSB,
            source_reports=[SourceReport(
                source=DataSource.ADSB, lat=39.95, lon=-75.16,
                altitude=12000, timestamp=100.0,
            )],
        )
        result = detector.process(stale)

        baseline = detector.statistical.get_baseline("ABC123")
        self.assertEqual(list(baseline.altitudes), [])
        self.assertEqual(detector.integrity.integrity_history.get("ABC123", []), [])
        self.assertFalse(any(
            flag.detector == "impossible_speed" for flag in result.anomalies
        ))

    def test_delayed_new_adsb_report_after_mlat_uses_adsb_event_time(self):
        detector = AnomalyDetector()
        delayed = StateVector(
            icao24="ABC123", lat=39.95, lon=-75.16, altitude_baro=12100,
            nic=7, nac_p=9, last_seen=200.0, update_count=2,
            last_update_source=DataSource.ADSB, last_update_timestamp=150.0,
            source_reports=[SourceReport(
                source=DataSource.ADSB, lat=39.95, lon=-75.16,
                altitude=12100, timestamp=150.0,
            )],
        )
        detector.process(delayed)
        self.assertEqual(
            detector.integrity._integrity_results["ABC123"][0][0], 150.0
        )
        self.assertEqual(
            detector.statistical.get_baseline("ABC123").last_observation["altitude_timestamp"],
            150.0,
        )

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

    async def test_integrity_evidence_uses_adsb_event_time_not_newer_fused_time(self):
        detector = AnomalyDetector()
        state = StateVector(
            icao24="ABC123", lat=39.95, lon=-75.16,
            nic=0, nac_p=0,
            last_seen=200.0, update_count=1,
            last_update_source=DataSource.ADSB,
            last_update_timestamp=100.0,
            source_reports=[SourceReport(
                source=DataSource.ADSB, lat=39.95, lon=-75.16,
                timestamp=100.0,
            )],
        )

        result = detector.process(state)

        flag = next(
            item for item in result.anomalies
            if item.detector == "integrity_metadata"
        )
        self.assertEqual(flag.timestamp, 100.0)

    async def test_prune_clears_all_per_aircraft_detector_state(self):
        detector = AnomalyDetector()
        state = self._state(t=100.0, velocity=400.0, heading=0.0)
        state.nic, state.nac_p = 8, 10
        detector.process(state)
        detector._last_l2_access["ABC123"] = 0.0

        self.assertEqual(detector.prune_l2_state(max_idle_seconds=0), 1)

        self.assertNotIn("ABC123", detector.integrity.integrity_history)
        self.assertNotIn("ABC123", detector.integrity._integrity_results)
        self.assertNotIn("ABC123", detector.integrity.previous_states)
        self.assertNotIn("ABC123", detector.lstm._sequences)
        self.assertNotIn("ABC123", detector.scorer._last_scores)

    async def test_restart_hydrates_prior_enriched_result_before_replay(self):
        original = AnomalyDetector().process(
            self._state(t=100.0, velocity=410.0, heading=20.0)
        )
        stored = AircraftBaseline()
        stored.update(self._state(t=100.0, velocity=410.0, heading=20.0))
        redis = AsyncMock()
        redis.get.side_effect = [original.to_bytes(), orjson.dumps(stored.to_dict())]
        restarted = AnomalyDetector()

        await restarted.hydrate_last_result(redis, "ABC123")
        await restarted.hydrate_l2_baseline(redis, "ABC123")
        replay = restarted.process(self._state(t=100.0, velocity=410.0, heading=20.0))

        self.assertEqual(
            list(restarted.statistical.get_baseline("ABC123").velocities), [410.0]
        )
        self.assertEqual(replay.layer_evaluations["L2"].timestamp, 100.0)

    async def test_pending_alert_survives_state_commit_and_replay_restart(self):
        detector = AnomalyDetector()
        state = detector.process(self._state(t=100.0, velocity=1300.0, heading=0.0))
        state.risk_band = RiskBand.ALERT
        pipeline = MagicMock()
        pipeline.execute = AsyncMock()
        redis = AsyncMock()
        redis.pipeline = MagicMock(return_value=pipeline)

        await detector.persist_event_state(redis, state, pending_alert=True)

        outbox_key = detector.alert_outbox_key(state)
        pipeline.set.assert_called_once_with(outbox_key, state.to_bytes(), nx=True)
        redis.get.return_value = state.to_bytes()
        redis.set.return_value = True
        redis.eval.return_value = 1
        producer = AsyncMock()
        restarted = AnomalyDetector()
        restarted._last_results["ABC123"] = state
        self.assertTrue(restarted.is_replay(self._state(t=100.0, velocity=1300.0, heading=0.0)))

        published = await restarted.publish_pending_alert_key(redis, producer, outbox_key)

        self.assertTrue(published)
        producer.send_and_wait.assert_awaited_once()
        self.assertEqual(
            producer.send_and_wait.await_args.kwargs["key"], b"ABC123:100.000000:0"
        )
        redis.eval.assert_awaited_once()

    async def test_restart_drains_pending_alert_without_source_replay(self):
        state = self._state(t=100.0, velocity=1300.0, heading=0.0)
        key = AnomalyDetector.alert_outbox_key(state)
        redis = AsyncMock()
        redis.scan.return_value = (0, [key.encode()])
        redis.get.return_value = state.to_bytes()
        redis.set.return_value = True
        redis.eval.return_value = 1
        producer = AsyncMock()

        published = await AnomalyDetector().drain_pending_alerts(redis, producer)

        self.assertEqual(published, 1)
        producer.send_and_wait.assert_awaited_once()
        redis.eval.assert_awaited_once()

    async def test_outbox_publish_failure_preserves_entry(self):
        state = self._state(t=100.0, velocity=1300.0, heading=0.0)
        key = AnomalyDetector.alert_outbox_key(state)
        redis = AsyncMock()
        redis.scan.return_value = (0, [key])
        redis.get.return_value = state.to_bytes()
        redis.set.return_value = True
        producer = AsyncMock()
        producer.send_and_wait.side_effect = RuntimeError("Kafka unavailable")

        published = await AnomalyDetector().drain_pending_alerts(redis, producer)

        self.assertEqual(published, 0)
        redis.eval.assert_awaited_once()
        self.assertEqual(redis.eval.await_args.args[1], 1)
        self.assertTrue(str(redis.eval.await_args.args[2]).startswith("outbox-claim:"))

    async def test_malformed_outbox_entry_does_not_block_valid_entries(self):
        valid = self._state(t=100.0, velocity=1300.0, heading=0.0)
        malformed_key = "outbox:anomaly-alert:BROKEN"
        valid_key = AnomalyDetector.alert_outbox_key(valid)
        redis = AsyncMock()
        redis.scan.return_value = (0, [malformed_key, valid_key])
        redis.get.side_effect = [b"not-a-state-vector", valid.to_bytes()]
        redis.set.return_value = True
        redis.eval.return_value = 1
        producer = AsyncMock()

        published = await AnomalyDetector().drain_pending_alerts(redis, producer)

        self.assertEqual(published, 1)
        producer.send_and_wait.assert_awaited_once()
        self.assertEqual(redis.eval.await_count, 2)

    async def test_scan_page_overflow_is_drained_before_advancing_again(self):
        states = [
            self._state(t=100.0 + index, velocity=1300.0 + index, heading=0.0)
            for index in range(3)
        ]
        keys = [AnomalyDetector.alert_outbox_key(state) for state in states]
        payloads = dict(zip(keys, (state.to_bytes() for state in states)))
        redis = AsyncMock()
        redis.scan.return_value = (17, keys)
        redis.get.side_effect = lambda key: payloads[key]
        redis.set.return_value = True
        redis.eval.return_value = 1
        producer = AsyncMock()
        detector = AnomalyDetector()

        first = await detector.drain_pending_alerts(
            redis, producer, max_items=2
        )
        second = await detector.drain_pending_alerts(
            redis, producer, max_items=2
        )

        self.assertEqual((first, second), (2, 1))
        self.assertEqual(producer.send_and_wait.await_count, 3)
        redis.scan.assert_awaited_once()

    async def test_multiple_alerts_for_same_icao_get_distinct_immutable_entries(self):
        detector = AnomalyDetector()
        first = self._state(t=100.0, velocity=1300.0, heading=0.0)
        second = self._state(t=101.0, velocity=1400.0, heading=1.0)
        pipeline = MagicMock()
        pipeline.execute = AsyncMock()
        redis = AsyncMock()
        redis.pipeline = MagicMock(return_value=pipeline)

        await detector.persist_event_state(redis, first, pending_alert=True)
        await detector.persist_event_state(redis, second, pending_alert=True)

        alert_sets = [
            call for call in pipeline.set.call_args_list
            if call.args[0].startswith("outbox:anomaly-alert:")
        ]
        self.assertEqual(len(alert_sets), 2)
        self.assertNotEqual(alert_sets[0].args[0], alert_sets[1].args[0])
        self.assertTrue(all(call.kwargs == {"nx": True} for call in alert_sets))

    async def test_ack_does_not_delete_payload_replaced_between_read_and_ack(self):
        state = self._state(t=100.0, velocity=1300.0, heading=0.0)
        replacement = self._state(t=101.0, velocity=1400.0, heading=0.0).to_bytes()
        key = AnomalyDetector.alert_outbox_key(state)
        redis = self._ConditionalRedis(key, state.to_bytes())
        producer = AsyncMock()

        async def replace_during_send(**_kwargs):
            redis.store[key] = replacement

        producer.send_and_wait.side_effect = replace_during_send

        published = await AnomalyDetector().publish_pending_alert_key(
            redis, producer, key
        )

        self.assertTrue(published)
        self.assertEqual(redis.store[key], replacement)
        self.assertNotIn(AnomalyDetector._claim_key(key), redis.store)

    async def test_simultaneous_drains_publish_claimed_entry_once(self):
        state = self._state(t=100.0, velocity=1300.0, heading=0.0)
        key = AnomalyDetector.alert_outbox_key(state)
        redis = AsyncMock()
        redis.scan.return_value = (0, [key])
        redis.set.side_effect = [True, False]
        redis.get.return_value = state.to_bytes()
        redis.eval.return_value = 1
        producer = AsyncMock()

        results = await asyncio.gather(
            AnomalyDetector().drain_pending_alerts(redis, producer),
            AnomalyDetector().drain_pending_alerts(redis, producer),
        )

        self.assertEqual(sum(results), 1)
        producer.send_and_wait.assert_awaited_once()

    async def test_drain_is_bounded_and_kafka_outage_stops_batch(self):
        states = [
            self._state(t=float(i), velocity=1300.0, heading=0.0)
            for i in range(20)
        ]
        keys = [AnomalyDetector.alert_outbox_key(state) for state in states]
        redis = AsyncMock()
        redis.scan.return_value = (1, keys)
        redis.set.return_value = True
        redis.get.side_effect = [state.to_bytes() for state in states]
        producer = AsyncMock()
        producer.send_and_wait.side_effect = RuntimeError("Kafka unavailable")

        published = await AnomalyDetector().drain_pending_alerts(
            redis, producer, max_items=5, publish_timeout=0.01
        )

        self.assertEqual(published, 0)
        producer.send_and_wait.assert_awaited_once()
        redis.get.assert_awaited_once()

    async def test_producer_start_failure_cleans_consumer_and_redis(self):
        consumer = AsyncMock()
        producer = AsyncMock()
        producer.start.side_effect = RuntimeError("startup failed")
        redis = AsyncMock()

        with (
            patch("anomaly.detector.AIOKafkaConsumer", return_value=consumer),
            patch("anomaly.detector.AIOKafkaProducer", return_value=producer),
            patch("anomaly.detector.aioredis.from_url", return_value=redis),
        ):
            with self.assertRaisesRegex(RuntimeError, "startup failed"):
                await run()

        consumer.stop.assert_awaited_once()
        producer.stop.assert_awaited_once()
        redis.close.assert_awaited_once()


if __name__ == "__main__":
    unittest.main()
