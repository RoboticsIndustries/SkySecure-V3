import asyncio
import time
import unittest
from unittest.mock import AsyncMock, patch

from anomaly.detector import AnomalyDetector, RuleEngine
from config import settings
from models import (
    AnomalyFlag,
    AnomalyType,
    DataSource,
    DetectionLayer,
    LayerEvaluation,
    LayerStatus,
    RawADSBMessage,
    RawMLATReport,
    SourceReport,
    StateVector,
)
from processing.fusion_engine import FusionEngine


def make_mlat_report(**values):
    count = values.get("num_receivers", 4)
    trusted = ["receiver-london", "receiver-paris", "receiver-brussels", "receiver-amsterdam"]
    values.setdefault("receiver_ids", trusted[:count])
    values.setdefault("source_event_ids", [f"{i:064x}" for i in range(count)])
    return RawMLATReport(**values)


class MultilayerIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_fusion_rejects_unknown_or_unsafe_mlat_receiver_geometry(self):
        redis = AsyncMock()
        engine = FusionEngine(redis)
        base = dict(
            session_id="registry", solve_time=time.time(), icao24="ABC123",
            lat=40.0, lon=-75.0, altitude_baro=10000, num_receivers=4,
            tdoa_residual=10.0, cep90=100.0,
            source_event_ids=[f"{i:064x}" for i in range(4)],
        )

        self.assertIsNone(await engine.process_mlat(RawMLATReport(
            **base, receiver_ids=["unknown-a", "unknown-b", "unknown-c", "unknown-d"]
        )))

        unsafe = {
            "receiver-london": [40.0, -75.0, 10.0],
            "receiver-paris": [40.0, -75.0, 10.0],
            "receiver-brussels": [40.0, -75.0, 10.0],
            "receiver-amsterdam": [40.0, -75.0, 10.0],
        }
        with patch.object(settings, "MLAT_RECEIVER_LOCATIONS", unsafe):
            self.assertIsNone(await engine.process_mlat(RawMLATReport(
                **base,
                receiver_ids=["receiver-london", "receiver-paris",
                              "receiver-brussels", "receiver-amsterdam"],
            )))

        redis.get.assert_not_awaited()

    async def test_fusion_rejects_stale_and_future_source_event_times_before_state_load(self):
        now = time.time()
        redis = AsyncMock()
        engine = FusionEngine(redis)
        adsb_base = dict(
            icao24="ABC123", lat=40.0, lon=-75.0, receiver_id="feed",
            raw_message="8DABC123", msg_type=17,
        )
        mlat_base = dict(
            session_id="clock", icao24="ABC123", lat=40.0, lon=-75.0,
            altitude_baro=10000, num_receivers=4, tdoa_residual=10.0,
            cep90=100.0,
            receiver_ids=["receiver-london", "receiver-paris",
                          "receiver-brussels", "receiver-amsterdam"],
            source_event_ids=[f"{i:064x}" for i in range(4)],
        )

        for timestamp in (now - 301.0, now + 6.0):
            with self.subTest(source="adsb", timestamp=timestamp):
                self.assertIsNone(await engine.process_adsb(
                    RawADSBMessage(**adsb_base, recv_time=timestamp)
                ))
            with self.subTest(source="mlat", timestamp=timestamp):
                self.assertIsNone(await engine.process_mlat(
                    RawMLATReport(**mlat_base, solve_time=timestamp)
                ))

        redis.get.assert_not_awaited()

    async def test_fusion_rejects_operationally_unacceptable_mlat_quality(self):
        redis = AsyncMock()
        engine = FusionEngine(redis)
        base = dict(
            session_id="quality", solve_time=time.time(), icao24="ABC123",
            lat=40.0, lon=-75.0, altitude_baro=10000, num_receivers=4,
            receiver_ids=["receiver-london", "receiver-paris",
                          "receiver-brussels", "receiver-amsterdam"],
        )
        for override in (
            {"tdoa_residual": 501.0, "cep90": 100.0},
            {"tdoa_residual": 10.0, "cep90": 10_001.0},
        ):
            with self.subTest(override=override):
                self.assertIsNone(await engine.process_mlat(
                    RawMLATReport.model_construct(**(base | override))
                ))
        redis.get.assert_not_awaited()

    async def test_exact_mlat_replay_is_ignored(self):
        redis = AsyncMock()
        redis.get.return_value = None
        engine = FusionEngine(redis)
        report = make_mlat_report(
            session_id="replay-test",
            solve_time=time.time(),
            icao24="ABC123",
            lat=40.0,
            lon=-75.0,
            altitude_baro=12000,
            num_receivers=4,
            tdoa_residual=50.0,
            cep90=100.0,
        )

        first = await engine.process_mlat(report)
        self.assertIsNotNone(first)
        assert first is not None
        redis.get.return_value = first.to_bytes()

        replay = await engine.process_mlat(report)

        self.assertIsNone(replay)
        self.assertEqual(first.update_count, 1)
        self.assertEqual(
            len([r for r in first.source_reports if r.source == DataSource.MLAT]),
            1,
        )

    async def test_adsb_only_track_marks_l4_skipped(self):
        redis = AsyncMock()
        redis.get.return_value = None
        message = RawADSBMessage(
            receiver_id="opensky",
            recv_time=time.time(),
            icao24="ABC123",
            raw_message="",
            msg_type=17,
            lat=40.0,
            lon=-75.0,
            altitude_baro=None,
            altitude_geo=None,
            velocity=None,
            heading=None,
            vertical_rate=None,
            nic=None,
            nac_p=None,
            raim=None,
        )

        result = await FusionEngine(redis).process_adsb(message)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.layer_evaluations["L4"].status, LayerStatus.SKIPPED)
        self.assertIn("MLAT", result.layer_evaluations["L4"].skipped_reason or "")

    async def test_adsb_mlat_disagreement_records_l4_evaluation(self):
        event_time = time.time()
        existing = StateVector(
            icao24="ABC123",
            lat=40.0,
            lon=-75.0,
            sources=[DataSource.ADSB],
            last_seen=event_time,
            source_reports=[SourceReport(
                source=DataSource.ADSB,
                lat=40.0,
                lon=-75.0,
                timestamp=event_time,
            )],
        )
        redis = AsyncMock()
        redis.get.return_value = existing.to_bytes()
        engine = FusionEngine(redis)
        report = make_mlat_report(
            session_id="test",
            solve_time=event_time,
            icao24="ABC123",
            lat=41.0,
            lon=-75.0,
            altitude_baro=12000,
            num_receivers=4,
            tdoa_residual=50.0,
            cep90=100.0,
        )

        result = await engine.process_mlat(report)

        self.assertIsNotNone(result)
        assert result is not None
        evaluation = result.layer_evaluations["L4"]
        self.assertEqual(evaluation.status, LayerStatus.TRIGGERED)
        self.assertEqual(evaluation.triggered_detectors, ["adsb_mlat_disagreement"])
        self.assertEqual(result.anomalies[0].layer, DetectionLayer.L4)
        self.assertEqual(result.anomalies[0].timestamp, event_time)
        self.assertEqual(evaluation.timestamp, event_time)

    async def test_delayed_mlat_does_not_compare_against_newer_adsb(self):
        now = time.time()
        existing = StateVector(
            icao24="ABC123",
            lat=40.0,
            lon=-75.0,
            sources=[DataSource.ADSB],
            last_seen=now,
            source_reports=[SourceReport(
                source=DataSource.ADSB,
                lat=40.0,
                lon=-75.0,
                timestamp=now,
            )],
        )
        redis = AsyncMock()
        redis.get.return_value = existing.to_bytes()
        report = make_mlat_report(
            session_id="delayed",
            solve_time=now - 20,
            icao24="ABC123",
            lat=41.0,
            lon=-75.0,
            altitude_baro=20000,
            velocity=None,
            heading=None,
            num_receivers=4,
            tdoa_residual=100.0,
            cep90=50.0,
        )

        result = await FusionEngine(redis).process_mlat(report)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertFalse(any(flag.layer == DetectionLayer.L4 for flag in result.anomalies))
        evaluation = result.layer_evaluations["L4"]
        self.assertEqual(evaluation.status, LayerStatus.SKIPPED)
        self.assertIn("aligned", evaluation.skipped_reason or "")
        self.assertEqual(result.last_seen, now)

    async def test_mlat_selects_latest_event_time_aligned_adsb_report(self):
        redis = AsyncMock()
        now = time.time()
        state = StateVector(
            icao24="ABC123",
            last_seen=now,
            source_reports=[
                SourceReport(
                    source=DataSource.ADSB, lat=40.0, lon=-75.0, timestamp=now - 20
                ),
                SourceReport(
                    source=DataSource.ADSB, lat=10.0, lon=10.0, timestamp=now
                ),
            ],
        )
        redis.get.return_value = state.to_bytes()
        engine = FusionEngine(redis)

        result = await engine.process_mlat(make_mlat_report(
            session_id="aligned", solve_time=now - 18, icao24="ABC123",
            lat=41.0, lon=-75.0, altitude_baro=30000,
            num_receivers=4, tdoa_residual=10.0, cep90=10.0,
        ))

        self.assertEqual(result.layer_evaluations["L4"].status, LayerStatus.TRIGGERED)
        self.assertEqual(result.layer_evaluations["L4"].timestamp, now - 18)

    async def test_slightly_delayed_mlat_can_compare_with_newer_adsb(self):
        redis = AsyncMock()
        now = time.time()
        state = StateVector(
            icao24="ABC123", last_seen=now,
            source_reports=[SourceReport(
                source=DataSource.ADSB, lat=40.0, lon=-75.0, timestamp=now
            )],
            layer_evaluations={"L4": LayerEvaluation(
                layer=DetectionLayer.L4, status=LayerStatus.SKIPPED, timestamp=now
            )},
        )
        redis.get.return_value = state.to_bytes()

        result = await FusionEngine(redis).process_mlat(make_mlat_report(
            session_id="late-aligned", solve_time=now - 1, icao24="ABC123",
            lat=40.0, lon=-75.0, altitude_baro=10000,
            num_receivers=4, tdoa_residual=10.0, cep90=10.0,
        ))

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.last_update_source, DataSource.MLAT)
        self.assertEqual(result.layer_evaluations["L4"].status, LayerStatus.EVALUATED)
        self.assertEqual(result.layer_evaluations["L4"].timestamp, now - 1)

    async def test_delayed_mlat_cannot_overwrite_newer_l4_lifecycle(self):
        redis = AsyncMock()
        newer_flag = AnomalyFlag(
            anomaly_type=AnomalyType.GHOST_AIRCRAFT,
            layer=DetectionLayer.L4,
            detector="adsb_mlat_disagreement",
            score_delta=25,
            description="newer",
            timestamp=120.0,
        )
        state = StateVector(
            icao24="ABC123", last_seen=120.0, anomalies=[newer_flag],
            layer_evaluations={"L4": LayerEvaluation(
                layer=DetectionLayer.L4,
                status=LayerStatus.TRIGGERED,
                timestamp=120.0,
            )},
        )
        redis.get.return_value = state.to_bytes()
        engine = FusionEngine(redis)

        result = await engine.process_mlat(make_mlat_report(
            session_id="old", solve_time=110.0, icao24="ABC123",
            lat=0.0, lon=0.0, altitude_baro=10000,
            num_receivers=4, tdoa_residual=10.0, cep90=10.0,
        ))

        self.assertIsNone(result)

    async def test_fusion_handlers_serialize_same_aircraft_through_save(self):
        redis = AsyncMock()
        engine = FusionEngine(redis)
        producer = AsyncMock()
        duplicate = AsyncMock()
        duplicate.check.return_value = None
        sequence = []
        first_started = asyncio.Event()
        release_first = asyncio.Event()

        async def adsb_process(_message):
            sequence.append("adsb-process")
            first_started.set()
            await release_first.wait()
            return StateVector(icao24="ABC123")

        async def mlat_process(_report):
            sequence.append("mlat-process")
            return StateVector(icao24="ABC123")

        async def save(state, _producer, **_event):
            sequence.append(f"save-{state.primary_source.value}")

        engine.process_adsb = AsyncMock(side_effect=adsb_process)
        engine.process_mlat = AsyncMock(side_effect=mlat_process)
        engine.save = AsyncMock(side_effect=save)
        adsb = RawADSBMessage(
            receiver_id="opensky", recv_time=100.0, icao24="ABC123",
            raw_message="", msg_type=17, lat=0.0, lon=0.0,
        )
        mlat = make_mlat_report(
            session_id="same", solve_time=100.0, icao24="ABC123",
            lat=0.0, lon=0.0, altitude_baro=10000,
            num_receivers=4, tdoa_residual=10.0, cep90=10.0,
        )

        first = asyncio.create_task(engine.handle_adsb(adsb, producer, duplicate))
        await first_started.wait()
        second = asyncio.create_task(engine.handle_mlat(mlat, producer))
        await asyncio.sleep(0)
        self.assertEqual(sequence, ["adsb-process"])
        release_first.set()
        await asyncio.gather(first, second)

        self.assertEqual(sequence, [
            "adsb-process", "save-UNKNOWN", "mlat-process", "save-UNKNOWN",
        ])

    async def test_new_adsb_expires_old_l4_trigger(self):
        now = time.time()
        old_flag = AnomalyFlag(
            anomaly_type=AnomalyType.GHOST_AIRCRAFT,
            layer=DetectionLayer.L4,
            detector="adsb_mlat_disagreement",
            score_delta=25,
            description="old disagreement",
            timestamp=now - 61,
        )
        existing = StateVector(
            icao24="ABC123",
            lat=40.0,
            lon=-75.0,
            last_seen=now - 10,
            anomalies=[old_flag],
            layer_evaluations={"L4": LayerEvaluation(
                layer=DetectionLayer.L4,
                status=LayerStatus.TRIGGERED,
                timestamp=now - 61,
            )},
        )
        redis = AsyncMock()
        redis.get.return_value = existing.to_bytes()
        message = RawADSBMessage(
            receiver_id="opensky",
            recv_time=now,
            icao24="ABC123",
            raw_message="",
            msg_type=17,
            lat=40.0,
            lon=-75.0,
            altitude_baro=None,
            altitude_geo=None,
            velocity=None,
            heading=None,
            vertical_rate=None,
            nic=None,
            nac_p=None,
            raim=None,
        )

        result = await FusionEngine(redis).process_adsb(message)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertFalse(any(flag.layer == DetectionLayer.L4 for flag in result.anomalies))
        self.assertEqual(result.layer_evaluations["L4"].status, LayerStatus.SKIPPED)

    async def test_out_of_order_adsb_is_ignored(self):
        now = time.time()
        existing = StateVector(
            icao24="ABC123",
            lat=40.0,
            lon=-75.0,
            last_seen=now,
            source_reports=[SourceReport(
                source=DataSource.ADSB,
                receiver_id="physical-r1",
                lat=40.0,
                lon=-75.0,
                timestamp=now,
            )],
        )
        redis = AsyncMock()
        redis.get.return_value = existing.to_bytes()
        delayed = RawADSBMessage(
            receiver_id="physical-r1",
            recv_time=now - 1,
            icao24="ABC123",
            raw_message="",
            msg_type=17,
            lat=50.0,
            lon=-75.0,
            altitude_baro=None,
            altitude_geo=None,
            velocity=None,
            heading=None,
            vertical_rate=None,
            nic=None,
            nac_p=None,
            raim=None,
        )

        result = await FusionEngine(redis).process_adsb(delayed)

        self.assertIsNone(result)

    async def test_upstream_l5_trigger_is_preserved_and_scored(self):
        duplicate = AnomalyFlag(
            anomaly_type=AnomalyType.DUPLICATE_ICAO,
            layer=DetectionLayer.L5,
            detector="duplicate_icao",
            score_delta=60,
            description="duplicate identity",
        )
        state = StateVector(
            icao24="ABC123",
            last_seen=time.time(),
            anomalies=[duplicate],
        )

        pristine = state.model_copy(deep=True)
        result = AnomalyDetector().process(state)

        self.assertIn(duplicate, result.anomalies)
        self.assertGreaterEqual(result.risk_score, 60)
        first_score = result.risk_score

        repeated = AnomalyDetector()
        first = repeated.process(pristine)
        second = repeated.process(first.model_copy(deep=True))
        self.assertEqual(second.risk_score, first.risk_score)
        self.assertEqual(first.risk_score, first_score)

    async def test_upstream_l2_trigger_is_in_l2_evaluation(self):
        conflict = AnomalyFlag(
            anomaly_type=AnomalyType.GNSS_SPOOF,
            layer=DetectionLayer.L2,
            detector="adsb_position_conflict",
            score_delta=35,
            description="source position conflict",
        )
        state = StateVector(
            icao24="ABC123",
            last_seen=time.time(),
            anomalies=[conflict],
        )

        result = AnomalyDetector().process(state)

        evaluation = result.layer_evaluations["L2"]
        self.assertEqual(evaluation.status, LayerStatus.TRIGGERED)
        self.assertIn("adsb_position_conflict", evaluation.triggered_detectors)
        self.assertGreaterEqual(evaluation.score_delta, 35)

    async def test_reload_preserves_other_layer_trigger_evidence(self):
        fusion_flag = AnomalyFlag(
            anomaly_type=AnomalyType.GHOST_AIRCRAFT,
            layer=DetectionLayer.L4,
            detector="adsb_mlat_disagreement",
            score_delta=25,
            description="sensor disagreement",
        )
        existing = StateVector(
            icao24="ABC123",
            last_seen=time.time(),
            anomalies=[fusion_flag],
        )
        redis = AsyncMock()
        redis.get.return_value = existing.to_bytes()

        reloaded = await FusionEngine(redis)._load_or_create("ABC123")

        self.assertIn(fusion_flag, reloaded.anomalies)

    async def test_interleaved_mlat_does_not_corrupt_adsb_kinematic_rate(self):
        detector = AnomalyDetector()

        def state(timestamp, velocity, source):
            return StateVector(
                icao24="ABC123",
                velocity=velocity,
                heading=90.0,
                last_seen=timestamp,
                source_reports=[SourceReport(
                    source=source,
                    velocity=velocity if source == DataSource.ADSB else None,
                    heading=90.0 if source == DataSource.ADSB else None,
                    timestamp=timestamp,
                )],
            )

        detector.process(state(100.0, 200.0, DataSource.ADSB))
        detector.process(state(101.0, 200.0, DataSource.MLAT))
        result = detector.process(state(102.0, 230.0, DataSource.ADSB))

        self.assertNotIn(
            "acceleration_rate",
            [flag.detector for flag in result.anomalies],
        )

    async def test_adsb_integrity_advances_against_adsb_time_not_newer_mlat_time(self):
        now = time.time()
        state = StateVector(
            icao24="ABC123",
            last_seen=now,
            nic=8,
            nac_p=10,
            source_reports=[
                SourceReport(
                    source=DataSource.ADSB,
                    lat=40.0,
                    lon=-75.0,
                    timestamp=now - 100,
                ),
                SourceReport(
                    source=DataSource.MLAT,
                    lat=40.1,
                    lon=-75.1,
                    timestamp=now,
                ),
            ],
        )
        redis = AsyncMock()
        redis.get.return_value = state.to_bytes()
        engine = FusionEngine(redis)
        report = RawADSBMessage(
            receiver_id="adsb_lol",
            icao24="ABC123",
            recv_time=now - 50,
            raw_message="",
            msg_type=17,
            lat=40.05,
            lon=-75.05,
            nic=2,
            nac_p=3,
        )

        result = await engine.process_adsb(report)

        self.assertEqual(result.nic, 2)
        self.assertEqual(result.nac_p, 3)
        self.assertEqual(result.last_seen, now)

    async def test_current_adsb_report_without_integrity_clears_stale_metadata(self):
        now = time.time()
        existing = StateVector(
            icao24="ABC123",
            nic=8,
            nac_p=10,
            last_seen=now,
        )
        redis = AsyncMock()
        redis.get.return_value = existing.to_bytes()
        message = RawADSBMessage(
            receiver_id="opensky",
            recv_time=now + 1,
            icao24="ABC123",
            raw_message="",
            msg_type=17,
            lat=40.0,
            lon=-75.0,
            nic=None,
            nac_p=None,
        )

        result = await FusionEngine(redis).process_adsb(message)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertIsNone(result.nic)
        self.assertIsNone(result.nac_p)

    async def test_delayed_adsb_report_does_not_clear_newer_integrity_metadata(self):
        now = time.time()
        existing = StateVector(
            icao24="ABC123",
            nic=8,
            nac_p=10,
            last_seen=now,
        )
        redis = AsyncMock()
        redis.get.return_value = existing.to_bytes()
        message = RawADSBMessage(
            receiver_id="opensky",
            recv_time=now - 1,
            icao24="ABC123",
            raw_message="",
            msg_type=17,
            lat=40.0,
            lon=-75.0,
            nic=None,
            nac_p=None,
        )

        result = await FusionEngine(redis).process_adsb(message)

        self.assertIsNotNone(result)
        assert result is not None
        self.assertEqual(result.nic, 8)
        self.assertEqual(result.nac_p, 10)

    async def test_aggregator_dropout_is_not_called_transponder_loss(self):
        state = StateVector(
            icao24="ABC123",
            altitude_baro=15000,
            on_ground=False,
            last_seen=time.time() - 300,
            source_reports=[SourceReport(
                source=DataSource.ADSB,
                receiver_id="opensky",
                timestamp=time.time() - 300,
            )],
        )

        flags = RuleEngine().check_all(state)

        self.assertNotIn("transponder_loss", [flag.detector for flag in flags])


if __name__ == "__main__":
    unittest.main()
