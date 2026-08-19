import time
import unittest
from unittest.mock import AsyncMock, MagicMock

import numpy as np
import orjson
from pydantic import ValidationError

from config import Settings
from models import RawADSBMessage, RawMLATReport
from processing.mlat_solver import (
    FrameAccumulator, MLATSolver, ReceiverRegistry, TDOAFrame,
    geodetic_to_ecef, horizontal_cep90, process_adsb_for_mlat,
    validate_receiver_configuration,
)


class MlatDeliveryTests(unittest.IsolatedAsyncioTestCase):
    def test_cep90_projects_covariance_into_local_enu(self):
        covariance_lon0 = np.diag([1e12, 25.0, 100.0])
        covariance_lon90 = np.diag([25.0, 1e12, 100.0])
        at_lon0 = geodetic_to_ecef(0.0, 0.0, 10_000.0)
        at_lon90 = geodetic_to_ecef(0.0, 90.0, 10_000.0)
        self.assertAlmostEqual(horizontal_cep90(at_lon0, covariance_lon0), 21.46, places=2)
        self.assertAlmostEqual(horizontal_cep90(at_lon90, covariance_lon90), 21.46, places=2)

    def test_receiver_configuration_is_validated_centrally(self):
        valid = {
            "a": (40.00, -75.00, 10.0),
            "b": (40.00, -74.98, 10.0),
            "c": (40.02, -75.00, 10.0),
            "d": (40.02, -74.98, 10.0),
        }
        self.assertEqual(set(validate_receiver_configuration(valid, 4)), set(valid))
        for invalid in (
            dict(list(valid.items())[:3]),
            {**valid, "e": (float("nan"), 0.0, 0.0)},
            {"a": (40.0, -75.0, 0.0), "b": (40.01, -75.0, 0.0),
             "c": (40.02, -75.0, 0.0), "d": (40.03, -75.0, 0.0)},
        ):
            with self.subTest(invalid=invalid):
                with self.assertRaises(ValueError):
                    validate_receiver_configuration(invalid, 4)

    async def test_stale_and_future_receptions_do_not_mutate_or_persist_accumulator(self):
        for recv_time in (
            time.time() - 301.0,
            time.time() + 6.0,
        ):
            with self.subTest(recv_time=recv_time):
                registry = ReceiverRegistry()
                registry.register("receiver", 40.0, -75.0)
                accumulator = FrameAccumulator(registry, MLATSolver())
                producer = AsyncMock()
                state_store = AsyncMock()
                message = RawADSBMessage(
                    receiver_id="receiver", recv_time=recv_time,
                    raw_message="frame", msg_type=17, icao24="ABC123",
                )

                solved = await process_adsb_for_mlat(
                    accumulator, producer, message.to_bytes(), state_store,
                )

                self.assertFalse(solved)
                self.assertEqual(accumulator.snapshot(), {"frames": {}, "solved": {}})
                producer.send_and_wait.assert_not_awaited()
                state_store.setex.assert_not_awaited()

    def test_mlat_rejects_colocated_and_collinear_receiver_geometry(self):
        now = time.time()
        for layout in (
            [(40.0, -75.0)] * 4,
            [(40.0 + i * 0.02, -75.0) for i in range(4)],
            [(10.0 + i * 20.0, -75.0) for i in range(4)],
        ):
            with self.subTest(layout=layout):
                registry = ReceiverRegistry()
                solver = MagicMock()
                solver.solve.return_value = {
                    "lat": 40.05, "lon": -74.95, "alt_ft": 10_000,
                    "num_receivers": 4, "tdoa_residual": 10.0, "cep90": 100.0,
                }
                accumulator = FrameAccumulator(registry, solver)
                for i, (lat, lon) in enumerate(layout):
                    registry.register(f"r{i}", lat, lon)
                    accumulator.add_message(RawADSBMessage(
                        receiver_id=f"r{i}", recv_time=now + i * 0.000001,
                        icao24="ABC123", raw_message="GEOMETRY", msg_type=17,
                    ))
                solver.solve.assert_not_called()

    def test_mlat_accepts_well_spread_noncollinear_receiver_geometry(self):
        now = time.time()
        registry = ReceiverRegistry()
        solver = MagicMock()
        solver.solve.return_value = {
            "lat": 40.05, "lon": -74.95, "alt_ft": 10_000,
            "num_receivers": 4, "tdoa_residual": 10.0, "cep90": 100.0,
        }
        accumulator = FrameAccumulator(registry, solver)
        layout = [
            (40.0, -75.0), (40.0, -74.9),
            (40.1, -75.0), (40.1, -74.9),
        ]
        report = None
        for i, (lat, lon) in enumerate(layout):
            registry.register(f"r{i}", lat, lon)
            report = accumulator.add_message(RawADSBMessage(
                receiver_id=f"r{i}", recv_time=now + i * 0.000001,
                icao24="ABC123", raw_message="GEOMETRY", msg_type=17,
            ))

        self.assertIsNotNone(report)
        solver.solve.assert_called_once()

    def test_overlapping_transmission_frames_choose_nearest_not_first(self):
        registry = ReceiverRegistry()
        registry.register("new", 40.0, -75.0)
        accumulator = FrameAccumulator(registry, MLATSolver())
        base = "ABC123:PAYLOAD"
        older = TDOAFrame("ABC123", "PAYLOAD")
        older.add_reception("old-a", 100.000)
        newer = TDOAFrame("ABC123", "PAYLOAD")
        newer.add_reception("new-a", 100.009)
        accumulator._frames[f"{base}:old"] = older
        accumulator._frames[f"{base}:new"] = newer

        accumulator.add_message(RawADSBMessage(
            receiver_id="new", recv_time=100.007, icao24="ABC123",
            raw_message="PAYLOAD", msg_type=17,
        ))

        self.assertEqual(len(older.receptions), 1)
        self.assertEqual(newer.receptions[-1], ("new", 100.007))

    def test_exactly_ambiguous_transmission_reception_is_not_mixed(self):
        registry = ReceiverRegistry()
        registry.register("new", 40.0, -75.0)
        accumulator = FrameAccumulator(registry, MLATSolver())
        base = "ABC123:PAYLOAD"
        left = TDOAFrame("ABC123", "PAYLOAD")
        left.add_reception("left", 100.000)
        right = TDOAFrame("ABC123", "PAYLOAD")
        right.add_reception("right", 100.010)
        accumulator._frames[f"{base}:left"] = left
        accumulator._frames[f"{base}:right"] = right

        accumulator.add_message(RawADSBMessage(
            receiver_id="new", recv_time=100.005, icao24="ABC123",
            raw_message="PAYLOAD", msg_type=17,
        ))

        self.assertEqual(left.receptions, [("left", 100.000)])
        self.assertEqual(right.receptions, [("right", 100.010)])

    def test_duplicate_receiver_cannot_migrate_across_overlapping_frames(self):
        registry = ReceiverRegistry()
        registry.register("duplicate", 40.0, -75.0)
        accumulator = FrameAccumulator(registry, MLATSolver())
        base = "ABC123:PAYLOAD"
        first = TDOAFrame("ABC123", "PAYLOAD")
        first.add_reception("duplicate", 100.000)
        second = TDOAFrame("ABC123", "PAYLOAD")
        second.add_reception("other", 100.006)
        accumulator._frames[f"{base}:first"] = first
        accumulator._frames[f"{base}:second"] = second

        accumulator.add_message(RawADSBMessage(
            receiver_id="duplicate", recv_time=100.007,
            icao24="ABC123", raw_message="PAYLOAD", msg_type=17,
        ))

        self.assertEqual(first.receptions, [("duplicate", 100.000)])
        self.assertEqual(second.receptions, [("other", 100.006)])

    def test_mlat_receiver_threshold_cannot_be_configured_below_solver_minimum(self):
        with self.assertRaises(ValueError):
            Settings(MLAT_MIN_RECEIVERS=3)

    def test_partial_frame_survives_snapshot_restore(self):
        accumulator = FrameAccumulator(ReceiverRegistry(), MLATSolver())
        frame = TDOAFrame("ABC123", "raw-frame")
        frame.add_reception("receiver-a", 100.0)
        accumulator._frames["ABC123:raw-frame"] = frame

        restored = FrameAccumulator(ReceiverRegistry(), MLATSolver())
        restored.restore(accumulator.snapshot())

        self.assertEqual(
            restored._frames["ABC123:raw-frame"].receptions,
            [("receiver-a", 100.0)],
        )
        restored.registry.register("receiver-b", 51.51, -0.1)
        restored.add_message(RawADSBMessage(
            receiver_id="receiver-b", recv_time=100.000001,
            raw_message="raw-frame", msg_type=17, icao24="ABC123",
        ))
        self.assertEqual(len(restored._frames), 1)
        self.assertEqual(len(restored._frames["ABC123:raw-frame"].receptions), 2)

    async def test_partial_frame_is_persisted_before_input_can_commit(self):
        registry = ReceiverRegistry()
        registry.register("receiver", 51.5, -0.1)
        accumulator = FrameAccumulator(registry, MLATSolver())
        producer = AsyncMock()
        state_store = AsyncMock()
        message = RawADSBMessage(
            receiver_id="receiver", recv_time=time.time(), raw_message="frame",
            msg_type=17, icao24="ABC123", lat=0.0, lon=0.0,
        )

        solved = await process_adsb_for_mlat(
            accumulator, producer, message.to_bytes(), state_store
        )

        self.assertFalse(solved)
        state_store.setex.assert_awaited_once()
        restored = FrameAccumulator(registry, MLATSolver())
        restored.restore(orjson.loads(state_store.setex.await_args.args[2]))
        self.assertEqual(len(restored._frames), 1)
        self.assertEqual(next(iter(restored._frames.values())).raw_message, "frame")

    def test_raw_mlat_rejects_untrusted_quality_and_receiver_shapes(self):
        base = dict(
            session_id="session", solve_time=100.0, icao24="ABC123",
            lat=0.0, lon=0.0, altitude_baro=10000, num_receivers=4,
            tdoa_residual=10.0, cep90=10.0,
            receiver_ids=["a", "b", "c", "d"],
            source_event_ids=[f"{i:064x}" for i in range(4)],
        )
        invalid = [
            {"tdoa_residual": -1.0}, {"tdoa_residual": 500.0001},
            {"cep90": -1.0}, {"cep90": 10_000.0001},
            {"num_receivers": 3, "receiver_ids": ["a", "b", "c"]},
            {"receiver_ids": ["a", "b", "c"]},
            {"receiver_ids": ["a", "a", "b", "c"]},
            {"velocity": -1.0}, {"heading": 360.0},
        ]
        for override in invalid:
            with self.subTest(override=override), self.assertRaises(ValidationError):
                RawMLATReport(**(base | override))

    def test_unconstrained_solver_requires_four_receivers(self):
        solver = MLATSolver()
        positions = [np.array([6371000.0 + i, i * 10.0, 0.0]) for i in range(3)]
        self.assertIsNone(solver.solve(positions, [100.0, 100.000001, 100.000002]))

    def test_repeated_payloads_in_distinct_event_windows_are_not_suppressed(self):
        registry = ReceiverRegistry()
        layout = [(40.0, -75.0), (40.0, -74.9), (40.1, -75.0), (40.1, -74.9)]
        for i, (lat, lon) in enumerate(layout):
            registry.register(f"r{i}", lat, lon)
        solver = MagicMock()
        solver.solve.return_value = {
            "lat": 40.0, "lon": -75.0, "alt_ft": 10000,
            "num_receivers": 4, "tdoa_residual": 10.0, "cep90": 10.0,
        }
        accumulator = FrameAccumulator(registry, solver)

        def transmit(base_time):
            result = None
            for i in range(4):
                result = accumulator.add_message(RawADSBMessage(
                    receiver_id=f"r{i}", recv_time=base_time + i * 0.000001,
                    raw_message="same-payload", msg_type=17, icao24="ABC123",
                ))
            return result

        first = transmit(100.0)
        second = transmit(100.1)
        self.assertIsNotNone(first)
        self.assertIsNotNone(second)
        self.assertAlmostEqual(first.solve_time, 100.0000015, places=6)
        self.assertAlmostEqual(second.solve_time, 100.1000015, places=6)

    async def test_report_is_acknowledged_before_handler_returns(self):
        now = time.time()
        report = RawMLATReport(
            session_id="session", solve_time=now, icao24="ABC123",
            lat=0.0, lon=0.0, altitude_baro=10000, num_receivers=4,
            tdoa_residual=10.0, cep90=10.0,
            receiver_ids=["a", "b", "c", "d"],
            source_event_ids=[f"{i:064x}" for i in range(4)],
        )
        accumulator = MagicMock()
        accumulator.add_message.return_value = report
        producer = AsyncMock()
        message = RawADSBMessage(
            receiver_id="receiver", recv_time=now, raw_message="",
            msg_type=17, icao24="ABC123", lat=0.0, lon=0.0,
        )

        solved = await process_adsb_for_mlat(accumulator, producer, message.to_bytes())

        self.assertTrue(solved)
        producer.send_and_wait.assert_awaited_once()
        producer.send.assert_not_called()

    async def test_delivery_failure_propagates_to_prevent_offset_commit(self):
        now = time.time()
        report = RawMLATReport(
            session_id="session", solve_time=now, icao24="ABC123",
            lat=0.0, lon=0.0, altitude_baro=10000, num_receivers=4,
            tdoa_residual=10.0, cep90=10.0,
            receiver_ids=["a", "b", "c", "d"],
            source_event_ids=[f"{i:064x}" for i in range(4)],
        )
        accumulator = MagicMock()
        accumulator.add_message.return_value = report
        producer = AsyncMock()
        producer.send_and_wait.side_effect = RuntimeError("broker unavailable")
        message = RawADSBMessage(
            receiver_id="receiver", recv_time=now, raw_message="",
            msg_type=17, icao24="ABC123", lat=0.0, lon=0.0,
        )

        with self.assertRaisesRegex(RuntimeError, "broker unavailable"):
            await process_adsb_for_mlat(accumulator, producer, message.to_bytes())


if __name__ == "__main__":
    unittest.main()