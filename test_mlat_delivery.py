import time
import unittest
from unittest.mock import AsyncMock, MagicMock

import orjson

from models import RawADSBMessage, RawMLATReport
from processing.mlat_solver import (
    FrameAccumulator, MLATSolver, ReceiverRegistry, TDOAFrame,
    process_adsb_for_mlat,
)


class MlatDeliveryTests(unittest.IsolatedAsyncioTestCase):
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
        self.assertIn("ABC123:frame", restored._frames)

    async def test_report_is_acknowledged_before_handler_returns(self):
        report = RawMLATReport(
            session_id="session", solve_time=100.0, icao24="ABC123",
            lat=0.0, lon=0.0, altitude_baro=10000, num_receivers=4,
            tdoa_residual=10.0, cep90=10.0,
        )
        accumulator = MagicMock()
        accumulator.add_message.return_value = report
        producer = AsyncMock()
        message = RawADSBMessage(
            receiver_id="receiver", recv_time=100.0, raw_message="",
            msg_type=17, icao24="ABC123", lat=0.0, lon=0.0,
        )

        solved = await process_adsb_for_mlat(accumulator, producer, message.to_bytes())

        self.assertTrue(solved)
        producer.send_and_wait.assert_awaited_once()
        producer.send.assert_not_called()

    async def test_delivery_failure_propagates_to_prevent_offset_commit(self):
        report = RawMLATReport(
            session_id="session", solve_time=100.0, icao24="ABC123",
            lat=0.0, lon=0.0, altitude_baro=10000, num_receivers=4,
            tdoa_residual=10.0, cep90=10.0,
        )
        accumulator = MagicMock()
        accumulator.add_message.return_value = report
        producer = AsyncMock()
        producer.send_and_wait.side_effect = RuntimeError("broker unavailable")
        message = RawADSBMessage(
            receiver_id="receiver", recv_time=100.0, raw_message="",
            msg_type=17, icao24="ABC123", lat=0.0, lon=0.0,
        )

        with self.assertRaisesRegex(RuntimeError, "broker unavailable"):
            await process_adsb_for_mlat(accumulator, producer, message.to_bytes())


if __name__ == "__main__":
    unittest.main()