import unittest
from unittest.mock import AsyncMock

from models import DataSource, StateVector
from processing.fusion_engine import FusionEngine


class FusionPersistenceTests(unittest.IsolatedAsyncioTestCase):
    async def test_save_persists_track_to_postgres(self):
        redis = AsyncMock()
        producer = AsyncMock()
        pool = AsyncMock()
        engine = FusionEngine(redis, postgres_pool=pool)
        sv = StateVector(
            icao24="ABC123",
            callsign="TEST1",
            lat=39.9,
            lon=-75.1,
            altitude_baro=10000,
            velocity=250,
            heading=90,
            vertical_rate=500,
            primary_source=DataSource.ADSB,
            confidence=0.8,
            last_seen=1_700_000_000,
        )

        await engine.save(sv, producer)

        pool.execute.assert_awaited_once()
        sql, *values = pool.execute.await_args.args
        self.assertIn("INSERT INTO track_points", sql)
        self.assertEqual(values[1], "ABC123")
        self.assertEqual(values[2], "TEST1")
        self.assertEqual(values[3], 39.9)
        self.assertEqual(values[4], -75.1)


if __name__ == "__main__":
    unittest.main()
