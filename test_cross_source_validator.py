import unittest
from unittest.mock import AsyncMock

from processing.cross_source_validator import (
    CrossSourceValidator,
    SourceReport,
)


class CrossSourceValidatorVerdictTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.validator = CrossSourceValidator()

    def test_one_report_without_claim_is_insufficient(self):
        report = SourceReport(
            source="adsb_lol",
            icao="A1B2C3",
            lat=39.95,
            lon=-75.16,
            alt_ft=10000,
            velocity_kts=None,
            heading_deg=None,
            observed_at=1000.0,
        )

        result = self.validator.validate_reports("A1B2C3", [report])

        self.assertEqual(result.verdict, "INSUFFICIENT_SOURCES")
        self.assertFalse(result.is_valid)
        self.assertEqual(result.confidence, 0.0)
        self.assertEqual(result.per_pair_m, {})

    async def test_validate_aircraft_compares_supplied_position_by_default(self):
        report = SourceReport(
            source="adsb_lol",
            icao="A1B2C3",
            lat=39.95,
            lon=-75.16,
            alt_ft=10000,
            velocity_kts=None,
            heading_deg=None,
            observed_at=1000.0,
        )
        self.validator.gather_reports = AsyncMock(return_value=[report])

        result = await self.validator.validate_aircraft(
            "A1B2C3",
            approx_lat=40.95,
            approx_lon=-75.16,
            claimed_observed_at=1000.0,
        )

        self.assertEqual(result.verdict, "SPOOFED")
        self.assertGreater(result.max_disagreement_m, 100_000)
        self.assertIn("adsb_lol-claimed", result.per_pair_m)

    async def test_claim_source_cannot_corroborate_itself(self):
        report = SourceReport(
            source="adsb_lol",
            icao="A1B2C3",
            lat=39.95,
            lon=-75.16,
            alt_ft=10000,
            velocity_kts=None,
            heading_deg=None,
            observed_at=1000.0,
        )
        self.validator.gather_reports = AsyncMock(return_value=[report])

        result = await self.validator.validate_aircraft(
            "A1B2C3",
            approx_lat=39.95,
            approx_lon=-75.16,
            claimed_observed_at=1000.0,
            claimed_source="adsb_lol",
        )

        self.assertEqual(result.verdict, "INSUFFICIENT_SOURCES")
        self.assertEqual(result.sources_used, [])

    async def test_claim_source_is_not_queried_again(self):
        self.validator._fetch_opensky = AsyncMock(return_value=None)
        self.validator._fetch_point_source = AsyncMock(return_value=None)

        await self.validator.validate_aircraft(
            "A1B2C3",
            approx_lat=39.95,
            approx_lon=-75.16,
            claimed_source="adsb_lol",
        )

        queried_sources = [call.args[0] for call in self.validator._fetch_point_source.await_args_list]
        self.assertNotIn("adsb_lol", queried_sources)
        self.assertIn("adsb_fi", queried_sources)


if __name__ == "__main__":
    unittest.main()
