import unittest
from pathlib import Path


README = Path("README.md").read_text()
LAYERS = Path("docs/detection-layers.md").read_text()


class ConflictDocumentationTests(unittest.TestCase):
    def test_public_conflict_analytics_are_not_documented_as_tcas_advisories(self):
        self.assertIn("TCAS-inspired conflict analytics", LAYERS)
        self.assertIn("not an onboard TCAS/ACAS Traffic Advisory or Resolution Advisory", LAYERS)
        self.assertIn("never generates climb or descend commands", LAYERS)

    def test_conflict_endpoint_is_documented(self):
        self.assertIn("GET /api/conflicts", README)
        self.assertIn("PASSIVE_NON_OPERATIONAL", README)

    def test_conflict_monitor_architecture_and_lifecycle_are_documented(self):
        self.assertIn("dedicated `conflict-monitor` service", LAYERS)
        self.assertIn("coherent ADS-B `SourceReport`", LAYERS)
        self.assertIn("simultaneous horizontal and vertical violation intervals", LAYERS)
        self.assertIn("`conflict:latest`", LAYERS)
        self.assertIn("two consecutive", LAYERS)
        self.assertIn("three consecutive", LAYERS)


if __name__ == "__main__":
    unittest.main()
