import unittest
from pathlib import Path


class FrontendSecurityRegressionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.html = (Path(__file__).parent / "frontend" / "index.html").read_text()

    def test_browser_does_not_send_telemetry_directly_to_anthropic(self):
        self.assertNotIn("api.anthropic.com", self.html)
        self.assertNotIn("Analyse with Claude", self.html)

    def test_csv_cells_are_html_escaped(self):
        self.assertIn("function esc(value)", self.html)
        self.assertIn("esc(r[h]||'--')", self.html)

    def test_detection_wording_is_qualified(self):
        self.assertIn("Heuristically Suspicious", self.html)
        self.assertIn("not independent confirmation of an attack", self.html)


if __name__ == "__main__":
    unittest.main()
