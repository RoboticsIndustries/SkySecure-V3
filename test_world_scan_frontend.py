import unittest
from pathlib import Path

HTML = Path("frontend/index.html").read_text()
APP = Path("frontend/public/app.js").read_text()


class WorldScanFrontendTests(unittest.TestCase):
    def test_dashboard_exposes_world_scan_and_historical_time_controls(self):
        for element_id in (
            'id="world-scan-toggle"',
            'id="world-scan-dwell"',
            'id="world-scan-status"',
            'id="history-window"',
            'id="history-markers"',
            'id="hotspot-layer"',
        ):
            self.assertIn(element_id, HTML)

    def test_dashboard_loads_durable_anomalies_and_hotspots(self):
        self.assertIn("/api/anomalies/history?hours=", APP)
        self.assertIn("/api/anomalies/hotspots?hours=", APP)
        self.assertIn("loadHistoricalAnomalies", APP)
        self.assertIn("renderHistoricalAnomalies", APP)
        self.assertIn("renderHotspots", APP)
        self.assertIn("#a855f7", APP)
        self.assertIn("Evidence window", APP)
        self.assertIn("Active within", APP)

    def test_historical_markers_remain_a_distinct_layer_for_live_icaos(self):
        history_renderer = APP.split("function renderHistoricalAnomalies", 1)[1].split(
            "function renderHotspots", 1
        )[0]
        self.assertIn("historicalLayerGroup", history_renderer)
        self.assertNotIn("liveIds", history_renderer)

    def test_live_updates_do_not_rebuild_all_historical_markers(self):
        aircraft_renderer = APP.split("function renderAircraft", 1)[1].split(
            "function buildPopup", 1
        )[0]
        self.assertNotIn("renderHistoricalAnomalies", aircraft_renderer)

    def test_dashboard_can_start_and_stop_operator_authorized_world_scan(self):
        self.assertTrue("'/api/world-scan'" in APP or '"/api/world-scan"' in APP)
        self.assertIn("updateWorldScan", APP)
        self.assertIn("X-SkySecure-Operator-Key", APP)
        self.assertIn("dwell_seconds", APP)

    def test_world_scan_reprompts_and_retries_after_expired_operator_key(self):
        self.assertIn("operatorFetch", APP)
        self.assertIn("if(r.status===403)", APP)
        self.assertIn("if(r.status===503)", APP)
        self.assertIn("forcePrompt", APP)
        self.assertIn("Scanner authorization failed", APP)

    def test_anomaly_aircraft_identifiers_open_internal_aircraft_details(self):
        self.assertIn("data-aircraft-details", APP)
        self.assertIn("openAircraftDetails", APP)
        self.assertIn("historicalEvents.filter", APP)
        self.assertNotIn("target=\"_blank\"", APP)


if __name__ == "__main__":
    unittest.main()
