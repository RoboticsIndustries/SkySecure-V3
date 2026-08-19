import unittest
from pathlib import Path


class FrontendSecurityRegressionTests(unittest.TestCase):
    def test_nginx_reresolves_api_after_container_recreation(self):
        nginx = (Path(__file__).parent / "frontend" / "nginx.conf").read_text()
        self.assertIn("resolver 127.0.0.11", nginx)
        self.assertIn("set $api_upstream http://api:8000;", nginx)
        self.assertGreaterEqual(nginx.count("proxy_pass $api_upstream;"), 2)

    def test_rest_snapshot_stays_warm_for_immediate_websocket_fallback(self):
        script = (Path(__file__).parent / "frontend" / "public" / "app.js").read_text()
        self.assertIn("_backendCache = {};", script)
        self.assertIn("if (a.icao) _directCache[a.icao] = a;", script)
        self.assertNotIn("(!_backendAlive || !_backendCache[a.icao])", script)

    @classmethod
    def setUpClass(cls):
        root = Path(__file__).parent / "frontend"
        cls.html = (root / "index.html").read_text() + (root / "public" / "app.js").read_text()

    def test_browser_does_not_send_telemetry_directly_to_anthropic(self):
        self.assertNotIn("api.anthropic.com", self.html)
        self.assertNotIn("Analyse with Claude", self.html)

    def test_csv_cells_are_html_escaped(self):
        self.assertIn("function esc(value)", self.html)
        self.assertIn("esc(r[h]||'--')", self.html)

    def test_anomaly_text_is_escaped_before_inner_html_rendering(self):
        self.assertIn("const anomalyText=", self.html)
        self.assertIn("esc(anomalyText)", self.html)

    def test_live_feed_icao_is_restricted_before_inline_handler_use(self):
        self.assertIn("/^[0-9A-F]{6}$/.test(icao)", self.html)

    def test_all_reviewed_inner_html_text_values_are_escaped(self):
        self.assertIn("+esc(cls)+'</span>", self.html)
        self.assertIn("+esc(e.ts)+'</div><div class=\"evtmsg\">'+esc(e.msg)", self.html)
        self.assertIn("+esc(v)+'</div><div class=\"l\">'+esc(l)", self.html)
        self.assertIn("Result — '+esc(a.icao||'?')", self.html)
        self.assertIn("Callsign</span><span>'+esc(a.cs||'--')", self.html)
        self.assertIn("reasons.map(r=>'<span style=\"color:#ef4444;font-size:12px\">+ '+esc(r)", self.html)

    def test_executable_assets_are_local_and_nginx_sets_csp(self):
        self.assertNotIn("cdn.jsdelivr.net", self.html)
        self.assertIn('/vendor/leaflet-1.9.4.min.js', self.html)
        self.assertIn('/vendor/chart-4.4.0.umd.min.js', self.html)
        nginx = (Path(__file__).parent / "frontend" / "nginx.conf").read_text()
        self.assertIn("Content-Security-Policy", nginx)
        self.assertIn("object-src 'none'", nginx)
        self.assertIn("frame-ancestors 'none'", nginx)
        self.assertIn("const API = window.SKYSECURE_API_URL || '';", self.html)
        self.assertIn("window.location.host + '/ws/tracks'", self.html)
        self.assertIn("https://*.basemaps.cartocdn.com", nginx)
        self.assertNotIn("script-src 'self' 'unsafe-inline'", nginx)
        self.assertIn("connect-src 'self';", nginx)
        self.assertNotIn("connect-src 'self' ws: wss:", nginx)
        self.assertIn("resolver 127.0.0.11", nginx)
        self.assertIn("proxy_pass $api_upstream", nginx)
        self.assertIn('/app.js', self.html)
        self.assertIn("_backendCache = {};", self.html)
        self.assertIn("if (a.icao) _directCache[a.icao] = a;", self.html)

    def test_detection_wording_is_qualified(self):
        self.assertIn("Heuristically Suspicious", self.html)
        self.assertIn("not independent confirmation of an attack", self.html)


if __name__ == "__main__":
    unittest.main()
