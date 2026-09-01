import unittest
from pathlib import Path


APP = Path("frontend/public/app.js").read_text()
HTML = Path("frontend/index.html").read_text()


class ConflictFrontendTests(unittest.TestCase):
    def test_dashboard_displays_passive_conflict_status_and_controls(self):
        self.assertIn('id="src-conflicts"', HTML)
        self.assertIn('id="chk-conflicts"', HTML)
        self.assertIn('id="cconf"', HTML)
        self.assertIn("not TCAS/ACAS advisories", HTML)

    def test_websocket_conflicts_render_as_noninteractive_bounded_links(self):
        self.assertIn("m.conflict_analysis", APP)
        self.assertIn("function renderConflicts", APP)
        renderer = APP.split("function renderConflicts", 1)[1].split("function", 1)[0]
        self.assertIn("conflictLayerGroup.clearLayers()", renderer)
        self.assertIn(".slice(0,500)", renderer)
        self.assertIn("L.polyline", renderer)
        self.assertIn("interactive:false", renderer)
        self.assertIn("PREDICTED_LOSS_OF_SEPARATION", renderer)

    def test_conflict_links_use_shortest_arc_across_dateline(self):
        self.assertIn("function conflictLineEndpoints", APP)
        helper = APP.split("function conflictLineEndpoints", 1)[1].split("function", 1)[0]
        self.assertIn("if(delta>180) secondLon-=360", helper)
        self.assertIn("else if(delta<-180) secondLon+=360", helper)
        renderer = APP.split("function renderConflicts", 1)[1].split("function", 1)[0]
        self.assertIn("L.polyline(conflictLineEndpoints(first,second)", renderer)

    def test_unavailable_conflict_analysis_is_not_shown_as_healthy(self):
        self.assertIn(
            "const conflictReady=_conflictAnalysis.analysis_available!==false;",
            APP,
        )
        self.assertIn(
            "updateSourceStatus('conflicts',conflictReady?",
            APP,
        )


if __name__ == "__main__":
    unittest.main()
