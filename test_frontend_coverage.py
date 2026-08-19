from pathlib import Path


HTML = Path("frontend/index.html").read_text()
APP = Path("frontend/public/app.js").read_text()


def test_dashboard_has_live_airport_and_custom_area_controls():
    for element_id in (
        'id="coverage-airport"',
        'id="coverage-lat"',
        'id="coverage-lon"',
        'id="coverage-radius"',
        'id="coverage-apply"',
        'id="coverage-map-center"',
        'id="coverage-status"',
    ):
        assert element_id in HTML


def test_dashboard_loads_and_updates_runtime_coverage_api():
    assert '<script src="/app.js"></script>' in HTML
    assert "'/api/coverage'" in APP or '"/api/coverage"' in APP
    assert "loadCoverage" in APP
    assert "applyCoverage" in APP
    assert "method:'PUT'" in APP
    assert "function insideCoverage" in APP
    assert "aircraft tracked in current area" in APP
    assert "if (aircraft.length === 0) return;" not in APP
