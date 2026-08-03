from pathlib import Path


HTML = Path("frontend/index.html").read_text()


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
    assert "'/api/coverage'" in HTML or '"/api/coverage"' in HTML
    assert "loadCoverage" in HTML
    assert "applyCoverage" in HTML
    assert "method:'PUT'" in HTML
    assert "function insideCoverage" in HTML
    assert "aircraft tracked in current area" in HTML
    assert "if (aircraft.length === 0) return;" not in HTML
