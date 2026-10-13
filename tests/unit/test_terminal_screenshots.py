"""Unit tests for the documentation terminal-screenshot generator.

The generator renders the real ``moppy-tui`` layout, so these tests fail
whenever a dashboard change breaks it — before the documentation build does.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

pytest.importorskip("rich")

GENERATOR = Path(__file__).resolve().parents[2] / "docs" / "terminal_screenshots.py"


@pytest.fixture(scope="module")
def screenshots(tmp_path_factory):
    spec = importlib.util.spec_from_file_location("_terminal_screenshots", GENERATOR)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    out_dir = tmp_path_factory.mktemp("terminal")
    written = module.generate(out_dir)
    return {path.stem: path.read_text() for path in written}


def test_every_documented_screenshot_is_written(screenshots):
    assert set(screenshots) == {
        "moppy-cmorise-submit",
        "moppy-tui-running",
        "moppy-tui-complete",
        "moppy-tui-failures",
        "moppy-tui-filtered",
    }
    for svg in screenshots.values():
        assert svg.lstrip().startswith("<svg")


def test_tui_screenshots_render_the_real_dashboard(screenshots):
    svg = screenshots["moppy-tui-failures"]
    # Panel titles and status labels come from cli_dashboard.render().
    for expected in ("CMORisation", "Monitor", "Progress", "Summary", "failed"):
        assert expected in svg


def test_filtered_screenshot_shows_the_filtered_task_count(screenshots):
    assert "filtered" in screenshots["moppy-tui-filtered"]


def test_screenshots_do_not_fetch_a_webfont(screenshots):
    """rich links Fira Code from a CDN; the docs ship self-contained SVGs."""
    for name, svg in screenshots.items():
        assert "@font-face" not in svg, name
        assert "cdnjs" not in svg, name
