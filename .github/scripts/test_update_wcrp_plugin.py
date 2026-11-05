"""Offline regression tests for the scheduled WCRP dependency update."""

import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch
from urllib.error import URLError

from packaging.version import Version
from update_wcrp_plugin import latest_stable_release, main, update_pin

MANIFEST = """[tool.pixi.pypi-dependencies]
access_moppy = { path = ".", editable = true }
cc-plugin-wcrp = "==2.3.5"  # Reviewed release
esgvoc = "*"

[tool.pixi.pypi-exclude-newer]
cc-plugin-wcrp = "0d"
"""


class LatestStableReleaseTests(unittest.TestCase):
    def test_skips_prereleases_development_yanked_and_empty_releases(self):
        releases = {
            "2.3.5": [{"yanked": False}],
            "2.4.0a1": [{"yanked": False}],
            "2.4.0b3": [{"yanked": False}],
            "2.4.0rc1": [{"yanked": False}],
            "2.4.0.dev1": [{"yanked": False}],
            "2.4.0": [{"yanked": True}, {"yanked": True}],
            "2.5.0": [],
            "2.6.0+local": [{"yanked": False}],
            "not-a-version": [{"yanked": False}],
        }
        self.assertEqual(latest_stable_release(releases), Version("2.3.5"))

    def test_compares_versions_numerically_and_accepts_post_releases(self):
        releases = {
            "2.10.0.post1": [{"yanked": True}, {"yanked": False}],
            "2.9.0": [{"yanked": False}],
            "2.10.0": [{"yanked": False}],
        }
        self.assertEqual(latest_stable_release(releases), Version("2.10.0.post1"))

    def test_fails_when_no_usable_release_exists(self):
        for releases in ({}, {"2.4.0b3": [{"yanked": False}]}):
            with self.subTest(releases=releases), self.assertRaises(ValueError):
                latest_stable_release(releases)


class UpdatePinTests(unittest.TestCase):
    def setUp(self):
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        self.path = Path(temporary_directory.name) / "pyproject.toml"
        self.path.write_text(MANIFEST, encoding="utf-8")

    def test_changes_only_dependency_pin_and_is_idempotent(self):
        self.assertEqual(
            update_pin(self.path, Version("2.4.0")), (Version("2.3.5"), True)
        )
        expected = MANIFEST.replace("==2.3.5", "==2.4.0")
        self.assertEqual(self.path.read_text(encoding="utf-8"), expected)
        self.assertEqual(
            update_pin(self.path, Version("2.4.0")), (Version("2.4.0"), False)
        )
        self.assertEqual(self.path.read_text(encoding="utf-8"), expected)

    def test_never_downgrades_or_rewrites_unchanged_file(self):
        before = self.path.stat().st_mtime_ns
        for latest in ("2.3.4", "2.3.5"):
            with self.subTest(latest=latest):
                self.assertEqual(
                    update_pin(self.path, Version(latest)), (Version("2.3.5"), False)
                )
        self.assertEqual(self.path.stat().st_mtime_ns, before)
        self.assertEqual(self.path.read_text(encoding="utf-8"), MANIFEST)

    def test_rejects_unexpected_constraint_without_changing_file(self):
        for constraint in ('"*"', '">=2.3.5"', '"==2.*"', '{ version = "==2.3.5" }'):
            with self.subTest(constraint=constraint):
                content = MANIFEST.replace('"==2.3.5"', constraint)
                self.path.write_text(content, encoding="utf-8")
                with self.assertRaises(ValueError):
                    update_pin(self.path, Version("2.4.0"))
                self.assertEqual(self.path.read_text(encoding="utf-8"), content)

    def test_cli_fetches_pypi_and_reports_outputs_for_update_and_no_change(self):
        payload = {
            "releases": {"2.4.0": [{"yanked": False}], "3.0.0b1": [{"yanked": False}]}
        }
        output = self.path.with_name("github-output")
        for current, changed in (("2.3.5", "true"), ("2.4.0", "false")):
            with (
                self.subTest(current=current),
                patch(
                    "sys.argv", ["update_wcrp_plugin.py", "--pyproject", str(self.path)]
                ),
                patch.dict("os.environ", {"GITHUB_OUTPUT": str(output)}),
                patch(
                    "update_wcrp_plugin.urlopen",
                    return_value=io.BytesIO(json.dumps(payload).encode()),
                ) as fetch,
            ):
                output.write_text("", encoding="utf-8")
                main()
                fetch.assert_called_once_with(
                    "https://pypi.org/pypi/cc-plugin-wcrp/json", timeout=30
                )
                self.assertEqual(
                    output.read_text(encoding="utf-8"),
                    f"current={current}\nlatest=2.4.0\nchanged={changed}\n",
                )

    def test_network_failure_leaves_manifest_unchanged(self):
        with (
            patch("sys.argv", ["update_wcrp_plugin.py", "--pyproject", str(self.path)]),
            patch("update_wcrp_plugin.urlopen", side_effect=URLError("unavailable")),
            self.assertRaises(URLError),
        ):
            main()
        self.assertEqual(self.path.read_text(encoding="utf-8"), MANIFEST)


if __name__ == "__main__":
    unittest.main()
