"""
Tests for check_for_updates utility.
"""

import warnings
from unittest.mock import MagicMock, patch

from access_moppy.utilities import check_for_updates


class TestCheckForUpdates:
    """Tests for the check_for_updates function."""

    def _make_response(self, version: str) -> MagicMock:
        """Create a mock requests.Response returning the given PyPI version."""
        mock_response = MagicMock()
        mock_response.json.return_value = {"info": {"version": version}}
        mock_response.raise_for_status.return_value = None
        return mock_response

    @patch("access_moppy.utilities.requests")
    def test_newer_version_warns(self, mock_requests):
        """A newer version on PyPI should produce a UserWarning."""
        mock_requests.get.return_value = self._make_response("99.99.99")

        with patch(
            "access_moppy.utilities.importlib_metadata.version", return_value="0.1.0"
        ):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                check_for_updates()

        assert len(caught) == 1
        assert issubclass(caught[0].category, UserWarning)
        assert "99.99.99" in str(caught[0].message)
        assert "0.1.0" in str(caught[0].message)

    @patch("access_moppy.utilities.requests")
    def test_up_to_date_no_warning(self, mock_requests):
        """No warning should be issued when already on the latest version."""
        mock_requests.get.return_value = self._make_response("0.1.0")

        with patch(
            "access_moppy.utilities.importlib_metadata.version", return_value="0.1.0"
        ):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                check_for_updates()

        version_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        assert len(version_warnings) == 0

    @patch("access_moppy.utilities.requests")
    def test_network_error_silent(self, mock_requests):
        """Network errors should be swallowed silently."""
        import requests as real_requests

        mock_requests.get.side_effect = real_requests.exceptions.ConnectionError(
            "No internet"
        )

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            check_for_updates()  # Must not raise

        version_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        assert len(version_warnings) == 0

    @patch("access_moppy.utilities.requests")
    def test_timeout_error_silent(self, mock_requests):
        """Timeout errors should be swallowed silently."""
        import requests as real_requests

        mock_requests.get.side_effect = real_requests.exceptions.Timeout("Timed out")

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            check_for_updates()  # Must not raise

        version_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        assert len(version_warnings) == 0

    @patch("access_moppy.utilities.requests")
    def test_bad_response_silent(self, mock_requests):
        """A malformed PyPI response should be swallowed silently."""
        mock_response = MagicMock()
        mock_response.json.return_value = {}  # missing 'info' key
        mock_response.raise_for_status.return_value = None
        mock_requests.get.return_value = mock_response

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            check_for_updates()  # Must not raise

        version_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        assert len(version_warnings) == 0

    @patch("access_moppy.utilities.requests")
    def test_env_var_disables_check(self, mock_requests):
        """Setting ACCESS_MOPPY_DISABLE_UPDATE_CHECK should skip the check."""
        import os

        with patch.dict(os.environ, {"ACCESS_MOPPY_DISABLE_UPDATE_CHECK": "1"}):
            check_for_updates()

        mock_requests.get.assert_not_called()
