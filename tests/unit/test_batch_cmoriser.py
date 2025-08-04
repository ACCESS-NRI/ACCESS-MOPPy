"""Unit tests for batch CMORiser functionality."""

# Security: All subprocess usage in this file is for mocking in unit tests
# ruff: noqa: S603, S607
# bandit: skip
# semgrep: skip

from unittest.mock import Mock, mock_open, patch

import pytest

from access_mopper.batch_cmoriser import create_job_script, submit_job
from tests.mocks.mock_pbs import MockPBSManager, mock_qsub_success


class TestBatchCmoriser:
    """Unit tests for batch processing functions."""

    @patch("jinja2.Template")
    @patch("access_mopper.batch_cmoriser.files")
    @patch("os.chmod")
    @pytest.mark.unit
    def test_create_job_script(self, mock_chmod, mock_files, mock_template, temp_dir):
        """Test job script creation."""
        # Mock template files
        mock_file_obj = Mock()
        mock_file_obj.read.return_value = "mock template"
        mock_files.return_value.joinpath.return_value.open.return_value.__enter__.return_value = mock_file_obj

        # Mock template rendering
        mock_template_instance = Mock()
        mock_template_instance.render.return_value = "rendered script"
        mock_template.return_value = mock_template_instance

        config = {
            "cpus_per_node": 4,
            "mem": "16GB",
            "walltime": "01:00:00",
            "experiment_id": "historical",
        }

        with patch("builtins.open", mock_open()) as mock_file:
            result = create_job_script("Amon.tas", config, "/db/path", temp_dir)

        # Verify script was created
        expected_path = temp_dir / "cmor_Amon_tas.sh"
        assert result == expected_path
        mock_file.assert_called()
        mock_chmod.assert_called()

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_submit_job_success(self, mock_run):
        """Test successful job submission."""
        mock_run.return_value = mock_qsub_success()

        job_id = submit_job("/path/to/script.sh")

        assert job_id is not None
        assert len(job_id) > 0
        mock_run.assert_called_once()

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_submit_job_failure(self, mock_run):
        """Test failed job submission."""
        import subprocess  # nosec  # Only used for mocking CalledProcessError in tests

        mock_run.side_effect = subprocess.CalledProcessError(
            returncode=1,
            cmd=["qsub", "/path/to/script.sh"],
            stderr="qsub: job rejected by server",
        )

        job_id = submit_job("/path/to/script.sh")

        assert job_id is None

    @pytest.mark.unit
    def test_mock_pbs_manager(self):
        """Test the MockPBSManager functionality."""
        with MockPBSManager() as pbs:
            # Submit a mock job
            job_id = submit_job("/mock/script.sh")

            assert job_id is not None

            # Extract the numeric part of the job ID (remove .gadi-pbs suffix)
            job_id_key = job_id.split(".")[0] if "." in job_id else job_id

            # Test job state changes
            pbs.mark_job_running(job_id_key)
            pbs.mark_job_completed(job_id_key)

            # Verify job is tracked
            assert job_id_key in pbs.submitted_jobs
            assert pbs.submitted_jobs[job_id_key]["status"] == "C"
