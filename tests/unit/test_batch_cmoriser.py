"""Unit tests for batch CMORiser functionality."""

# Security: All subprocess usage in this file is for mocking in unit tests
# ruff: noqa: S603, S607
# bandit: skip
# semgrep: skip

from unittest.mock import Mock, mock_open, patch

import pytest

from access_moppy.batch_cmoriser import (
    create_job_script,
    start_dashboard,
    submit_job,
    wait_for_jobs,
)
from tests.mocks.mock_pbs import MockPBSManager, mock_qsub_success


class TestBatchCmoriser:
    """Unit tests for batch processing functions."""

    @patch("jinja2.Template")
    @patch("access_moppy.batch_cmoriser.files")
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

    @patch("jinja2.Template")
    @patch("access_moppy.batch_cmoriser.files")
    @patch("os.chmod")
    @pytest.mark.unit
    def test_create_job_script_with_variable_resources(
        self, mock_chmod, mock_files, mock_template, temp_dir
    ):
        """Test job script creation with variable-specific resource overrides."""
        mock_file_obj = Mock()
        mock_file_obj.read.return_value = "mock template"
        mock_files.return_value.joinpath.return_value.open.return_value.__enter__.return_value = mock_file_obj

        mock_pbs_template = Mock()
        mock_python_template = Mock()
        mock_pbs_template.render.return_value = "pbs script"
        mock_python_template.render.return_value = "python script"
        mock_template.side_effect = [mock_pbs_template, mock_python_template]

        config = {
            "cpus_per_node": 4,
            "mem": "16GB",
            "walltime": "01:00:00",
            "experiment_id": "historical",
            "variable_resources": {
                "Amon.tas": {
                    "cpus_per_node": 8,
                    "mem": "32GB",
                }
            },
        }

        with patch("builtins.open", mock_open()):
            create_job_script("Amon.tas", config, "/db/path", temp_dir)

        python_render_call = mock_python_template.render.call_args.kwargs
        pbs_render_call = mock_pbs_template.render.call_args.kwargs

        assert python_render_call["config"]["cpus_per_node"] == 8
        assert python_render_call["config"]["mem"] == "32GB"
        assert pbs_render_call["config"]["cpus_per_node"] == 8
        assert pbs_render_call["config"]["mem"] == "32GB"
        mock_chmod.assert_called()

    @patch("subprocess.Popen")
    @patch("pathlib.Path.exists", return_value=True)
    @pytest.mark.unit
    def test_start_dashboard_success(self, mock_exists, mock_popen):
        """Test dashboard starts with valid python script path."""
        start_dashboard("/tmp/dashboard.py", "/tmp/tracker.db")

        assert mock_exists.called
        mock_popen.assert_called_once()
        _, kwargs = mock_popen.call_args
        assert kwargs["env"]["CMOR_TRACKER_DB"] == "/tmp/tracker.db"

    @patch("subprocess.Popen")
    @patch("pathlib.Path.exists", return_value=False)
    @pytest.mark.unit
    def test_start_dashboard_missing_script(self, mock_exists, mock_popen):
        """Test dashboard startup fails cleanly when script does not exist."""
        start_dashboard("/tmp/dashboard.py", "/tmp/tracker.db")

        assert mock_exists.called
        mock_popen.assert_not_called()

    @patch("subprocess.Popen")
    @patch("pathlib.Path.exists", return_value=True)
    @pytest.mark.unit
    def test_start_dashboard_invalid_extension(self, mock_exists, mock_popen):
        """Test dashboard startup rejects non-python script files."""
        start_dashboard("/tmp/dashboard.txt", "/tmp/tracker.db")

        assert mock_exists.called
        mock_popen.assert_not_called()

    @patch("subprocess.Popen")
    @patch("pathlib.Path.exists", return_value=True)
    @pytest.mark.unit
    def test_start_dashboard_path_traversal(self, mock_exists, mock_popen):
        """Test dashboard startup rejects traversal-like paths."""
        start_dashboard("../dashboard.py", "/tmp/tracker.db")

        assert mock_exists.called
        mock_popen.assert_not_called()

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

    @patch("subprocess.run")
    @pytest.mark.unit
    def test_submit_job_invalid_script_path(self, mock_run):
        """Test invalid script paths are rejected before submission."""
        job_id = submit_job("../unsafe_script.sh")

        assert job_id is None
        mock_run.assert_not_called()

    @patch("time.sleep")
    @patch("subprocess.run")
    @pytest.mark.unit
    def test_wait_for_jobs_completes_when_jobs_leave_queue(self, mock_run, mock_sleep):
        """Test wait loop exits once queued jobs are no longer reported by qstat."""
        running_result = Mock(stdout="1234.server R")
        done_result = Mock(stdout="")
        mock_run.side_effect = [running_result, done_result]

        wait_for_jobs(["1234.server"], poll_interval=0)

        assert mock_sleep.called
        assert mock_run.call_count == 2

    @patch("time.sleep")
    @patch("subprocess.run")
    @pytest.mark.unit
    def test_wait_for_jobs_handles_timeout(self, mock_run, mock_sleep):
        """Test timeout during qstat keeps job running until next successful poll."""
        import subprocess

        done_result = Mock(stdout="")
        mock_run.side_effect = [
            subprocess.TimeoutExpired(cmd=["qstat", "-x", "1234.server"], timeout=30),
            done_result,
        ]

        wait_for_jobs(["1234.server"], poll_interval=0)

        assert mock_sleep.called
        assert mock_run.call_count == 2

    @patch("time.sleep")
    @patch("subprocess.run")
    @pytest.mark.unit
    def test_wait_for_jobs_skips_invalid_job_ids(self, mock_run, mock_sleep):
        """Test invalid job IDs are ignored safely."""
        wait_for_jobs(["invalid job id"], poll_interval=0)

        assert mock_sleep.called
        mock_run.assert_not_called()

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
