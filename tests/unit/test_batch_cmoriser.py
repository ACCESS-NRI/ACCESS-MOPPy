from unittest.mock import Mock, mock_open, patch

from access_mopper.batch_cmoriser import create_job_script, submit_job
from tests.mocks.mock_pbs import MockPBSManager, mock_qsub_failure, mock_qsub_success


class TestBatchCmoriser:
    """Unit tests for batch processing functions."""

    @patch("access_mopper.batch_cmoriser.Template")
    @patch("access_mopper.batch_cmoriser.files")
    @patch("os.chmod")
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
    def test_submit_job_success(self, mock_run):
        """Test successful job submission."""
        mock_run.return_value = mock_qsub_success()

        job_id = submit_job("/path/to/script.sh")

        assert job_id is not None
        assert len(job_id) > 0
        mock_run.assert_called_once()

    @patch("subprocess.run")
    def test_submit_job_failure(self, mock_run):
        """Test failed job submission."""
        mock_run.return_value = mock_qsub_failure()

        job_id = submit_job("/path/to/script.sh")

        assert job_id is None

    def test_mock_pbs_manager(self):
        """Test the MockPBSManager functionality."""
        with MockPBSManager() as pbs:
            # Submit a mock job
            with patch("access_mopper.batch_cmoriser.subprocess.run") as mock_run:
                mock_run.return_value = mock_qsub_success()
                job_id = submit_job("/mock/script.sh")

            assert job_id is not None

            # Test job state changes
            pbs.mark_job_running(job_id)
            pbs.mark_job_completed(job_id)

            # Verify job is tracked
            assert job_id in pbs.submitted_jobs
            assert pbs.submitted_jobs[job_id]["status"] == "C"
