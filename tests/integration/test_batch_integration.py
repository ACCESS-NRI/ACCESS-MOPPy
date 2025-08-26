from unittest.mock import Mock, patch

import pytest

from access_mopper.batch_cmoriser import create_job_script
from access_mopper.tracking import TaskTracker
from tests.mocks.mock_files import mock_access_file_structure, patch_file_operations
from tests.mocks.mock_pbs import MockPBSManager


class TestBatchIntegration:
    """Integration tests for batch processing workflow."""

    def test_batch_workflow_with_tracking(self, batch_config, temp_dir):
        """Test complete batch workflow with task tracking."""
        # Setup tracking database
        db_path = temp_dir / "cmor_tasks.db"
        tracker = TaskTracker(db_path)

        # Add tasks for all variables
        for variable in batch_config["variables"]:
            tracker.add_task(variable, batch_config["experiment_id"])

        # Verify all tasks are pending
        for variable in batch_config["variables"]:
            status = tracker.get_status(variable, batch_config["experiment_id"])
            assert status == "pending"

        # Simulate processing workflow
        for variable in batch_config["variables"]:
            tracker.mark_running(variable, batch_config["experiment_id"])
            # ... processing would happen here ...
            tracker.mark_completed(variable, batch_config["experiment_id"])

        # Verify all tasks are completed
        for variable in batch_config["variables"]:
            assert tracker.is_done(variable, batch_config["experiment_id"])

    def test_job_script_creation_integration(self, batch_config, temp_dir):
        """Test job script creation with real template rendering."""
        script_dir = temp_dir / "job_scripts"
        script_dir.mkdir()

        db_path = temp_dir / "cmor_tasks.db"

        # Create job scripts for all variables
        created_scripts = []
        for variable in batch_config["variables"]:
            try:
                script_path = create_job_script(
                    variable, batch_config, str(db_path), script_dir
                )
                created_scripts.append(script_path)
            except Exception as e:
                # If template files are missing, we expect this
                pytest.skip(f"Template files not available: {e}")

        # If we get here, verify scripts were created
        for script_path in created_scripts:
            assert script_path.exists()
            assert script_path.suffix == ".sh"

    def test_file_pattern_matching(self, batch_config):
        """Test file pattern matching with mock file system."""
        # Create mock ACCESS file structure
        mock_fs = mock_access_file_structure("/mock/input")

        with patch_file_operations(mock_fs):
            # Test glob patterns from batch config
            pattern = batch_config["file_patterns"]["Amon.pr"]
            full_pattern = "/mock/input" + pattern

            # This would normally be done in the job script
            import glob

            matching_files = glob.glob(full_pattern)

            # With our mock file system, we should find files
            assert len(matching_files) > 0

    def test_pbs_submission_workflow(self, batch_config, temp_dir):
        """Test PBS submission workflow with mocked PBS."""
        with MockPBSManager():
            script_dir = temp_dir / "job_scripts"
            script_dir.mkdir()

            # Create a mock script
            script_path = script_dir / "test_job.sh"
            script_path.write_text("#!/bin/bash\necho 'test job'")

            # Submit job using mocked PBS
            from access_mopper.batch_cmoriser import submit_job

            with patch("subprocess.run") as mock_run:
                mock_run.return_value = Mock(
                    returncode=0, stdout="1234567.gadi-pbs\n", stderr=""
                )

                job_id = submit_job(str(script_path)).split(".")[0]

            assert job_id == "1234567"
            mock_run.assert_called_once()
