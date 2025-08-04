"""Mock PBS/qsub functionality for testing batch processing without a real scheduler."""

import secrets
import string
from unittest.mock import Mock, patch


class MockPBSManager:
    """Context manager to mock PBS operations."""

    def __init__(self):
        self.submitted_jobs = {}
        self.job_counter = 1000000

    def __enter__(self):
        self.qsub_patcher = patch("subprocess.run", side_effect=self._mock_qsub)
        self.qstat_patcher = patch("subprocess.run", side_effect=self._mock_qstat)
        self.qsub_patcher.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.qsub_patcher.stop()

    def _mock_qsub(self, cmd, **kwargs):
        """Mock qsub command."""
        if cmd[0] == "qsub":
            job_id = str(self.job_counter)
            self.job_counter += 1

            # Store job info
            script_path = cmd[1]
            self.submitted_jobs[job_id] = {
                "script": script_path,
                "status": "Q",  # Queued
                "name": f"cmor_job_{job_id}",
            }

            # Mock successful qsub response
            mock_result = Mock()
            mock_result.returncode = 0
            mock_result.stdout = f"{job_id}.gadi-pbs\n"
            mock_result.stderr = ""
            return mock_result

        # For other commands, return mock failure
        mock_result = Mock()
        mock_result.returncode = 1
        mock_result.stderr = "Mock: Command not recognized"
        return mock_result

    def _mock_qstat(self, cmd, **kwargs):
        """Mock qstat command."""
        if cmd[0] == "qstat":
            # Return mock job status
            mock_result = Mock()
            mock_result.returncode = 0

            # Generate mock qstat output
            output_lines = [
                "Job ID          Name             User     State  Cores  Memory     Time     Queue"
            ]
            for job_id, info in self.submitted_jobs.items():
                line = f"{job_id}.gadi-pbs  {info['name']:<15} testuser   {info['status']}     4      16GB    00:30:00  normal"
                output_lines.append(line)

            mock_result.stdout = "\n".join(output_lines)
            mock_result.stderr = ""
            return mock_result

        return Mock(returncode=1, stderr="Mock: Command not recognized")

    def mark_job_running(self, job_id):
        """Simulate job starting to run."""
        if job_id in self.submitted_jobs:
            self.submitted_jobs[job_id]["status"] = "R"

    def mark_job_completed(self, job_id):
        """Simulate job completion."""
        if job_id in self.submitted_jobs:
            self.submitted_jobs[job_id]["status"] = "C"

    def mark_job_failed(self, job_id):
        """Simulate job failure."""
        if job_id in self.submitted_jobs:
            self.submitted_jobs[job_id]["status"] = "F"


def mock_qsub_success():
    """Simple mock for successful qsub."""
    job_id = "".join(secrets.choice(string.digits) for _ in range(7))
    mock_result = Mock()
    mock_result.returncode = 0
    mock_result.stdout = f"{job_id}.gadi-pbs\n"
    mock_result.stderr = ""
    return mock_result


def mock_qsub_failure():
    """Simple mock for failed qsub."""
    mock_result = Mock()
    mock_result.returncode = 1
    mock_result.stdout = ""
    mock_result.stderr = "qsub: job rejected by server"
    return mock_result


def mock_qstat_empty():
    """Mock qstat with no jobs."""
    mock_result = Mock()
    mock_result.returncode = 0
    mock_result.stdout = "Job ID          Name             User     State  Cores  Memory     Time     Queue\n"
    mock_result.stderr = ""
    return mock_result
