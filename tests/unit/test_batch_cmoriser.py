"""Unit tests for batch CMORiser functionality."""

# Security: All subprocess usage in this file is for mocking in unit tests
# ruff: noqa: S603, S607
# bandit: skip
# semgrep: skip

from pathlib import Path
from unittest.mock import Mock, mock_open, patch

import pytest

from access_moppy.batch_cmoriser import (
    SIDECAR_FILENAME,
    compute_monitor_walltime,
    create_job_script,
    create_monitor_script,
    finalize_monitor,
    format_pbs_error,
    format_walltime,
    main,
    monitor_loop,
    monitor_main,
    parse_walltime,
    qstat_full,
    qstat_state,
    reconcile_one,
    start_dashboard,
    submit_job,
    wait_for_jobs,
)
from access_moppy.tracking import TaskTracker
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

        # Verify script was created in per-variable subdirectory
        expected_path = temp_dir / "Amon_tas" / "cmor_Amon_tas.sh"
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


class TestMainScriptDir:
    """Tests for script_dir resolution in main()."""

    BASE_CONFIG = {
        "variables": ["Amon.tas"],
        "experiment_id": "historical",
        "source_id": "ACCESS-ESM1-5",
        "variant_label": "r1i1p1f1",
        "grid_label": "gn",
        "activity_id": "CMIP",
        "input_folder": "/input",
        "output_folder": "/output",
    }

    def _run_main(self, config, tmp_path, monkeypatch):
        """Helper: run main() with a given config dict inside tmp_path."""
        config_file = tmp_path / "config.yml"
        config_file.write_text("")

        # Redirect output_folder into tmp_path so mkdir succeeds in CI
        config = {**config, "output_folder": str(tmp_path / "output")}

        monkeypatch.setattr("sys.argv", ["moppy-cmorise", str(config_file)])
        monkeypatch.chdir(tmp_path)

        with (
            patch("access_moppy.batch_cmoriser.yaml.safe_load", return_value=config),
            patch("access_moppy.batch_cmoriser.TaskTracker"),
            patch("access_moppy.batch_cmoriser.start_dashboard"),
            patch("access_moppy.batch_cmoriser.files"),
            patch(
                "access_moppy.batch_cmoriser.create_job_script",
                return_value=tmp_path / "job.sh",
            ),
            patch(
                "access_moppy.batch_cmoriser.create_monitor_script",
                return_value=tmp_path / "moppy_monitor.sh",
            ),
            patch(
                "access_moppy.batch_cmoriser.submit_job", return_value="12345.gadi-pbs"
            ),
        ):
            main()

    @pytest.mark.unit
    def test_main_creates_default_script_dir(self, tmp_path, monkeypatch):
        """When script_dir is absent from config, cmor_job_scripts is created."""
        self._run_main(self.BASE_CONFIG.copy(), tmp_path, monkeypatch)

        assert (tmp_path / "cmor_job_scripts").is_dir()

    @pytest.mark.unit
    def test_main_creates_custom_script_dir(self, tmp_path, monkeypatch):
        """When script_dir is set in config, that directory is created."""
        config = {**self.BASE_CONFIG, "script_dir": str(tmp_path / "my_scripts")}
        self._run_main(config, tmp_path, monkeypatch)

        assert (tmp_path / "my_scripts").is_dir()

    @pytest.mark.unit
    def test_main_exits_when_monitor_submit_returns_none(
        self, tmp_path, monkeypatch, capsys
    ):
        """If qsub-ing the monitor fails (submit_job returns None), main exits 1
        with an explanatory message."""
        config_file = tmp_path / "config.yml"
        config_file.write_text("")
        config = {**self.BASE_CONFIG, "output_folder": str(tmp_path / "output")}

        monkeypatch.setattr("sys.argv", ["moppy-cmorise", str(config_file)])
        monkeypatch.chdir(tmp_path)

        with (
            patch("access_moppy.batch_cmoriser.yaml.safe_load", return_value=config),
            patch("access_moppy.batch_cmoriser.TaskTracker"),
            patch("access_moppy.batch_cmoriser.start_dashboard"),
            patch("access_moppy.batch_cmoriser.files"),
            patch(
                "access_moppy.batch_cmoriser.create_monitor_script",
                return_value=tmp_path / "moppy_monitor.sh",
            ),
            patch(
                "access_moppy.batch_cmoriser.submit_job",
                return_value=None,  # qsub fails
            ),
        ):
            with pytest.raises(SystemExit) as excinfo:
                main()

        assert excinfo.value.code == 1
        captured = capsys.readouterr()
        assert "Failed to submit monitor job" in captured.out

    @pytest.mark.unit
    def test_main_does_not_wait_by_default(self, tmp_path, monkeypatch):
        """Without wait_for_completion in config, main exits without polling."""
        config_file = tmp_path / "config.yml"
        config_file.write_text("")
        config = {**self.BASE_CONFIG, "output_folder": str(tmp_path / "output")}

        monkeypatch.setattr("sys.argv", ["moppy-cmorise", str(config_file)])
        monkeypatch.chdir(tmp_path)

        with (
            patch("access_moppy.batch_cmoriser.yaml.safe_load", return_value=config),
            patch("access_moppy.batch_cmoriser.TaskTracker"),
            patch("access_moppy.batch_cmoriser.start_dashboard"),
            patch("access_moppy.batch_cmoriser.files"),
            patch(
                "access_moppy.batch_cmoriser.create_monitor_script",
                return_value=tmp_path / "moppy_monitor.sh",
            ),
            patch(
                "access_moppy.batch_cmoriser.submit_job",
                return_value="42.gadi-pbs",
            ),
            patch("access_moppy.batch_cmoriser.wait_for_jobs") as mock_wait,
        ):
            main()

        mock_wait.assert_not_called()

    @pytest.mark.unit
    def test_main_waits_when_config_requests_it(self, tmp_path, monkeypatch):
        """When wait_for_completion=true, main calls wait_for_jobs for the monitor's
        PBS job ID (not for each sub-job — the monitor handles those internally)."""
        config_file = tmp_path / "config.yml"
        config_file.write_text("")
        config = {
            **self.BASE_CONFIG,
            "output_folder": str(tmp_path / "output"),
            "wait_for_completion": True,
        }

        monkeypatch.setattr("sys.argv", ["moppy-cmorise", str(config_file)])
        monkeypatch.chdir(tmp_path)

        with (
            patch("access_moppy.batch_cmoriser.yaml.safe_load", return_value=config),
            patch("access_moppy.batch_cmoriser.TaskTracker"),
            patch("access_moppy.batch_cmoriser.start_dashboard"),
            patch("access_moppy.batch_cmoriser.files"),
            patch(
                "access_moppy.batch_cmoriser.create_monitor_script",
                return_value=tmp_path / "moppy_monitor.sh",
            ),
            patch(
                "access_moppy.batch_cmoriser.submit_job",
                return_value="42.gadi-pbs",
            ),
            patch("access_moppy.batch_cmoriser.wait_for_jobs") as mock_wait,
        ):
            main()

        mock_wait.assert_called_once_with(["42.gadi-pbs"])

    @pytest.mark.unit
    def test_main_writes_sidecar_file(self, tmp_path, monkeypatch):
        """The sidecar file is written after the monitor is qsub'd."""
        config_file = tmp_path / "config.yml"
        config_file.write_text("")
        config = {**self.BASE_CONFIG, "output_folder": str(tmp_path / "output")}

        monkeypatch.setattr("sys.argv", ["moppy-cmorise", str(config_file)])
        monkeypatch.chdir(tmp_path)

        with (
            patch("access_moppy.batch_cmoriser.yaml.safe_load", return_value=config),
            patch("access_moppy.batch_cmoriser.TaskTracker"),
            patch("access_moppy.batch_cmoriser.start_dashboard"),
            patch("access_moppy.batch_cmoriser.files"),
            patch(
                "access_moppy.batch_cmoriser.create_monitor_script",
                return_value=tmp_path / "moppy_monitor.sh",
            ),
            patch(
                "access_moppy.batch_cmoriser.submit_job",
                return_value="42.gadi-pbs",
            ),
        ):
            main()

        sidecar = tmp_path / "output" / SIDECAR_FILENAME
        assert sidecar.exists()
        contents = sidecar.read_text().splitlines()
        assert contents[0] == "42.gadi-pbs"


class TestWalltimeHelpers:
    """Unit tests for walltime parsing and monitor walltime computation."""

    @pytest.mark.unit
    def test_parse_walltime_hms(self):
        assert parse_walltime("02:30:45") == 2 * 3600 + 30 * 60 + 45

    @pytest.mark.unit
    def test_parse_walltime_ms(self):
        """Two-component form is interpreted as MM:SS."""
        assert parse_walltime("05:30") == 5 * 60 + 30

    @pytest.mark.unit
    def test_parse_walltime_zero(self):
        assert parse_walltime("00:00:00") == 0

    @pytest.mark.unit
    def test_parse_walltime_invalid(self):
        with pytest.raises(ValueError):
            parse_walltime("not-a-walltime")

    @pytest.mark.unit
    def test_format_walltime_round_trip(self):
        for s in ("00:00:00", "01:00:00", "04:30:00", "23:59:59"):
            assert format_walltime(parse_walltime(s)) == s

    @pytest.mark.unit
    def test_format_walltime_zero_pads(self):
        assert format_walltime(65) == "00:01:05"

    @pytest.mark.unit
    def test_compute_monitor_walltime_default_only(self):
        """All variables use the top-level default walltime."""
        cfg = {
            "walltime": "02:00:00",
            "variables": ["Amon.tas", "Omon.tos"],
        }
        # max = 2h, +30min = 2:30
        assert compute_monitor_walltime(cfg) == "02:30:00"

    @pytest.mark.unit
    def test_compute_monitor_walltime_with_override(self):
        """variable_resources override is used when longer than default."""
        cfg = {
            "walltime": "02:00:00",
            "variables": ["Amon.tas", "Amon.cl"],
            "variable_resources": {"Amon.cl": {"walltime": "04:00:00"}},
        }
        # max = 4h (override), +30min = 4:30
        assert compute_monitor_walltime(cfg) == "04:30:00"

    @pytest.mark.unit
    def test_compute_monitor_walltime_override_shorter_than_default(self):
        """A shorter per-variable override is not used; default still wins."""
        cfg = {
            "walltime": "04:00:00",
            "variables": ["Amon.tas", "Amon.cl"],
            "variable_resources": {"Amon.cl": {"walltime": "01:00:00"}},
        }
        assert compute_monitor_walltime(cfg) == "04:30:00"

    @pytest.mark.unit
    def test_compute_monitor_walltime_no_walltime_in_config(self):
        """Without 'walltime' key, the helper falls back to its 02:00:00 default."""
        cfg = {"variables": ["Amon.tas"]}
        assert compute_monitor_walltime(cfg) == "02:30:00"


class TestQstatHelpers:
    """Unit tests for qstat parsing and PBS error formatting."""

    SAMPLE_QSTAT = (
        "Job Id: 12345.gadi-pbs\n"
        "    Job_Name = cmor_Omon_zostoga\n"
        "    job_state = F\n"
        "    Exit_status = 271\n"
        "    comment = job killed: vmem 192GB exceeded limit 190GB\n"
        "    resources_used.mem = 186465316kb\n"
        "    resources_used.walltime = 00:34:18\n"
    )

    @pytest.mark.unit
    def test_qstat_full_parses_well_formed(self):
        result = Mock(returncode=0, stdout=self.SAMPLE_QSTAT)
        with patch("subprocess.run", return_value=result):
            info = qstat_full("12345.gadi-pbs")
        assert info["job_state"] == "F"
        assert info["Exit_status"] == "271"
        assert info["comment"].startswith("job killed")
        assert info["resources_used.walltime"] == "00:34:18"

    @pytest.mark.unit
    def test_qstat_full_returns_none_on_nonzero_returncode(self):
        result = Mock(returncode=153, stdout="")
        with patch("subprocess.run", return_value=result):
            assert qstat_full("12345.gadi-pbs") is None

    @pytest.mark.unit
    def test_qstat_full_returns_none_on_empty_stdout(self):
        result = Mock(returncode=0, stdout="")
        with patch("subprocess.run", return_value=result):
            assert qstat_full("12345.gadi-pbs") is None

    @pytest.mark.unit
    def test_qstat_full_returns_none_on_timeout(self):
        import subprocess

        with patch(
            "subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd=["qstat"], timeout=30),
        ):
            assert qstat_full("12345.gadi-pbs") is None

    @pytest.mark.unit
    def test_qstat_full_handles_continuation_lines(self):
        """PBS Pro wraps long values onto continuation lines starting with whitespace."""
        output = (
            "Job Id: 12345.gadi-pbs\n"
            "    comment = job killed: this is a very long comment that\n"
            "\twraps onto a second line because PBS Pro breaks long strings\n"
            "    job_state = F\n"
        )
        result = Mock(returncode=0, stdout=output)
        with patch("subprocess.run", return_value=result):
            info = qstat_full("12345.gadi-pbs")
        assert "wraps onto a second line" in info["comment"]
        assert info["job_state"] == "F"

    @pytest.mark.unit
    def test_qstat_state_handles_none(self):
        assert qstat_state(None) == "gone"

    @pytest.mark.unit
    def test_qstat_state_returns_job_state(self):
        assert qstat_state({"job_state": "R"}) == "R"
        assert qstat_state({"job_state": "F"}) == "F"

    @pytest.mark.unit
    def test_qstat_state_handles_missing_field(self):
        assert qstat_state({"Job_Name": "x"}) == "gone"

    @pytest.mark.unit
    def test_format_pbs_error_with_full_info(self, tmp_path):
        info = {
            "Exit_status": "271",
            "comment": "job killed: memory exceeded",
            "resources_used.mem": "186465316kb",
            "resources_used.walltime": "00:34:18",
        }
        msg = format_pbs_error("Omon.zostoga", "12345.gadi-pbs", info, tmp_path)
        assert "12345.gadi-pbs" in msg
        assert "exit_status=271" in msg
        assert "memory exceeded" in msg
        assert "mem_used=" in msg
        assert "walltime_used=00:34:18" in msg

    @pytest.mark.unit
    def test_format_pbs_error_with_none_info(self, tmp_path):
        msg = format_pbs_error("Omon.zostoga", "12345.gadi-pbs", None, tmp_path)
        assert "vanished" in msg
        assert "12345.gadi-pbs" in msg

    @pytest.mark.unit
    def test_format_pbs_error_includes_err_tail_when_file_exists(self, tmp_path):
        """If the worker's .err file exists, the tail is appended."""
        var_dir = tmp_path / "Omon_zostoga"
        var_dir.mkdir()
        err_path = var_dir / "cmor_Omon_zostoga.err"
        err_path.write_text("\n".join(f"line {i}" for i in range(1, 25)))

        info = {"Exit_status": "1", "comment": "task failed"}
        msg = format_pbs_error("Omon.zostoga", "12345", info, tmp_path)
        assert "err_tail:" in msg
        # tail -20 returns the last 20 lines
        assert "line 24" in msg
        assert "line 1\n" not in msg  # earliest lines dropped

    @pytest.mark.unit
    def test_format_pbs_error_skips_missing_fields(self, tmp_path):
        """When optional fields are absent, they are simply omitted."""
        info = {"Exit_status": "1"}  # no comment, no resources_used
        msg = format_pbs_error("Amon.tas", "12345", info, tmp_path)
        assert "exit_status=1" in msg
        assert "pbs_comment" not in msg
        assert "mem_used" not in msg
        assert "walltime_used" not in msg

    @pytest.mark.unit
    def test_format_pbs_error_swallows_tail_timeout(self, tmp_path):
        """If `tail` subprocess times out, the error string is still returned,
        just without the err_tail section. The exception must not propagate."""
        import subprocess

        # err file exists so the tail path is entered
        var_dir = tmp_path / "Amon_tas"
        var_dir.mkdir()
        (var_dir / "cmor_Amon_tas.err").write_text("some error log")

        info = {"Exit_status": "1", "comment": "task failed"}
        with patch(
            "subprocess.run",
            side_effect=subprocess.TimeoutExpired(cmd=["tail"], timeout=10),
        ):
            msg = format_pbs_error("Amon.tas", "12345", info, tmp_path)

        # Other fields still rendered
        assert "exit_status=1" in msg
        assert "task failed" in msg
        # tail content omitted because of timeout
        assert "err_tail" not in msg

    @pytest.mark.unit
    def test_format_pbs_error_swallows_tail_missing_binary(self, tmp_path):
        """If `tail` binary is not on PATH (FileNotFoundError), result is still sane."""
        var_dir = tmp_path / "Amon_tas"
        var_dir.mkdir()
        (var_dir / "cmor_Amon_tas.err").write_text("some error log")

        info = {"Exit_status": "1"}
        with patch("subprocess.run", side_effect=FileNotFoundError("tail not found")):
            msg = format_pbs_error("Amon.tas", "12345", info, tmp_path)

        assert "exit_status=1" in msg
        assert "err_tail" not in msg


class TestReconcileOne:
    """Unit tests for reconcile_one — the DB write decision logic."""

    @pytest.mark.unit
    def test_exit_zero_with_completed_db_is_noop(self, temp_dir):
        """Worker successfully wrote 'completed'; reconcile does nothing."""
        db_path = temp_dir / "test.db"
        tracker = TaskTracker(db_path)
        tracker.add_task("Amon.tas", "historical")
        tracker.mark_running("Amon.tas", "historical")
        tracker.mark_completed("Amon.tas", "historical")

        info = {"Exit_status": "0", "job_state": "F"}
        reconcile_one(tracker, "Amon.tas", "historical", "12345", info, temp_dir)

        assert tracker.get_status("Amon.tas", "historical") == "completed"

    @pytest.mark.unit
    def test_exit_zero_with_stale_running_backfills_completed(self, temp_dir):
        """Worker exited 0 but mark_done failed; reconcile backfills 'completed'."""
        db_path = temp_dir / "test.db"
        tracker = TaskTracker(db_path)
        tracker.add_task("Amon.tas", "historical")
        tracker.mark_running("Amon.tas", "historical")

        info = {"Exit_status": "0", "job_state": "F"}
        reconcile_one(tracker, "Amon.tas", "historical", "12345", info, temp_dir)

        assert tracker.get_status("Amon.tas", "historical") == "completed"

    @pytest.mark.unit
    def test_exit_nonzero_with_running_marks_failed(self, temp_dir):
        """SIGKILL/OOM case: worker died mid-task, DB still 'running'."""
        db_path = temp_dir / "test.db"
        tracker = TaskTracker(db_path)
        tracker.add_task("Omon.zostoga", "historical")
        tracker.mark_running("Omon.zostoga", "historical")

        info = {"Exit_status": "271", "comment": "job killed: memory exceeded"}
        reconcile_one(
            tracker, "Omon.zostoga", "historical", "12345.gadi-pbs", info, temp_dir
        )

        assert tracker.get_status("Omon.zostoga", "historical") == "failed"
        # Error message captures the PBS detail
        row = tracker.conn.execute(
            "SELECT error_message FROM cmor_tasks WHERE variable=?",
            ("Omon.zostoga",),
        ).fetchone()
        assert "exit_status=271" in row[0]
        assert "memory exceeded" in row[0]

    @pytest.mark.unit
    def test_failed_terminal_state_is_not_overwritten(self, temp_dir):
        """Once worker has written 'failed', reconcile must not clobber it."""
        db_path = temp_dir / "test.db"
        tracker = TaskTracker(db_path)
        tracker.add_task("Amon.tas", "historical")
        tracker.mark_failed("Amon.tas", "historical", "worker-reported error")

        info = {"Exit_status": "1"}
        reconcile_one(tracker, "Amon.tas", "historical", "12345", info, temp_dir)

        row = tracker.conn.execute(
            "SELECT error_message FROM cmor_tasks WHERE variable=?",
            ("Amon.tas",),
        ).fetchone()
        # Original error survives — reconcile_one bailed out
        assert row[0] == "worker-reported error"

    @pytest.mark.unit
    def test_missing_info_marks_failed_with_vanished_message(self, temp_dir):
        """PBS history purged: info is None → record as failed with 'vanished'."""
        db_path = temp_dir / "test.db"
        tracker = TaskTracker(db_path)
        tracker.add_task("Amon.tas", "historical")
        tracker.mark_running("Amon.tas", "historical")

        reconcile_one(tracker, "Amon.tas", "historical", "12345", None, temp_dir)

        assert tracker.get_status("Amon.tas", "historical") == "failed"
        row = tracker.conn.execute(
            "SELECT error_message FROM cmor_tasks WHERE variable=?",
            ("Amon.tas",),
        ).fetchone()
        assert "vanished" in row[0]

    @pytest.mark.unit
    def test_non_integer_exit_status_treated_as_failure(self, temp_dir):
        """Garbled Exit_status (e.g. PBS bug) is treated as failure, not success."""
        db_path = temp_dir / "test.db"
        tracker = TaskTracker(db_path)
        tracker.add_task("Amon.tas", "historical")
        tracker.mark_running("Amon.tas", "historical")

        info = {"Exit_status": "not-a-number"}
        reconcile_one(tracker, "Amon.tas", "historical", "12345", info, temp_dir)

        assert tracker.get_status("Amon.tas", "historical") == "failed"


class TestCreateMonitorScript:
    """Unit tests for monitor PBS script generation."""

    BASE_CONFIG = {
        "variables": ["Amon.tas"],
        "walltime": "02:00:00",
        "queue": "normal",
        "storage": "gdata/tm70",
        "scheduler_options": "#PBS -P abc",
        "worker_init": "module load python",
    }

    @pytest.mark.unit
    def test_writes_script_with_correct_walltime(self, tmp_path):
        """The rendered script includes the computed monitor walltime."""
        path = create_monitor_script(
            self.BASE_CONFIG,
            tmp_path / "config.yml",
            tmp_path / "db.db",
            tmp_path,
        )
        content = path.read_text()
        # max sub walltime is 2h, so monitor gets 2:30:00
        assert "#PBS -l walltime=02:30:00" in content

    @pytest.mark.unit
    def test_script_executable(self, tmp_path):
        import stat as _stat

        path = create_monitor_script(
            self.BASE_CONFIG,
            tmp_path / "config.yml",
            tmp_path / "db.db",
            tmp_path,
        )
        mode = path.stat().st_mode
        assert mode & _stat.S_IXUSR

    @pytest.mark.unit
    def test_script_exports_required_env_vars(self, tmp_path):
        path = create_monitor_script(
            self.BASE_CONFIG,
            tmp_path / "config.yml",
            tmp_path / "db.db",
            tmp_path,
        )
        content = path.read_text()
        assert "MOPPY_CONFIG_PATH=" in content
        assert "MOPPY_DB_PATH=" in content
        assert "python -m access_moppy.batch_cmoriser --monitor" in content

    @pytest.mark.unit
    def test_script_includes_storage_and_scheduler_options(self, tmp_path):
        path = create_monitor_script(
            self.BASE_CONFIG,
            tmp_path / "config.yml",
            tmp_path / "db.db",
            tmp_path,
        )
        content = path.read_text()
        assert "#PBS -P abc" in content
        assert "#PBS -l storage=gdata/tm70" in content


class TestMonitorLoop:
    """Unit tests for the main poll-and-reconcile loop."""

    @pytest.mark.unit
    def test_loop_exits_when_all_jobs_finished(self, temp_dir, monkeypatch):
        """One sub-job, immediately in F state → loop exits after one iteration."""
        db_path = temp_dir / "test.db"
        tracker = TaskTracker(db_path)
        tracker.add_task("Amon.tas", "historical")
        tracker.mark_running("Amon.tas", "historical")

        job_map = {"12345.gadi-pbs": "Amon.tas"}

        # qstat reports F with exit 0 immediately
        finished_info = {"job_state": "F", "Exit_status": "0"}
        monkeypatch.setattr(
            "access_moppy.batch_cmoriser.qstat_full",
            lambda jid: finished_info,
        )
        monkeypatch.setattr("time.sleep", lambda _: None)

        monitor_loop(tracker, job_map, "historical", temp_dir)

        assert tracker.get_status("Amon.tas", "historical") == "completed"

    @pytest.mark.unit
    def test_loop_keeps_polling_while_state_is_running(self, temp_dir, monkeypatch):
        """Jobs in R state stay pending until they transition to F."""
        db_path = temp_dir / "test.db"
        tracker = TaskTracker(db_path)
        tracker.add_task("Amon.tas", "historical")
        tracker.mark_running("Amon.tas", "historical")

        job_map = {"12345.gadi-pbs": "Amon.tas"}

        # First three polls return R, then F (sub-job exits cleanly)
        states = iter(
            [
                {"job_state": "R"},
                {"job_state": "R"},
                {"job_state": "R"},
                {"job_state": "F", "Exit_status": "0"},
            ]
        )
        monkeypatch.setattr(
            "access_moppy.batch_cmoriser.qstat_full",
            lambda jid: next(states),
        )
        monkeypatch.setattr("time.sleep", lambda _: None)

        monitor_loop(tracker, job_map, "historical", temp_dir)
        assert tracker.get_status("Amon.tas", "historical") == "completed"

    @pytest.mark.unit
    def test_loop_marks_failed_on_nonzero_exit(self, temp_dir, monkeypatch):
        """SIGKILL/OOM end-to-end: sub-job finishes with exit 271, DB ends 'failed'."""
        db_path = temp_dir / "test.db"
        tracker = TaskTracker(db_path)
        tracker.add_task("Omon.zostoga", "historical")
        tracker.mark_running("Omon.zostoga", "historical")

        job_map = {"168282805.gadi-pbs": "Omon.zostoga"}

        info = {
            "job_state": "F",
            "Exit_status": "271",
            "comment": "job killed: memory exceeded",
        }
        monkeypatch.setattr("access_moppy.batch_cmoriser.qstat_full", lambda jid: info)
        monkeypatch.setattr("time.sleep", lambda _: None)

        monitor_loop(tracker, job_map, "historical", temp_dir)
        assert tracker.get_status("Omon.zostoga", "historical") == "failed"

    @pytest.mark.unit
    def test_loop_treats_gone_qstat_as_finished(self, temp_dir, monkeypatch):
        """If qstat returns None (history purged), the job is reconciled as gone."""
        db_path = temp_dir / "test.db"
        tracker = TaskTracker(db_path)
        tracker.add_task("Amon.tas", "historical")
        tracker.mark_running("Amon.tas", "historical")

        job_map = {"12345.gadi-pbs": "Amon.tas"}

        monkeypatch.setattr("access_moppy.batch_cmoriser.qstat_full", lambda jid: None)
        monkeypatch.setattr("time.sleep", lambda _: None)

        monitor_loop(tracker, job_map, "historical", temp_dir)
        # qstat_state(None) returns "gone" which is not in Q/R/H, so reconcile fires
        assert tracker.get_status("Amon.tas", "historical") == "failed"


class TestFinalizeMonitor:
    """Unit tests for finalize_monitor: consistency sweep + sidecar cleanup."""

    @pytest.mark.unit
    def test_finalize_marks_stale_running_as_failed(self, temp_dir):
        """Any 'running' rows at finalize time are corrected to 'failed'."""
        db_path = temp_dir / "test.db"
        tracker = TaskTracker(db_path)
        tracker.add_task("Amon.tas", "historical")
        tracker.mark_running("Amon.tas", "historical")

        config = {"variables": ["Amon.tas"]}
        finalize_monitor(tracker, config, "historical", db_path)

        assert tracker.get_status("Amon.tas", "historical") == "failed"
        row = tracker.conn.execute(
            "SELECT error_message FROM cmor_tasks WHERE variable=?",
            ("Amon.tas",),
        ).fetchone()
        assert "finalize" in row[0]

    @pytest.mark.unit
    def test_finalize_marks_pending_as_failed(self, temp_dir):
        """Variables that never left 'pending' (qsub failed early) are marked failed."""
        db_path = temp_dir / "test.db"
        tracker = TaskTracker(db_path)
        tracker.add_task("Amon.tas", "historical")  # stays pending

        config = {"variables": ["Amon.tas"]}
        finalize_monitor(tracker, config, "historical", db_path)

        assert tracker.get_status("Amon.tas", "historical") == "failed"

    @pytest.mark.unit
    def test_finalize_preserves_completed_and_failed(self, temp_dir):
        """Terminal-state rows are not touched."""
        db_path = temp_dir / "test.db"
        tracker = TaskTracker(db_path)
        tracker.add_task("Amon.done", "historical")
        tracker.mark_completed("Amon.done", "historical")
        tracker.add_task("Amon.bad", "historical")
        tracker.mark_failed("Amon.bad", "historical", "previously failed")

        config = {"variables": ["Amon.done", "Amon.bad"]}
        finalize_monitor(tracker, config, "historical", db_path)

        assert tracker.get_status("Amon.done", "historical") == "completed"
        # Original error message survives
        row = tracker.conn.execute(
            "SELECT error_message FROM cmor_tasks WHERE variable=?",
            ("Amon.bad",),
        ).fetchone()
        assert row[0] == "previously failed"

    @pytest.mark.unit
    def test_finalize_removes_sidecar_file(self, temp_dir):
        """Sidecar file is deleted on successful finalize."""
        db_path = temp_dir / "test.db"
        tracker = TaskTracker(db_path)
        sidecar = temp_dir / SIDECAR_FILENAME
        sidecar.write_text("12345.gadi-pbs\n2026-05-15T00:00:00\n")

        finalize_monitor(tracker, {"variables": []}, "historical", db_path)
        assert not sidecar.exists()

    @pytest.mark.unit
    def test_finalize_tolerates_missing_sidecar(self, temp_dir):
        """No exception when sidecar was already deleted (or never created)."""
        db_path = temp_dir / "test.db"
        tracker = TaskTracker(db_path)
        # No sidecar to begin with — should not raise
        finalize_monitor(tracker, {"variables": []}, "historical", db_path)


class TestMonitorMain:
    """Unit tests for monitor_main entry point: env validation and submit flow."""

    @pytest.mark.unit
    def test_exits_with_code_2_when_env_missing(self, monkeypatch):
        """monitor_main bails out cleanly if MOPPY_CONFIG_PATH or MOPPY_DB_PATH absent."""
        monkeypatch.delenv("MOPPY_CONFIG_PATH", raising=False)
        monkeypatch.delenv("MOPPY_DB_PATH", raising=False)

        with pytest.raises(SystemExit) as excinfo:
            monitor_main()
        assert excinfo.value.code == 2

    @pytest.mark.unit
    def test_monitor_submits_one_job_per_variable(self, temp_dir, monkeypatch):
        """Each variable in the config gets one qsub call and one pbs_job_id stored."""
        db_path = temp_dir / "test.db"

        # Real DB so we can verify pbs_job_id storage
        tracker = TaskTracker(db_path)
        tracker.add_task("Amon.tas", "historical")
        tracker.add_task("Amon.pr", "historical")
        tracker.conn.close()

        config_path = temp_dir / "config.yml"
        config_path.write_text(
            "experiment_id: historical\n" "variables:\n  - Amon.tas\n  - Amon.pr\n"
        )

        monkeypatch.setenv("MOPPY_CONFIG_PATH", str(config_path))
        monkeypatch.setenv("MOPPY_DB_PATH", str(db_path))
        monkeypatch.setenv("MOPPY_SCRIPT_DIR", str(temp_dir / "scripts"))

        # Mock the script-creation + qsub + qstat layer
        job_ids = iter(["111.gadi-pbs", "222.gadi-pbs"])
        submit_calls = []

        def fake_create_job_script(variable, config, db_path, script_dir):
            return (
                Path(script_dir)
                / variable.replace(".", "_")
                / f"cmor_{variable.replace('.', '_')}.sh"
            )

        def fake_submit_job(path):
            submit_calls.append(path)
            return next(job_ids)

        monkeypatch.setattr(
            "access_moppy.batch_cmoriser.create_job_script", fake_create_job_script
        )
        monkeypatch.setattr("access_moppy.batch_cmoriser.submit_job", fake_submit_job)
        # Sub-jobs immediately finish OK so the loop terminates
        monkeypatch.setattr(
            "access_moppy.batch_cmoriser.qstat_full",
            lambda jid: {"job_state": "F", "Exit_status": "0"},
        )
        monkeypatch.setattr("time.sleep", lambda _: None)

        monitor_main()

        # Both variables submitted
        assert len(submit_calls) == 2

        # pbs_job_id stored for each variable
        verify = TaskTracker(db_path)
        assert verify.get_pbs_job_id("Amon.tas", "historical") in (
            "111.gadi-pbs",
            "222.gadi-pbs",
        )
        assert verify.get_pbs_job_id("Amon.pr", "historical") in (
            "111.gadi-pbs",
            "222.gadi-pbs",
        )
        # Both end up completed
        assert verify.get_status("Amon.tas", "historical") == "completed"
        assert verify.get_status("Amon.pr", "historical") == "completed"
        verify.conn.close()

    @pytest.mark.unit
    def test_monitor_skips_already_completed(self, temp_dir, monkeypatch):
        """Variables already marked 'completed' are not re-submitted."""
        db_path = temp_dir / "test.db"
        tracker = TaskTracker(db_path)
        tracker.add_task("Amon.tas", "historical")
        tracker.mark_completed("Amon.tas", "historical")
        tracker.add_task("Amon.pr", "historical")
        tracker.conn.close()

        config_path = temp_dir / "config.yml"
        config_path.write_text(
            "experiment_id: historical\nvariables:\n  - Amon.tas\n  - Amon.pr\n"
        )
        monkeypatch.setenv("MOPPY_CONFIG_PATH", str(config_path))
        monkeypatch.setenv("MOPPY_DB_PATH", str(db_path))
        monkeypatch.setenv("MOPPY_SCRIPT_DIR", str(temp_dir / "scripts"))

        submitted = []

        def fake_create_job_script(variable, config, db_path, script_dir):
            return Path(script_dir) / "x.sh"

        def fake_submit_job(path):
            submitted.append(path)
            return "999.gadi-pbs"

        monkeypatch.setattr(
            "access_moppy.batch_cmoriser.create_job_script", fake_create_job_script
        )
        monkeypatch.setattr("access_moppy.batch_cmoriser.submit_job", fake_submit_job)
        monkeypatch.setattr(
            "access_moppy.batch_cmoriser.qstat_full",
            lambda jid: {"job_state": "F", "Exit_status": "0"},
        )
        monkeypatch.setattr("time.sleep", lambda _: None)

        monitor_main()

        # Only Amon.pr should have been submitted; Amon.tas was already done
        assert len(submitted) == 1

    @pytest.mark.unit
    def test_monitor_marks_failed_when_create_job_script_raises(
        self, temp_dir, monkeypatch
    ):
        """When create_job_script raises for one variable, monitor marks that
        variable as failed and continues to the next rather than aborting the batch."""
        db_path = temp_dir / "test.db"
        tracker = TaskTracker(db_path)
        tracker.add_task("Amon.broken", "historical")
        tracker.add_task("Amon.ok", "historical")
        tracker.conn.close()

        config_path = temp_dir / "config.yml"
        config_path.write_text(
            "experiment_id: historical\n" "variables:\n  - Amon.broken\n  - Amon.ok\n"
        )
        monkeypatch.setenv("MOPPY_CONFIG_PATH", str(config_path))
        monkeypatch.setenv("MOPPY_DB_PATH", str(db_path))
        monkeypatch.setenv("MOPPY_SCRIPT_DIR", str(temp_dir / "scripts"))

        submitted = []

        def flaky_create_job_script(variable, *args, **kwargs):
            if variable == "Amon.broken":
                raise RuntimeError("jinja template missing")
            return temp_dir / "x.sh"

        monkeypatch.setattr(
            "access_moppy.batch_cmoriser.create_job_script",
            flaky_create_job_script,
        )
        monkeypatch.setattr(
            "access_moppy.batch_cmoriser.submit_job",
            lambda p: submitted.append(p) or "999.gadi-pbs",
        )
        monkeypatch.setattr(
            "access_moppy.batch_cmoriser.qstat_full",
            lambda jid: {"job_state": "F", "Exit_status": "0"},
        )
        monkeypatch.setattr("time.sleep", lambda _: None)

        monitor_main()

        verify = TaskTracker(db_path)
        # Broken variable: hit the except branch → failed with descriptive message
        assert verify.get_status("Amon.broken", "historical") == "failed"
        row = verify.conn.execute(
            "SELECT error_message FROM cmor_tasks WHERE variable=?",
            ("Amon.broken",),
        ).fetchone()
        assert "failed to create script" in row[0]
        assert "jinja template missing" in row[0]

        # The `continue` after the except let the next variable proceed normally
        assert verify.get_status("Amon.ok", "historical") == "completed"
        assert len(submitted) == 1
        verify.conn.close()

    @pytest.mark.unit
    def test_monitor_marks_failed_when_qsub_returns_none(self, temp_dir, monkeypatch):
        """When submit_job returns None (qsub failed), DB row is set to 'failed'."""
        db_path = temp_dir / "test.db"
        tracker = TaskTracker(db_path)
        tracker.add_task("Amon.tas", "historical")
        tracker.conn.close()

        config_path = temp_dir / "config.yml"
        config_path.write_text("experiment_id: historical\nvariables:\n  - Amon.tas\n")
        monkeypatch.setenv("MOPPY_CONFIG_PATH", str(config_path))
        monkeypatch.setenv("MOPPY_DB_PATH", str(db_path))
        monkeypatch.setenv("MOPPY_SCRIPT_DIR", str(temp_dir / "scripts"))

        monkeypatch.setattr(
            "access_moppy.batch_cmoriser.create_job_script",
            lambda *a, **k: temp_dir / "x.sh",
        )
        monkeypatch.setattr(
            "access_moppy.batch_cmoriser.submit_job",
            lambda p: None,  # qsub failed
        )
        monkeypatch.setattr("time.sleep", lambda _: None)

        monitor_main()

        verify = TaskTracker(db_path)
        assert verify.get_status("Amon.tas", "historical") == "failed"
        row = verify.conn.execute(
            "SELECT error_message FROM cmor_tasks WHERE variable=?",
            ("Amon.tas",),
        ).fetchone()
        assert "qsub" in row[0]
        verify.conn.close()


class TestMainDispatch:
    """Tests for the argument-parsing branches at the top of main()."""

    @pytest.mark.unit
    def test_monitor_flag_delegates_to_monitor_main(self, monkeypatch):
        """`moppy-cmorise --monitor` invokes monitor_main and returns without
        running the login-side path (no config file is parsed)."""
        monkeypatch.setattr("sys.argv", ["moppy-cmorise", "--monitor"])

        called = {"monitor_main": 0, "yaml_load": 0}

        def fake_monitor_main():
            called["monitor_main"] += 1

        def fake_yaml_load(*args, **kwargs):
            called["yaml_load"] += 1
            return {}

        monkeypatch.setattr(
            "access_moppy.batch_cmoriser.monitor_main", fake_monitor_main
        )
        monkeypatch.setattr(
            "access_moppy.batch_cmoriser.yaml.safe_load", fake_yaml_load
        )

        main()  # should not raise; should not call yaml.safe_load

        assert called["monitor_main"] == 1
        assert called["yaml_load"] == 0  # login-side path never reached

    @pytest.mark.unit
    def test_no_args_exits_with_usage(self, monkeypatch, capsys):
        """`moppy-cmorise` with no arg prints usage to stdout and exits 1."""
        monkeypatch.setattr("sys.argv", ["moppy-cmorise"])

        with pytest.raises(SystemExit) as excinfo:
            main()

        assert excinfo.value.code == 1
        captured = capsys.readouterr()
        assert "Usage:" in captured.out

    @pytest.mark.unit
    def test_too_many_args_exits_with_usage(self, monkeypatch, capsys):
        """Extra positional args trigger the usage error too."""
        monkeypatch.setattr("sys.argv", ["moppy-cmorise", "config.yml", "extra-arg"])

        with pytest.raises(SystemExit) as excinfo:
            main()

        assert excinfo.value.code == 1
        captured = capsys.readouterr()
        assert "Usage:" in captured.out

    @pytest.mark.unit
    def test_nonexistent_config_exits_with_error(self, monkeypatch, tmp_path, capsys):
        """Pointing main() at a missing config file exits 1 with a clear message."""
        missing = tmp_path / "does_not_exist.yml"
        monkeypatch.setattr("sys.argv", ["moppy-cmorise", str(missing)])

        with pytest.raises(SystemExit) as excinfo:
            main()

        assert excinfo.value.code == 1
        captured = capsys.readouterr()
        assert "config file not found" in captured.out
        # Resolved absolute path appears in the error
        assert str(missing) in captured.out

    @pytest.mark.unit
    def test_monitor_flag_with_extra_args_still_dispatches(self, monkeypatch):
        """The --monitor branch uses `>=2` so extra args (e.g. config_path
        forwarded by the launcher) don't fall into the usage-error path."""
        monkeypatch.setattr(
            "sys.argv", ["moppy-cmorise", "--monitor", "/some/config.yml"]
        )

        called = []
        monkeypatch.setattr(
            "access_moppy.batch_cmoriser.monitor_main",
            lambda: called.append("ok"),
        )

        main()
        assert called == ["ok"]


class TestMonitorShutdownHandler:
    """Tests for the SIGTERM handler registered inside monitor_main.

    The handler is a closure over (tracker, job_map, experiment_id) defined
    locally; we capture it by mocking signal.signal and then invoke it
    directly to verify behavior, which is the only way to exercise the
    handler without sending real signals.
    """

    @staticmethod
    def _setup_monitor(temp_dir, monkeypatch, variables, completed=()):
        """Common scaffolding: prime DB + config + mocks, return (db_path, captured)."""
        db_path = temp_dir / "test.db"
        tracker = TaskTracker(db_path)
        for var in variables:
            tracker.add_task(var, "historical")
        for var in completed:
            tracker.mark_completed(var, "historical")
        tracker.conn.close()

        config_path = temp_dir / "config.yml"
        var_lines = "\n".join(f"  - {v}" for v in variables)
        config_path.write_text(f"experiment_id: historical\nvariables:\n{var_lines}\n")
        monkeypatch.setenv("MOPPY_CONFIG_PATH", str(config_path))
        monkeypatch.setenv("MOPPY_DB_PATH", str(db_path))
        monkeypatch.setenv("MOPPY_SCRIPT_DIR", str(temp_dir / "scripts"))

        # Submit returns deterministic per-variable job IDs
        ids = iter(f"{1000 + i}.gadi-pbs" for i in range(len(variables)))
        monkeypatch.setattr(
            "access_moppy.batch_cmoriser.create_job_script",
            lambda *a, **k: temp_dir / "x.sh",
        )
        monkeypatch.setattr(
            "access_moppy.batch_cmoriser.submit_job", lambda p: next(ids)
        )

        # Capture the signal handler instead of actually registering it
        captured = {}
        monkeypatch.setattr(
            "signal.signal", lambda sig, handler: captured.update({sig: handler})
        )
        return db_path, captured

    @pytest.mark.unit
    def test_handler_marks_running_subs_as_failed(self, temp_dir, monkeypatch):
        """SIGTERM mid-batch: any sub still in 'running' state gets marked failed."""
        import signal as _signal

        db_path, captured = self._setup_monitor(
            temp_dir, monkeypatch, variables=["Amon.tas", "Amon.pr"]
        )

        # Fire SIGTERM as soon as the monitor loop is reached
        def fake_loop(*_args, **_kwargs):
            captured[_signal.SIGTERM](_signal.SIGTERM, None)

        monkeypatch.setattr("access_moppy.batch_cmoriser.monitor_loop", fake_loop)

        with pytest.raises(SystemExit) as excinfo:
            monitor_main()
        assert excinfo.value.code == 143  # 128 + SIGTERM

        verify = TaskTracker(db_path)
        assert verify.get_status("Amon.tas", "historical") == "failed"
        assert verify.get_status("Amon.pr", "historical") == "failed"
        verify.conn.close()

    @pytest.mark.unit
    def test_handler_error_message_includes_signal_and_job_id(
        self, temp_dir, monkeypatch
    ):
        """Error message records both the signal number and the PBS job id."""
        import signal as _signal

        db_path, captured = self._setup_monitor(
            temp_dir, monkeypatch, variables=["Amon.tas"]
        )

        def fake_loop(*_args, **_kwargs):
            captured[_signal.SIGTERM](_signal.SIGTERM, None)

        monkeypatch.setattr("access_moppy.batch_cmoriser.monitor_loop", fake_loop)

        with pytest.raises(SystemExit):
            monitor_main()

        verify = TaskTracker(db_path)
        row = verify.conn.execute(
            "SELECT error_message FROM cmor_tasks WHERE variable='Amon.tas'"
        ).fetchone()
        assert "monitor terminated" in row[0]
        assert f"sig={int(_signal.SIGTERM)}" in row[0]
        assert "1000.gadi-pbs" in row[0]  # first submit id from _setup_monitor
        verify.conn.close()

    @pytest.mark.unit
    def test_handler_does_not_overwrite_completed(self, temp_dir, monkeypatch):
        """A sub that finished cleanly before SIGTERM must keep its 'completed' state."""
        import signal as _signal

        db_path, captured = self._setup_monitor(
            temp_dir,
            monkeypatch,
            variables=["Amon.tas", "Amon.done"],
            completed=["Amon.done"],
        )

        def fake_loop(*_args, **_kwargs):
            captured[_signal.SIGTERM](_signal.SIGTERM, None)

        monkeypatch.setattr("access_moppy.batch_cmoriser.monitor_loop", fake_loop)

        with pytest.raises(SystemExit):
            monitor_main()

        verify = TaskTracker(db_path)
        assert verify.get_status("Amon.done", "historical") == "completed"
        assert verify.get_status("Amon.tas", "historical") == "failed"
        verify.conn.close()

    @pytest.mark.unit
    def test_handler_tolerates_db_errors(self, temp_dir, monkeypatch):
        """If mark_failed inside the handler raises, the handler still calls
        sys.exit(143) — DB hiccups must not block PBS termination."""
        import signal as _signal

        db_path, captured = self._setup_monitor(
            temp_dir, monkeypatch, variables=["Amon.tas"]
        )

        def fake_loop(*_args, **_kwargs):
            # Break mark_failed (the exact failure point inside the handler)
            with patch.object(
                TaskTracker,
                "mark_failed",
                side_effect=RuntimeError("DB unavailable"),
            ):
                captured[_signal.SIGTERM](_signal.SIGTERM, None)

        monkeypatch.setattr("access_moppy.batch_cmoriser.monitor_loop", fake_loop)

        with pytest.raises(SystemExit) as excinfo:
            monitor_main()
        assert excinfo.value.code == 143

    @pytest.mark.unit
    def test_handler_registered_for_sigterm(self, temp_dir, monkeypatch):
        """Sanity: monitor_main must register a SIGTERM handler before looping."""
        import signal as _signal

        _, captured = self._setup_monitor(temp_dir, monkeypatch, variables=["Amon.tas"])

        # Make the loop a no-op so monitor_main exits normally
        monkeypatch.setattr(
            "access_moppy.batch_cmoriser.monitor_loop", lambda *a, **k: None
        )

        monitor_main()

        assert _signal.SIGTERM in captured
        assert callable(captured[_signal.SIGTERM])
