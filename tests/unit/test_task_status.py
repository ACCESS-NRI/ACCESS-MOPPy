"""Unit tests for the single-writer status files workers report through."""

import json
import os

import pytest

from access_moppy.task_status import (
    STATUS_FILENAME,
    TaskStatusFile,
    clear_status,
    enqueue_monitor_request,
    read_status,
    take_monitor_requests,
    variable_dir,
)

VARIABLE = "atmos.tas.tavg-h2m-hxy-u.mon.glb"


class TestTaskStatusFile:
    """A worker's own status file: one writer, whole-document rewrites."""

    @pytest.mark.unit
    def test_variable_dir_matches_the_generated_script_location(self, temp_dir):
        """The status file must land beside the sub-job's .py/.sh/.err files."""
        assert variable_dir(temp_dir, VARIABLE) == (
            temp_dir / "logs" / "atmos_tas_tavg-h2m-hxy-u_mon_glb"
        )

    @pytest.mark.unit
    def test_lifecycle_is_recorded(self, temp_dir):
        """running -> completed keeps timestamps and clears the error message."""
        var_dir = variable_dir(temp_dir, VARIABLE)
        status = TaskStatusFile(var_dir, VARIABLE, "historical")

        status.mark_running()
        running = read_status(temp_dir, VARIABLE)
        assert running["status"] == "running"
        assert running["start_time"] is not None
        assert running["end_time"] is None

        status.mark_completed()
        done = read_status(temp_dir, VARIABLE)
        assert done["status"] == "completed"
        assert done["start_time"] == running["start_time"]
        assert done["end_time"] is not None
        assert done["error_message"] is None

    @pytest.mark.unit
    def test_failure_keeps_the_message(self, temp_dir):
        status = TaskStatusFile(
            variable_dir(temp_dir, VARIABLE), VARIABLE, "historical"
        )
        status.mark_running()
        status.mark_failed("no pattern for frequency 'fx'")

        doc = read_status(temp_dir, VARIABLE)
        assert doc["status"] == "failed"
        assert doc["error_message"] == "no pattern for frequency 'fx'"
        assert doc["end_time"] is not None

    @pytest.mark.unit
    def test_later_writes_preserve_earlier_fields(self, temp_dir):
        """Each write rewrites the whole document, so nothing may be dropped.

        compliance is recorded mid-run, output_summary and worker_memory at the
        end; all three have to survive to the monitor.
        """
        status = TaskStatusFile(
            variable_dir(temp_dir, VARIABLE), VARIABLE, "historical"
        )
        status.mark_running()
        status.set_compliance({"passed": True})
        status.set_output_summary({"file_count": 3, "total_bytes": 42})
        status.mark_completed()
        status.set_worker_memory({"n_workers": 4})

        doc = read_status(temp_dir, VARIABLE)
        assert doc["compliance"] == {"passed": True}
        assert doc["output_summary"] == {"file_count": 3, "total_bytes": 42}
        assert doc["worker_memory"] == {"n_workers": 4}
        assert doc["status"] == "completed"

    @pytest.mark.unit
    def test_rerun_clears_the_previous_attempts_result(self, temp_dir):
        """mark_running must not leave the last attempt's outcome behind."""
        status = TaskStatusFile(
            variable_dir(temp_dir, VARIABLE), VARIABLE, "historical"
        )
        status.mark_running()
        status.set_output_summary({"file_count": 3})
        status.mark_failed("boom")

        status.mark_running()
        doc = read_status(temp_dir, VARIABLE)
        assert doc["status"] == "running"
        assert doc["error_message"] is None
        assert doc["output_summary"] is None
        assert doc["end_time"] is None

    @pytest.mark.unit
    def test_write_leaves_no_temporary_file_behind(self, temp_dir):
        """The atomic write's scratch file must not survive as batch litter."""
        var_dir = variable_dir(temp_dir, VARIABLE)
        status = TaskStatusFile(var_dir, VARIABLE, "historical")
        status.mark_running()
        status.mark_completed()

        assert [p.name for p in var_dir.iterdir()] == [STATUS_FILENAME]


class TestReadStatus:
    """Reading is best-effort: the monitor falls back to the PBS exit status."""

    @pytest.mark.unit
    def test_missing_file_reads_as_none(self, temp_dir):
        """The normal case before a sub-job starts is not an error."""
        assert read_status(temp_dir, VARIABLE) is None

    @pytest.mark.unit
    def test_unparseable_file_reads_as_none(self, temp_dir):
        """A truncated or hand-edited document must not raise at the monitor."""
        var_dir = variable_dir(temp_dir, VARIABLE)
        var_dir.mkdir(parents=True)
        (var_dir / STATUS_FILENAME).write_text('{"status": "run')

        assert read_status(temp_dir, VARIABLE) is None

    @pytest.mark.unit
    def test_non_object_json_reads_as_none(self, temp_dir):
        var_dir = variable_dir(temp_dir, VARIABLE)
        var_dir.mkdir(parents=True)
        (var_dir / STATUS_FILENAME).write_text("[1, 2, 3]")

        assert read_status(temp_dir, VARIABLE) is None

    @pytest.mark.unit
    def test_clear_removes_the_file_and_tolerates_absence(self, temp_dir):
        status = TaskStatusFile(
            variable_dir(temp_dir, VARIABLE), VARIABLE, "historical"
        )
        status.mark_completed()

        clear_status(temp_dir, VARIABLE)
        assert read_status(temp_dir, VARIABLE) is None
        clear_status(temp_dir, VARIABLE)  # idempotent


class TestAtomicReplacement:
    """A reader must never observe a half-written document."""

    @pytest.mark.unit
    def test_reader_sees_only_whole_documents(self, temp_dir):
        """Interleave writes and reads; every read must parse.

        This is what makes it safe for the monitor to read a status file at any
        moment without coordinating with the worker writing it -- coordination
        being exactly what Lustre's localflock mounts cannot provide.
        """
        var_dir = variable_dir(temp_dir, VARIABLE)
        status = TaskStatusFile(var_dir, VARIABLE, "historical")
        status.mark_running()

        for i in range(200):
            status.set_output_summary({"file_count": i, "pad": "x" * (i * 7)})
            doc = read_status(temp_dir, VARIABLE)
            assert doc is not None, "reader observed a partial document"
            assert doc["output_summary"]["file_count"] == i

    @pytest.mark.unit
    def test_failed_write_does_not_damage_the_previous_document(
        self, temp_dir, monkeypatch
    ):
        """If the replace fails, the last good document must still be readable."""
        status = TaskStatusFile(
            variable_dir(temp_dir, VARIABLE), VARIABLE, "historical"
        )
        status.mark_running()
        good = read_status(temp_dir, VARIABLE)

        failing_replace = _raiser(OSError("no space left on device"))
        monkeypatch.setattr(os, "replace", failing_replace)
        with pytest.raises(OSError):
            status.mark_completed()
        assert failing_replace.called

        monkeypatch.undo()
        assert read_status(temp_dir, VARIABLE) == good
        # The scratch file must not be left behind either.
        assert [p.name for p in variable_dir(temp_dir, VARIABLE).iterdir()] == [
            STATUS_FILENAME
        ]


class TestMonitorRequests:
    """--append-variable drops a file; the monitor consumes it once."""

    @pytest.mark.unit
    def test_request_round_trip(self, temp_dir):
        enqueue_monitor_request(temp_dir, "Amon.pr", "historical")

        assert take_monitor_requests(temp_dir, "historical") == ["Amon.pr"]

    @pytest.mark.unit
    def test_requests_are_delivered_only_once(self, temp_dir):
        enqueue_monitor_request(temp_dir, "Amon.pr", "historical")
        take_monitor_requests(temp_dir, "historical")

        assert take_monitor_requests(temp_dir, "historical") == []

    @pytest.mark.unit
    def test_requests_are_scoped_to_one_experiment(self, temp_dir):
        """A shared output directory must not bleed requests across experiments."""
        enqueue_monitor_request(temp_dir, "Amon.pr", "historical")
        enqueue_monitor_request(temp_dir, "Amon.tas", "piControl")

        assert take_monitor_requests(temp_dir, "historical") == ["Amon.pr"]
        # The other experiment's request is still waiting for its own monitor.
        assert take_monitor_requests(temp_dir, "piControl") == ["Amon.tas"]

    @pytest.mark.unit
    def test_no_requests_directory_is_not_an_error(self, temp_dir):
        """Every poll calls this; a batch that never appends has no directory."""
        assert take_monitor_requests(temp_dir, "historical") == []

    @pytest.mark.unit
    def test_unparseable_request_is_discarded_not_retried(self, temp_dir):
        """A bad file must not be re-read on every poll for the rest of the batch."""
        enqueue_monitor_request(temp_dir, "Amon.pr", "historical")
        bad = temp_dir / "monitor_requests" / "broken.json"
        bad.write_text("{not json")

        assert take_monitor_requests(temp_dir, "historical") == ["Amon.pr"]
        assert not bad.exists()

    @pytest.mark.unit
    def test_request_payload_records_who_asked_for_what(self, temp_dir):
        enqueue_monitor_request(temp_dir, "atmos.pr.tavg-u-hxy-u.mon.glb", "historical")

        written = next((temp_dir / "monitor_requests").glob("*.json"))
        payload = json.loads(written.read_text())
        assert payload["variable"] == "atmos.pr.tavg-u-hxy-u.mon.glb"
        assert payload["experiment_id"] == "historical"
        assert payload["requested_at"]


def _raiser(exc):
    """Return a callable that always raises *exc* and records that it ran."""

    def raise_it(*_args, **_kwargs):
        raise_it.called = True
        raise exc

    raise_it.called = False
    return raise_it
