"""Single-writer status files that carry sub-job state back to the monitor.

Gadi mounts every Lustre filesystem with ``localflock``, so ``fcntl``/``flock``
are node-local and are not coordinated between nodes. SQLite's whole
concurrency model rests on POSIX advisory locks, which means the ~100 worker
jobs of a batch -- one per compute node -- could each hold an "EXCLUSIVE" lock
on ``cmor_tasks.db`` and write at the same time. That corrupts the database
outright (``2nd reference to page N``) or silently loses writes, because SQLite
writes whole pages and a row's page is shared with many other rows.

The fix is to stop writing the database from more than one node. Each worker
writes its own state to ``logs/<variable>/status.json`` instead -- a file with
exactly one writer, on one node -- and the monitor, the single remaining
database writer, ingests those files as it polls. Writes go through a
temporary file plus ``os.replace`` so a reader never observes a half-written
document.

The same single-writer rule applies to ``--append-variable``: it runs on the
login node while a monitor is live, so it drops a request file here rather than
writing the database itself.
"""

from __future__ import annotations

import json
import os
import time
from collections.abc import Mapping
from pathlib import Path
from typing import Any

STATUS_FILENAME = "status.json"
REQUESTS_DIRNAME = "monitor_requests"

# Fields a worker owns. pbs_job_id/pbs_info_json are not here: the monitor
# learns those from qsub/qstat and writes them to the database directly.
_WORKER_FIELDS = (
    "status",
    "start_time",
    "end_time",
    "error_message",
    "output_summary",
    "worker_memory",
    "compliance",
)


def _sanitize(variable: str) -> str:
    """Return the on-disk form of a variable name (``.`` is a path separator)."""
    return variable.replace(".", "_")


def variable_dir(script_dir: str | Path, variable: str) -> Path:
    """Return the per-variable log directory a sub-job writes into."""
    return Path(script_dir) / "logs" / _sanitize(variable)


def _write_atomically(path: Path, payload: str) -> None:
    """Replace *path* with *payload* in one step.

    ``os.replace`` of a file within its own directory is atomic on Lustre, so a
    concurrent reader always sees either the whole previous document or the
    whole new one. The temporary name carries the pid so two writers -- which
    should never happen, but a hand-run script could -- cannot clobber each
    other's partial file.
    """
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        tmp.write_text(payload, encoding="utf-8")
        os.replace(tmp, path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise


def read_status(script_dir: str | Path, variable: str) -> dict[str, Any] | None:
    """Return a variable's status document, or None if unusable.

    A missing file is the normal case before a sub-job starts. Unparseable
    content is treated the same way rather than raised: the monitor falls back
    to the PBS exit status, which is what it did for every sub-job before
    status files existed.
    """
    path = variable_dir(script_dir, variable) / STATUS_FILENAME
    try:
        raw = path.read_text(encoding="utf-8")
    except (FileNotFoundError, NotADirectoryError, OSError):
        return None
    try:
        loaded = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return loaded if isinstance(loaded, dict) else None


def clear_status(script_dir: str | Path, variable: str) -> None:
    """Delete a variable's status file. Idempotent.

    The monitor calls this immediately before qsub so a rerun never inherits
    the previous attempt's terminal state.
    """
    path = variable_dir(script_dir, variable) / STATUS_FILENAME
    try:
        path.unlink(missing_ok=True)
    except OSError:
        pass


class TaskStatusFile:
    """The one status file a single sub-job writes.

    Mirrors the ``TaskTracker`` methods the worker used to call, so a worker
    reports its state exactly as before -- just to its own file instead of to a
    database shared with 99 other nodes.

    Every method rewrites the whole document from an in-memory copy, so no
    method ever needs to read the file back.

    Args:
        var_dir: The sub-job's own log directory (``logs/<variable>/``).
        variable: CMOR variable name or compound variable identifier.
        experiment_id: Experiment identifier associated with the task.
    """

    def __init__(self, var_dir: str | Path, variable: str, experiment_id: str) -> None:
        self.path = Path(var_dir) / STATUS_FILENAME
        self._doc: dict[str, Any] = {
            "variable": variable,
            "experiment_id": experiment_id,
            "status": "pending",
            **{field: None for field in _WORKER_FIELDS if field != "status"},
        }

    def _update(self, **fields: Any) -> None:
        self._doc.update(fields)
        self._doc["updated_at"] = _utc_now()
        self.path.parent.mkdir(parents=True, exist_ok=True)
        _write_atomically(
            self.path, json.dumps(self._doc, sort_keys=True, separators=(",", ":"))
        )

    def mark_running(self) -> None:
        """Record that processing has started, clearing any previous attempt."""
        self._update(
            status="running",
            start_time=_utc_now(),
            end_time=None,
            error_message=None,
            output_summary=None,
            worker_memory=None,
        )

    def mark_completed(self) -> None:
        """Record successful completion."""
        self._update(status="completed", end_time=_utc_now(), error_message=None)

    def mark_done(self) -> None:
        """Alias for mark_completed, matching TaskTracker's naming."""
        self.mark_completed()

    def mark_failed(self, error_message: str) -> None:
        """Record failure and the message explaining it."""
        self._update(
            status="failed", end_time=_utc_now(), error_message=str(error_message)
        )

    def set_output_summary(self, summary: Mapping[str, Any] | None) -> None:
        """Record the file count and volume this variable produced."""
        self._update(output_summary=dict(summary) if summary is not None else None)

    def set_worker_memory(self, info: Mapping[str, Any] | None) -> None:
        """Record observed peak RSS per Dask worker against the sizing used."""
        self._update(worker_memory=dict(info) if info is not None else None)

    def set_compliance(self, result: Mapping[str, Any] | None) -> None:
        """Record the compliance verdict for this variable's first output file."""
        self._update(compliance=dict(result) if result is not None else None)


def enqueue_monitor_request(
    output_dir: str | Path, variable: str, experiment_id: str
) -> None:
    """Ask the live monitor to pick up *variable*.

    Written from the login node while a monitor is running, so it is a file
    rather than a database row. The monitor decides whether the variable is
    already completed or already scheduled; this only records the request.
    """
    requests_dir = Path(output_dir) / REQUESTS_DIRNAME
    requests_dir.mkdir(parents=True, exist_ok=True)
    _write_atomically(
        requests_dir / f"{_sanitize(variable)}.json",
        json.dumps(
            {
                "variable": variable,
                "experiment_id": experiment_id,
                "requested_at": _utc_now(),
            },
            sort_keys=True,
            separators=(",", ":"),
        ),
    )


def take_monitor_requests(output_dir: str | Path, experiment_id: str) -> list[str]:
    """Return and remove all queued requests for an experiment.

    Each request file is unlinked as it is read, so a request is delivered
    once. A file that cannot be parsed is removed too rather than left to be
    retried on every poll for the rest of the batch.
    """
    requests_dir = Path(output_dir) / REQUESTS_DIRNAME
    try:
        entries = sorted(requests_dir.glob("*.json"))
    except OSError:
        return []

    variables: list[str] = []
    for entry in entries:
        try:
            payload = json.loads(entry.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            payload = None
        if (
            isinstance(payload, dict)
            and payload.get("experiment_id") == experiment_id
            and isinstance(payload.get("variable"), str)
        ):
            variables.append(payload["variable"])
        elif (
            isinstance(payload, dict) and payload.get("experiment_id") != experiment_id
        ):
            # Another experiment's request sharing this output directory:
            # leave it for the monitor that owns it.
            continue
        entry.unlink(missing_ok=True)
    return variables


def _utc_now() -> str:
    """Return the current UTC time in the format the task table stores."""
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime())
