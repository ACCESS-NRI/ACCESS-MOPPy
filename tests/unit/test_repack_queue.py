"""Tests for the background cmip7repack queue."""

import threading

import pytest

from access_moppy.executors.repack_queue import RepackQueue, auto_repack_workers


def _file(tmp_path, name, size):
    path = tmp_path / name
    path.write_bytes(b"\0" * size)
    return path


@pytest.mark.unit
def test_repack_runs_on_a_background_thread(tmp_path):
    caller = threading.get_ident()
    started, release = threading.Event(), threading.Event()
    ran_on = []

    def repack():
        ran_on.append(threading.get_ident())
        started.set()
        release.wait(5)

    queue = RepackQueue()
    queue.submit("a", _file(tmp_path, "a.nc", 10), repack)

    # submit() returns while the repack is still running.
    assert started.wait(5)
    release.set()
    assert list(queue.drain()) == [("a", None)]
    assert ran_on != [caller]
    queue.close()


@pytest.mark.unit
def test_results_come_back_in_submission_order(tmp_path):
    """A later file that finishes first still waits for the earlier one."""
    second_done = threading.Event()

    queue = RepackQueue(workers=2)
    queue.submit("first", _file(tmp_path, "a.nc", 10), lambda: second_done.wait(5))
    queue.submit("second", _file(tmp_path, "b.nc", 10), second_done.set)

    assert [item for item, _ in queue.drain()] == ["first", "second"]
    queue.close()


@pytest.mark.unit
def test_make_room_returns_finished_files_without_waiting(tmp_path):
    release = threading.Event()

    queue = RepackQueue(workers=2)
    queue.submit("done", _file(tmp_path, "a.nc", 10), None)
    queue.submit("running", _file(tmp_path, "b.nc", 10), lambda: release.wait(5))

    # Plenty of budget: only the already-finished file is handed back.
    assert list(queue.make_room()) == [("done", None)]
    release.set()
    assert list(queue.drain()) == [("running", None)]
    queue.close()


@pytest.mark.unit
def test_make_room_waits_for_the_oldest_file_when_the_next_would_not_fit(tmp_path):
    """Each file is charged twice its size; the next is assumed as big as the last."""
    size = 100
    release_first, release_second = threading.Event(), threading.Event()

    # Room for 2 charges of 2*size = 200 bytes each, not a third.
    queue = RepackQueue(workers=2, buffer_max_bytes=5 * size)
    queue.submit("first", _file(tmp_path, "a.nc", size), lambda: release_first.wait(5))
    queue.submit(
        "second", _file(tmp_path, "b.nc", size), lambda: release_second.wait(5)
    )

    # 400 charged + 200 for the next file > 500, so make_room waits for the
    # still-running oldest file; 200 + 200 then fits, and the second file,
    # still running, is left alone.
    threading.Timer(0.2, release_first.set).start()
    assert [item for item, _ in queue.make_room()] == ["first"]
    assert release_first.is_set()
    release_second.set()
    assert [item for item, _ in queue.drain()] == ["second"]
    queue.close()


@pytest.mark.unit
def test_a_file_larger_than_the_budget_is_still_accepted(tmp_path):
    """With nothing pending, a file is never held back, so nothing deadlocks."""
    queue = RepackQueue(buffer_max_bytes=10)
    queue.submit("big", _file(tmp_path, "a.nc", 1000), None)

    assert [item for item, _ in queue.make_room()] == ["big"]
    assert list(queue.make_room()) == []
    queue.close()


@pytest.mark.unit
def test_repack_errors_are_returned_with_their_file(tmp_path):
    def fail():
        raise RuntimeError("cmip7repack failed")

    queue = RepackQueue()
    queue.submit("bad", _file(tmp_path, "a.nc", 10), fail)
    queue.submit("good", _file(tmp_path, "b.nc", 10), None)

    (bad, error), (good, ok) = queue.drain()
    assert (bad, good, ok) == ("bad", "good", None)
    assert isinstance(error, RuntimeError)
    queue.close()


@pytest.mark.unit
def test_budget_is_a_share_of_the_free_space(tmp_path, monkeypatch):
    import shutil
    from collections import namedtuple

    usage = namedtuple("usage", "total used free")
    monkeypatch.setattr(shutil, "disk_usage", lambda path: usage(0, 0, 1000))

    queue = RepackQueue(buffer_fraction=0.5, buffer_max_bytes=10**9)
    queue.submit("a", _file(tmp_path, "a.nc", 100), None)

    assert queue.budget_bytes == 500
    queue.close()


@pytest.mark.unit
def test_budget_falls_back_to_the_cap_when_free_space_is_unreadable(
    tmp_path, monkeypatch
):
    import shutil

    def unreadable(path):
        raise OSError("statvfs failed")

    monkeypatch.setattr(shutil, "disk_usage", unreadable)

    queue = RepackQueue(buffer_max_bytes=1234)
    queue.submit("a", _file(tmp_path, "a.nc", 10), None)

    assert queue.budget_bytes == 1234
    queue.close()


@pytest.mark.unit
def test_close_cancels_repacks_that_have_not_started(tmp_path):
    release = threading.Event()
    ran = []

    queue = RepackQueue(workers=1)
    queue.submit("running", _file(tmp_path, "a.nc", 10), lambda: release.wait(5))
    queue.submit("queued", _file(tmp_path, "b.nc", 10), lambda: ran.append("queued"))

    threading.Timer(0.2, release.set).start()
    queue.close()

    assert ran == []


@pytest.mark.unit
@pytest.mark.parametrize(
    "kwargs",
    [
        {"workers": 0},
        {"workers": True},
        {"buffer_fraction": 0},
        {"buffer_fraction": 1.5},
        {"buffer_max_bytes": 0},
    ],
)
def test_rejects_invalid_settings(kwargs):
    with pytest.raises(ValueError):
        RepackQueue(**kwargs)


@pytest.mark.unit
@pytest.mark.parametrize(
    ("n_cpus", "dask_threads", "expected"),
    [
        (28, 14, 8),  # memory-bound workers leave 13 CPUs; capped at 8
        (16, 8, 7),  # the CPUs left after the workers and the main process
        (7, 7, 2),  # the workers take every CPU: still overlap two repacks
        (4, 4, 2),
        (1, 1, 2),
    ],
)
def test_auto_repack_workers_uses_the_cpus_the_dask_workers_leave(
    n_cpus, dask_threads, expected
):
    assert auto_repack_workers(dask_threads, n_cpus=n_cpus) == expected


@pytest.mark.unit
def test_auto_repack_workers_reads_the_pbs_cpu_count(monkeypatch):
    monkeypatch.setenv("PBS_NCPUS", "16")

    assert auto_repack_workers(8) == 7
