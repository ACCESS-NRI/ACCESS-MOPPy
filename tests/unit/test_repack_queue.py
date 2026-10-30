"""Tests for the background cmip7repack queue."""

import threading
import time

import pytest

from access_moppy.executors.repack_queue import (
    RepackQueue,
    adapt_cpu_split,
    auto_cpu_split,
    auto_repack_workers,
    repack_slots_needed,
)


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
def test_one_budget_covers_files_left_pending_between_writes(tmp_path, monkeypatch):
    """A queue shared across source partitions measures its budget once, and a
    file one partition left repacking still counts against the next."""
    import shutil
    from collections import namedtuple

    usage = namedtuple("usage", "total used free")
    free = [1000]
    monkeypatch.setattr(shutil, "disk_usage", lambda path: usage(0, 0, free[0]))
    release = threading.Event()

    queue = RepackQueue(workers=2, buffer_fraction=1.0, buffer_max_bytes=10**9)
    queue.submit("first", _file(tmp_path, "a.nc", 100), lambda: release.wait(5))
    # The first partition ends with its file still repacking.
    assert list(queue.make_room()) == []

    free[0] = 10**6
    queue.submit("second", _file(tmp_path, "b.nc", 300), None)
    assert queue.budget_bytes == 1000

    # 200 + 600 charged and 600 more for the next file > 1000: the next
    # partition waits for the first partition's file.
    threading.Timer(0.2, release.set).start()
    assert [item for item, _ in queue.make_room()] == ["first", "second"]
    assert release.is_set()
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


@pytest.mark.unit
@pytest.mark.parametrize(
    ("n_cpus", "dask_workers", "repack_heavy", "expected"),
    [
        (16, 16, False, (16, 2)),  # the usual split: workers take every CPU
        (28, 14, False, (14, 8)),
        (16, 16, True, (8, 7)),  # half of what the main process leaves
        (8, 8, True, (4, 3)),
        (8, 2, True, (2, 5)),  # memory-bound workers leave more than half
        (28, 14, True, (14, 13)),
        (2, 2, True, (1, 2)),  # always one worker and two repacks
    ],
)
def test_auto_cpu_split_gives_repack_heavy_output_half_the_cpus(
    n_cpus, dask_workers, repack_heavy, expected
):
    assert auto_cpu_split(dask_workers, repack_heavy, n_cpus=n_cpus) == expected


@pytest.mark.unit
def test_auto_cpu_split_reads_the_pbs_cpu_count(monkeypatch):
    monkeypatch.setenv("PBS_NCPUS", "16")

    assert auto_cpu_split(16, True) == (8, 7)


@pytest.mark.unit
def test_window_stats_measure_repack_time_and_waits_then_reset(tmp_path):
    queue = RepackQueue(workers=2)
    queue.submit("a", _file(tmp_path, "a.nc", 10), lambda: time.sleep(0.2))
    queue.submit("b", _file(tmp_path, "b.nc", 10), None)  # no repack: not counted
    assert len(list(queue.drain())) == 2

    stats = queue.window_stats()
    assert stats["submitted"] == stats["files"] == 1
    assert stats["workers"] == 2
    assert stats["busy_s"] >= 0.2
    assert stats["mean_repack_s"] == pytest.approx(stats["busy_s"])
    # drain() blocked on the unfinished repack.
    assert 0 < stats["wait_s"] <= stats["busy_s"]
    assert stats["elapsed_s"] >= stats["busy_s"]

    stats = queue.window_stats()
    assert stats["submitted"] == stats["files"] == 0
    assert stats["busy_s"] == stats["wait_s"] == 0
    # The mean covers every repack so far, not only the window's.
    assert stats["mean_repack_s"] >= 0.2
    queue.close()


@pytest.mark.unit
def test_window_stats_count_a_failed_repack(tmp_path):
    def fail():
        raise RuntimeError("cmip7repack failed")

    queue = RepackQueue()
    assert queue.window_stats()["mean_repack_s"] is None
    queue.submit("bad", _file(tmp_path, "a.nc", 10), fail)
    list(queue.drain())

    assert queue.window_stats()["files"] == 1
    queue.close()


@pytest.mark.unit
def test_window_stats_split_a_running_repack_between_windows(tmp_path):
    """A repack still running at the end of a window is busy time in it, and
    only its remainder is busy time in the next."""
    release = threading.Event()
    queue = RepackQueue()
    queue.submit("a", _file(tmp_path, "a.nc", 10), lambda: release.wait(5))
    time.sleep(0.2)

    first = queue.window_stats()
    assert (first["submitted"], first["files"]) == (1, 0)
    assert first["busy_s"] >= 0.2
    assert first["mean_repack_s"] is None

    time.sleep(0.1)
    release.set()
    list(queue.drain())
    second = queue.window_stats()
    assert (second["submitted"], second["files"]) == (0, 1)
    assert 0.1 <= second["busy_s"] < first["busy_s"] + 0.1
    assert second["mean_repack_s"] >= 0.3
    queue.close()


@pytest.mark.unit
def test_repacks_wait_for_a_slot_and_start_in_order(tmp_path):
    started = []
    release = {name: threading.Event() for name in "abc"}

    def repack(name):
        def run():
            started.append(name)
            release[name].wait(5)

        return run

    queue = RepackQueue(workers=1)
    for name in "abc":
        queue.submit(name, _file(tmp_path, f"{name}.nc", 10), repack(name))
    time.sleep(0.1)
    assert started == ["a"]

    release["a"].set()
    release["b"].set()
    release["c"].set()
    assert [item for item, _ in queue.drain()] == ["a", "b", "c"]
    assert started == ["a", "b", "c"]
    queue.close()


@pytest.mark.unit
def test_resize_starts_waiting_repacks_and_never_stops_running_ones(tmp_path):
    running = threading.Semaphore(0)
    release = threading.Event()

    def repack():
        running.release()
        release.wait(5)

    queue = RepackQueue(workers=1)
    for name in "abc":
        queue.submit(name, _file(tmp_path, f"{name}.nc", 10), repack)
    assert running.acquire(timeout=5)
    assert not running.acquire(timeout=0.1)

    queue.resize(3)  # both waiting repacks start
    assert running.acquire(timeout=5) and running.acquire(timeout=5)
    queue.resize(1)  # the three running ones carry on
    assert queue.window_stats()["workers"] == 1
    release.set()
    assert [item for item, _ in queue.drain()] == ["a", "b", "c"]

    with pytest.raises(ValueError, match="positive integer"):
        queue.resize(0)
    queue.close()


def _window(submitted, mean_repack_s, elapsed_s, workers, wait_s=0.0):
    return {
        "elapsed_s": elapsed_s,
        "submitted": submitted,
        "files": submitted,
        "busy_s": 0.0,
        "wait_s": wait_s,
        "workers": workers,
        "mean_repack_s": mean_repack_s,
    }


@pytest.mark.unit
def test_repack_slots_needed_leaves_out_time_spent_waiting_on_repacks():
    # 8 files of 197 s queued in 375 s, 172 s of it waiting on repacks.
    assert repack_slots_needed(_window(8, 197.0, 375.0, 7, 172.0)) == pytest.approx(
        8 * 197 / 203
    )
    # Too little to tell: one file, no finished repack, all time waiting.
    assert repack_slots_needed(_window(1, 197.0, 375.0, 7)) is None
    assert repack_slots_needed(_window(8, None, 375.0, 7)) is None
    assert repack_slots_needed(_window(8, 197.0, 100.0, 7, 100.0)) is None


@pytest.mark.unit
@pytest.mark.parametrize(
    ("window", "n_cpus", "dask_workers", "dask_workers_max", "expected"),
    [
        # piControl Omon.umo on 8 CPUs, 4/3: ~5.3 repacks needed at 4 workers;
        # 4 are enough at the 3 workers that leaves (not 6 and 1 worker).
        (_window(8, 173.0, 263.0, 3), 8, 4, 8, (3, 4)),
        # Omon.umo on 16 CPUs, 8/7: ~5.5 needed of 7, kept.
        (_window(8, 197.0, 285.0, 7), 16, 8, 16, (8, 7)),
        # 3hr tas on 16 CPUs, 16/2: ~6.6 needed. Its files come as fast with
        # fewer workers, so each recreation moves it on towards 8/7.
        (_window(80, 19.8, 240.0, 2), 16, 16, 16, (10, 5)),
        (_window(80, 19.8, 240.0, 5), 16, 10, 16, (9, 6)),
        (_window(80, 19.8, 240.0, 6), 16, 9, 16, (8, 7)),
        # Needed just above the slots, but enough once a worker goes: kept.
        (_window(80, 19.8, 750.0, 2), 16, 16, 16, (16, 2)),
        # Never fewer than one worker, and one CPU stays for the main process.
        (_window(80, 100.0, 100.0, 2), 8, 8, 8, (1, 6)),
        # Repacks mostly idle while the workers were cut: half again what
        # was needed, and the CPUs left go back to the workers.
        (_window(74, 19.8, 441.0, 7), 16, 8, 16, (10, 5)),
        (_window(8, 30.0, 400.0, 7), 16, 8, 16, (13, 2)),
        # ...but not above what the memory sizing allows, nor when the workers
        # already have all of it.
        (_window(8, 30.0, 400.0, 7), 16, 8, 10, (10, 2)),
        (_window(8, 30.0, 400.0, 3), 8, 8, 8, (8, 3)),
        # Nothing to go on: unchanged.
        (_window(0, None, 400.0, 3), 8, 4, 8, (4, 3)),
    ],
)
def test_adapt_cpu_split(window, n_cpus, dask_workers, dask_workers_max, expected):
    assert (
        adapt_cpu_split(window, dask_workers, dask_workers_max, n_cpus=n_cpus)
        == expected
    )
