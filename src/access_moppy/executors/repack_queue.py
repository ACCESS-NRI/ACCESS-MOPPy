"""Run ``cmip7repack`` on finished output files in the background.

``cmip7repack`` rewrites each CMIP7 file single-threaded, and for sub-daily
output it takes about 80% of a file's wall time while the Dask workers have
nothing to do (piControl 6hr ua: ~40 s of a ~49 s file). The queue runs it on
background threads so the next file can be computed meanwhile.

The threads run only the external process. netCDF-C keeps a process-global file
table with no lock (see :mod:`access_moppy.executors.nc_anchor`), so every
netCDF call -- sizing the repack, checking its result, validating and
publishing the file -- stays on the caller's thread. Finished files are handed
back strictly in submission order, so they are published, and their completion
markers written, in time order.

Files waiting for or undergoing a repack sit uncompressed on local disk (Gadi's
``$PBS_JOBFS``), so the space they take is bounded by a byte budget.
"""

from __future__ import annotations

import logging
import math
import os
import shutil
import threading
import time
from collections import deque
from concurrent.futures import Future
from pathlib import Path
from typing import Callable, Iterator

logger = logging.getLogger(__name__)

# cmip7repack writes the repacked copy before it replaces the original, so a
# file briefly takes twice its size. Every queued file is charged that much.
_REPACK_COPY_FACTOR = 2

# Bounds for auto_repack_workers. Full piControl runs never kept more than
# about six repacks busy: one only needs to overlap the few files computed
# while it runs. (Those runs drained the queue at the end of each source
# partition; a queue shared across partitions has not been measured.)
_AUTO_WORKERS_MIN = 2
_AUTO_WORKERS_MAX = 8


def auto_repack_workers(dask_threads: int, n_cpus: int | None = None) -> int:
    """Repacks to run beside *dask_threads* Dask threads on *n_cpus* CPUs.

    Uses the CPUs the Dask workers and the main process leave free, kept
    between 2 and 8. Two still run when the workers take every CPU, because
    the workers are busy for only part of each file. *n_cpus* defaults to the
    PBS allocation, as in :func:`~access_moppy.executors.dask_config.recommend_dask_config`.
    """
    if n_cpus is None:
        n_cpus = int(os.environ.get("PBS_NCPUS", os.cpu_count() or 1))
    return max(_AUTO_WORKERS_MIN, min(_AUTO_WORKERS_MAX, n_cpus - dask_threads - 1))


def auto_cpu_split(
    dask_workers: int, repack_heavy: bool, n_cpus: int | None = None
) -> tuple[int, int]:
    """Dask workers and repacks for ``cmip7repack_workers: auto``.

    *dask_workers* is what the memory sizing allows. Most variables keep it
    and get :func:`auto_repack_workers`. A *repack_heavy* variable (see
    :func:`~access_moppy.executors.dask_config.repack_heavy_output`) produces
    files faster than a few repacks can rewrite them, so it gives the repacks
    half the CPUs the main process leaves, or every CPU its memory-bound
    workers leave if that is more. On 16 CPUs that is 8 workers and 7 repacks,
    which ran 3hr tas and Omon.umo about twice as fast as 16 and 2 (piControl,
    20 ten-year partitions); monthly cl and cVeg were 10-20% slower with it.
    """
    if n_cpus is None:
        n_cpus = int(os.environ.get("PBS_NCPUS", os.cpu_count() or 1))
    if not repack_heavy:
        return dask_workers, auto_repack_workers(dask_workers, n_cpus)
    spare = n_cpus - 1
    repacks = max(_AUTO_WORKERS_MIN, spare // 2, spare - dask_workers)
    return max(1, min(dask_workers, spare - repacks)), repacks


def repack_slots_needed(stats: dict[str, float | None]) -> float | None:
    """Repacks that must run at once to keep up with the computation.

    From one :meth:`RepackQueue.window_stats` window: the files queued in it,
    times the mean repack, over the time the computation was not held up by
    the repacks. ``None`` when the window says too little to tell.
    """
    computing_s = stats["elapsed_s"] - stats["wait_s"]
    if not stats["mean_repack_s"] or stats["submitted"] < 2 or computing_s <= 0:
        return None
    return stats["submitted"] * stats["mean_repack_s"] / computing_s


def adapt_cpu_split(
    stats: dict[str, float | None],
    dask_workers: int,
    dask_workers_max: int,
    n_cpus: int | None = None,
) -> tuple[int, int]:
    """Dask workers and repacks for the next partitions, from one window.

    *stats* is a :meth:`RepackQueue.window_stats` window run with
    *dask_workers* workers; *dask_workers_max* is what the memory sizing
    allows. When the computation queued more repack work than the slots could
    take, the repacks get more slots, taken from the workers (one always
    stays). Fewer workers queue work more slowly, so the repacks get only what
    they would need if the files came at a rate proportional to the workers:
    on 8 CPUs, Omon.umo at 4 workers and 3 repacks needed about 5.3 repacks,
    which would leave 1 worker; this gives 3 and 4. A variable whose files
    come no slower with fewer workers gets the rest at later recreations.
    When the repacks needed under half their slots
    and the workers were cut below *dask_workers_max*, the workers get CPUs
    back and the repacks keep half again what they needed. Otherwise the split
    stays: the repack buffer absorbs a window's ups and downs.
    """
    if n_cpus is None:
        n_cpus = int(os.environ.get("PBS_NCPUS", os.cpu_count() or 1))
    repacks = stats["workers"]
    needed = repack_slots_needed(stats)
    if needed is None:
        return dask_workers, repacks
    spare = n_cpus - 1
    if needed > repacks:
        # needed * new_workers / dask_workers <= repacks, new_workers = spare - repacks
        enough = math.ceil(needed * spare / (dask_workers + needed))
        if enough <= repacks:
            return dask_workers, repacks
        repacks = min(max(_AUTO_WORKERS_MIN, spare - 1), enough)
        return max(1, min(dask_workers, spare - repacks)), repacks
    if needed < repacks / 2 and dask_workers < dask_workers_max:
        repacks = max(_AUTO_WORKERS_MIN, math.ceil(1.5 * needed))
        return max(dask_workers, min(dask_workers_max, spare - repacks)), repacks
    return dask_workers, repacks


class RepackQueue:
    """Background repacks of finished files, returned in submission order.

    Each repack runs on its own thread, started in submission order as a slot
    frees up; :meth:`resize` changes the number of slots.

    Parameters
    ----------
    workers:
        Number of repacks that may run at once.
    buffer_fraction:
        Share of the free space on the disk the files are written to that
        queued files may take.
    buffer_max_bytes:
        Hard cap on that space, for writes that go straight to a large shared
        filesystem rather than to local staging.
    """

    def __init__(
        self,
        workers: int = 1,
        buffer_fraction: float = 0.4,
        buffer_max_bytes: int = 50 * 1024**3,
    ):
        if not isinstance(workers, int) or isinstance(workers, bool) or workers < 1:
            raise ValueError("workers must be a positive integer")
        if not 0 < buffer_fraction <= 1:
            raise ValueError("buffer_fraction must be in (0, 1]")
        if buffer_max_bytes <= 0:
            raise ValueError("buffer_max_bytes must be positive")

        self._buffer_fraction = buffer_fraction
        self._buffer_max_bytes = buffer_max_bytes
        self.budget_bytes: int | None = None
        self._pending: deque[tuple[Future, object, int]] = deque()
        self._charged = 0
        self._last_charge = 0
        self._workers = workers
        # Guards the repacks waiting for a slot, the running ones (with their
        # start times) and the counters for window_stats(), which the repack
        # threads update.
        self._lock = threading.Lock()
        self._backlog: deque[tuple[Future, Callable[[], None]]] = deque()
        self._running: dict[Future, float] = {}
        self._threads: set[threading.Thread] = set()
        self._closed = False
        self._started = 0
        self._window_start = time.monotonic()
        self._window_submitted = 0
        self._window_files = 0
        self._window_busy_s = 0.0
        self._window_wait_s = 0.0
        self._total_files = 0
        self._total_repack_s = 0.0

    def submit(
        self, item: object, path: Path, repack: Callable[[], None] | None
    ) -> None:
        """Start *repack* (of the file at *path*); *item* is handed back later.

        ``repack=None`` queues a file that needs no repack, so it is published
        in order with the others.
        """
        if self.budget_bytes is None:
            self.budget_bytes = self._budget_for(path)

        if repack is None:
            future: Future = Future()
            future.set_result(None)
        else:
            future = Future()
            with self._lock:
                self._window_submitted += 1
                self._backlog.append((future, repack))
                self._start_ready()

        charge = _REPACK_COPY_FACTOR * path.stat().st_size
        self._pending.append((future, item, charge))
        self._charged += charge
        self._last_charge = charge

    def make_room(self) -> Iterator[tuple[object, BaseException | None]]:
        """Hand back finished files, waiting only while the next would not fit.

        The next file is assumed to be as large as the last one submitted. A
        file is never held back when nothing is pending, so one larger than the
        whole budget is still written, alone.
        """
        while self._pending and (
            self._pending[0][0].done()
            or self._charged + self._last_charge > self.budget_bytes
        ):
            yield self._pop()

    def drain(self) -> Iterator[tuple[object, BaseException | None]]:
        """Hand back every pending file, waiting for each repack to finish."""
        while self._pending:
            yield self._pop()

    def resize(self, workers: int) -> None:
        """Let *workers* repacks run at once from now on.

        Running repacks are never stopped: after a shrink, waiting ones start
        only once the running ones are below the new number.
        """
        if not isinstance(workers, int) or isinstance(workers, bool) or workers < 1:
            raise ValueError("workers must be a positive integer")
        with self._lock:
            self._workers = workers
            self._start_ready()

    def window_stats(self) -> dict[str, float | None]:
        """How busy the repacks were since the last call (or since creation).

        ``submitted`` counts the files queued for a repack in the window and
        ``files`` the repacks that finished in it. ``busy_s`` is the slot time
        spent repacking within the window, including the part of a repack that
        started before it or is still running, so ``busy_s / (elapsed_s *
        workers)`` is the share of the slots in use. ``wait_s`` is the time
        the caller spent blocked on an unfinished repack: time in which the
        repacks, not the computation, set the pace. ``mean_repack_s`` is the
        mean of every repack finished so far, ``None`` before the first.
        """
        now = time.monotonic()
        with self._lock:
            busy = self._window_busy_s + sum(
                now - max(start, self._window_start) for start in self._running.values()
            )
            stats = {
                "elapsed_s": now - self._window_start,
                "submitted": self._window_submitted,
                "files": self._window_files,
                "busy_s": busy,
                "wait_s": self._window_wait_s,
                "workers": self._workers,
                "mean_repack_s": (
                    self._total_repack_s / self._total_files
                    if self._total_files
                    else None
                ),
            }
            self._window_start = now
            self._window_submitted = 0
            self._window_files = 0
            self._window_busy_s = 0.0
            self._window_wait_s = 0.0
        return stats

    def close(self) -> None:
        """Wait for running repacks and cancel those that have not started."""
        with self._lock:
            self._closed = True
            backlog = list(self._backlog)
            self._backlog.clear()
            threads = list(self._threads)
        for future, _ in backlog:
            future.cancel()
        for thread in threads:
            thread.join()
        self._pending.clear()
        self._charged = 0

    def _pop(self) -> tuple[object, BaseException | None]:
        future, item, charge = self._pending.popleft()
        if future.done():
            error = future.exception()
        else:
            start = time.monotonic()
            error = future.exception()
            with self._lock:
                self._window_wait_s += time.monotonic() - start
        self._charged -= charge
        return item, error

    def _start_ready(self) -> None:
        # Called with self._lock held.
        while not self._closed and self._backlog and len(self._running) < self._workers:
            future, repack = self._backlog.popleft()
            if not future.set_running_or_notify_cancel():
                continue
            self._running[future] = time.monotonic()
            self._started += 1
            thread = threading.Thread(
                target=self._run,
                args=(future, repack),
                name=f"cmip7repack_{self._started}",
            )
            self._threads.add(thread)
            thread.start()

    def _run(self, future: Future, repack: Callable[[], None]) -> None:
        error: BaseException | None = None
        try:
            repack()
        except BaseException as exc:
            error = exc
        # Counted, failed or not, before the caller can see the result.
        with self._lock:
            now = time.monotonic()
            start = self._running.pop(future)
            self._window_files += 1
            self._window_busy_s += now - max(start, self._window_start)
            self._total_files += 1
            self._total_repack_s += now - start
            self._threads.discard(threading.current_thread())
            self._start_ready()
        if error is None:
            future.set_result(None)
        else:
            future.set_exception(error)

    def _budget_for(self, path: Path) -> int:
        try:
            free = shutil.disk_usage(path.parent).free
        except OSError as exc:
            logger.warning(
                "Could not read the free space under %s (%s); capping the "
                "repack buffer at %d bytes",
                path.parent,
                exc,
                self._buffer_max_bytes,
            )
            return self._buffer_max_bytes
        return min(int(free * self._buffer_fraction), self._buffer_max_bytes)
