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
import os
import shutil
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
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


class RepackQueue:
    """Background repacks of finished files, returned in submission order.

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

        self._pool = ThreadPoolExecutor(
            max_workers=workers, thread_name_prefix="cmip7repack"
        )
        self._buffer_fraction = buffer_fraction
        self._buffer_max_bytes = buffer_max_bytes
        self.budget_bytes: int | None = None
        self._pending: deque[tuple[Future, object, int]] = deque()
        self._charged = 0
        self._last_charge = 0

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
            future = self._pool.submit(repack)

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

    def close(self) -> None:
        """Wait for running repacks and cancel those that have not started."""
        self._pool.shutdown(wait=True, cancel_futures=True)
        self._pending.clear()
        self._charged = 0

    def _pop(self) -> tuple[object, BaseException | None]:
        future, item, charge = self._pending.popleft()
        error = future.exception()
        self._charged -= charge
        return item, error

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
