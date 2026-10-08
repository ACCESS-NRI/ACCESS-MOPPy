"""Keep one netCDF file open in every Dask worker for the worker's lifetime.

netCDF-C keeps every open file in a process-global table that has no lock, and
frees the whole table when its open-file count reaches zero
(``libdispatch/nclistmgr.c``: ``del_from_NCList`` calls ``free_NCList`` when
``numfiles == 0``; unchanged on netcdf-c ``main``). A Dask worker touches
netCDF-C from two threads: the compute thread opens files under xarray's
backend lock, while ``CachingFileManager.__del__`` closes them from whichever
thread drops the last reference -- without that lock. If such a close takes the
worker's count to zero while the compute thread is inside ``nc_open``, the
table is freed under it and the new ncid no longer resolves:
``RuntimeError: NetCDF: Not a valid ID``, raised from ``_get_format`` or
``_get_vars``.

Batch workers reach zero often: each source partition opens a handful of
files, and they are closed as soon as the partition's tasks are released. Two
threads opening and closing with nothing else held crash within seconds in an
isolated netCDF4 test; holding one extra file removes the failures entirely.

This is a workaround for an unsynchronised close, not a fix for it. It closes
the path where the count legitimately reaches zero; a lost update on the
non-atomic counter could still, in principle, drive it there.
"""

from __future__ import annotations

import os

from distributed import WorkerPlugin

ANCHOR_FILENAME = "moppy_nc_anchor.nc"


class NetCDFAnchorPlugin(WorkerPlugin):
    """Hold a tiny read-only netCDF file open so netCDF-C never frees its table.

    Registered with ``client.register_plugin``; the scheduler re-applies it to
    any worker the nanny restarts, so the anchor survives worker restarts.
    """

    name = "moppy-nc-anchor"

    def __init__(self) -> None:
        self._anchor = None

    def setup(self, worker) -> None:
        import netCDF4

        path = os.path.join(worker.local_directory, ANCHOR_FILENAME)
        if not os.path.exists(path):
            with netCDF4.Dataset(path, "w", format="NETCDF4") as ds:
                ds.createDimension("x", 1)
        self._anchor = netCDF4.Dataset(path, "r")

    def teardown(self, worker) -> None:
        if self._anchor is not None and self._anchor.isopen():
            self._anchor.close()
        self._anchor = None
