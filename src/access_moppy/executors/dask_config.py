"""Adaptive Dask client sizing for batch CMORisation.

Each variable is CMORised in its own PBS job. The job's cost is dominated by
reading thousands of netCDF4 files, and the netCDF4/HDF5 library serialises
every read through a single global lock, so a threaded client (processes=False)
collapses to ~1 core. Multi-process workers each hold their own HDF5 lock, so
reads parallelise across workers.

Worker *count* is a memory trade-off, and the right count depends on the
variable:

* Light single-field variables (e.g. ``Amon.tas``) read a fraction of a MB per
  file, so the allocation can be split into many small workers -> maximum read
  parallelism.
* Carbon-pool variables (``cVeg``/``cSoil``/``nbp``) read several tiled model
  variables per file and need a moderate per-worker budget.
* Daily-input variables that resample to monthly (``tasmax``/``tasmin``) push
  large time-axis intermediates plus unmanaged HDF5 buffers through a worker and
  need a generous per-worker budget, or a worker exceeds its limit and is killed.

Rather than hard-code a worker count, :func:`recommend_dask_config` probes one
input file for the variable's actual read footprint and picks the largest worker
count whose per-worker budget covers that footprint.
"""

from __future__ import annotations

import os
import sys

# Per-worker memory floors (GB) for the three intensity tiers. These are the
# minimum RAM a single worker needs to process a variable of that class without
# being killed -- a property of the *workload*, not the node -- so they are
# absolute GB, not fractions of the allocation. Each can be overridden per job
# via an environment variable set in the batch config's worker_init, so nothing
# is locked in code.
#
# Calibrated from full-scale (5352-file / ~446-year) runs once the pipeline was
# made fully streaming (lazy missing-value masks + chunked reads/writes). At that
# point measured per-worker peaks were ~12.5GB (daily), ~10GB (carbon pools) and
# ~6GB (light single-field), all with zero memory-pressure warnings, so the
# floors sit ~1.3-1.5x above the observed peaks. Before streaming, daily
# variables materialised the whole ~18GB series into one worker and needed a
# 28GB floor; that is no longer the case.
_FLOOR_ENV = {
    "light": ("MOPPY_WORKER_GB_LIGHT", 12),
    "medium": ("MOPPY_WORKER_GB_MEDIUM", 14),
    "heavy": ("MOPPY_WORKER_GB_HEAVY", 16),
}

# A variable reading at least this many MB of model-variable data per file is
# treated as at least "medium" intensity (carbon pools read ~8-10 MB/file).
# Overridable for unusual mappings via MOPPY_MEDIUM_MB_PER_FILE.
_MEDIUM_MB_PER_FILE_DEFAULT = 4.0

# The calibrated worker floors already include this default write window. Larger
# windows retain additional completed slices and therefore need extra headroom.
_DEFAULT_MAX_CHUNK_SIZE_MB = 128.0
_DEFAULT_WRITE_PREFETCH = 4
_UNCHUNKED_WORKER_FLOOR_GB = 28.0


def _floor_gb(tier):
    """Per-worker memory floor (GB) for a tier, honouring an env override."""
    env_name, default = _FLOOR_ENV[tier]
    try:
        return max(1, int(os.environ.get(env_name, default)))
    except (TypeError, ValueError):
        return default


def _estimate_worker_memory_gb(variable, input_files, model_id):
    """Estimate the per-worker memory floor (GB) for ``variable``.

    Probes the first input file for (a) the number of time steps per file --
    sub-monthly input implies a resample-to-monthly with large time-axis
    intermediates -- and (b) the total size of the variable's model variables,
    which captures tiled / multi-pool variables. Falls back to the heavy tier if
    anything about the probe is uncertain, so a bad estimate never under-sizes
    memory.
    """
    if not input_files:
        return _floor_gb("heavy")
    try:
        import xarray as xr

        from access_moppy.utilities import load_model_mappings

        _, cmor_name = variable.split(".", 1)
        mapping = load_model_mappings(variable, model_id)
        model_vars = (mapping.get(cmor_name, {}) or {}).get("model_variables", []) or []

        with xr.open_dataset(input_files[0], decode_cf=False) as ds0:
            steps_per_file = int(ds0.sizes.get("time", 1))
            present = [v for v in model_vars if v in ds0.variables]
            per_file_mb = (
                sum(ds0[v].size * ds0[v].dtype.itemsize for v in present) / 1e6
            )

        try:
            medium_mb = float(
                os.environ.get("MOPPY_MEDIUM_MB_PER_FILE", _MEDIUM_MB_PER_FILE_DEFAULT)
            )
        except (TypeError, ValueError):
            medium_mb = _MEDIUM_MB_PER_FILE_DEFAULT

        # Sub-monthly input -> monthly resampling: fat workers.
        if steps_per_file > 1:
            return _floor_gb("heavy")
        # Multiple tiled model variables (carbon pools): moderate workers.
        if per_file_mb >= medium_mb:
            return _floor_gb("medium")
        return _floor_gb("light")
    except Exception as exc:  # noqa: BLE001 - never let sizing crash the job
        print(
            f"Worker-memory estimate failed ({exc}); assuming heavy tier.",
            file=sys.stderr,
        )
        return _floor_gb("heavy")


def recommend_dask_config(
    variable,
    input_files,
    model_id,
    n_cpus=None,
    mem_gb=None,
    *,
    enable_chunking=True,
    max_chunk_size_mb=_DEFAULT_MAX_CHUNK_SIZE_MB,
    write_prefetch=_DEFAULT_WRITE_PREFETCH,
):
    """Return ``dask.distributed.Client`` kwargs sized to ``variable``.

    Parameters mirror what the generated batch script already has to hand. The
    result is a dict with ``n_workers``, ``threads_per_worker`` and
    ``memory_limit`` (per worker), for use as ``Client(processes=True, **cfg)``.
    The write settings must match the CMORiser so larger prefetched write
    windows reserve enough worker memory. Unchunked writes use the conservative
    pre-streaming floor.
    """
    import psutil

    if n_cpus is None:
        n_cpus = int(os.environ.get("PBS_NCPUS", psutil.cpu_count() or 1))
    if mem_gb is None:
        mem_gb = int(os.environ.get("PBS_MEM_GB", "16"))

    if max_chunk_size_mb <= 0:
        raise ValueError("max_chunk_size_mb must be positive")
    if not isinstance(write_prefetch, int) or isinstance(write_prefetch, bool):
        raise TypeError("write_prefetch must be an integer")
    if write_prefetch < 1:
        raise ValueError("write_prefetch must be at least 1")

    system_memory_gb = psutil.virtual_memory().total / (1024**3)
    effective_memory = min(mem_gb, system_memory_gb * 0.9)

    floor_gb = _estimate_worker_memory_gb(variable, input_files, model_id)
    if enable_chunking:
        default_write_window_mb = _DEFAULT_MAX_CHUNK_SIZE_MB * _DEFAULT_WRITE_PREFETCH
        write_window_mb = max_chunk_size_mb * write_prefetch
        extra_write_memory_gb = max(
            0.0, (write_window_mb - default_write_window_mb) / 1024
        )
        required_worker_gb = floor_gb + extra_write_memory_gb
    else:
        required_worker_gb = max(floor_gb, _UNCHUNKED_WORKER_FLOOR_GB)

    if effective_memory < required_worker_gb:
        mode = "chunked" if enable_chunking else "unchunked"
        adjustment = (
            "Increase PBS memory or reduce max_chunk_size_mb/write_prefetch."
            if enable_chunking
            else "Increase PBS memory or enable chunking."
        )
        raise MemoryError(
            f"Dask allocation for {variable} provides {effective_memory:.2f}GB "
            f"usable memory but {mode} processing requires at least "
            f"{required_worker_gb:.2f}GB per worker. {adjustment}"
        )

    # Largest worker count whose per-worker share still meets the floor, capped
    # by CPU count. All parallelism comes from *processes*: netCDF4/HDF5 reads
    # serialise on a global lock (and the GIL), so extra threads per worker add
    # no read throughput while doubling the in-flight data resident per worker --
    # the opposite of what the memory-limited heavy tier needs. Hence one thread
    # per worker; memory-limited variables simply use fewer cores.
    n_workers = max(1, min(n_cpus, int(effective_memory // required_worker_gb)))
    threads_per_worker = 1
    per_worker_gb = effective_memory / n_workers

    idle_cores = max(0, n_cpus - n_workers)
    print(
        f"Dask sizing for {variable}: {n_cpus} CPUs, {effective_memory:.0f}GB usable, "
        f"per-worker requirement {required_worker_gb:.2f}GB -> {n_workers} workers "
        f"x {threads_per_worker} thread, {per_worker_gb:.2f}GB each"
        + (f" ({idle_cores} cores idle: memory-limited)" if idle_cores else "")
    )

    return {
        "n_workers": n_workers,
        "threads_per_worker": threads_per_worker,
        "memory_limit": f"{per_worker_gb:.2f}GB",
    }
