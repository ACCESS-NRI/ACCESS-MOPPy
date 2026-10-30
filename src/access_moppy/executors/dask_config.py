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

import json
import os
import platform
import resource
import sys
from datetime import datetime, timezone
from pathlib import Path

# Per-worker memory floors (GB) for the intensity tiers. These are the
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
    # Self-contained mappings (an internal calculation, or a formula pulling
    # every field from a bundled resource) read no input files at all, so a
    # worker only ever holds one small derived field. Without a tier of their
    # own they fall through to "heavy", which makes recommend_dask_config raise
    # MemoryError for any allocation under 16GB -- on variables measured at
    # 2-3GB for the whole job, and so blocks right-sizing them at all.
    "trivial": ("MOPPY_WORKER_GB_TRIVIAL", 2),
    # Light and medium variables whose workers each live through only a few
    # short source partitions (see _bounded_worker_lifetime). The tiers above
    # were measured on unpartitioned whole-run jobs; a worker that only ever
    # holds one short partition's graph needs far less. On piControl 101-1120
    # in 10-year partitions with dask_recreate_every 8 and QC plots off, the
    # highest worker peak of tas (3hr), tos, cVeg, cl and umo was 2.12GB.
    "bounded": ("MOPPY_WORKER_GB_BOUNDED", 3),
    "light": ("MOPPY_WORKER_GB_LIGHT", 12),
    "medium": ("MOPPY_WORKER_GB_MEDIUM", 14),
    "heavy": ("MOPPY_WORKER_GB_HEAVY", 16),
}

# The conditions the "bounded" floor was measured under: source partitions of
# at most this many years, each worker living through at most this many of
# them (the partition count, or dask_recreate_every if smaller).
_BOUNDED_MAX_PARTITION_YEARS = 10
_BOUNDED_MAX_PARTITIONS_PER_WORKER = 8

# Input whose time step is shorter than this fraction of the output's is being
# resampled (daily -> monthly, 3-hourly -> 6-hourly), which builds large
# time-axis intermediates and needs the heavy tier. The margin absorbs month
# lengths of 28-31 days.
_RESAMPLE_STEP_RATIO = 0.75

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


def _bounded_worker_lifetime(
    source_partition_years, n_partitions, recreate_every, qc_plots
):
    """Whether a job's workers stay within what the "bounded" floor covers.

    Worker RSS grows with every source partition a worker processes (by ~1-14MB
    per 10-year partition, up to ~3GB for a carbon pool), so the floor holds
    only while each worker sees a few short partitions: partitions of at most
    ``_BOUNDED_MAX_PARTITION_YEARS`` years, and at most
    ``_BOUNDED_MAX_PARTITIONS_PER_WORKER`` of them before the job ends or the
    cluster is recreated. QC plots take a worker to 4-9GB, so they rule it out.
    ``source_partition_years`` is ``None`` when the job is not partitioned.
    """
    if qc_plots or source_partition_years is None:
        return False
    if source_partition_years > _BOUNDED_MAX_PARTITION_YEARS:
        return False
    partitions_per_worker = min(n_partitions, recreate_every or n_partitions)
    return partitions_per_worker <= _BOUNDED_MAX_PARTITIONS_PER_WORKER


# Time units of an undecoded netCDF time axis, in steps per day.
_TIME_UNITS_PER_DAY = {
    "days": 1,
    "day": 1,
    "hours": 24,
    "hour": 24,
    "minutes": 24 * 60,
    "minute": 24 * 60,
    "seconds": 24 * 3600,
    "second": 24 * 3600,
}


def _time_step_days(ds):
    """Median time step (days) of an undecoded dataset, or ``None`` if unknown."""
    import numpy as np

    if "time" not in ds.variables or ds.sizes.get("time", 1) < 2:
        return None
    unit = str(ds["time"].attrs.get("units", "")).split(" ", 1)[0].lower()
    per_day = _TIME_UNITS_PER_DAY.get(unit)
    if per_day is None:
        return None
    steps = np.diff(np.asarray(ds["time"].values, dtype=float))
    return float(np.median(steps)) / per_day


def _output_step_days(variable):
    """Output time step (days) of a ``table.variable`` name, or ``None`` (fx)."""
    from access_moppy.utilities import parse_cmip6_table_frequency

    try:
        step = parse_cmip6_table_frequency(variable).total_seconds() / 86400
    except ValueError:
        return None
    return step or None


# Output above this many uncompressed MB per simulated year is written faster
# than two repacks rewrite it. cmip7repack manages ~15 MB/s, so a year of
# Omon.umo (~260 MB) takes ~17 s to repack and ~2 s to compute; Amon.cl
# (~50 MB) takes ~3 s and ~2 s, and keeps up.
_REPACK_HEAVY_MB_PER_YEAR = 150.0


def repack_heavy_output(variable, input_files, model_id, *, self_contained=False):
    """Whether ``cmip7repack`` rather than Dask will set ``variable``'s pace.

    True for sub-daily output, whose files take a second or two to compute and
    tens of seconds to repack, and for output of at least
    ``_REPACK_HEAVY_MB_PER_YEAR`` uncompressed MB per simulated year. The size
    is estimated from the first input file: its largest model variable per
    year, scaled down when the input is finer than the output. Any doubt
    answers False, which keeps the usual split
    (:func:`~access_moppy.executors.repack_queue.auto_cpu_split`).
    """
    output_step_days = _output_step_days(variable)
    if output_step_days is not None and output_step_days < 1:
        print(f"Repack-heavy output for {variable}: sub-daily")
        return True
    if self_contained or not input_files or output_step_days is None:
        return False
    try:
        import xarray as xr

        from access_moppy.utilities import load_model_mappings

        _, cmor_name = variable.split(".", 1)
        mapping = load_model_mappings(variable, model_id)
        model_vars = (mapping.get(cmor_name, {}) or {}).get("model_variables", []) or []

        with xr.open_dataset(input_files[0], engine="netcdf4", decode_cf=False) as ds0:
            steps_per_file = int(ds0.sizes.get("time", 1))
            # A file of one step (UM monthly output) holds one output step.
            input_step_days = _time_step_days(ds0) or (
                output_step_days if steps_per_file == 1 else None
            )
            var_mb = max(
                (
                    ds0[v].size * ds0[v].dtype.itemsize / 1e6
                    for v in model_vars
                    if v in ds0.variables
                ),
                default=0.0,
            )
    except Exception as exc:  # noqa: BLE001 - never let sizing crash the job
        print(
            f"Output-size estimate failed ({exc}); not repack-heavy.", file=sys.stderr
        )
        return False
    if not input_step_days or not var_mb:
        return False
    years_per_file = steps_per_file * input_step_days / 365.25
    mb_per_year = var_mb / years_per_file * min(1.0, input_step_days / output_step_days)
    heavy = mb_per_year >= _REPACK_HEAVY_MB_PER_YEAR
    print(
        f"Output estimate for {variable}: {mb_per_year:.0f} MB per year"
        + (" -> repack-heavy" if heavy else "")
    )
    return heavy


# Cross-experiment calibration history: the light/medium/heavy tiers below are
# a *guess* from probing one input file, and that guess can be wrong in either
# direction for variables whose peak memory is driven by computation (e.g.
# vertical interpolation to pressure levels) rather than by how much data they
# read. Once a variable has actually been run, its measured peak RSS
# (recorded via `record_measured_peak`, called from the job's teardown) is
# strictly better information than the file probe, and -- unlike the
# per-experiment TaskTracker db -- is worth sharing across experiments of the
# same model, since the cost is a property of the variable+model's
# processing, not of which experiment it's being run for.
#
# Entirely opt-in: unset MOPPY_WORKER_MEMORY_HISTORY (the default) reproduces
# today's behaviour exactly, with no file ever touched.
_HISTORY_ENV = "MOPPY_WORKER_MEMORY_HISTORY"
_HISTORY_SAFETY_FACTOR_ENV = "MOPPY_WORKER_MEMORY_SAFETY_FACTOR"
_HISTORY_SAFETY_FACTOR_DEFAULT = 1.5
_HISTORY_MIN_FLOOR_GB = 2.0

# How much larger the current run's file count may be than what a history
# entry was measured on before that entry is still trusted (see
# `_load_measured_floor_gb`). Small enough to absorb e.g. a leap-year file
# count wobble between otherwise-identical runs, not so large that a
# moderately bigger run is silently waved through.
_HISTORY_SCALE_TOLERANCE = 1.05


def _history_path():
    raw = os.environ.get(_HISTORY_ENV)
    return Path(raw) if raw else None


def _history_safety_factor():
    try:
        return max(
            1.0,
            float(
                os.environ.get(
                    _HISTORY_SAFETY_FACTOR_ENV, _HISTORY_SAFETY_FACTOR_DEFAULT
                )
            ),
        )
    except (TypeError, ValueError):
        return _HISTORY_SAFETY_FACTOR_DEFAULT


def _load_measured_floor_gb(variable, model_id, n_files_per_partition):
    """Per-worker memory floor (GB) derived from a previously measured peak,
    or ``None`` if no history is configured, none is recorded yet for this
    ``(model_id, variable)``, or the recorded measurement doesn't cover a run
    this large (see below).

    Gated on scale: a history entry records how many input files went into a
    single graph when it was measured (``n_files_per_partition``), and is only
    trusted if this run's ``n_files_per_partition`` is no more than
    ``_HISTORY_SCALE_TOLERANCE`` times that. Chunked writing keeps *array*
    memory bounded regardless of dataset length, but bookkeeping overhead (the
    file list, coordinate metadata, the Dask task graph itself) still grows
    with file count -- so a peak measured on a short test run (say, a "one
    year" sanity-check config) understates what a multi-century production run
    of the same variable will need. Without this gate, one small run could
    silently poison the shared cache for every larger run of that variable
    afterwards.

    The scale that matters is one ``source_partition_years`` partition, not the
    whole run: each partition builds and tears down its own graph, so a
    1000-year run partitioned into decades has exactly the working set of a
    173-year run partitioned the same way, and should reuse its measurement.
    Gating on the total instead would reject it for being "much larger". With
    partitioning off there is a single partition holding every input file, so
    this is the whole file list and the gate behaves as it always did.

    Entries written before this key existed recorded a whole-run total under a
    different name; they are missing ``n_files_per_partition`` and so raise
    below and are ignored, rather than being silently compared against a
    per-partition count they cannot be reconciled with.

    Best-effort by design: any missing file, unreadable JSON, or malformed
    record is treated the same as "no usable history" so a corrupt or
    in-progress-write history file can never break a job, only fall back to
    the static file-probe heuristic.
    """
    path = _history_path()
    if path is None or not path.exists():
        return None
    try:
        with open(path) as f:
            history = json.load(f)
        record = history[model_id][variable]
        peak_mb = float(record["peak_rss_mb"])
        recorded_n_files = int(record["n_files_per_partition"])
        if n_files_per_partition > recorded_n_files * _HISTORY_SCALE_TOLERANCE:
            return None
    except Exception:
        return None
    return max(_HISTORY_MIN_FLOOR_GB, (peak_mb / 1024.0) * _history_safety_factor())


def record_measured_peak(variable, model_id, worker_peaks_mb, n_files_per_partition):
    """Persist the peak worker RSS observed this run for ``(model_id,
    variable)`` into the shared calibration history at
    ``MOPPY_WORKER_MEMORY_HISTORY``, so later runs of this variable can size
    workers from measured reality via :func:`_load_measured_floor_gb` instead
    of the static file-probe heuristic.

    A no-op if the env var isn't set or ``worker_peaks_mb`` is empty.
    ``n_files_per_partition`` records the scale this measurement covers -- the
    files in one source partition, which is what a single graph holds (see
    :func:`_load_measured_floor_gb`) -- and gates how the record is updated:

    * A run at least as large-scale as anything recorded so far (the common
      case, since jobs tend to grow over an experiment's history rather than
      shrink) becomes the new reference -- taking the larger of the two peaks
      if it happens to match the previous largest scale exactly, otherwise
      replacing it outright.
    * A *smaller*-scale run than what's on record cannot tell us anything new
      about whether larger runs need more, and must never be allowed to
      lower an established floor -- so it only bumps the observation count,
      leaving ``peak_rss_mb``/``n_files`` untouched.

    Concurrent sibling PBS jobs updating the same file are serialised with an
    flock where available; on platforms without ``fcntl`` (or under any other
    failure) this degrades to best-effort and never raises -- this is
    calibration data, and must never be allowed to fail the CMORisation job
    that measured it.
    """
    path = _history_path()
    if path is None or not worker_peaks_mb:
        return
    peak_mb = max(worker_peaks_mb.values())
    n_files_per_partition = int(n_files_per_partition or 0)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        # 'a+' creates the file if missing and never truncates on open (unlike
        # 'w+'), so a sibling job's already-written history survives until we
        # explicitly read, merge and rewrite it under the lock below.
        with open(path, "a+") as f:
            try:
                import fcntl

                fcntl.flock(f.fileno(), fcntl.LOCK_EX)
            except (ImportError, OSError):
                pass

            f.seek(0)
            raw = f.read()
            try:
                history = json.loads(raw) if raw.strip() else {}
            except json.JSONDecodeError:
                history = {}

            model_history = history.setdefault(model_id, {})
            existing = model_history.get(variable, {})
            existing_n_files = int(existing.get("n_files_per_partition", 0))
            existing_peak_mb = float(existing.get("peak_rss_mb", 0.0))
            n_observations = int(existing.get("n_observations", 0)) + 1

            if n_files_per_partition >= existing_n_files:
                new_peak_mb = (
                    max(peak_mb, existing_peak_mb)
                    if n_files_per_partition == existing_n_files
                    else peak_mb
                )
                model_history[variable] = {
                    "peak_rss_mb": new_peak_mb,
                    "n_files_per_partition": n_files_per_partition,
                    "n_observations": n_observations,
                    "updated": datetime.now(timezone.utc).isoformat(),
                }
            else:
                existing["n_observations"] = n_observations
                existing["updated"] = datetime.now(timezone.utc).isoformat()
                model_history[variable] = existing

            f.seek(0)
            f.truncate()
            json.dump(history, f, indent=2)

            try:
                import fcntl

                fcntl.flock(f.fileno(), fcntl.LOCK_UN)
            except (ImportError, OSError):
                pass
    except Exception as exc:  # noqa: BLE001 - calibration data must never fail the job
        print(
            f"Could not record worker-memory history for {variable}: {exc}",
            file=sys.stderr,
        )


def _estimate_worker_memory_gb(
    variable,
    input_files,
    model_id,
    *,
    self_contained=False,
    n_files_per_partition=None,
    bounded_lifetime=False,
):
    """Estimate the per-worker memory floor (GB) for ``variable``.

    If ``MOPPY_WORKER_MEMORY_HISTORY`` is set and a measured peak already
    exists for this ``(model_id, variable)`` from a run that processed at
    least as many input files as this one (see :func:`_load_measured_floor_gb`),
    that measurement is used directly -- it reflects what the variable's
    processing actually needed, which the static probe below can only guess
    at (e.g. it has no way to see that a pressure-level variable's peak comes
    from vertical interpolation rather than from how much it reads).
    Self-contained mappings read nothing, so no probe is possible and none is
    needed: they get the trivial tier rather than the heavy fallback below,
    which exists for a probe that failed and must not under-size.
    Otherwise, probes the first input file for (a) its time step -- input
    finer than the output (daily -> monthly) is resampled, with large
    time-axis intermediates, and gets the heavy tier; when the step cannot be
    read, any file with several time steps is treated as such -- and (b) the
    total size of the variable's model variables, which captures tiled /
    multi-pool variables (medium rather than light). A light or medium variable
    gets the smaller "bounded" floor instead when ``bounded_lifetime`` says its
    workers only ever see a few short partitions
    (:func:`_bounded_worker_lifetime`). Falls back to the heavy tier if
    anything about the probe is uncertain, so a bad estimate never under-sizes
    memory.
    """
    if n_files_per_partition is None:
        n_files_per_partition = len(input_files)
    measured = _load_measured_floor_gb(variable, model_id, n_files_per_partition)
    if measured is not None:
        return measured
    if self_contained:
        return _floor_gb("trivial")
    if not input_files:
        return _floor_gb("heavy")
    try:
        import xarray as xr

        from access_moppy.utilities import load_model_mappings

        _, cmor_name = variable.split(".", 1)
        mapping = load_model_mappings(variable, model_id)
        model_vars = (mapping.get(cmor_name, {}) or {}).get("model_variables", []) or []

        with xr.open_dataset(input_files[0], engine="netcdf4", decode_cf=False) as ds0:
            steps_per_file = int(ds0.sizes.get("time", 1))
            input_step_days = _time_step_days(ds0)
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

        # Input finer than the output (daily -> monthly): resampling, fat
        # workers. A monthly ocean file holding a year of months is not.
        output_step_days = _output_step_days(variable)
        if input_step_days is not None and output_step_days is not None:
            resampled = input_step_days < output_step_days * _RESAMPLE_STEP_RATIO
        else:
            resampled = steps_per_file > 1
        if resampled:
            return _floor_gb("heavy")
        if bounded_lifetime:
            return _floor_gb("bounded")
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
    self_contained=False,
    n_files_per_partition=None,
    source_partition_years=None,
    n_partitions=1,
    recreate_every=None,
    qc_plots=False,
):
    """Return ``dask.distributed.Client`` kwargs sized to ``variable``.

    Parameters mirror what the generated batch script already has to hand. The
    result is a dict with ``n_workers``, ``threads_per_worker`` and
    ``memory_limit`` (per worker), for use as ``Client(processes=True, **cfg)``.
    The write settings must match the CMORiser so larger prefetched write
    windows reserve enough worker memory. Unchunked writes use the conservative
    pre-streaming floor.

    ``self_contained`` marks a mapping that reads no input files.
    ``n_files_per_partition`` is how many input files one source partition
    holds -- what a single Dask graph is built from, and the scale the
    worker-memory history is keyed on; it defaults to the whole of
    ``input_files``, which is what one partition holds when source
    partitioning is off.

    ``source_partition_years`` (``None`` when the job is not partitioned),
    ``n_partitions``, ``recreate_every`` (the resolved ``dask_recreate_every``)
    and ``qc_plots`` decide whether the smaller "bounded" floor applies
    (:func:`_bounded_worker_lifetime`).
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

    floor_gb = _estimate_worker_memory_gb(
        variable,
        input_files,
        model_id,
        self_contained=self_contained,
        n_files_per_partition=n_files_per_partition,
        bounded_lifetime=_bounded_worker_lifetime(
            source_partition_years, n_partitions, recreate_every, qc_plots
        ),
    )
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


def _peak_rss_mb():
    """Return this process's peak resident-set size in MB.

    ``ru_maxrss`` is a high-water mark for the process's whole lifetime (unlike
    Dask's own ``dask_worker.monitor``, which only retains a short rolling
    sample window), so it survives being read at the very end of the job. It is
    KB on Linux and bytes on macOS.
    """
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return peak / (1024 * 1024) if platform.system() == "Darwin" else peak / 1024


def peak_worker_memory_mb(client):
    """Return each live worker's peak RSS in MB, keyed by worker address.

    Intended to be called once, just before ``client.close()``, so the
    ``recommend_dask_config`` per-worker memory floors (:data:`_FLOOR_ENV`) can
    be checked against what a variable actually used -- the sizing function
    only ever estimates from a probed file, it never measures. Returns ``{}``
    if the client has no live workers to query (e.g. Dask itself failed to
    start), rather than raising, since this is best-effort diagnostics run
    during teardown.
    """
    try:
        return client.run(_peak_rss_mb)
    except Exception:  # noqa: BLE001 - best-effort teardown diagnostics
        return {}
