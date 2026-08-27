import logging
import math
import os
import random
import shutil
import subprocess
import sys
import time
import uuid
import warnings
from collections import deque
from collections.abc import Iterable, Mapping
from contextlib import contextmanager
from datetime import datetime
from functools import partial
from itertools import product
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Union

import cftime
import dask.array as da
import netCDF4 as nc
import numpy as np
import pandas as pd
import psutil
import xarray as xr
from cftime import date2num
from dask.core import flatten
from distributed import get_client

from access_moppy.defaults import DEFAULT_CHUNK_YEARS
from access_moppy.derivations import TIME_REDUCTION_OPERATIONS
from access_moppy.qc import ValidationResult, validate_cmip7_output
from access_moppy.qc.plots import generate_qc_plots
from access_moppy.utilities import (
    FrequencyMismatchError,
    IncompatibleFrequencyError,
    ResamplingRequiredWarning,
    calculate_latitude_bounds,
    calculate_longitude_bounds,
    calculate_time_bounds,
    normalize_cf_time_units,
    parse_cmip6_table_frequency,
    type_mapping,
    validate_and_resample_if_needed,
    validate_cmip6_frequency_compatibility,
)

logger = logging.getLogger(__name__)


@contextmanager
def _publication_slot():
    """Bound concurrent staged-file publications across independent jobs."""
    lock_root_value = os.environ.get("MOPPY_PUBLICATION_LOCK_DIR")
    max_publications = int(os.environ.get("MOPPY_MAX_CONCURRENT_PUBLICATIONS", "0"))
    if not lock_root_value or max_publications <= 0:
        yield
        return

    jitter = float(os.environ.get("MOPPY_PUBLICATION_JITTER_SECONDS", "0"))
    if jitter > 0:
        time.sleep(random.uniform(0, jitter))

    lock_root = Path(lock_root_value)
    lock_root.mkdir(parents=True, exist_ok=True)
    stale_after = float(os.environ.get("MOPPY_PUBLICATION_STALE_SECONDS", "86400"))
    acquired: Path | None = None
    while acquired is None:
        for slot_number in range(max_publications):
            slot = lock_root / f"slot-{slot_number:03d}"
            try:
                slot.mkdir()
                acquired = slot
                break
            except FileExistsError:
                try:
                    if time.time() - slot.stat().st_mtime <= stale_after:
                        continue
                    stale = lock_root / f".stale-{uuid.uuid4().hex}"
                    slot.rename(stale)
                    shutil.rmtree(stale, ignore_errors=True)
                except FileNotFoundError:
                    pass
        if acquired is None:
            time.sleep(random.uniform(1, 3))

    try:
        yield
    finally:
        try:
            acquired.rmdir()
        except FileNotFoundError:
            pass


# Ordered list mapping pd.Timedelta → canonical CMIP frequency key.
# Entries are checked for equality in order; last entry acts as default.
_TIMEDELTA_TO_FREQ: list[tuple[pd.Timedelta, str]] = [
    (pd.Timedelta(0), "fx"),
    (pd.Timedelta(minutes=30), "1hr"),
    (pd.Timedelta(hours=1), "1hr"),
    (pd.Timedelta(hours=3), "3hr"),
    (pd.Timedelta(hours=6), "6hr"),
    (pd.Timedelta(days=1), "day"),
    (pd.Timedelta(days=30), "mon"),
    (pd.Timedelta(days=365), "yr"),
]


def _canonical_frequency(compound_name: str) -> str:
    """Return the canonical CMIP frequency key for *compound_name*.

    Resolves to one of ``'fx'``, ``'1hr'``, ``'3hr'``, ``'6hr'``,
    ``'day'``, ``'mon'``, or ``'yr'``.  Falls back to ``'mon'`` for
    unrecognized table identifiers.
    """
    try:
        td = parse_cmip6_table_frequency(compound_name)
        for threshold, name in _TIMEDELTA_TO_FREQ:
            if td == threshold:
                return name
    except ValueError:
        pass
    return "mon"


def _parallel_open_is_safe() -> bool:
    """Whether ``xr.open_mfdataset(..., parallel=True)`` is safe to use.

    ``parallel=True`` dispatches each file's ``open_dataset``/``preprocess``
    as a ``dask.delayed`` task. With no active ``distributed`` client, those
    tasks run on dask's default local *threaded* scheduler, and netCDF4/HDF5
    is not safe to call concurrently from multiple threads in one process --
    this has been observed to silently misread data or crash (segfault /
    "NetCDF: Not a valid ID"). It's only safe when every worker is a
    separate OS process running a single thread, which is what
    ``Client(processes=True, ...)`` sized by ``recommend_dask_config()``
    always produces in production batch runs.
    """
    try:
        client = get_client()
    except ValueError:
        return False

    workers = client.scheduler_info().get("workers", {})
    if not workers:
        return False
    if any(info.get("nthreads", 1) != 1 for info in workers.values()):
        return False

    pids = client.run(os.getpid)
    return len(set(pids.values())) == len(pids)


def _drop_duplicate_non_time_indexes(ds: xr.Dataset) -> xr.Dataset:
    """Keep duplicate coordinates as data without using them for alignment."""
    duplicate_indexes = [
        name for name, index in ds.indexes.items() if name != "time" and index.has_duplicates
    ]
    if duplicate_indexes:
        ds = ds.drop_indexes(duplicate_indexes)
    return ds


def _preprocess_open_mfdataset(
    ds: xr.Dataset, *, required_vars: frozenset[str]
) -> xr.Dataset:
    """Preprocess one file for ``xr.open_mfdataset`` without capturing ``self``.

    ``parallel=True`` sends this callable to Dask workers, so it must stay
    picklable even when the surrounding ``CMORiser`` instance holds objects
    like SQLite-backed trackers elsewhere in the job.
    """
    if required_vars:
        # Requested coordinates are retained alongside the data variables
        # (see the xarray-input branch in ``CMORiser.load_dataset`` for why).
        ds = ds[list(required_vars & (set(ds.data_vars) | set(ds.coords)))]

    # Canonicalize UM auxiliary time dimensions (time_0/time_1) to a single
    # "time" axis when the selected variables use exactly one such axis. Keep
    # that primary axis and drop the unused one.
    selected_data_vars = list(ds.data_vars)
    used_time_dims = {
        dim
        for var in selected_data_vars
        for dim in ds[var].dims
        if dim.startswith("time")
    }
    primary_time_dim = "time" if "time" in used_time_dims else None
    if primary_time_dim is None and len(used_time_dims) == 1:
        primary_time_dim = next(iter(used_time_dims))

    if primary_time_dim and primary_time_dim != "time":
        ds = ds.rename({primary_time_dim: "time"})
        used_time_dims.discard(primary_time_dim)
        used_time_dims.add("time")

    aux_time_coords = [
        c for c in ("time_0", "time_1") if c in ds.coords and c not in used_time_dims
    ]
    if aux_time_coords:
        ds = ds.drop_vars(aux_time_coords)
    return _drop_duplicate_non_time_indexes(ds)


class DatasetChunker:
    """
    Bound Dask task sizes used to compute and manually write dataset slices.

    These chunks control how much data MOPPy pulls into memory per write. They
    do not define NetCDF/HDF5 storage chunks: ``_write_single`` deliberately
    does not pass ``chunksizes`` to ``netCDF4.Dataset.createVariable``. For
    CMIP7 output, ``cmip7repack`` is authoritative for final storage chunking,
    compression filters, and HDF5 metadata layout.

    Rules:
    - Time coordinates: one Dask chunk
    - Time bounds: one Dask chunk
    - Data variables: target at least 4MB without exceeding 128MB per task
    - Spatial dimensions: split when a full spatial slab exceeds 128MB
    """

    def __init__(
        self, target_chunk_size_mb: float = 4.0, max_chunk_size_mb: float = 128.0
    ):
        """
        Initialize the DatasetChunker.

        Args:
            target_chunk_size_mb: Minimum target Dask task size in megabytes
            max_chunk_size_mb: Hard maximum Dask task size in megabytes
        """
        if target_chunk_size_mb <= 0:
            raise ValueError("target_chunk_size_mb must be positive")
        if max_chunk_size_mb < target_chunk_size_mb:
            raise ValueError(
                "max_chunk_size_mb must be greater than or equal to "
                "target_chunk_size_mb"
            )

        self.target_chunk_size_mb = target_chunk_size_mb
        self.target_chunk_size_bytes = target_chunk_size_mb * 1024 * 1024
        self.max_chunk_size_mb = max_chunk_size_mb
        self.max_chunk_size_bytes = max_chunk_size_mb * 1024 * 1024

    def calculate_chunk_size_for_variable(self, var: xr.DataArray) -> Dict[str, int]:
        """
        Calculate Dask/write chunks within the target and memory bound.

        Args:
            var: xarray DataArray

        Returns:
            Dictionary of dimension names to chunk sizes
        """
        chunks = {dim: var.sizes[dim] for dim in var.dims}

        # Calculate total elements per chunk needed for minimum target size
        element_size = var.dtype.itemsize
        min_target_elements = self.target_chunk_size_bytes // element_size
        max_target_elements = self.max_chunk_size_bytes // element_size

        # For time-dependent variables, start with time dimension
        if "time" in var.dims:
            time_size = var.sizes["time"]

            # Calculate elements in other dimensions (spatial elements per time step)
            other_elements = 1
            for dim in var.dims:
                if dim != "time":
                    other_elements *= var.sizes[dim]

            if other_elements > 0:
                # How many time steps fit under the max bound -- at least 1,
                # even if a single step alone already exceeds it (the
                # spatial/vertical splitting loop below handles that case).
                max_time_steps = max(1, max_target_elements // other_elements)

                if other_elements >= min_target_elements:
                    # A single time step already meets the minimum target.
                    # Previously this always fell through to 1 step/task
                    # even when there was headroom left under the max bound
                    # (e.g. a few-MB/step 3D field, well under 128MB),
                    # inflating the write-task count for no reason. Batch
                    # as many steps together as fit under the max instead.
                    time_chunks = max_time_steps
                else:
                    # Multiple steps are needed to reach the minimum
                    # target; grow toward it, capped at the max bound.
                    min_time_steps = max(
                        1,
                        (min_target_elements + other_elements - 1) // other_elements,
                    )  # Ceiling division
                    time_chunks = min(min_time_steps, max_time_steps)

                # Don't exceed available time steps
                time_chunks = min(time_size, time_chunks)
            else:
                time_chunks = time_size

            chunks["time"] = time_chunks

        def chunk_bytes() -> int:
            return element_size * math.prod(chunks.values())

        splittable_dims = [dim for dim in var.dims if dim != "time"]
        while chunk_bytes() > self.max_chunk_size_bytes:
            splittable_dims = [dim for dim in splittable_dims if chunks[dim] > 1]
            if not splittable_dims:
                break
            largest_dim = max(splittable_dims, key=lambda dim: chunks[dim])
            chunks[largest_dim] = (chunks[largest_dim] + 1) // 2

        return chunks

    def rechunk_dataset(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Rechunk the dataset for bounded computation and manual writing.

        This does not set the storage chunk shape of the output NetCDF file.

        Args:
            ds: Input xarray Dataset

        Returns:
            Rechunked xarray Dataset
        """
        try:
            ds_chunks = ds.chunks
        except AttributeError:
            logger.debug("Dataset is not chunked, skipping rechunking")
            return ds
        except ValueError as err:
            if "inconsistent chunks along dimension" not in str(err):
                raise

            logger.debug(
                "Dataset has inconsistent chunking across variables; "
                "calling unify_chunks() before rechunking"
            )
            ds = ds.unify_chunks()
            ds_chunks = ds.chunks

        if not any(ds_chunks.values() if ds_chunks else []):
            logger.debug("Dataset is not chunked, skipping rechunking")
            return ds

        logger.debug(
            "Applying dataset rechunking with rules: "
            "time coordinates=single chunk, "
            "time bounds=single chunk, "
            "data variables=%s-%sMB chunks",
            self.target_chunk_size_mb,
            self.max_chunk_size_mb,
        )

        rechunked_coords = {}
        rechunked_data_vars = {}

        for var_name in ds.variables:
            var = ds[var_name]

            # Apply chunking rules based on variable type
            if var_name.endswith("_bnds") or var_name.endswith("_bounds"):
                # Time bounds: single chunk for all dimensions
                chunks = {dim: var.sizes[dim] for dim in var.dims}
                logger.debug("  %s: time bounds -> single chunk", var_name)

            elif (
                var_name
                in [
                    "time",
                    "lat",
                    "lon",
                    "latitude",
                    "longitude",
                    "x",
                    "y",
                    "height",
                    "lev",
                ]
                or var.dims == ()
            ):
                # Coordinate variables and scalars: single chunk
                chunks = {dim: var.sizes[dim] for dim in var.dims}
                if var.dims:
                    logger.debug("  %s: coordinate -> single chunk", var_name)

            else:
                # Data variables: calculate 4MB chunks
                chunks = self.calculate_chunk_size_for_variable(var)
                chunk_info = ", ".join(
                    [f"{dim}:{size}" for dim, size in chunks.items()]
                )
                logger.debug("  %s: data variable -> %s", var_name, chunk_info)

            try:
                rechunked_var = var.chunk(chunks)
            except Exception as e:
                logger.warning("Could not rechunk variable '%s': %s", var_name, e)
                rechunked_var = var

            if var_name in ds.coords:
                rechunked_coords[var_name] = rechunked_var
            else:
                rechunked_data_vars[var_name] = rechunked_var

        # Use assign_coords + assign to preserve all dataset metadata
        # (coordinate attributes, encoding, and dataset structure)
        rechunked_ds = ds.assign_coords(rechunked_coords).assign(rechunked_data_vars)
        logger.debug("Dataset rechunking completed")

        return rechunked_ds


class CMORiser:
    """
    Base class for CMORisers, providing shared logic for CMORisation across
    different CMIP versions.

    Subclasses (``Atmosphere_CMORiser``, ``Ocean_CMORiser_OM2``,
    ``Ocean_CMORiser_OM3``, ``SeaIce_CMORiser``) implement realm-specific
    variable selection, coordinate renaming, and attribute updates.

    The recommended entry point for end users is
    :class:`access_moppy.driver.ACCESS_ESM_CMORiser`, which selects the
    correct subclass automatically and exposes a richer parameter set.
    """

    type_mapping = type_mapping

    #: Called with the first published file, before any further split of the
    #: same :meth:`write` is written, so a caller can gate the rest of the
    #: write on it.  Raising from the hook aborts :meth:`write`.
    first_write_hook: Callable[[Path], None] | None = None

    def __init__(
        self,
        input_data: Optional[Union[str, List[str], xr.Dataset, xr.DataArray]] = None,
        *,
        output_path: str,
        vocab: Any,
        variable_mapping: Dict[str, Any],
        compound_name: str,
        drs_root: Optional[Path] = None,
        staging_path: Optional[Path] = None,
        validate_frequency: bool = False,
        enable_resampling: bool = False,
        resampling_method: str = "auto",
        enable_chunking: bool = True,
        chunk_size_mb: float = 4.0,
        max_chunk_size_mb: float = 128.0,
        write_prefetch: int = 4,
        enable_compression: bool = True,
        compression_level: int = 4,
        split_years: Optional[Union[int, str]] = "auto",
        enable_qc_plots: bool = False,
        # Backward compatibility
        input_paths: Optional[Union[str, List[str]]] = None,
    ):
        """Initialise a CMORiser.

        Parameters
        ----------
        input_data:
            Path(s) to input NetCDF files, or an already-loaded
            ``xr.Dataset`` / ``xr.DataArray``.  Mutually exclusive with the
            deprecated *input_paths* argument.
        output_path:
            Directory where output NetCDF files are written.
        vocab:
            Vocabulary object (``CMIP6Vocabulary``, ``CMIP7Vocabulary``, …)
            that provides attribute requirements, filename generation, and
            DRS path construction.
        variable_mapping:
            Dictionary describing how raw model variables map to the target
            CMIP variable (CF name, units, dimensions, derivation steps, …).
        compound_name:
            Dot-separated CMIP compound name, e.g. ``"Amon.tas"``.
        drs_root:
            Optional root directory for CMIP DRS output tree.  When set,
            output is placed under the full DRS hierarchy rather than
            directly in *output_path*.
        staging_path:
            Optional fast local scratch directory (e.g. Gadi's
            ``$PBS_JOBFS``).  When set, each NetCDF file is written, repacked,
            and validated here before being moved to the final
            *output_path*/*drs_root* location.  For split output, the first
            spatial snapshot is also generated here; the full-period
            timeseries is generated after all splits are published.
        validate_frequency:
            Validate that input file timestamps are consistent with the
            target CMIP frequency before loading.  Disabled automatically
            for xarray inputs.
        enable_resampling:
            Allow automatic temporal resampling when the input frequency
            differs from the CMIP target.
        resampling_method:
            Resampling aggregation method: ``"auto"``, ``"mean"``,
            ``"sum"``, ``"min"``, ``"max"``, ``"first"``, or ``"last"``.
        enable_chunking:
            Enable Dask-backed, memory-bounded computation and manual writing
            for large datasets. This does not set NetCDF storage chunks.
        chunk_size_mb:
            Minimum target Dask/write task size in MB when *enable_chunking*
            is ``True``.
        max_chunk_size_mb:
            Hard maximum Dask/write task size in MB. Spatial dimensions are
            split only when needed to keep in-memory slices below this bound.
        write_prefetch:
            Maximum number of Dask-backed output slices submitted ahead of the
            serial NetCDF writer. Set to ``1`` to disable prefetching.
        enable_compression:
            Apply shuffle + zlib + Fletcher32 compression to time-dependent
            data variables in the output file.
        compression_level:
            zlib compression level (1 = fastest, 9 = smallest; default 4).
        split_years:
            Controls output file splitting by time period.

            * ``"auto"`` *(default)* — apply the CMIP-standard chunk lengths
              from :data:`~access_moppy.defaults.DEFAULT_CHUNK_YEARS`:
              1 year for sub-daily, 5 years for daily, 10 years for monthly,
              no split for ``yr`` and ``fx``.
            * ``None`` — write the entire time series to a single file.
            * positive ``int`` — explicit override applied to all frequencies
              (e.g. ``split_years=1`` for annual files).

            Chunk boundaries are aligned to calendar-year multiples of
            *split_years* (e.g. 1850, 1855, 1860 … for ``split_years=5``).
            ``fx`` variables and datasets with no ``time`` dimension always
            produce a single file regardless of this setting.
        input_paths:
            Deprecated alias for *input_data*.  Will be removed in a future
            release.
        """
        # Handle backward compatibility and validation
        if input_paths is not None and input_data is None:
            warnings.warn(
                "The 'input_paths' parameter is deprecated. Use 'input_data' instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            input_data = input_paths
        elif input_paths is not None and input_data is not None:
            raise ValueError(
                "Cannot specify both 'input_data' and 'input_paths'. Use 'input_data'."
            )
        elif input_paths is None and input_data is None:
            raise ValueError("Must specify either 'input_data' or 'input_paths'.")

        # Determine input type and handle appropriately
        self.input_is_xarray = isinstance(input_data, (xr.Dataset, xr.DataArray))

        if self.input_is_xarray:
            # For xarray inputs, store the dataset directly
            if isinstance(input_data, xr.DataArray):
                self.input_dataset = input_data.to_dataset()
            else:
                self.input_dataset = input_data
            self.input_paths = []  # Empty list for compatibility
        else:
            # For file paths, store as before
            self.input_paths = (
                input_data
                if isinstance(input_data, list)
                else [input_data]
                if input_data
                else []
            )
            self.input_dataset = None
        self.output_path = output_path
        # Extract cmor_name from compound_name
        _, self.cmor_name = compound_name.split(".")
        self.vocab = vocab
        self.mapping = variable_mapping
        self.drs_root = Path(drs_root) if drs_root is not None else None
        self.staging_path = Path(staging_path) if staging_path else None
        self.version_date = datetime.now().strftime("%Y%m%d")
        self.validate_frequency = validate_frequency
        self.compound_name = compound_name
        self.enable_resampling = enable_resampling
        self.resampling_method = resampling_method
        self.enable_chunking = enable_chunking
        if not isinstance(write_prefetch, int) or isinstance(write_prefetch, bool):
            raise TypeError("write_prefetch must be an integer")
        if write_prefetch < 1:
            raise ValueError("write_prefetch must be at least 1")
        self.write_prefetch = write_prefetch
        self.enable_compression = enable_compression
        self.compression_level = compression_level
        self.enable_qc_plots = enable_qc_plots
        self.qc_comparison_store: str | Path | None = None
        self.qc_preferred_member: str | None = None
        self._split_write_index: int | None = None
        self.chunker = (
            DatasetChunker(
                target_chunk_size_mb=chunk_size_mb,
                max_chunk_size_mb=max_chunk_size_mb,
            )
            if enable_chunking
            else None
        )
        self.split_years = split_years
        self.ds = None
        self.written_files: list[Path] = []
        # Release gate outcomes for this variable, worst-of across every file
        # written. Carried through output_summary so the batch report — and the
        # submission tracker downstream of it — can show what was checked,
        # rather than having to infer it from the task simply not failing.
        self.qc_gates: dict[str, dict[str, object]] = {}
        self.output_summary: dict[str, object] = self._summarise_written_files()

    def __getitem__(self, key):
        return self.ds[key]

    def __getattr__(self, attr):
        # This is only called if the attr is not found on CMORiser itself.
        # Two guards, both required to avoid an infinite-recursion trap:
        # - Dunder probes (pickle's __getstate__/__reduce_ex__, copy's
        #   __deepcopy__, etc.) must never be forwarded to self.ds. Letting
        #   them through means "does self.ds have this dunder" decides
        #   whether pickling/copying the CMORiser itself works, which is
        #   never the intent -- and if self.ds itself lacks the dunder,
        #   accessing it can loop back into this same method.
        # - Reading via self.__dict__ instead of self.ds bypasses attribute
        #   lookup entirely, so it can never recurse -- unlike `self.ds`,
        #   which itself triggers __getattr__ (infinitely) whenever 'ds'
        #   is not yet in __dict__, e.g. mid-unpickling before __setstate__
        #   has run.
        if attr.startswith("__") and attr.endswith("__"):
            raise AttributeError(attr)
        ds = self.__dict__.get("ds")
        if ds is None:
            raise AttributeError(attr)
        return getattr(ds, attr)

    def __setitem__(self, key, value):
        self.ds[key] = value

    def __repr__(self):
        return repr(self.ds)

    def _is_fx_variable(self) -> bool:
        """Return True when compound_name corresponds to a fixed-field CMIP table."""
        if not self.compound_name:
            return False

        table_id = self.compound_name.split(".", 1)[0].lower()
        return table_id.endswith("fx")

    def _squeeze_fx_singleton_time(self) -> None:
        """Drop UM-style singleton time axis for fixed (fx) variables."""
        if (
            self.ds is not None
            and self._is_fx_variable()
            and "time" in self.ds.dims
            and self.ds.sizes.get("time") == 1
        ):
            self.ds = self.ds.isel(time=0, drop=True)

    @staticmethod
    def _drop_duplicate_non_time_indexes(ds: xr.Dataset) -> xr.Dataset:
        """Keep duplicate coordinates as data without using them for alignment."""
        return _drop_duplicate_non_time_indexes(ds)

    def _calculation_owns_temporal_reduction(self) -> bool:
        """Whether this variable's calculation performs the temporal reduction.

        tasmin/tasmax-style mapping entries reduce sub-monthly input to the
        target frequency inside their formula (calculate_monthly_*). The
        generic auto-resampling must then leave the input untouched:
        pre-aggregating here would apply a second, wrong reduction before the
        formula runs (#644).
        """
        entry = self.mapping.get(self.cmor_name) if self.mapping else None
        if not entry:
            return False
        calc = entry.get("calculation") or {}
        operation = calc.get("operation", "")
        formula = calc.get("formula", "") or ""
        return operation in TIME_REDUCTION_OPERATIONS or any(
            f"{name}(" in formula for name in TIME_REDUCTION_OPERATIONS
        )

    def load_dataset(self, required_vars: Optional[List[str]] = None):
        """
        Load dataset from input files or use provided xarray objects with optional frequency validation.

        Args:
            required_vars: Optional list of required variables to extract
        """

        # If input is already an xarray object, use it directly
        if self.input_is_xarray:
            self.ds = (
                self.input_dataset.copy()
            )  # Make a copy to avoid modifying original

            # SAFEGUARD: Convert cftime coordinates to numeric if present
            self.ds = self._ensure_numeric_time_coordinates(self.ds)

            # Apply variable filtering if required_vars is specified
            if required_vars:
                available_vars = set(self.ds.data_vars) | set(self.ds.coords)
                vars_to_keep = set(required_vars) & available_vars
                if vars_to_keep != set(required_vars):
                    missing_vars = set(required_vars) - available_vars
                    warnings.warn(
                        f"Some required variables not found in dataset: {missing_vars}. "
                        f"Available variables: {available_vars}"
                    )

                # Keep only required data variables
                data_vars_to_keep = vars_to_keep & set(self.ds.data_vars)

                # Collect dimensions used by these data variables
                used_dims = set()
                for var in data_vars_to_keep:
                    used_dims.update(self.ds[var].dims)

                # Exclude auxiliary time dimension
                if "time_0" in used_dims:
                    self.ds = self.ds.isel(time_0=0, drop=True)
                    used_dims.remove("time_0")

                # Step 1: Keep only required data variables
                self.ds = self.ds[list(data_vars_to_keep)]

                # Step 2: Drop coordinates not in used_dims. Coordinates the
                # caller asked for by name are kept even when they sit on a
                # dimension of their own (e.g. the ocean cell-edge variables
                # st_edges_ocean/sw_edges_ocean, needed to build lev_bnds).
                coords_to_drop = [
                    c
                    for c in self.ds.coords
                    if c not in used_dims and c not in vars_to_keep
                ]

                if coords_to_drop:
                    self.ds = self.ds.drop_vars(coords_to_drop)
                    logger.debug(
                        "Dropped %d unused coordinate(s): %s",
                        len(coords_to_drop),
                        coords_to_drop,
                    )

        else:
            # If no input files were provided, initialise an empty Dataset.
            # This supports self-contained formula calculations (e.g. using
            # load_ressource_data nested expressions) that do not need an
            # external primary dataset.
            if not self.input_paths:
                self.ds = xr.Dataset()
                return

            # Original file-based loading logic
            preprocess = partial(
                _preprocess_open_mfdataset,
                required_vars=frozenset(required_vars or ()),
            )

            # Open the first file once to probe its structure.  This single handle
            # is reused for both the frequency-validation time-independence check
            # and the _has_time check below, avoiding a duplicate open and the
            # file-handle leak that an unguarded open_dataset would cause.
            with xr.open_dataset(self.input_paths[0], decode_cf=False) as _probe:
                _probe_dims = set(_probe.dims)
                _probe_target_vars = (
                    [v for v in required_vars if v in _probe.data_vars]
                    if required_vars
                    else list(_probe.data_vars)
                )
                _has_time = any(
                    any(dim.startswith("time") for dim in _probe[v].dims)
                    for v in _probe_target_vars
                )

            # Validate frequency consistency and CMIP6 compatibility before concatenation
            # Skip validation for time-independent variables (e.g., areacello, static grids)
            if self.validate_frequency and len(self.input_paths) > 0:
                # Check if this is a time-dependent variable by examining the compound_name
                # Time-independent variables typically have "fx" (fixed) in their table ID
                is_time_independent = (
                    self.compound_name and "fx" in self.compound_name.lower()
                ) or not any(dim.startswith("time") for dim in _probe_dims)

                if is_time_independent:
                    logger.debug(
                        "Skipping frequency validation for time-independent variable"
                    )
                else:
                    try:
                        # Enhanced validation with CMIP frequency compatibility
                        # Use CMIP6-specific validation if available, otherwise skip
                        if (
                            hasattr(self.vocab, "__class__")
                            and "CMIP6" in self.vocab.__class__.__name__
                        ):
                            detected_freq, resampling_required = (
                                validate_cmip6_frequency_compatibility(
                                    self.input_paths,
                                    self.compound_name,
                                    time_coord="time",
                                    interactive=sys.stdin.isatty(),
                                )
                            )
                            if resampling_required:
                                logger.debug(
                                    "Temporal resampling will be applied: %s -> CMIP6 target frequency",
                                    detected_freq,
                                )
                            else:
                                logger.debug(
                                    "Validated compatible temporal frequency: %s",
                                    detected_freq,
                                )
                        else:
                            logger.debug(
                                "Skipping detailed frequency validation for this CMIP version"
                            )
                    except (FrequencyMismatchError, IncompatibleFrequencyError) as e:
                        raise e  # Re-raise these specific errors as-is
                    except InterruptedError as e:
                        raise e  # Re-raise user abort
                    except Exception as e:
                        warnings.warn(
                            f"Could not validate temporal frequency: {e}. "
                            f"Proceeding with concatenation but results may be inconsistent."
                        )

            if _has_time:
                # Multi-variable mappings (e.g. ocean formulas requiring temp +
                # rho_dzt + area_t) may source each variable from separate files
                # that share identical time axes. Nested concat along time can
                # duplicate timestamps in that case, so prefer coordinate-based
                # combine when multiple required variables are requested.
                prefer_by_coords = bool(required_vars and len(required_vars) > 1)

                # One dask chunk per file along time. Sub-monthly inputs (e.g.
                # daily tasmax/tasmin) are stored with per-timestep on-disk HDF5
                # chunking; the default chunks={} inherits that, so a 31-day file
                # becomes 31 chunks and the task graph explodes (~650k tasks over
                # a multi-decade run), which is what drives the distributed
                # workers out of memory on those variables. Collapsing to one
                # chunk per file cuts the graph ~26x and the compute memory ~5x
                # with bit-identical results; monthly inputs (time size 1) are
                # unaffected.
                common_kwargs = {
                    "engine": "netcdf4",
                    "decode_cf": False,
                    "chunks": {"time": -1},
                    "data_vars": "minimal",
                    "coords": "minimal",
                    "compat": "override",
                    "preprocess": preprocess,
                    # Only true when a Client(processes=True, ...) with one
                    # thread/worker is active; otherwise this would silently
                    # corrupt reads or crash (see _parallel_open_is_safe).
                    "parallel": _parallel_open_is_safe(),
                }

                if prefer_by_coords:
                    try:
                        self.ds = xr.open_mfdataset(
                            self.input_paths,
                            combine="by_coords",
                            **common_kwargs,
                        )
                    except Exception as err:
                        warnings.warn(
                            "Coordinate-based file combination failed; "
                            "falling back to nested time concatenation. "
                            f"Original error: {err}"
                        )
                        self.ds = xr.open_mfdataset(
                            self.input_paths,
                            combine="nested",
                            concat_dim="time",
                            **common_kwargs,
                        )
                else:
                    self.ds = xr.open_mfdataset(
                        self.input_paths,
                        combine="nested",  # avoids costly dimension alignment
                        concat_dim="time",
                        **common_kwargs,
                    )
            else:
                # Time-independent (fx) file — do not add a spurious time dimension
                self.ds = xr.open_dataset(
                    self.input_paths[0],
                    engine="netcdf4",
                    decode_cf=False,
                    chunks={},
                )
                if required_vars:
                    vars_to_keep = [v for v in required_vars if v in self.ds.data_vars]
                    self.ds = self.ds[vars_to_keep]
                # UM source files can include a time=1 axis for static fields.
                # Keep squeeze behavior centralized in _squeeze_fx_singleton_time().

            # UM source files can carry time=1 for fixed fields even when loaded
            # through the time-aware branch. Squeeze once here before any downstream
            # frequency handling, rechunking, or missing-value normalization.
            self._squeeze_fx_singleton_time()

        # Apply temporal resampling if enabled and needed. Skipped when the
        # variable's own calculation performs the temporal reduction: the
        # formula must receive the raw (e.g. daily) input.
        if (
            self.enable_resampling
            and self.compound_name
            and not self._calculation_owns_temporal_reduction()
        ):
            try:
                logger.debug(
                    "Checking if temporal resampling is needed for %s", self.cmor_name
                )

                self.ds, was_resampled = validate_and_resample_if_needed(
                    self.ds,
                    self.compound_name,
                    self.cmor_name,
                    time_coord="time",
                    method=self.resampling_method,
                )

                if was_resampled:
                    logger.debug(
                        "Applied temporal resampling to match CMIP requirements"
                    )
                else:
                    logger.debug("No resampling needed - frequency already compatible")

            except (FrequencyMismatchError, IncompatibleFrequencyError) as e:
                raise e  # Re-raise validation errors
            except Exception as e:
                raise RuntimeError(f"Failed to resample dataset: {e}")
        elif self.enable_resampling and not self.compound_name:
            warnings.warn(
                "Resampling enabled but no compound_name provided. "
                "Cannot determine target frequency for resampling.",
                ResamplingRequiredWarning,
            )

        # Apply intelligent rechunking if enabled
        if self.enable_chunking and self.chunker:
            logger.debug("Applying intelligent dataset rechunking...")
            self.ds = self.chunker.rechunk_dataset(self.ds)
            logger.debug("Dataset rechunking completed")

        # Normalize missing values to NaN early for consistent processing
        self._normalize_missing_values_early()

    def _ensure_numeric_time_coordinates(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Convert cftime objects in time-related coordinates to numeric values.

        This safeguard prevents TypeError when cftime objects are implicitly
        cast to numeric types in downstream operations (e.g., atmosphere.py line 174).

        Args:
            ds: Input dataset that may contain cftime coordinates

        Returns:
            Dataset with numeric time coordinates
        """
        # List of common time-related coordinate names to check
        time_coords = ["time", "time_bnds", "time_bounds"]

        for coord_name in time_coords:
            if coord_name not in ds.coords:
                continue

            coord = ds[coord_name]

            # Check if coordinate contains cftime objects
            if coord.size > 0:
                # Get first value to check type
                first_val = (
                    coord.isel({coord.dims[0]: 0}).values.item()
                    if coord.size > 0
                    else None
                )

                if first_val is not None and isinstance(first_val, cftime.datetime):
                    # Extract time encoding attributes
                    units = coord.attrs.get("units")
                    calendar = coord.attrs.get("calendar", "proleptic_gregorian")

                    if units is None:
                        warnings.warn(
                            f"Coordinate '{coord_name}' contains cftime objects but has no 'units' attribute. "
                            f"Using default: 'days since 0001-01-01'. "
                            f"Results may be incorrect.",
                            UserWarning,
                        )
                        units = "days since 0001-01-01"

                    # Convert cftime to numeric
                    try:
                        numeric_values = date2num(
                            coord.values, units=units, calendar=calendar
                        )

                        # Create new attributes dict with units and calendar
                        new_attrs = coord.attrs.copy()
                        new_attrs["units"] = units
                        new_attrs["calendar"] = calendar
                        # Replace coordinate with numeric values, preserving attributes
                        ds[coord_name] = (coord.dims, numeric_values, new_attrs)

                        logger.debug(
                            "Converted '%s' from cftime to numeric (%s, %s)",
                            coord_name,
                            units,
                            calendar,
                        )

                    except Exception as e:
                        warnings.warn(
                            f"Failed to convert '{coord_name}' from cftime to numeric: {e}. "
                            f"This may cause errors in downstream processing.",
                            UserWarning,
                        )

        return ds

    def sort_time_dimension(self):
        if "time" in self.ds.dims:
            self.ds = self.ds.sortby("time")
            self._validate_time_axis_integrity()

    def _validate_time_axis_integrity(self) -> None:
        """Enforce strict CMOR time-axis requirements.

        The time coordinate must be strictly increasing, have no duplicate
        timestamps, and contain no gaps for the expected sampling cadence.
        """
        if "time" not in self.ds.coords:
            return

        time_index = self.ds.get_index("time")

        duplicated = time_index.duplicated()
        if duplicated.any():
            duplicate_values = list(dict.fromkeys(time_index[duplicated].tolist()))
            preview = duplicate_values[:5]
            raise ValueError(
                "Time coordinate contains duplicate timestamps. "
                f"Found {len(duplicate_values)} duplicated value(s), including: {preview}"
            )

        if not time_index.is_monotonic_increasing:
            raise ValueError(
                "Time coordinate is not monotonic increasing after sorting. "
                "This indicates an invalid time axis for CMORisation."
            )

        if len(time_index) < 2:
            return

        self._validate_time_gaps_from_bounds_or_frequency()

    def _validate_time_gaps_from_bounds_or_frequency(self) -> None:
        """Validate that there are no missing timesteps.

        Prefer CF time-bounds continuity when available. Otherwise fall back to
        coarse frequency-aware checks derived from the target CMIP table.
        """
        bounds = self._get_time_bounds_for_gap_validation()
        if bounds is not None:
            if self._time_bounds_have_gaps(bounds):
                time_values = (
                    self.ds["time"].values if "time" in self.ds.coords else None
                )
                detail = self._describe_bounds_gaps(bounds, time_values)
                raise ValueError(
                    "Time bounds are not contiguous. Missing or overlapping "
                    f"timesteps detected in the CMOR time axis.\n{detail}"
                )
            return

        freq_hint = self._target_frequency_hint()
        if freq_hint is None:
            return

        time_da = self.ds["time"]
        time_values = time_da.values
        time_units = time_da.attrs.get("units")

        # Numeric time coordinates (e.g. 0, 1, 2) are often synthetic in unit
        # tests or pre-decoded placeholders. Frequency-fallback checks are not
        # reliable there without explicit decoded datetimes, so only apply this
        # fallback to datetime-like axes. Bounds-based checks above still apply.
        if np.issubdtype(np.asarray(time_values).dtype, np.number):
            logger.debug(
                "Skipping frequency-fallback gap validation for numeric time axis"
            )
            return

        deltas_days = [
            (
                time_values[i],
                time_values[i + 1],
                self._time_delta_days(
                    time_values[i],
                    time_values[i + 1],
                    time_units=time_units,
                ),
            )
            for i in range(len(time_values) - 1)
        ]

        if freq_hint == "daily":
            invalid = [
                (a, b, d)
                for a, b, d in deltas_days
                if not np.isclose(d, 1.0, atol=1e-6)
            ]
        elif freq_hint == "monthly":
            invalid = [(a, b, d) for a, b, d in deltas_days if d < 27.0 or d > 32.0]
        elif freq_hint == "yearly":
            invalid = [(a, b, d) for a, b, d in deltas_days if d < 360.0 or d > 370.0]
        else:
            invalid = []

        if invalid:
            examples = "; ".join(f"{a} → {b} ({d:.2f} days)" for a, b, d in invalid[:5])
            raise ValueError(
                "Missing timesteps detected in time coordinate for expected "
                f"'{freq_hint}' cadence. "
                f"Invalid interval(s) ({len(invalid)} total): {examples}"
            )

    def _get_time_bounds_for_gap_validation(self) -> Optional[xr.DataArray]:
        """Return the time bounds variable when available and shape-compatible."""
        time_var = self.ds.get("time")
        if time_var is None:
            return None

        candidate_names = []
        bounds_name = time_var.attrs.get("bounds")
        if bounds_name:
            candidate_names.append(bounds_name)
        candidate_names.extend(["time_bnds", "time_bounds", "time_bnd"])

        seen = set()
        for name in candidate_names:
            if name in seen:
                continue
            seen.add(name)
            if name not in self.ds:
                continue

            bounds = self.ds[name]
            if bounds.ndim != 2 or bounds.shape[-1] != 2:
                continue
            if bounds.shape[0] != self.ds.sizes.get("time"):
                continue
            return bounds

        return None

    @staticmethod
    def _time_bounds_have_gaps(bounds: xr.DataArray) -> bool:
        """Return True when adjacent intervals are not perfectly contiguous."""
        values = bounds.values
        if values.shape[0] < 2:
            return False

        left = values[:-1, 1]
        right = values[1:, 0]

        if np.issubdtype(np.asarray(left).dtype, np.number):
            scale = max(float(np.nanmax(np.abs(values))), 1.0)
            atol = scale * 1e-10
            return bool(np.any(~np.isclose(left, right, rtol=0.0, atol=atol)))

        return bool(np.any(left != right))

    @staticmethod
    def _describe_bounds_gaps(
        bounds: xr.DataArray,
        time_values: Optional[Any] = None,
    ) -> str:
        """Return a human-readable description of where bounds gaps occur."""
        values = bounds.values
        left = values[:-1, 1]  # end of interval i
        right = values[1:, 0]  # start of interval i+1

        is_numeric = np.issubdtype(np.asarray(left).dtype, np.number)
        if is_numeric:
            scale = max(float(np.nanmax(np.abs(values))), 1.0)
            atol = scale * 1e-10
            bad_mask = ~np.isclose(left, right, rtol=0.0, atol=atol)
        else:
            bad_mask = left != right

        bad_indices = np.where(bad_mask)[0]
        n_total = len(bad_indices)
        lines = [f"  {n_total} discontinuity(ies) found:"]
        for idx in bad_indices[:5]:
            end_val = left[idx]
            start_val = right[idx]
            if time_values is not None and idx + 1 < len(time_values):
                t_before = time_values[idx]
                t_after = time_values[idx + 1]
                lines.append(
                    f"  [index {idx}→{idx + 1}] time {t_before} → {t_after}: "
                    f"bound end={end_val}, next bound start={start_val}"
                )
            else:
                lines.append(
                    f"  [index {idx}→{idx + 1}] bound end={end_val}, next bound start={start_val}"
                )
        if n_total > 5:
            lines.append(f"  ... and {n_total - 5} more.")
        return "\n".join(lines)

    @staticmethod
    def _time_delta_days(
        start: Any, end: Any, time_units: Optional[str] = None
    ) -> float:
        """Compute day-length between two timestamps for numpy/cftime values."""
        diff = end - start
        if isinstance(diff, np.timedelta64):
            return float(diff / np.timedelta64(1, "s")) / 86400.0
        if np.isscalar(diff) and isinstance(
            diff, (int, float, np.integer, np.floating)
        ):
            return CMORiser._numeric_delta_to_days(float(diff), time_units)
        if hasattr(diff, "total_seconds"):
            return float(diff.total_seconds()) / 86400.0
        return float(diff.days) + float(getattr(diff, "seconds", 0)) / 86400.0

    @staticmethod
    def _numeric_delta_to_days(delta: float, time_units: Optional[str]) -> float:
        """Convert numeric coordinate deltas to days using CF units when possible."""
        if not time_units:
            return delta

        interval = str(time_units).split("since", 1)[0].strip().lower()
        if interval in {"day", "days", "d"}:
            return delta
        if interval in {"hour", "hours", "hr", "hrs", "h"}:
            return delta / 24.0
        if interval in {"minute", "minutes", "min", "mins", "m"}:
            return delta / 1440.0
        if interval in {"second", "seconds", "sec", "secs", "s"}:
            return delta / 86400.0
        return delta

    @staticmethod
    def _days_to_numeric_units(days: float, time_units: Optional[str]) -> float:
        """Convert day-length values back to the numeric coordinate units."""
        if not time_units:
            return days

        interval = str(time_units).split("since", 1)[0].strip().lower()
        if interval in {"day", "days", "d"}:
            return days
        if interval in {"hour", "hours", "hr", "hrs", "h"}:
            return days * 24.0
        if interval in {"minute", "minutes", "min", "mins", "m"}:
            return days * 1440.0
        if interval in {"second", "seconds", "sec", "secs", "s"}:
            return days * 86400.0
        return days

    def _align_subdaily_point_time_to_square_grid(self) -> None:
        """Align point-sampled sub-daily timestamps to day-boundary slots.

        Some ACCESS streams begin point-sampled sub-daily series at +3h/+6h.
        WCRP TIME001 expects these frequencies to be square on the canonical
        daily grid. Shift the whole time axis (and time bounds if present) by
        that leading offset.
        """
        if "time" not in self.ds.coords or self.cmor_name not in self.ds:
            return

        table_id = (self.compound_name or "").split(".", 1)[0]
        table_hours = {
            "3hr": 3.0,
            "3hrPt": 3.0,
            "6hrPlevPt": 6.0,
        }
        target_hours = table_hours.get(table_id)
        if target_hours is None:
            return

        cell_methods = str(self.ds[self.cmor_name].attrs.get("cell_methods", ""))
        if "time: point" not in cell_methods:
            return

        time_da = self.ds["time"]
        values = np.asarray(time_da.values)
        if values.size == 0 or not np.issubdtype(values.dtype, np.number):
            return

        time_units = time_da.attrs.get("units")
        if not isinstance(time_units, str) or "since" not in time_units:
            return

        first_value = float(values.flat[0])
        first_days = self._numeric_delta_to_days(first_value, time_units)
        leading_offset_days = first_days - np.floor(first_days)
        if np.isclose(leading_offset_days, 0.0, atol=1e-10):
            return

        target_days = target_hours / 24.0
        if not np.isclose(leading_offset_days, target_days, atol=1e-8):
            return

        shift_units = self._days_to_numeric_units(leading_offset_days, time_units)
        self.ds["time"] = xr.DataArray(
            values - shift_units,
            dims=time_da.dims,
            coords=time_da.coords,
            attrs=time_da.attrs,
        )

        bounds_name = time_da.attrs.get("bounds")
        if isinstance(bounds_name, str) and bounds_name in self.ds:
            bnds = self.ds[bounds_name]
            if np.issubdtype(np.asarray(bnds.values).dtype, np.number):
                self.ds[bounds_name] = xr.DataArray(
                    bnds.values - shift_units,
                    dims=bnds.dims,
                    coords=bnds.coords,
                    attrs=bnds.attrs,
                )

        logger.debug(
            "Shifted '%s' time axis by %.6f %s to align sub-daily point timestamps",
            self.cmor_name,
            shift_units,
            time_units.split("since", 1)[0].strip(),
        )

    def rechunk_dataset(self):
        """
        Rechunk the dataset for bounded computation and manual writing.

        This method can be called separately from load_dataset if rechunking
        is needed at a different stage in the processing pipeline. It does not
        configure NetCDF/HDF5 storage chunks.
        """
        if self.enable_chunking and self.chunker and self.ds is not None:
            logger.debug("Applying dataset rechunking...")
            self.ds = self.chunker.rechunk_dataset(self.ds)
            logger.debug("Dataset rechunking completed")
        else:
            if not self.enable_chunking:
                logger.debug("Chunking is disabled, skipping rechunking")
            elif not self.chunker:
                logger.debug("No chunker available, skipping rechunking")
            else:
                logger.debug("No dataset loaded, cannot rechunk")

    def _target_frequency_hint(self):
        """Map the CMOR table's target frequency to a coarse label
        ("daily"/"monthly"/"yearly") for time-bounds construction.

        Used only as a fallback when the time axis has a single point and the
        frequency cannot be inferred from point spacing. Returns None when the
        frequency is not determinable or is sub-daily.
        """
        if not self.compound_name:
            return None
        try:
            target = parse_cmip6_table_frequency(self.compound_name)
        except Exception:
            return None
        days = target.total_seconds() / 86400
        if 0.9 <= days <= 1.1:
            return "daily"
        if 28 <= days <= 31:
            return "monthly"
        if 360 <= days <= 366:
            return "yearly"
        return None

    def _preserve_bounds_time_encoding(self, bnds_var):
        """Move a time-bounds variable's ``units``/``calendar`` into ``encoding``.

        CF §7.1 keeps those attributes on the parent coordinate only, but the
        writer still needs them to turn a bounds variable that holds cftime or
        datetime64 objects back into numbers. It reads ``attrs`` first and then
        ``encoding``, so stashing them in ``encoding`` keeps the conversion
        correct while leaving the on-disk variable attribute-free -- the writer
        serialises ``attrs`` only. Without this the encoder silently falls back
        to "days since 1850-01-01" and the bounds land ~1850 years away from
        their own time coordinate.

        A no-op for numeric bounds, and for values already in ``encoding``.
        """
        if bnds_var not in self.ds:
            return
        bnds = self.ds[bnds_var]
        is_decoded_time = np.issubdtype(bnds.dtype, np.datetime64) or (
            bnds.dtype == object
            and bnds.size > 0
            and hasattr(np.asarray(bnds.values).flat[0], "year")
        )
        if not is_decoded_time:
            return
        parent = self.ds.get(bnds_var[: -len("_bnds")])
        for key, default in (("units", None), ("calendar", "standard")):
            if bnds.encoding.get(key):
                continue
            value = bnds.attrs.get(key)
            if value is None and parent is not None:
                value = parent.attrs.get(key) or parent.encoding.get(key)
            if value is None:
                value = default
            if value is not None:
                bnds.encoding[key] = value

    def calculate_missing_bounds_variables(self, bnds_required):
        """Calculate missing bounds variables for coordinates."""
        for bnds_var in bnds_required:
            # Extract coordinate name by removing "_bnds" suffix
            coord_name = bnds_var.replace("_bnds", "")
            if bnds_var not in self.ds.data_vars and bnds_var not in self.ds.coords:
                if coord_name not in self.ds.coords:
                    raise ValueError(
                        f"Cannot calculate bounds '{bnds_var}': coordinate '{coord_name}' not found. "
                        f"Available coordinates: {sorted(self.ds.coords)}"
                    )

                # Warn user that bounds are missing and will be calculated automatically
                warnings.warn(
                    f"'{bnds_var}' not found in raw data. Automatically calculating bounds for '{coord_name}' coordinate.",
                    UserWarning,
                    stacklevel=3,
                )

                # Determine which calculation function to use based on coordinate name
                if coord_name in ["time", "t"]:
                    # Calculate time bounds - atmosphere uses "bnds"
                    self.ds[bnds_var] = calculate_time_bounds(
                        self.ds,
                        time_coord=coord_name,
                        bnds_name="bnds",  # Atmosphere uses "bnds"
                        # Fallback for a single time point (e.g. one resampled
                        # year) where the frequency cannot be inferred.
                        freq_hint=self._target_frequency_hint(),
                    )

                elif coord_name in ["lat", "latitude", "y"]:
                    # Calculate latitude bounds - use "bnds" for atmosphere data
                    self.ds[bnds_var] = calculate_latitude_bounds(
                        self.ds, coord_name, bnds_name="bnds"
                    )

                elif coord_name in ["lon", "longitude", "x"]:
                    # Calculate longitude bounds - use "bnds" for atmosphere data
                    self.ds[bnds_var] = calculate_longitude_bounds(
                        self.ds, coord_name, bnds_name="bnds"
                    )

                else:
                    # For other coordinates, we could add more handlers or skip
                    warnings.warn(
                        f"No automatic calculation available for '{bnds_var}'. This may cause CMIP compliance issues.",
                        UserWarning,
                        stacklevel=3,
                    )
                    continue
            # Ensure the coordinate's bounds attribute always points to the bounds variable,
            # regardless of whether it was just calculated or already existed in the input data.
            if coord_name in self.ds.coords or coord_name in self.ds.data_vars:
                self.ds[coord_name].attrs["bounds"] = bnds_var

    def select_and_process_variables(self):
        raise NotImplementedError(
            "Subclasses must implement select_and_process_variables."
        )

    def _check_units(self, cmor_name: str, expected: str) -> None:
        """Check that the mapping's declared units are consistent with what CMIP expects."""
        declared = self.mapping.get(cmor_name, {}).get("units")
        if declared and expected and declared != expected:
            raise ValueError(
                f"Mapping units mismatch for '{cmor_name}': "
                f"mapping declares '{declared}' but CMIP expects '{expected}'. "
                f"Update the 'units' field in the variable mapping file."
            )

    def _check_calendar(self, var: str):
        calendar = self.ds[var].attrs.get("calendar")
        units = self.ds[var].attrs.get("units")

        # TODO: Remove at some point. ESM1.6 should have this fixed.
        if calendar == "GREGORIAN":
            # Replace GREGORIAN with Proleptic Gregorian
            self.ds[var].attrs["calendar"] = "proleptic_gregorian"
            # Replace calendar type attribute with proleptic_gregorian
            if "calendar_type" in self.ds[var].attrs:
                self.ds[var].attrs["calendar_type"] = "proleptic_gregorian"
        calendar = calendar.lower() if calendar else None

        if not calendar or not units:
            return
        try:
            dates = xr.cftime_range(
                start=units.split("since")[1].strip(), periods=3, calendar=calendar
            )
        except Exception as e:
            raise ValueError(
                f"Failed calendar check for '{var}' "
                f"(calendar='{calendar}', units='{units}'): {e}"
            )
        if calendar in ("noleap", "365_day"):
            for d in dates:
                if d.month == 2 and d.day == 29:
                    raise ValueError(f"{calendar} must not have 29 Feb: found {d}")
        elif calendar == "360_day":
            for d in dates:
                if d.day > 30:
                    raise ValueError(f"360_day calendar has day > 30: {d}")

    def _apply_time_coordinate_attributes(self):
        """Apply CF time-coordinate attributes from the active CMOR table.

        Ocean and sea-ice CMORisers build their coordinate set manually rather
        than via the atmosphere's axis loop, so the time coordinate would
        otherwise keep only whatever the model file provided (ocean files carry
        ``axis`` but no ``standard_name``; sea-ice files carry neither). Time
        values, units and calendar are left untouched here — they are managed by
        time decoding and ``_check_calendar``.
        """
        if "time" not in self.ds:
            return
        time_meta = next(
            (
                m
                for m in self.vocab.axes.values()
                if m.get("out_name") == "time" and m.get("standard_name") == "time"
            ),
            None,
        )
        if time_meta is None:
            return
        self.ds["time"].attrs.update(
            {
                k: time_meta[k]
                for k in ("standard_name", "long_name", "axis")
                if time_meta.get(k) not in (None, "")
            }
        )
        dtype = self.type_mapping.get(time_meta.get("type", "double"), np.float64)
        self._match_bounds_dtype("time", dtype)

    def _match_bounds_dtype(self, coord_name: str, dtype) -> None:
        """Cast ``coord_name``'s bounds variable to ``dtype``.

        CF §7.1 expects a bounds variable to share its parent coordinate's
        type. The CMOR tables only declare a "type" for the coordinate itself
        (e.g. time is "double"), so a bounds variable carried through unchanged
        from the source file — rather than computed by
        ``calculate_time_bounds()`` — can otherwise drift to whatever
        precision the model happened to store it in (observed: "time" written
        as double but "time_bnds" as float in the same file).
        """
        if coord_name not in self.ds:
            return
        bnds_name = self.ds[coord_name].attrs.get("bounds") or f"{coord_name}_bnds"
        if bnds_name not in self.ds:
            return
        bnds = self.ds[bnds_name]
        if np.issubdtype(bnds.dtype, np.number) and bnds.dtype != np.dtype(dtype):
            self.ds[bnds_name] = bnds.astype(dtype)

    def _check_range(self, var: str, vmin: float, vmax: float):
        arr = self.ds[var]
        if hasattr(arr.data, "map_blocks"):
            # Fuse both comparisons into one scheduler pass instead of two
            # separate .compute() calls.
            too_small, too_large = da.compute((arr < vmin).any(), (arr > vmax).any())
        else:
            too_small = (arr < vmin).any().item()
            too_large = (arr > vmax).any().item()
        if too_small:
            actual_min = arr.min().values
            warnings.warn(
                f"Variable '{var}' has values below valid_min={vmin}. "
                f"Actual minimum found: {actual_min}",
                UserWarning,
                stacklevel=2,
            )
        if too_large:
            actual_max = arr.max().values
            warnings.warn(
                f"Variable '{var}' has values above valid_max={vmax}. "
                f"Actual maximum found: {actual_max}",
                UserWarning,
                stacklevel=2,
            )

    def _drop_stale_range_attributes(self, cmor_attrs: Dict[str, Any]):
        """Drop range attributes inherited from the raw model output.

        CMOR writes ``valid_min``/``valid_max`` only when the table declares
        them, and never writes ``valid_range`` at all, so any range attribute
        that survives the rename to the CMOR name came from the source file.
        MOM's transport diagnostics carry
        ``valid_range = (-1e20, 1e20)`` — the model's own ``missing_value``
        reused as a sentinel pair — which reaches the output straddling the
        CMIP fill value instead of excluding it (umo/vmo/wmo, ACCESS-MOPPy
        #662). A reader clipping to ``valid_range`` would then take missing
        data for valid data, so the inherited values are dropped rather than
        passed through.
        """
        attrs = self.ds[self.cmor_name].attrs
        attrs.pop("valid_range", None)
        for attr in ("valid_min", "valid_max"):
            if cmor_attrs.get(attr) in (None, ""):
                attrs.pop(attr, None)

    #: CF Appendix D — the standard name of the quantity each parametric
    #: vertical coordinate computes from its ``formula_terms``. Neither
    #: ``CMIP7_coordinate.json`` nor ``CMIP6_coordinate.json`` carries a
    #: ``computed_standard_name`` field at all (ACCESS-MOPPy #662), so the
    #: mapping lives here until they do; a table value, once one exists, wins.
    #: Where Appendix D offers a choice between ``altitude`` and
    #: ``height_above_geopotential_datum``, the CMIP reference surfaces are
    #: geoid-referenced — ``orog`` is ``surface_altitude`` and ``deptho`` is
    #: ``sea_floor_depth_below_geoid`` — which is the ``altitude`` case.
    _COMPUTED_STANDARD_NAMES = {
        "atmosphere_ln_pressure_coordinate": "air_pressure",
        "atmosphere_sigma_coordinate": "air_pressure",
        "atmosphere_hybrid_sigma_pressure_coordinate": "air_pressure",
        "atmosphere_hybrid_height_coordinate": "altitude",
        "atmosphere_sleve_coordinate": "altitude",
        "ocean_sigma_coordinate": "altitude",
        "ocean_s_coordinate": "altitude",
        "ocean_s_coordinate_g1": "altitude",
        "ocean_s_coordinate_g2": "altitude",
        "ocean_sigma_z_coordinate": "altitude",
        "ocean_double_sigma_coordinate": "altitude",
    }

    def _apply_computed_standard_names(self):
        """Name the quantity a parametric vertical coordinate computes.

        CF §4.3.3 — a dimensionless or otherwise parametric vertical
        coordinate gives the standard name of the quantity its
        ``formula_terms`` evaluate to in ``computed_standard_name``; for the
        hybrid height coordinate MOPPy writes, that is ``altitude``
        (ACCESS-MOPPy #662, missing on ``cl``/``cli``/``clw``).

        A coordinate counts as parametric when its table entry declares
        ``z_factors``, so the attribute is driven by the vocabulary rather
        than by whatever the raw file happened to carry. It describes the
        coordinate alone: the bounds variable inherits the meaning and must
        not repeat it (CF §7.1), so this runs after the bounds attributes have
        been rebuilt.
        """
        for meta in self.vocab.axes.values():
            if not meta.get("z_factors"):
                continue
            name = meta.get("out_name")
            if not name or name not in self.ds:
                continue
            computed = meta.get("computed_standard_name") or (
                self._COMPUTED_STANDARD_NAMES.get(
                    self.ds[name].attrs.get("standard_name")
                )
            )
            if computed:
                self.ds[name].attrs["computed_standard_name"] = computed

    #: Units naming a temperature scale, where CF-1.11 §3.1 needs
    #: ``units_metadata`` to say whether a value is a point on that scale or a
    #: difference between two of them.
    _TEMPERATURE_SCALE_UNITS = frozenset(
        {"K", "degC", "degree_C", "degrees_C", "degF", "degree_F", "degrees_F"}
    )
    #: Endings CF standard names use for a quantity that is a difference of two
    #: temperatures rather than a temperature.
    _TEMPERATURE_DIFFERENCE_SUFFIXES = ("_difference", "_range", "_anomaly")
    #: Calendars tracking real-world time, the only ones for which leap seconds
    #: mean anything (CF-1.11 §4.4).
    _REAL_WORLD_CALENDARS = frozenset(
        {"standard", "gregorian", "proleptic_gregorian", "julian"}
    )

    def _apply_units_metadata(self):
        """Say how ambiguous units are to be read (CF-1.11, ACCESS-MOPPy #662).

        ``units_metadata`` is new in CF-1.11, and CMOR writes it from 3.9 on,
        for the two cases where the unit string alone does not fix the meaning:

        * §3.1 — a value in K or degC can be a point on the temperature scale
          or a difference between two of them. Every temperature variable in
          the CMIP7 tables is an absolute temperature, so the units alone pick
          out what to mark; the standard-name forms CF uses for differences are
          labelled as such rather than read off their units.
        * §4.4 — a time axis on a real-world calendar has to say whether leap
          seconds are counted. Model time is a plain count of SI days, so
          ``leap_seconds: none``. Idealized calendars (365_day, 360_day, ...)
          have no leap seconds to describe and are left alone.

        Written only when the file declares CF-1.11 or later: the CMIP6 tables
        declare CF-1.7, where the attribute does not exist.
        """
        if self._cf_version() < (1, 11):
            return

        attrs = self.ds[self.cmor_name].attrs
        if attrs.get("units") in self._TEMPERATURE_SCALE_UNITS:
            standard_name = attrs.get("standard_name") or ""
            attrs["units_metadata"] = (
                "temperature: difference"
                if standard_name.endswith(self._TEMPERATURE_DIFFERENCE_SUFFIXES)
                else "temperature: on_scale"
            )

        if "time" in self.ds:
            time_var = self.ds["time"]
            calendar = time_var.attrs.get("calendar") or time_var.encoding.get(
                "calendar", ""
            )
            if str(calendar).lower() in self._REAL_WORLD_CALENDARS:
                time_var.attrs["units_metadata"] = "leap_seconds: none"

    def _cf_version(self) -> tuple:
        """Parse the CF version out of the file's ``Conventions`` attribute.

        Returns ``(0, 0)`` when the file declares no CF version, so a caller
        gated on a minimum version stays silent rather than guessing one.
        """
        for token in str(self.ds.attrs.get("Conventions", "")).split():
            if token.startswith("CF-"):
                try:
                    return tuple(int(part) for part in token[3:].split("."))
                except ValueError:
                    break
        return (0, 0)

    def drop_intermediates(self):
        if self.mapping[self.cmor_name].get("model_variables"):
            for var in self.mapping[self.cmor_name]["model_variables"]:
                if var in self.ds.data_vars and var != self.cmor_name:
                    self.ds = self.ds.drop_vars(var)

    def _normalize_missing_values_early(self):
        """
        Normalize missing values to NaN early in the processing pipeline.

        This enables XArray's built-in missing value handling to work correctly
        during derivation calculations, eliminating the need for custom safe
        arithmetic operations.
        """
        try:
            from access_moppy.vocabulary_processors import CMIP6Vocabulary

            logger.debug(
                "Normalizing missing values to NaN for consistent processing..."
            )

            # Use the static method to normalize the entire dataset
            self.ds = CMIP6Vocabulary.normalize_dataset_missing_values(self.ds)

            logger.debug(
                "Missing values normalized to NaN - XArray will handle propagation correctly"
            )
        except ImportError:
            logger.warning(
                "Could not import CMIP6Vocabulary for missing value normalization"
            )
        except Exception as e:
            logger.warning("Could not normalize missing values early: %s", e)

    def standardize_missing_values(self):
        """
        Standardize missing values in the main variable to the active CMIP requirements.

        At this point, missing values should already be normalized to NaN from
        early processing, and XArray's built-in missing value propagation should
        have handled derivation calculations correctly. This method converts NaN
        to the final CMIP-compliant missing value.

        This is particularly important for:
        - Final CMIP compliance (converting NaN to the vocabulary missing value)
        - Ensuring consistent metadata attributes
        """
        if (
            hasattr(self, "vocab")
            and self.vocab
            and self.cmor_name in self.ds.data_vars
        ):
            mip_era = getattr(self.vocab, "mip_era", self.vocab.__class__.__name__)
            logger.debug(
                "Applying final %s missing value standardization for %s",
                mip_era,
                self.cmor_name,
            )

            # Get the main data variable
            data_var = self.ds[self.cmor_name]

            # At this point, data should have NaN for missing values
            # Convert only NaN to CMIP6 standard (don't convert other values)
            standardized_var = self.vocab.standardize_missing_values(
                data_var,
                convert_existing=False,  # Only convert NaN, preserve other values
            )

            # Update the dataset with the standardized variable
            self.ds[self.cmor_name] = standardized_var

            # Report the standardization
            missing_value = self.vocab.get_cmip_missing_value()
            logger.debug("Final %s missing value applied: %s", mip_era, missing_value)
        else:
            logger.warning(
                "Cannot standardize missing values for %s: vocabulary not available",
                self.cmor_name,
            )

    def update_attributes(self):
        raise NotImplementedError("Subclasses must implement update_attributes.")

    def reorder(self):
        def ordered(ds, core=("lat", "lon", "time", "height")):
            seen = set()
            order = []
            for name in core:
                if name in ds.variables:
                    order.append(name)
                    seen.add(name)
                bnds = f"{name}_bnds"
                if bnds in ds.variables:
                    order.append(bnds)
                    seen.add(bnds)
            for v in ds.variables:
                if v not in seen:
                    order.append(v)
            return ds[order]

        self.ds = ordered(self.ds)

    def _build_drs_path(self, attrs: Dict[str, str]) -> Path:
        """
        Build DRS path using the vocabulary class's controlled vocabulary specifications.
        """
        if not hasattr(self.vocab, "build_drs_path"):
            raise AttributeError(
                f"Vocabulary class {type(self.vocab).__name__} does not implement build_drs_path() method. "
                "Please ensure you are using a proper CMIP vocabulary class (CMIP6Vocabulary or CMIP7Vocabulary)."
            )

        return self.vocab.build_drs_path(self.drs_root, self.version_date)

    def _update_latest_symlink(self, versioned_path: Path):
        latest_link = versioned_path.parent / "latest"
        try:
            if latest_link.is_symlink() or latest_link.exists():
                latest_link.unlink()
            latest_link.symlink_to(versioned_path.name, target_is_directory=True)
        except Exception as e:
            logger.warning("Failed to update latest symlink at %s: %s", latest_link, e)

    def _finalize_staged_write(self, staged_path: Path, final_path: Path) -> None:
        """Move a file written to fast local staging (e.g. Gadi's ``$PBS_JOBFS``)
        to its final destination, verifying size to catch a truncated/failed move.
        """
        with _publication_slot():
            final_path.parent.mkdir(parents=True, exist_ok=True)
            staged_size = staged_path.stat().st_size
            shutil.move(str(staged_path), str(final_path))
            final_size = final_path.stat().st_size
        if final_size != staged_size:
            raise IOError(
                f"Staged file move verification failed for {final_path}: "
                f"staged size {staged_size} != final size {final_size}"
            )

    @staticmethod
    def _completion_marker(path: Path) -> Path:
        return path.parent / ".moppy_complete" / f"{path.name}.done"

    def _mark_output_complete(self, path: Path) -> None:
        marker = self._completion_marker(path)
        marker.parent.mkdir(parents=True, exist_ok=True)
        marker.write_text("complete\n", encoding="ascii")

    def _generate_qc_plots(self, source_path: Path) -> None:
        """Generate QC beside staged data, then publish completed plots."""
        split_index = self._split_write_index
        if split_index is not None and split_index > 0:
            return

        final_qc_dir = Path(self.output_path) / "qc_plots"
        qc_dir = (
            self.staging_path / "qc_plots"
            if self.staging_path is not None
            else final_qc_dir
        )
        result = generate_qc_plots(
            source_path,
            qc_dir=qc_dir,
            comparison_store=self.qc_comparison_store,
            preferred_member=self.qc_preferred_member,
            make_snapshot=True,
            make_timeseries=split_index is None,
        )
        if result is None or self.staging_path is None:
            return

        for staged_plot in qc_dir.glob(f"{source_path.stem}_*.png"):
            self._finalize_staged_write(staged_plot, final_qc_dir / staged_plot.name)

    def write(self):
        """Write the CMORised dataset to one or more NetCDF files.

        When ``split_years`` was supplied at construction time (or left at
        the default ``"auto"``), time-dependent datasets
        are split into consecutive chunks and each chunk is written to a
        separate file.  The filename time-range component reflects the actual
        first and last timestamps in each file, so filenames are automatically
        correct.

        For ``fx`` (fixed-field) variables, or when the dataset has no ``time``
        dimension, a single file is always written regardless of ``split_years``.

        ``first_write_hook``, when set, runs as soon as the first file has been
        published and before any remaining split is written, so a caller can
        stop the write on a bad first file instead of after the whole time
        series.

        See Also
        --------
        DEFAULT_CHUNK_YEARS : the default chunk lengths used by ``split_years="auto"``.
        """
        self.written_files = []
        self.qc_gates = {}
        self.output_summary = self._summarise_written_files()
        effective_split = self._resolve_split_years()
        if (
            effective_split is not None
            and "time" in self.ds.dims
            and not self._is_fx_variable()
        ):
            original_ds = self.ds
            try:
                for split_index, chunk_ds in enumerate(
                    self._iter_time_chunks(original_ds, effective_split)
                ):
                    self._split_write_index = split_index
                    self.ds = chunk_ds
                    self._write_single()
                    if split_index == 0:
                        self._run_first_write_hook()
            finally:
                self.ds = original_ds
                self._split_write_index = None
            self._update_output_summary()
            return
        self._write_single()
        self._run_first_write_hook()
        self._update_output_summary()

    def _run_first_write_hook(self) -> None:
        """Hand the first published file to ``first_write_hook``, if set."""
        if self.first_write_hook is not None:
            self.first_write_hook(self.written_files[0])

    @staticmethod
    def _format_bytes(num_bytes: int) -> str:
        """Return a compact binary-unit size string for *num_bytes*."""
        value = float(num_bytes)
        for unit in ("bytes", "KiB", "MiB", "GiB", "TiB", "PiB"):
            if abs(value) < 1024.0 or unit == "PiB":
                if unit == "bytes":
                    return f"{num_bytes} bytes"
                return f"{value:.1f} {unit}"
            value /= 1024.0
        return f"{value:.1f} PiB"

    def _summarise_written_files(self) -> dict[str, object]:
        """Summarise final output files written by this CMORiser."""
        files: list[dict[str, object]] = []
        total_bytes = 0
        for path in self.written_files:
            output_path = Path(path)
            try:
                size_bytes = output_path.stat().st_size
            except FileNotFoundError:
                size_bytes = 0
            total_bytes += size_bytes
            files.append(
                {
                    "path": str(output_path),
                    "size_bytes": size_bytes,
                    "size": self._format_bytes(size_bytes),
                }
            )
        summary: dict[str, object] = {
            "file_count": len(self.written_files),
            "total_bytes": total_bytes,
            "total_size": self._format_bytes(total_bytes),
            "files": files,
        }
        if self.qc_gates:
            summary["gates"] = {
                name: dict(record) for name, record in self.qc_gates.items()
            }
        return summary

    def _update_output_summary(self) -> dict[str, object]:
        """Refresh and log the final output file summary."""
        self.output_summary = self._summarise_written_files()
        logger.info(
            "Produced %s: %d files, %s",
            self.compound_name,
            self.output_summary["file_count"],
            self.output_summary["total_size"],
        )
        return self.output_summary

    def _resolve_split_years(self) -> Optional[int]:
        """Return the effective number of years per output file.

        Returns ``None`` when no splitting should be applied.

        Raises
        ------
        ValueError
            If ``self.split_years`` is not ``None``, ``"auto"``, or a positive
            integer.
        """
        raw = self.split_years
        if raw is None:
            return None
        if raw == "auto":
            freq_key = _canonical_frequency(self.compound_name)
            return DEFAULT_CHUNK_YEARS.get(freq_key)
        if isinstance(raw, int):
            if raw <= 0:
                raise ValueError(f"split_years must be a positive integer, got {raw!r}")
            return raw
        raise ValueError(
            f"split_years must be None, 'auto', or a positive integer, got {raw!r}"
        )

    def _iter_time_chunks(self, ds: xr.Dataset, split_years: int):
        """Yield successive time slices of *ds* each spanning at most *split_years* years.

        Slices are determined by grouping timesteps whose year satisfies
        ``floor(year / split_years) * split_years == chunk_start``, so chunk
        boundaries are always aligned to calendar-year multiples of
        *split_years* (e.g. 1850–1854, 1855–1859, … for ``split_years=5``).

        Parameters
        ----------
        ds:
            Dataset to slice.  Must have a ``time`` dimension.
        split_years:
            Maximum number of calendar years per chunk.

        Yields
        ------
        xr.Dataset
            A view of *ds* containing only the timesteps belonging to one chunk.
        """
        time_vals = ds.time.values
        sample = time_vals.flat[0] if hasattr(time_vals, "flat") else time_vals[0]

        if hasattr(sample, "year"):
            # cftime or datetime objects
            years = np.array([t.year for t in time_vals])
        elif np.issubdtype(time_vals.dtype, np.datetime64):
            years = pd.DatetimeIndex(time_vals).year.to_numpy()
        else:
            # Numeric time coordinate (decode_cf=False).  Decode years from
            # the CF units/calendar attrs.  If units is absent (should not
            # happen on a CMORised dataset) fall back to a single file so the
            # caller still gets valid output.
            units = ds.time.attrs.get("units", "")
            if not units:
                logger.debug(
                    "Numeric time coordinate has no 'units' attribute; "
                    "skipping file splitting."
                )
                yield ds
                return
            calendar = ds.time.attrs.get("calendar", "standard")
            decoded = cftime.num2date(time_vals, units=units, calendar=calendar)
            years = np.array([t.year for t in decoded])

        chunk_ids = (years // split_years) * split_years
        for chunk_start in np.unique(chunk_ids):
            indices = np.where(chunk_ids == chunk_start)[0]
            yield ds.isel(time=indices)

    def _write_dask_slices(self, destination, vdat, chunk_sizes):
        """Compute bounded slices concurrently and write them serially."""
        chunk_ranges = [
            range(0, vdat.sizes[dim], int(chunk_sizes[dim])) for dim in vdat.dims
        ]

        def iter_slices():
            for starts in product(*chunk_ranges):
                yield tuple(
                    slice(
                        start,
                        min(start + int(chunk_sizes[dim]), vdat.sizes[dim]),
                    )
                    for dim, start in zip(vdat.dims, starts)
                )

        try:
            client = get_client() if self.write_prefetch > 1 else None
        except ValueError:
            client = None

        if client is None:
            for slices in iter_slices():
                indexers = dict(zip(vdat.dims, slices))
                destination[slices] = vdat.isel(indexers).values
            return

        pending = deque()

        def write_next():
            slices, future = pending.popleft()
            try:
                destination[slices] = future.result()
            finally:
                future.release()

        try:
            for slices in iter_slices():
                indexers = dict(zip(vdat.dims, slices))
                sliced_data = vdat.isel(indexers).data
                culled_graph = sliced_data.dask.cull(
                    flatten(sliced_data.__dask_keys__())
                )
                sliced_data = da.Array(
                    culled_graph,
                    sliced_data.name,
                    sliced_data.chunks,
                    dtype=sliced_data.dtype,
                    meta=sliced_data._meta,
                )
                # Optimizing each view independently can rewrite shared
                # open/rechunk keys with different task specifications. Explicit
                # culling keeps submissions bounded without changing those tasks.
                future = client.compute(
                    sliced_data,
                    optimize_graph=False,
                )
                pending.append((slices, future))
                if len(pending) >= self.write_prefetch:
                    write_next()

            while pending:
                write_next()
        finally:
            for _, future in pending:
                future.release()

    def _write_single(self):
        """
        Write the CMORised dataset to an intermediate NetCDF4 file.

        ``DatasetChunker`` controls the Dask tasks and slices materialized by
        the manual write loop; it does not define final NetCDF/HDF5 storage
        chunks because variable creation does not pass ``chunksizes``. For
        CMIP7, the file is written without compression and ``cmip7repack`` then
        owns final storage chunking, shuffle/zlib/Fletcher32 filters, and HDF5
        metadata collation. Non-CMIP7 writes may apply the configured filters,
        but MOPPy does not impose a publication-quality storage chunk layout.

        Variable definitions and attributes are created before data is written
        so the manual data assignments can remain bounded and predictable.

        Automatically handles character/string coordinates with proper NetCDF encoding.
        """
        # ========== Normalize CF Time-Coordinate Units ==========
        # Source models differ in how they write the reference datetime: the UM
        # atmosphere and CABLE land models omit the seconds field (e.g.
        # "days since 0001-01-01 00:00"), which fails the WCRP units pattern
        # check, whereas the ocean/sea-ice models write the full HH:MM:SS form.
        # Re-emit any CF time units in canonical form here — the sole write path
        # for every realm — so all variables are normalized uniformly.  This is
        # meaning-preserving, so the numeric time values are unaffected.
        for var in self.ds.variables:
            units = self.ds[var].attrs.get("units")
            normalized = normalize_cf_time_units(units)
            if normalized != units:
                self.ds[var].attrs["units"] = normalized
                logger.debug("Normalized '%s' units: %r -> %r", var, units, normalized)

        # Align point-sampled sub-daily timestamps (e.g. 3hrPt/6hrPlevPt)
        # to WCRP TIME001 square-grid expectations before writing.
        self._align_subdaily_point_time_to_square_grid()

        # ========== Prepare String Coordinates ==========
        # Detect and prepare all string/character coordinates before writing
        string_coords_info = self._prepare_string_coordinates()

        # Extract auxiliary coordinates that need to be declared in the 'coordinates' attribute
        # This includes: 1) scalar coordinates, 2) non-dimension coordinates
        aux_coords = []
        for name, info in string_coords_info.items():
            # Scalar coordinates or non-dimension coordinates must be declared in coordinates attribute
            if info["is_scalar"] or name not in self.ds.dims:
                aux_coords.append(name)

        # Also include non-string scalar coordinates (e.g. float 'height')
        for coord_name in self.ds.coords:
            coord = self.ds[coord_name]
            is_scalar = coord.ndim == 0
            is_non_dim = coord_name not in self.ds.dims
            if (is_scalar or is_non_dim) and coord_name not in aux_coords:
                aux_coords.append(coord_name)

        attrs = self.ds.attrs

        # Get required attributes from the vocabulary (works for both CMIP6 and CMIP7)
        required_keys = self.vocab.get_required_attribute_names()

        missing = [k for k in required_keys if k not in attrs]
        if missing:
            logger.warning(
                "Missing required global attributes: %s. "
                "Some attributes may be required for CMIP compliance but file will still be written.",
                missing,
            )

        # ========== Chunked vs Eager Write Decision ==========
        # Use chunked writing only when the main variable is dask-backed and a
        # chunker is configured.  For dask arrays, memory is managed by the
        # dask scheduler; a system-level psutil check is not meaningful there.
        main_var = self.ds[self.cmor_name]
        is_dask_array = isinstance(main_var.data, da.Array)
        use_chunked_write = is_dask_array and self.chunker is not None

        if use_chunked_write:
            logger.debug("Using chunked writing with DatasetChunker")
        else:
            # Eager write: estimate size and guard against OOM before starting.
            def estimate_data_size(ds):
                total_size = 0
                for var in ds.variables:
                    vdat = ds[var]
                    var_size = vdat.dtype.itemsize
                    for dim in vdat.dims:
                        var_size *= ds.sizes[dim]
                    total_size += var_size
                return int(total_size * 1.5)

            data_size = estimate_data_size(self.ds)
            available_memory = psutil.virtual_memory().available

            if data_size > available_memory:
                raise MemoryError(
                    f"Data size ({data_size / 1024**3:.2f} GB) exceeds available system memory "
                    f"({available_memory / 1024**3:.2f} GB). "
                    f"Enable chunking or reduce dataset size."
                )
            logger.debug(
                "Data size: %.2f GB, Available memory: %.2f GB",
                data_size / 1024**3,
                available_memory / 1024**3,
            )

        # Generate filename using vocabulary-specific logic.
        # For CMIP7 runs, the CMORiser may carry a CMIP6 compound name while
        # the vocabulary stores the CMIP7-mapped compound name used to derive
        # variable_id/branding metadata. Prefer the vocabulary compound name
        # so filename components stay consistent with global attributes.
        filename_compound_name = getattr(
            self.vocab, "compound_name", self.compound_name
        )
        filename = self.vocab.generate_filename(
            attrs, self.ds, self.cmor_name, filename_compound_name
        )

        if self.drs_root:
            drs_path = self._build_drs_path(attrs)
            drs_path.mkdir(parents=True, exist_ok=True)
            final_path = drs_path / filename
            self._update_latest_symlink(drs_path)
        else:
            final_path = Path(self.output_path) / filename
            final_path.parent.mkdir(parents=True, exist_ok=True)

        if self.staging_path is not None:
            self.staging_path.mkdir(parents=True, exist_ok=True)
            write_path = self.staging_path / filename
        else:
            write_path = final_path

        self._completion_marker(final_path).unlink(missing_ok=True)

        with nc.Dataset(write_path, "w", format="NETCDF4") as dst:
            # Set global attributes
            for k, v in attrs.items():
                dst.setncattr(k, v)

            # Create dimensions
            for dim, size in self.ds.sizes.items():
                if dim == "time":
                    dst.createDimension(dim, None)  # Unlimited dimension
                else:
                    dst.createDimension(dim, size)

            # Create string length dimensions for character coordinates
            for coord_name, info in string_coords_info.items():
                strlen_dim = info["strlen_dim"]
                strlen_size = info["strlen_size"]
                if strlen_dim not in dst.dimensions:
                    dst.createDimension(strlen_dim, strlen_size)

            # PHASE 1: Create all variables and set their attributes. No
            # chunksizes are passed here: DatasetChunker sizes the later write
            # operations, not the NetCDF/HDF5 storage chunks.
            created_vars = {}
            # Cache decoded-time flag per variable so PHASE 2 never re-materialises.
            decoded_time_vars = {}
            for var in self.ds.variables:
                vdat = self.ds[var]

                # Check if this is a string coordinate
                if var in string_coords_info:
                    v = self._create_string_variable(
                        dst, var, vdat, string_coords_info[var]
                    )
                    created_vars[var] = v
                else:
                    # Regular variable creation
                    # CF §7.1: bounds variables must not have _FillValue — pass
                    # fill_value=False to explicitly suppress netCDF4's default.
                    if var.endswith("_bnds"):
                        fill = False
                    else:
                        fill = vdat.attrs.get("_FillValue")

                    # Decoded time coordinates (datetime64 or cftime) must be stored
                    # as float64 in netCDF4; use "f8" instead of str(vdat.dtype).
                    # For object-dtype arrays peek at a single element to avoid a
                    # full .compute() on potentially large dask arrays.
                    _is_decoded_time = np.issubdtype(vdat.dtype, np.datetime64) or (
                        vdat.dtype == object
                        and vdat.size > 0
                        and hasattr(
                            vdat.isel({d: 0 for d in vdat.dims}).values.flat[0],
                            "year",
                        )
                    )
                    decoded_time_vars[var] = _is_decoded_time
                    nc_dtype = "f8" if _is_decoded_time else str(vdat.dtype)

                    # Apply configured filters to non-CMIP7 time-dependent
                    # data. CMIP7 filters and storage layout are deferred to
                    # cmip7repack after this intermediate file is closed.
                    use_compression = (
                        self.enable_compression
                        and getattr(self.vocab, "mip_era", None) != "CMIP7"
                        and "time" in vdat.dims
                        and not var.endswith("_bnds")
                    )

                    if fill is False:
                        # Explicitly suppress fill value (bounds vars — CF §7.1)
                        v = dst.createVariable(
                            var,
                            nc_dtype,
                            vdat.dims,
                            fill_value=False,
                            shuffle=use_compression,
                            zlib=use_compression,
                            complevel=self.compression_level if use_compression else 0,
                            fletcher32=use_compression,
                        )
                    elif fill:
                        v = dst.createVariable(
                            var,
                            nc_dtype,
                            vdat.dims,
                            fill_value=fill,
                            shuffle=use_compression,
                            zlib=use_compression,
                            complevel=self.compression_level if use_compression else 0,
                            fletcher32=use_compression,
                        )
                    else:
                        v = dst.createVariable(
                            var,
                            nc_dtype,
                            vdat.dims,
                            shuffle=use_compression,
                            zlib=use_compression,
                            complevel=self.compression_level if use_compression else 0,
                            fletcher32=use_compression,
                        )

                    # Set attributes
                    for a, val in vdat.attrs.items():
                        if a == "_FillValue":
                            continue
                        # CF §7.1: bounds variables themselves must not carry a
                        # "bounds" or stale "coordinates" attribute; but the parent
                        # coordinate must keep its "bounds" pointer to the _bnds var.
                        if var.endswith("_bnds") and a in ("bounds", "coordinates"):
                            continue
                        v.setncattr(a, val)

                        # ========== Set coordinates attribute for main data variable ==========
                        # CF compliance: auxiliary coordinates (scalar and non-dimension
                        # coords) must be declared in the main data variable's
                        # 'coordinates' attribute. Tokens inherited from the raw input
                        # that no longer refer to a variable in the dataset (e.g. a stale
                        # 'height_0' carried over from UM output) are dropped to avoid
                        # emitting dangling coordinate references.
                        if var == self.cmor_name:
                            existing_tokens = vdat.attrs.get("coordinates", "").split()
                            valid_existing = [
                                t for t in existing_tokens if t in self.ds.variables
                            ]
                            merged = list(
                                dict.fromkeys(valid_existing + list(aux_coords))
                            )
                            if merged:
                                new_coords = " ".join(merged)
                                v.setncattr("coordinates", new_coords)
                                logger.debug(
                                    "  Set coordinates attribute on '%s': '%s'",
                                    var,
                                    new_coords,
                                )
                            elif "coordinates" in v.ncattrs():
                                # All inherited tokens were stale and no aux coords —
                                # drop the attribute rather than leave a dangling ref.
                                v.delncattr("coordinates")

                    created_vars[var] = v

            # Force NetCDF to write all metadata/B-tree information
            dst.sync()

            # PHASE 2: Write actual data chunks
            # Now all B-tree metadata is written, data chunks come after
            for var in self.ds.variables:
                vdat = self.ds[var]

                # Check if this is a string coordinate
                if var in string_coords_info:
                    self._write_string_variable(
                        created_vars[var], vdat, string_coords_info[var]
                    )
                else:
                    # Regular variable writing
                    is_var_dask = isinstance(vdat.data, da.Array)
                    has_time_dim = "time" in vdat.dims

                    if use_chunked_write and is_var_dask and has_time_dim:
                        # Bound each computed and assigned slice in memory. These
                        # sizes do not determine the variable's storage chunks.
                        chunk_sizes = self.chunker.calculate_chunk_size_for_variable(
                            vdat
                        )
                        logger.debug(
                            "  Writing %s (%d timesteps/chunk; chunks: %s; prefetch: %d)...",
                            var,
                            chunk_sizes["time"],
                            chunk_sizes,
                            self.write_prefetch,
                        )
                        self._write_dask_slices(created_vars[var], vdat, chunk_sizes)

                        logger.debug(
                            "    %s: %d timesteps written",
                            var,
                            vdat.sizes["time"],
                        )
                    else:
                        # Direct write for small/non-Dask/non-time variables
                        # Encode decoded time back to numeric float64 for netCDF4
                        # Reuse the flag cached during PHASE 1 — no extra compute.
                        _is_decoded_time = decoded_time_vars.get(var, False)
                        if _is_decoded_time:
                            units = vdat.attrs.get("units") or vdat.encoding.get(
                                "units"
                            )
                            if units is None:
                                import warnings

                                warnings.warn(
                                    f"Variable '{var}' has no 'units' in attrs or encoding; "
                                    "defaulting to 'days since 1850-01-01 00:00:00'",
                                    UserWarning,
                                    stacklevel=2,
                                )
                                units = "days since 1850-01-01 00:00:00"
                            calendar = vdat.attrs.get("calendar") or vdat.encoding.get(
                                "calendar", "standard"
                            )

                            if np.issubdtype(vdat.dtype, np.datetime64):
                                import pandas as pd

                                raw = pd.DatetimeIndex(vdat.values).to_pydatetime()
                            else:
                                raw = vdat.values
                            created_vars[var][:] = date2num(
                                raw, units=units, calendar=calendar
                            )
                        else:
                            created_vars[var][:] = vdat.values

        try:
            self._repack_cmip7_output(write_path)
            if self.enable_qc_plots:
                self._generate_qc_plots(write_path)
            if getattr(self.vocab, "mip_era", None) == "CMIP7":
                self._record_range_gate(validate_cmip7_output(write_path))
        finally:
            if self.staging_path is not None:
                self._finalize_staged_write(write_path, final_path)

        self.written_files.append(final_path)
        self._mark_output_complete(final_path)
        logger.info("CMORised output written to %s", final_path)
        logger.debug("Completed bounded two-phase NetCDF write")
        if self.enable_compression and getattr(self.vocab, "mip_era", None) != "CMIP7":
            logger.debug(
                "HDF5 compression: shuffle + zlib(level %d) + fletcher32 for data variables",
                self.compression_level,
            )
        elif getattr(self.vocab, "mip_era", None) == "CMIP7":
            logger.debug("Compression deferred to cmip7repack")
        else:
            logger.debug("Compression disabled")

        if string_coords_info:
            logger.debug(
                "String coordinates processed: %s", ", ".join(string_coords_info.keys())
            )

    #: Gate outcomes ordered worst first. A variable that writes several files
    #: keeps the worst result each gate produced across them.
    _GATE_SEVERITY = ("fail", "warn", "pass")

    @classmethod
    def merge_gate_results(
        cls, records: Iterable[Mapping[str, Mapping[str, object]]]
    ) -> dict[str, dict[str, object]]:
        """Combine several files' gate results, keeping the worst of each gate.

        Used to fold the per-file results of a split time series — or of the
        several CMORisers a batch worker runs for one variable — into the
        single set of outcomes recorded against that variable.
        """
        merged: dict[str, dict[str, object]] = {}
        for record in records:
            for name, gate in (record or {}).items():
                if not isinstance(gate, Mapping):
                    continue
                current = merged.get(name)
                if current is None or cls._is_worse(gate, current):
                    merged[name] = dict(gate)
        return merged

    @classmethod
    def _is_worse(
        cls, candidate: Mapping[str, object], current: Mapping[str, object]
    ) -> bool:
        """Whether *candidate* is a more severe gate outcome than *current*."""

        def rank(gate: Mapping[str, object]) -> int:
            result = gate.get("result")
            if result in cls._GATE_SEVERITY:
                return cls._GATE_SEVERITY.index(str(result))
            return len(cls._GATE_SEVERITY)

        return rank(candidate) < rank(current)

    def _record_gate(self, name: str, result: str, **detail: object) -> None:
        """Record one gate outcome, keeping the worst seen for this variable."""
        record: dict[str, object] = {"result": result, **detail}
        current = self.qc_gates.get(name)
        if current is None or self._is_worse(record, current):
            self.qc_gates[name] = record

    def _record_range_gate(self, result: object) -> None:
        """Record the value-range gate from a validator result."""
        if not isinstance(result, ValidationResult):
            # A test double, or a caller that patched the validator out.
            return
        record: dict[str, object] = {"check_id": "cmip7_ranges"}
        if result.warning:
            record["result"] = "warn"
            record["message"] = result.warning
        else:
            record["result"] = "pass"
        if result.observed_min is not None and result.observed_max is not None:
            record["observed"] = [
                float(result.observed_min),
                float(result.observed_max),
            ]
        if result.allowed_min is not None and result.allowed_max is not None:
            record["allowed"] = [float(result.allowed_min), float(result.allowed_max)]
        if result.units:
            record["units"] = result.units
        self._record_gate("range", str(record.pop("result")), **record)

    def _repack_cmip7_output(self, path: Path):
        """Repack a CMIP7 netCDF file in place after writing it."""
        if getattr(self.vocab, "mip_era", None) != "CMIP7":
            return

        cmd = ["cmip7repack", "-o", str(path)]
        logger.info("Repacking CMIP7 output with cmip7repack: %s", path)

        try:
            subprocess.run(  # noqa: S603  # nosec B603
                cmd,
                check=True,
                capture_output=True,
                text=True,
            )
        except FileNotFoundError as exc:
            raise RuntimeError(
                "cmip7repack is required to repack CMIP7 netCDF output but was not found on PATH"
            ) from exc
        except subprocess.CalledProcessError as exc:
            logger.error(
                "cmip7repack failed for %s: %s%s",
                path,
                exc.stdout or "",
                exc.stderr or "",
            )
            raise

        # A repack failure aborts the variable, so reaching here means the file
        # was repacked. Record it: without this, a downstream reader can only
        # infer the repack from the task not having failed.
        self._record_gate("repack", "pass", tool="cmip7repack")

    def _prepare_string_coordinates(self):
        """
        Detect and prepare all string/character coordinates in the dataset.

        Returns:
            dict: Information about each string coordinate including:
                - strlen_dim: name of the string length dimension
                - strlen_size: size of the string length dimension
                - values: converted byte string values
                - is_scalar: whether this is a scalar coordinate
                - dims: original dimensions of the coordinate
        """
        string_coords_info = {}

        for coord_name in self.ds.coords:
            coord = self.ds[coord_name]

            # Check if this is a string/character type
            # dtype.kind: 'S' = byte string, 'U' = unicode string, 'O' = object (often strings)
            # Exclude cftime objects (dtype=object but have .year attribute - they are time, not strings)
            if coord.dtype.kind in ("S", "U", "O"):
                if (
                    coord.dtype.kind == "O"
                    and coord.size > 0
                    and hasattr(coord.values.flat[0], "year")
                ):
                    continue
                info = {}

                # Determine if this is a scalar or array coordinate
                is_scalar = coord.ndim == 0
                info["is_scalar"] = is_scalar
                info["dims"] = coord.dims

                # Convert to byte strings if needed
                if coord.dtype.kind == "S":
                    # Already byte strings
                    values = coord.values
                    if is_scalar:
                        # Scalar: single byte string
                        max_len = (
                            len(values)
                            if isinstance(values, bytes)
                            else values.dtype.itemsize
                        )
                    else:
                        # Array: find max length
                        max_len = max(len(s) for s in values.flat)
                else:
                    # Unicode or object - convert to byte strings
                    if is_scalar:
                        str_val = str(
                            coord.values.item()
                            if hasattr(coord.values, "item")
                            else coord.values
                        )
                        max_len = len(str_val)
                        values = str_val.encode("utf-8")
                    else:
                        # Handle array of strings — materialise as a list so the
                        # iterator is not exhausted by max() before encode step
                        str_values = [str(s) for s in coord.values.flat]

                        # NetCDF fixed-width byte strings must have a width of at
                        # least 1, including when all values are empty strings.
                        max_len = max(1, max((len(s) for s in str_values), default=0))
                        values = np.array(
                            [s.encode("utf-8") for s in str_values], dtype=f"S{max_len}"
                        )

                        # Reshape to original shape if needed
                        if coord.ndim > 0:
                            values = values.reshape(coord.shape)

                # Ensure values is in proper format for netCDF4.stringtochar
                if is_scalar and not isinstance(values, np.ndarray):
                    values = np.array(values, dtype=f"S{max_len}")

                info["strlen_dim"] = f"{coord_name}_strlen"
                info["strlen_size"] = max_len
                info["values"] = values

                string_coords_info[coord_name] = info

                logger.debug(
                    "Detected string coordinate '%s': max_len=%d, shape=%s, dims=%s",
                    coord_name,
                    max_len,
                    coord.shape,
                    coord.dims,
                )

        return string_coords_info

    def _create_string_variable(self, dst, var_name, vdat, string_info):
        """
        Create a NetCDF variable for a string coordinate with proper encoding.

        Args:
            dst: NetCDF4 Dataset object
            var_name: Name of the variable
            vdat: xarray DataArray
            string_info: Dictionary with string coordinate information

        Returns:
            NetCDF4 Variable object
        """
        strlen_dim = string_info["strlen_dim"]
        is_scalar = string_info["is_scalar"]

        # Build dimensions tuple
        if is_scalar:
            # Scalar coordinate: only strlen dimension
            dims = (strlen_dim,)
        else:
            # Array coordinate: original dims + strlen dimension
            dims = tuple(string_info["dims"]) + (strlen_dim,)

        # Create variable with 'S1' dtype (single character)
        v = dst.createVariable(
            var_name,
            "S1",
            dims,
            fill_value=None,  # Character coordinates typically don't have fill values
        )

        # Set attributes (excluding _FillValue)
        for attr_name, attr_val in vdat.attrs.items():
            if attr_name != "_FillValue":
                v.setncattr(attr_name, attr_val)

        logger.debug("  Created string variable '%s' with dims: %s", var_name, dims)

        return v

    def _write_string_variable(self, nc_var, vdat, string_info):
        """
        Write string data using CF-compliant character array encoding.

        Manually converts strings to character arrays to avoid version-specific
        behavior in nc.stringtochar() between Python 3.11 and 3.13.

        Args:
            nc_var: NetCDF variable to write to
            vdat: xarray variable (not used, for signature consistency)
            string_info: Dictionary with string coordinate metadata
        """
        values = string_info["values"]
        is_scalar = string_info["is_scalar"]
        strlen_size = string_info["strlen_size"]

        if is_scalar:
            # Extract scalar value if it's a 0-dimensional array
            if isinstance(values, np.ndarray) and values.ndim == 0:
                scalar_val = values.item()
            else:
                scalar_val = values

            # Convert to bytes if unicode
            if isinstance(scalar_val, str):
                scalar_val = scalar_val.encode("utf-8")
            elif not isinstance(scalar_val, bytes):
                # Handle numpy.bytes_ or other types
                scalar_val = bytes(scalar_val)

            # Manually create character array (avoid nc.stringtochar)
            char_array = np.zeros(strlen_size, dtype="S1")
            for i in range(min(len(scalar_val), strlen_size)):
                char_array[i] = scalar_val[i : i + 1]

            nc_var[:] = char_array

        else:
            # Array case: manually create 2D character array
            # First ensure we have bytes
            flat_values = []
            for val in values.flat:
                if isinstance(val, str):
                    flat_values.append(val.encode("utf-8"))
                elif isinstance(val, bytes):
                    flat_values.append(val)
                else:
                    # Handle numpy.bytes_ or other types
                    flat_values.append(bytes(val))

            values_bytes = np.array(flat_values).reshape(values.shape)

            # Create character array with shape (n_strings, strlen)
            shape = values_bytes.shape + (strlen_size,)
            char_array = np.zeros(shape, dtype="S1")

            # Fill character array manually
            for idx in np.ndindex(values_bytes.shape):
                byte_str = values_bytes[idx]
                for i in range(min(len(byte_str), strlen_size)):
                    char_array[idx + (i,)] = byte_str[i : i + 1]

            nc_var[:] = char_array

        logger.debug("  Written string data for '%s'", nc_var.name)

    def run(self, write_output: bool = False):
        self.select_and_process_variables()
        self.drop_intermediates()
        # Standardize missing values to CMIP6 requirements after processing
        self.standardize_missing_values()
        self.update_attributes()
        self.reorder()
        # Final rechunking before writing for optimal I/O performance
        if write_output:
            self.rechunk_dataset()
            self.write()
