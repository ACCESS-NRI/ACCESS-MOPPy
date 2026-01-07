import warnings
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import cftime
import dask.array as da
import netCDF4 as nc
import psutil
import xarray as xr
from cftime import date2num, num2date
from dask.distributed import get_client

from access_moppy.utilities import (
    FrequencyMismatchError,
    IncompatibleFrequencyError,
    ResamplingRequiredWarning,
    type_mapping,
    validate_and_resample_if_needed,
    validate_cmip6_frequency_compatibility,
)


class DatasetChunker:
    """
    Handles rechunking of xarray datasets according to rules introduced in the CMIP7 standard.

    Rules:
    - Time coordinates: single chunk (no chunking along time for coordinates)
    - Time bounds: single chunk (no chunking along time for bounds)
    - Data variables: chunked into at least 4MB blocks
    """

    def __init__(self, target_chunk_size_mb: float = 4.0):
        """
        Initialize the DatasetChunker.

        Args:
            target_chunk_size_mb: Target chunk size in megabytes for data variables
        """
        self.target_chunk_size_mb = target_chunk_size_mb
        self.target_chunk_size_bytes = target_chunk_size_mb * 1024 * 1024

    def calculate_chunk_size_for_variable(self, var: xr.DataArray) -> Dict[str, int]:
        """
        Calculate appropriate chunk sizes for a variable to achieve at least 4MB chunks.

        Args:
            var: xarray DataArray

        Returns:
            Dictionary of dimension names to chunk sizes
        """
        chunks = {}

        # Calculate total elements per chunk needed for minimum target size
        element_size = var.dtype.itemsize
        min_target_elements = self.target_chunk_size_bytes // element_size

        # For time-dependent variables, start with time dimension
        if "time" in var.dims:
            time_size = var.sizes["time"]

            # Calculate elements in other dimensions (spatial elements per time step)
            other_elements = 1
            for dim in var.dims:
                if dim != "time":
                    other_elements *= var.sizes[dim]

            # Determine minimum time steps needed for at least 4MB
            if other_elements > 0:
                # Calculate minimum time steps needed
                min_time_steps = max(
                    1, (min_target_elements + other_elements - 1) // other_elements
                )  # Ceiling division
                # Don't exceed available time steps
                time_chunks = min(time_size, min_time_steps)
            else:
                time_chunks = time_size

            chunks["time"] = time_chunks

            # Other dimensions: keep as single chunks for simplicity
            for dim in var.dims:
                if dim != "time":
                    chunks[dim] = var.sizes[dim]
        else:
            # Non-time variables: keep as single chunks
            for dim in var.dims:
                chunks[dim] = var.sizes[dim]

        return chunks

    def rechunk_dataset(self, ds: xr.Dataset) -> xr.Dataset:
        """
        Apply chunking rules to rechunk the dataset.

        Args:
            ds: Input xarray Dataset

        Returns:
            Rechunked xarray Dataset
        """
        if not hasattr(ds, "chunks") or not any(
            ds.chunks.values() if ds.chunks else []
        ):
            print("Dataset is not chunked, skipping rechunking")
            return ds

        print("🔧 Applying dataset rechunking with rules:")
        print("  - Time coordinates: single chunk")
        print("  - Time bounds: single chunk")
        print(f"  - Data variables: at least {self.target_chunk_size_mb}MB chunks")

        rechunked_vars = {}

        for var_name in ds.variables:
            var = ds[var_name]

            # Apply chunking rules based on variable type
            if var_name.endswith("_bnds") or var_name.endswith("_bounds"):
                # Time bounds: single chunk for all dimensions
                chunks = {dim: var.sizes[dim] for dim in var.dims}
                print(f"  {var_name}: time bounds → single chunk")

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
                    print(f"  {var_name}: coordinate → single chunk")

            else:
                # Data variables: calculate 4MB chunks
                chunks = self.calculate_chunk_size_for_variable(var)
                chunk_info = ", ".join(
                    [f"{dim}:{size}" for dim, size in chunks.items()]
                )
                print(f"  {var_name}: data variable → {chunk_info}")

            try:
                rechunked_vars[var_name] = var.chunk(chunks)
            except Exception as e:
                print(f"Warning: Could not rechunk variable '{var_name}': {e}")
                rechunked_vars[var_name] = var

        # Reconstruct dataset with rechunked variables
        rechunked_ds = xr.Dataset(rechunked_vars, attrs=ds.attrs)
        print("✅ Dataset rechunking completed")

        return rechunked_ds


class CMIP6_CMORiser:
    """
    Base class for CMIP6 CMORisers, providing shared logic for CMORisation.
    """

    type_mapping = type_mapping

    def __init__(
        self,
        input_data: Optional[Union[str, List[str], xr.Dataset, xr.DataArray]] = None,
        *,
        output_path: str,
        cmip6_vocab: Any,
        variable_mapping: Dict[str, Any],
        compound_name: str,
        drs_root: Optional[Path] = None,
        validate_frequency: bool = False,
        enable_resampling: bool = False,
        resampling_method: str = "auto",
        enable_chunking: bool = True,
        chunk_size_mb: float = 4.0,
        enable_compression: bool = True,
        compression_level: int = 4,
        # Backward compatibility
        input_paths: Optional[Union[str, List[str]]] = None,
    ):
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
        self.vocab = cmip6_vocab
        self.mapping = variable_mapping
        self.drs_root = Path(drs_root) if drs_root is not None else None
        self.version_date = datetime.now().strftime("%Y%m%d")
        self.validate_frequency = validate_frequency
        self.compound_name = compound_name
        self.enable_resampling = enable_resampling
        self.resampling_method = resampling_method
        self.enable_chunking = enable_chunking
        self.enable_compression = enable_compression
        self.compression_level = compression_level
        self.chunker = (
            DatasetChunker(
                target_chunk_size_mb=chunk_size_mb,
            )
            if enable_chunking
            else None
        )
        self.ds = None

    def __getitem__(self, key):
        return self.ds[key]

    def __getattr__(self, attr):
        # This is only called if the attr is not found on CMORiser itself
        return getattr(self.ds, attr)

    def __setitem__(self, key, value):
        self.ds[key] = value

    def __repr__(self):
        return repr(self.ds)

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

                # Step 2: Drop coordinates not in used_dims
                coords_to_drop = [c for c in self.ds.coords if c not in used_dims]

                if coords_to_drop:
                    self.ds = self.ds.drop_vars(coords_to_drop)
                    print(
                        f"✓ Dropped {len(coords_to_drop)} unused coordinate(s): {coords_to_drop}"
                    )

        else:
            # Original file-based loading logic
            def _preprocess(ds):
                return ds[list(required_vars & set(ds.data_vars))]

            # Validate frequency consistency and CMIP6 compatibility before concatenation
            if self.validate_frequency and len(self.input_paths) > 0:
                try:
                    # Enhanced validation with CMIP6 frequency compatibility
                    detected_freq, resampling_required = (
                        validate_cmip6_frequency_compatibility(
                            self.input_paths,
                            self.compound_name,
                            time_coord="time",
                            interactive=True,
                        )
                    )
                    if resampling_required:
                        print(
                            f"✓ Temporal resampling will be applied: {detected_freq} → CMIP6 target frequency"
                        )
                    else:
                        print(
                            f"✓ Validated compatible temporal frequency: {detected_freq}"
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

            self.ds = xr.open_mfdataset(
                self.input_paths,
                combine="nested",  # avoids costly dimension alignment
                concat_dim="time",
                engine="netcdf4",
                decode_cf=False,
                chunks={},
                preprocess=_preprocess,
                parallel=True,  # <--- enables concurrent preprocessing
            )

        # Apply temporal resampling if enabled and needed
        if self.enable_resampling and self.compound_name:
            try:
                print(
                    f"🔍 Checking if temporal resampling is needed for {self.cmor_name}..."
                )

                self.ds, was_resampled = validate_and_resample_if_needed(
                    self.ds,
                    self.compound_name,
                    self.cmor_name,
                    time_coord="time",
                    method=self.resampling_method,
                )

                if was_resampled:
                    print("✅ Applied temporal resampling to match CMIP6 requirements")
                else:
                    print("✅ No resampling needed - frequency already compatible")

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
            print("🔧 Applying intelligent dataset rechunking...")
            self.ds = self.chunker.rechunk_dataset(self.ds)
            print("✅ Dataset rechunking completed")

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
                first_val = coord.values.flat[0] if coord.values.size > 0 else None

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

                        print(
                            f"✓ Converted '{coord_name}' from cftime to numeric ({units}, {calendar})"
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
            # Clean up potential duplication
            self.ds = self.ds.sel(time=~self.ds.get_index("time").duplicated())

    def rechunk_dataset(self):
        """
        Apply intelligent rechunking to the dataset.

        This method can be called separately from load_dataset if rechunking
        is needed at a different stage in the processing pipeline.
        """
        if self.enable_chunking and self.chunker and self.ds is not None:
            print("🔧 Applying dataset rechunking...")
            self.ds = self.chunker.rechunk_dataset(self.ds)
            print("✅ Dataset rechunking completed")
        else:
            if not self.enable_chunking:
                print("Chunking is disabled, skipping rechunking")
            elif not self.chunker:
                print("No chunker available, skipping rechunking")
            else:
                print("No dataset loaded, cannot rechunk")

    def select_and_process_variables(self):
        raise NotImplementedError(
            "Subclasses must implement select_and_process_variables."
        )

    def _check_units(self, var: str, expected: str) -> bool:
        actual = self.ds[var].attrs.get("units")
        if "days since ?" in expected:
            return actual and actual.lower().startswith("days since")
        if actual and expected and actual != expected:
            raise ValueError(f"Mismatch units for {var}: {actual} != {expected}")
        return True

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
            raise ValueError(f"Failed calendar check for {var}: {e}")
        if calendar in ("noleap", "365_day"):
            for d in dates:
                if d.month == 2 and d.day == 29:
                    raise ValueError(f"{calendar} must not have 29 Feb: found {d}")
        elif calendar == "360_day":
            for d in dates:
                if d.day > 30:
                    raise ValueError(f"360_day calendar has day > 30: {d}")

    def _check_range(self, var: str, vmin: float, vmax: float):
        arr = self.ds[var]
        if hasattr(arr.data, "map_blocks"):
            too_small = (arr < vmin).any().compute()
            too_large = (arr > vmax).any().compute()
        else:
            too_small = (arr < vmin).any().item()
            too_large = (arr > vmax).any().item()
        if too_small:
            raise ValueError(f"Values of '{var}' below valid_min: {vmin}")
        if too_large:
            raise ValueError(f"Values of '{var}' above valid_max: {vmax}")

    def drop_intermediates(self):
        for var in self.mapping[self.cmor_name]["model_variables"]:
            if var in self.ds.data_vars and var != self.cmor_name:
                self.ds = self.ds.drop_vars(var)

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
        drs_components = [
            attrs.get("mip_era", "CMIP6"),
            attrs["activity_id"],
            attrs["institution_id"],
            attrs["source_id"],
            attrs["experiment_id"],
            attrs["variant_label"],
            attrs["table_id"],
            attrs["variable_id"],
            attrs["grid_label"],
            f"v{self.version_date}",
        ]
        return self.drs_root.joinpath(*drs_components)

    def _update_latest_symlink(self, versioned_path: Path):
        latest_link = versioned_path.parent / "latest"
        try:
            if latest_link.is_symlink() or latest_link.exists():
                latest_link.unlink()
            latest_link.symlink_to(versioned_path.name, target_is_directory=True)
        except Exception as e:
            print(f"Warning: Failed to update latest symlink at {latest_link}: {e}")

    def write(self):
        """
        Write the CMORised dataset to NetCDF file with optimized layout and compression.

        The write process is structured to ensure optimal NetCDF4/HDF5 file layout:
        1. Create all variable definitions and metadata first (B-tree fragments)
        2. Apply HDF5 optimization features to chunked data variables:
           - Shuffle filter: De-interlaces bytes to improve compression ratios
           - Zlib compression: Standard deflate compression algorithm
           - Fletcher32: Checksum algorithm for data integrity verification
        3. Force synchronization to ensure metadata is written
        4. Write actual data chunks after all metadata is complete

        This ensures that for each variable, its first data chunk appears later
        in the file than its last B-tree (metadata) fragment, improving read performance.
        Compression features are only applied to time-dependent data variables.
        """
        attrs = self.ds.attrs
        required_keys = [
            "variable_id",
            "table_id",
            "source_id",
            "experiment_id",
            "variant_label",
            "grid_label",
        ]
        missing = [k for k in required_keys if k not in attrs]
        if missing:
            raise ValueError(
                f"Missing required CMIP6 global attributes for filename: {missing}"
            )

        # ========== Memory Check ==========
        # This section estimates the data size and compares it against available memory
        # to prevent out-of-memory errors during the write operation.

        def estimate_data_size(ds, cmor_name):
            total_size = 0
            for var in ds.variables:
                vdat = ds[var]
                # Start with the size of a single element (e.g., 4 bytes for float32)
                var_size = vdat.dtype.itemsize
                # Multiply by the size of each dimension to get total elements
                for dim in vdat.dims:
                    var_size *= ds.sizes[dim]
                total_size += var_size
            # Apply 1.5x overhead factor for safe memory estimation
            return int(total_size * 1.5)

        # Calculate the estimated data size for this dataset
        data_size = estimate_data_size(self.ds, self.cmor_name)

        # Get system memory information using psutil
        available_memory = psutil.virtual_memory().available

        # ========== Dask Client Detection ==========
        # Check if a Dask distributed client exists, as this affects how we handle
        # memory management. Dask clusters have their own memory limits separate
        # from system memory.

        client = None
        worker_memory = None  # Memory limit of a single worker

        try:
            # Attempt to get an existing Dask client
            client = get_client()

            # Retrieve information about all workers in the cluster
            worker_info = client.scheduler_info()["workers"]

            if worker_info:
                # Get the minimum memory_limit across all workers
                worker_memory = min(w["memory_limit"] for w in worker_info.values())

        except ValueError:
            # No Dask client exists - we'll use local/system memory for writing
            pass

        # ========== Memory Validation Logic ==========
        # This section implements a decision tree based on data size vs available memory:

        if client is not None:
            # Dask client exists - check against cluster memory limits
            if data_size > worker_memory:
                # WARNING: Data fits in total cluster memory but exceeds single worker capacity
                print(
                    f"Warning: Data size ({data_size / 1024**3:.2f} GB) exceeds single worker memory "
                    f"({worker_memory / 1024**3:.2f} GB)."
                )
                print("Closing Dask client to use local memory for writing...")
                client.close()
                client = None
                # Refresh available memory after closing client
                available_memory = psutil.virtual_memory().available

        # Check if chunked writing is needed
        main_var = self.ds[self.cmor_name]
        is_dask_array = isinstance(main_var.data, da.Array)
        use_chunked_write = is_dask_array and self.chunker is not None

        if use_chunked_write:
            print(f"📦 Dataset size: {data_size / 1024**3:.2f} GB")
            print("   Using chunked writing with DatasetChunker")
        else:
            if data_size > available_memory:
                raise MemoryError(
                    f"Data size ({data_size / 1024**3:.2f} GB) exceeds available system memory "
                    f"({available_memory / 1024**3:.2f} GB). "
                    f"Enable chunking or reduce dataset size."
                )
            print(
                f"Data size: {data_size / 1024**3:.2f} GB, Available memory: {available_memory / 1024**3:.2f} GB"
            )

        time_var = self.ds[self.cmor_name].coords["time"]
        units = time_var.attrs["units"]
        calendar = time_var.attrs.get("calendar", "standard").lower()
        times = num2date(time_var.values[[0, -1]], units=units, calendar=calendar)
        start, end = [f"{t.year:04d}{t.month:02d}" for t in times]
        time_range = f"{start}-{end}"

        filename = (
            f"{attrs['variable_id']}_{attrs['table_id']}_{attrs['source_id']}_"
            f"{attrs['experiment_id']}_{attrs['variant_label']}_"
            f"{attrs['grid_label']}_{time_range}.nc"
        )

        if self.drs_root:
            drs_path = self._build_drs_path(attrs)
            drs_path.mkdir(parents=True, exist_ok=True)
            path = drs_path / filename
            self._update_latest_symlink(drs_path)
        else:
            path = Path(self.output_path) / filename
            path.parent.mkdir(parents=True, exist_ok=True)

        with nc.Dataset(path, "w", format="NETCDF4") as dst:
            # Set global attributes
            for k, v in attrs.items():
                dst.setncattr(k, v)

            # Create dimensions
            for dim, size in self.ds.sizes.items():
                if dim == "time":
                    dst.createDimension(dim, None)  # Unlimited dimension
                else:
                    dst.createDimension(dim, size)

            # PHASE 1: Create all variables and set their attributes (B-tree metadata)
            # This ensures all B-tree fragments are written before any data chunks.
            # Combined with our chunking strategy (at least 4MB chunks), this optimizes
            # both file layout and chunk size for efficient I/O operations.
            created_vars = {}
            for var in self.ds.variables:
                vdat = self.ds[var]
                fill = None if var.endswith("_bnds") else vdat.attrs.get("_FillValue")

                # Apply HDF5 optimization features for chunked variables:
                # - shuffle: De-interlaces bytes to improve compression
                # - zlib: Compression with zlib algorithm
                # - fletcher32: Checksum for data integrity
                # These are only applied to chunked variables (data variables with time dimension)
                use_compression = (
                    self.enable_compression
                    and "time" in vdat.dims
                    and not var.endswith("_bnds")
                )

                if fill:
                    v = dst.createVariable(
                        var,
                        str(vdat.dtype),
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
                        str(vdat.dtype),
                        vdat.dims,
                        shuffle=use_compression,
                        zlib=use_compression,
                        complevel=self.compression_level if use_compression else 0,
                        fletcher32=use_compression,
                    )
                if not var.endswith("_bnds"):
                    for a, val in vdat.attrs.items():
                        if a != "_FillValue":
                            v.setncattr(a, val)
                created_vars[var] = v

            # Force NetCDF to write all metadata/B-tree information
            dst.sync()

            # PHASE 2: Write actual data chunks
            # Now all B-tree metadata is written, data chunks come after
            for var in self.ds.variables:
                vdat = self.ds[var]
                is_var_dask = isinstance(vdat.data, da.Array)
                has_time_dim = "time" in vdat.dims

                if use_chunked_write and is_var_dask and has_time_dim:
                    # Use self.chunker to calculate optimal write chunk size
                    chunk_sizes = self.chunker.calculate_chunk_size_for_variable(vdat)
                    time_chunk = chunk_sizes.get("time", self.ds.sizes["time"])
                    total_timesteps = self.ds.sizes["time"]
                    time_idx = vdat.dims.index("time")

                    print(f"  Writing {var} ({time_chunk} timesteps/chunk)...")

                    for t_start in range(0, total_timesteps, time_chunk):
                        t_end = min(t_start + time_chunk, total_timesteps)

                        # Load only this chunk into memory
                        chunk_data = vdat.isel(time=slice(t_start, t_end)).values

                        # Build slice tuple for writing
                        slices = [slice(None)] * len(vdat.dims)
                        slices[time_idx] = slice(t_start, t_end)

                        created_vars[var][tuple(slices)] = chunk_data

                    print(f"    ✓ {var}: {total_timesteps} timesteps written")
                else:
                    # Direct write for small/non-Dask/non-time variables
                    created_vars[var][:] = vdat.values

        print(f"CMORised output written to {path}")
        print("📁 Optimized layout: metadata → data chunks")
        if self.enable_compression:
            print(
                f"🗜️ HDF5 compression: shuffle + zlib(level {self.compression_level}) + fletcher32 for data variables"
            )
        else:
            print("🗜️ Compression disabled")

    def run(self, write_output: bool = False):
        self.select_and_process_variables()
        self.drop_intermediates()
        self.update_attributes()
        self.reorder()
        # Final rechunking before writing for optimal I/O performance
        if write_output:
            self.rechunk_dataset()
            self.write()
