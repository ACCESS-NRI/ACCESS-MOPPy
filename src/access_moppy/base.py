import logging
import warnings
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import cftime
import dask.array as da
import netCDF4 as nc
import numpy as np
import psutil
import xarray as xr
from cftime import date2num

from access_moppy.utilities import (
    FrequencyMismatchError,
    IncompatibleFrequencyError,
    ResamplingRequiredWarning,
    type_mapping,
    validate_and_resample_if_needed,
    validate_cmip6_frequency_compatibility,
)

logger = logging.getLogger(__name__)


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
            logger.debug("Dataset is not chunked, skipping rechunking")
            return ds

        logger.debug(
            "Applying dataset rechunking with rules: "
            "time coordinates=single chunk, "
            "time bounds=single chunk, "
            "data variables=at least %sMB chunks",
            self.target_chunk_size_mb,
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
    Base class for CMORisers, providing shared logic for CMORisation across different CMIP versions.
    """

    type_mapping = type_mapping

    def __init__(
        self,
        input_data: Optional[Union[str, List[str], xr.Dataset, xr.DataArray]] = None,
        *,
        output_path: str,
        vocab: Any,
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
        self.vocab = vocab
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
                    logger.debug(
                        "Dropped %d unused coordinate(s): %s",
                        len(coords_to_drop),
                        coords_to_drop,
                    )

        else:
            # Original file-based loading logic
            def _preprocess(ds):
                ds = ds[list(required_vars & set(ds.data_vars))]
                # Drop auxiliary UM time coordinates (time_0, time_1) that differ
                # across files. Without this, xr.open_mfdataset's join='outer'
                # unions every distinct value into a growing dimension, inflating
                # the Dask task graph to several GiB before any computation starts.
                aux_time_coords = [c for c in ("time_0", "time_1") if c in ds]
                if aux_time_coords:
                    ds = ds.drop_vars(aux_time_coords)
                return ds

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
                if not _probe_target_vars:
                    # None of the required variables are in the probe file; fall back
                    # to checking whether the file itself is time-dependent so all
                    # files are still concatenated (the missing-variable error will
                    # surface downstream with proper context).
                    _has_time = "time" in _probe.dims and any(
                        "time" in _probe[v].dims for v in _probe.data_vars
                    )
                    logger.warning(
                        "Required variables %s not found in probe file %s; "
                        "inferring time-dependency from other variables in file (_has_time=%s).",
                        list(required_vars) if required_vars else [],
                        self.input_paths[0],
                        _has_time,
                    )
                else:
                    _has_time = any(
                        "time" in _probe[v].dims for v in _probe_target_vars
                    )

            # Fix #334: the CMOR table is the authoritative source for whether a
            # variable is time-independent (fx).  A source file may carry a time
            # dimension even for fx variables (e.g. orog from a UM dump); using the
            # probe alone would then set _has_time=True and trigger a spurious
            # concat_dim="time" that fails later.  Override here using the same
            # compound_name check already used for frequency validation below.
            if self.compound_name and "fx" in self.compound_name.lower():
                _has_time = False

            # Validate frequency consistency and CMIP6 compatibility before concatenation
            # Skip validation for time-independent variables (e.g., areacello, static grids)
            if self.validate_frequency and len(self.input_paths) > 0:
                # Check if this is a time-dependent variable by examining the compound_name
                # Time-independent variables typically have "fx" (fixed) in their table ID
                is_time_independent = (
                    self.compound_name and "fx" in self.compound_name.lower()
                ) or "time" not in _probe_dims

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
                                    interactive=True,
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
                self.ds = xr.open_mfdataset(
                    self.input_paths,
                    combine="nested",  # avoids costly dimension alignment
                    concat_dim="time",
                    engine="netcdf4",
                    decode_cf=False,
                    chunks={},
                    data_vars="minimal",  # Only concat variables with the time dim; avoids FutureWarning
                    coords="minimal",  # Required when compat='override'; only concat coords along time
                    compat="override",  # Take first file's value for non-concat vars; avoids FutureWarning
                    preprocess=_preprocess,
                    parallel=True,  # <--- enables concurrent preprocessing
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
                # UM source files always include a time dimension (size=1) even for
                # static fields.  Drop it so downstream CMOR processing sees the
                # expected (lat, lon) shape rather than (time=1, lat, lon).
                if "time" in self.ds.dims:
                    self.ds = self.ds.isel(time=0, drop=True)

        # Apply temporal resampling if enabled and needed
        if self.enable_resampling and self.compound_name:
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
            # Clean up potential duplication
            self.ds = self.ds.sel(time=~self.ds.get_index("time").duplicated())

    def rechunk_dataset(self):
        """
        Apply intelligent rechunking to the dataset.

        This method can be called separately from load_dataset if rechunking
        is needed at a different stage in the processing pipeline.
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

    def select_and_process_variables(self):
        raise NotImplementedError(
            "Subclasses must implement select_and_process_variables."
        )

    def _check_units(self, cmor_name: str, expected: str) -> None:
        """Check that the mapping's declared units are consistent with what CMIP expects."""
        declared = self.mapping.get(cmor_name, {}).get("units")
        if declared and expected and declared != expected:
            raise ValueError(
                f"Mapping units mismatch for {cmor_name}: "
                f"mapping declares '{declared}' but CMIP expects '{expected}'"
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
            # Fuse both comparisons into one scheduler pass instead of two
            # separate .compute() calls.
            too_small, too_large = da.compute((arr < vmin).any(), (arr > vmax).any())
        else:
            too_small = (arr < vmin).any().item()
            too_large = (arr > vmax).any().item()
        if too_small:
            raise ValueError(f"Values of '{var}' below valid_min: {vmin}")
        if too_large:
            raise ValueError(f"Values of '{var}' above valid_max: {vmax}")

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
        Standardize missing values in the main variable to CMIP6 requirements.

        At this point, missing values should already be normalized to NaN from
        early processing, and XArray's built-in missing value propagation should
        have handled derivation calculations correctly. This method converts NaN
        to the final CMIP6-compliant missing value.

        This is particularly important for:
        - Final CMIP6 compliance (converting NaN to 1e20)
        - Ensuring consistent metadata attributes
        """
        if (
            hasattr(self, "vocab")
            and self.vocab
            and self.cmor_name in self.ds.data_vars
        ):
            logger.debug(
                "Applying final CMIP6 missing value standardization for %s",
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
            logger.debug("Final CMIP6 missing value applied: %s", missing_value)
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

        Automatically handles character/string coordinates with proper NetCDF encoding.
        """
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

        # Generate filename using vocabulary-specific logic
        filename = self.vocab.generate_filename(
            attrs, self.ds, self.cmor_name, self.compound_name
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

            # Create string length dimensions for character coordinates
            for coord_name, info in string_coords_info.items():
                strlen_dim = info["strlen_dim"]
                strlen_size = info["strlen_size"]
                if strlen_dim not in dst.dimensions:
                    dst.createDimension(strlen_dim, strlen_size)

            # PHASE 1: Create all variables and set their attributes (B-tree metadata)
            # This ensures all B-tree fragments are written before any data chunks.
            # Combined with our chunking strategy (at least 4MB chunks), this optimizes
            # both file layout and chunk size for efficient I/O operations.
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
                        if a in ("_FillValue", "bounds"):
                            continue
                        if var.endswith("_bnds") and a == "coordinates":
                            continue  # strip stale auxiliary coordinates reference on bounds
                        v.setncattr(a, val)

                        # ========== Add coordinates attribute for main data variable ==========
                        # For CF compliance, auxiliary coordinates (scalar and non-dimension coords)
                        # must be declared in the main data variable's 'coordinates' attribute
                        if var == self.cmor_name and aux_coords:
                            # Get existing coordinates attribute if present
                            existing_coords = vdat.attrs.get("coordinates", "")

                            # Add string coordinates that aren't already in the attribute
                            coords_to_add = [
                                c for c in aux_coords if c != existing_coords
                            ]

                            if coords_to_add:
                                if existing_coords:
                                    # Append to existing coordinates
                                    new_coords = (
                                        existing_coords + " " + " ".join(coords_to_add)
                                    )
                                else:
                                    # Create new coordinates attribute
                                    new_coords = " ".join(coords_to_add)

                                v.setncattr("coordinates", new_coords)
                                logger.debug(
                                    "  Added coordinates attribute to '%s': '%s'",
                                    var,
                                    new_coords,
                                )

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
                        # Use self.chunker to calculate optimal write chunk size
                        chunk_sizes = self.chunker.calculate_chunk_size_for_variable(
                            vdat
                        )
                        time_chunk = int(chunk_sizes.get("time", self.ds.sizes["time"]))
                        total_timesteps = self.ds.sizes["time"]
                        time_idx = vdat.dims.index("time")

                        logger.debug(
                            "  Writing %s (%d timesteps/chunk)...", var, time_chunk
                        )

                        for t_start in range(0, total_timesteps, time_chunk):
                            t_end = min(t_start + time_chunk, total_timesteps)

                            # Load only this chunk into memory
                            chunk_data = vdat.isel(time=slice(t_start, t_end)).values

                            # Build slice tuple for writing
                            slices = [slice(None)] * len(vdat.dims)
                            slices[time_idx] = slice(t_start, t_end)

                            created_vars[var][tuple(slices)] = chunk_data

                        logger.debug(
                            "    %s: %d timesteps written", var, total_timesteps
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

        logger.info("CMORised output written to %s", path)
        logger.debug("Optimized layout: metadata -> data chunks")
        if self.enable_compression:
            logger.debug(
                "HDF5 compression: shuffle + zlib(level %d) + fletcher32 for data variables",
                self.compression_level,
            )
        else:
            logger.debug("Compression disabled")

        if string_coords_info:
            logger.debug(
                "String coordinates processed: %s", ", ".join(string_coords_info.keys())
            )

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
