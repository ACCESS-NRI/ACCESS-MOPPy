import warnings
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import netCDF4 as nc
import psutil
import xarray as xr
from cftime import num2date
from dask.distributed import get_client

from access_moppy.utilities import (
    FrequencyMismatchError,
    IncompatibleFrequencyError,
    ResamplingRequiredWarning,
    type_mapping,
    validate_and_resample_if_needed,
    validate_cmip6_frequency_compatibility,
)


class CMIP6_CMORiser:
    """
    Base class for CMIP6 CMORisers, providing shared logic for CMORisation.
    """

    type_mapping = type_mapping

    def __init__(
        self,
        input_paths: Union[str, List[str]],
        output_path: str,
        cmip6_vocab: Any,
        variable_mapping: Dict[str, Any],
        compound_name: str,
        drs_root: Optional[Path] = None,
        validate_frequency: bool = False,
        enable_resampling: bool = False,
        resampling_method: str = "auto",
    ):
        self.input_paths = (
            input_paths if isinstance(input_paths, list) else [input_paths]
        )
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
        Load dataset from input files with optional frequency validation.

        Args:
            required_vars: Optional list of required variables to extract
        """

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
                    print(f"✓ Validated compatible temporal frequency: {detected_freq}")
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

    def sort_time_dimension(self):
        if "time" in self.ds.dims:
            self.ds = self.ds.sortby("time")
            # Clean up potential duplication
            self.ds = self.ds.sel(time=~self.ds.get_index("time").duplicated())

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
        total_cluster_memory = None  # Sum of all workers' memory limits

        try:
            # Attempt to get an existing Dask client
            client = get_client()

            # Retrieve information about all workers in the cluster
            worker_info = client.scheduler_info()["workers"]

            if worker_info:
                # Get the minimum memory_limit across all workers
                worker_memory = min(w["memory_limit"] for w in worker_info.values())

                # Sum up all workers' memory for total cluster capacity
                total_cluster_memory = sum(
                    w["memory_limit"] for w in worker_info.values()
                )

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
                    f"({worker_memory / 1024**3:.2f} GB) but fits in total cluster memory "
                    f"({total_cluster_memory / 1024**3:.2f} GB)."
                )
                print("Closing Dask client to use local memory for writing...")
                client.close()
                client = None

            # If data < worker_memory: No action needed, proceed with write

        if data_size > available_memory:
            # Data exceeds available system memory
            raise MemoryError(
                f"Data size ({data_size / 1024**3:.2f} GB) exceeds available system memory "
                f"({available_memory / 1024**3:.2f} GB). "
                f"Consider using write_parallel() for chunked writing."
            )

        # Log the memory status for user awareness
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
            for k, v in attrs.items():
                dst.setncattr(k, v)
            for dim, size in self.ds.sizes.items():
                if dim == "time":
                    dst.createDimension(dim, None)  # Unlimited dimension
                else:
                    dst.createDimension(dim, size)
            for var in self.ds.variables:
                vdat = self.ds[var]
                fill = None if var.endswith("_bnds") else vdat.attrs.get("_FillValue")
                v = (
                    dst.createVariable(var, str(vdat.dtype), vdat.dims, fill_value=fill)
                    if fill
                    else dst.createVariable(var, str(vdat.dtype), vdat.dims)
                )
                if not var.endswith("_bnds"):
                    for a, val in vdat.attrs.items():
                        if a != "_FillValue":
                            v.setncattr(a, val)
                v[:] = vdat.values

        print(f"CMORised output written to {path}")

    def run(self, write_output: bool = False):
        self.select_and_process_variables()
        self.drop_intermediates()
        self.update_attributes()
        self.reorder()
        if write_output:
            self.write()
