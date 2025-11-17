import warnings
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import netCDF4 as nc
import xarray as xr
from cftime import num2date

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
        backend: str = "xarray",
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
        self.backend = backend.lower()
        self._validate_backend()
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

    def _validate_backend(self):
        """Validate and potentially adjust the backend choice."""
        if self.backend not in ["xarray", "netcdf4"]:
            raise ValueError(f"Invalid backend '{self.backend}'. Must be 'xarray' or 'netcdf4'")
            
    def _can_use_netcdf4_backend(self) -> bool:
        """
        Check if the NetCDF4 backend can be used for this variable.
        
        Returns:
            bool: True if NetCDF4 backend is suitable, False otherwise
        """
        # Check if this is a direct mapping
        mapping = self.mapping[self.cmor_name]
        calc = mapping["calculation"]
        
        # Must be direct type
        if calc["type"] != "direct":
            return False
            
        # Must have exactly one source variable
        if len(mapping["model_variables"]) != 1:
            return False
            
        # Skip if frequency validation/resampling is enabled (might need complex operations)
        if self.validate_frequency or self.enable_resampling:
            return False
            
        # Check for complex bounds handling - NetCDF4 backend doesn't support this yet
        for dim, v in self.vocab.axes.items():
            if v.get("must_have_bounds") == "yes":
                input_dim = None
                for k, val in mapping["dimensions"].items():
                    if val == v["out_name"]:
                        input_dim = k
                        break
                if input_dim:
                    return False  # Has bounds - use xarray
                    
        return True
        
    def _get_effective_backend(self) -> str:
        """
        Determine the actual backend to use, with automatic fallback.
        
        Returns:
            str: The backend to use ('xarray' or 'netcdf4')
        """
        if self.backend == "netcdf4":
            if self._can_use_netcdf4_backend():
                return "netcdf4"
            else:
                print(f"⚠️  NetCDF4 backend not suitable for {self.cmor_name}, falling back to xarray")
                return "xarray"
        else:
            return "xarray"

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
        effective_backend = self._get_effective_backend()
        
        if effective_backend == "netcdf4":
            # Use fast NetCDF4 aggregation
            if write_output:
                self._run_netcdf4_aggregation()
            else:
                print(f"ℹ️  {self.cmor_name} would use NetCDF4 fast aggregation (dry run)")
        else:
            # Use traditional xarray processing
            self.select_and_process_variables()
            self.drop_intermediates()
            self.update_attributes()
            self.reorder()
            if write_output:
                self.write()
                
    def _run_netcdf4_aggregation(self):
        """Run fast NetCDF4-based aggregation."""
        print(f"🚀 Using NetCDF4 fast aggregation for {self.cmor_name}")
        
        from access_moppy.fast_aggregator import FastNetCDFAggregator
        
        # Generate output path using existing logic
        output_path = self._generate_netcdf4_output_path()
        
        aggregator = FastNetCDFAggregator(
            input_paths=self.input_paths,
            output_path=output_path,
            variable_mapping=self.mapping,
            cmor_name=self.cmor_name,
            cmip6_vocab=self.vocab,
            chunk_size=2000,  # Configurable chunk size
        )
        
        try:
            result_path = aggregator.aggregate()
            print(f"✅ NetCDF4 fast aggregation completed: {result_path}")
        except Exception as e:
            print(f"❌ NetCDF4 aggregation failed: {e}")
            print("🔄 Falling back to xarray processing...")
            # Fallback to xarray processing
            self.select_and_process_variables()
            self.drop_intermediates()
            self.update_attributes()
            self.reorder()
            self.write()
            
    def _generate_netcdf4_output_path(self) -> str:
        """Generate output file path for NetCDF4 backend."""
        # Use similar logic to the existing write() method but return path instead of writing
        attrs = {}
        
        # Get basic attributes from vocabulary
        variable_info = self.vocab.variable
        attrs['variable_id'] = self.cmor_name
        
        # Add required CMIP6 attributes (simplified version)
        attrs['table_id'] = getattr(self.vocab, 'table_id', 'unknown')
        attrs['source_id'] = 'ACCESS-ESM1-6'  # This should come from config
        attrs['experiment_id'] = 'historical'  # This should come from config
        attrs['variant_label'] = 'r1i1p1f1'  # This should come from config
        attrs['grid_label'] = 'gn'  # This should come from config
        
        # Simple filename generation (can be enhanced)
        if self.drs_root:
            # Use DRS structure
            from datetime import datetime
            version_date = datetime.now().strftime("%Y%m%d")
            
            drs_components = [
                "CMIP6",
                attrs.get("activity_id", "CMIP"),
                attrs.get("institution_id", "CSIRO-ARCCSS"),
                attrs["source_id"],
                attrs["experiment_id"], 
                attrs["variant_label"],
                attrs["table_id"],
                attrs["variable_id"],
                attrs["grid_label"],
                f"v{version_date}",
            ]
            
            drs_path = self.drs_root.joinpath(*drs_components)
            drs_path.mkdir(parents=True, exist_ok=True)
            
            # Generate time range from input files (simplified)
            filename = f"{self.cmor_name}_{attrs['table_id']}_*.nc"  # Will be updated by aggregator
            return str(drs_path / filename)
        else:
            return str(Path(self.output_path).parent / f"{self.cmor_name}_aggregated.nc")
