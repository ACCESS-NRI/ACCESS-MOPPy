import re
import warnings

import numpy as np
import xarray as xr

from access_moppy.base import CMORiser
from access_moppy.derivations import custom_functions, evaluate_expression
from access_moppy.utilities import (
    calculate_latitude_bounds,
    calculate_longitude_bounds,
    calculate_time_bounds,
)


class Atmosphere_CMORiser(CMORiser):
    """
    Handles CMORisation of NetCDF datasets for Atmosphere/Land variables across CMIP versions.
    """

    def calculate_missing_bounds_variables(self, bnds_required):
        """Calculate missing bounds variables for coordinates."""
        for bnds_var in bnds_required:
            if bnds_var not in self.ds.data_vars and bnds_var not in self.ds.coords:
                # Extract coordinate name by removing "_bnds" suffix
                coord_name = bnds_var.replace("_bnds", "")

                if coord_name not in self.ds.coords:
                    raise ValueError(
                        f"Cannot calculate {bnds_var}: coordinate '{coord_name}' not found in dataset"
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
                    )
                    self.ds[coord_name].attrs["bounds"] = bnds_var

                elif coord_name in ["lat", "latitude", "y"]:
                    # Calculate latitude bounds - use "bnds" for atmosphere data
                    self.ds[bnds_var] = calculate_latitude_bounds(
                        self.ds, coord_name, bnds_name="bnds"
                    )
                    self.ds[coord_name].attrs["bounds"] = bnds_var

                elif coord_name in ["lon", "longitude", "x"]:
                    # Calculate longitude bounds - use "bnds" for atmosphere data
                    self.ds[bnds_var] = calculate_longitude_bounds(
                        self.ds, coord_name, bnds_name="bnds"
                    )
                    self.ds[coord_name].attrs["bounds"] = bnds_var

                else:
                    # For other coordinates, we could add more handlers or skip
                    warnings.warn(
                        f"No automatic calculation available for '{bnds_var}'. This may cause CMIP compliance issues.",
                        UserWarning,
                        stacklevel=3,
                    )

    def remove_spurious_time_dimensions(self, required_vars):
        """
        Remove spurious time dimensions from coordinate and auxiliary variables.

        This method addresses a common issue in xarray when combining datasets:
        spatial bounds (lat_bnds, lon_bnds) and other coordinate variables can incorrectly
        gain time dimensions during multi-file dataset operations, even though they are time-invariant.

        Why this is necessary:
        - When using xr.open_mfdataset() with combine_coords="time", xarray
          conservatively assumes all coordinate-linked variables might vary with time
        - This causes spatial bounds and coordinates to be broadcasted along the time dimension
        - Results in redundant data storage and non-CF-compliant files

        Why this is reasonable for ACCESS Models:
        - ACCESS Models use static grids throughout model runs
        - Latitude, longitude coordinates (and their bounds) are time-invariant
        - The grid definition remains constant across all timesteps
        - Only time_bnds and data variables should legitimately have a time dimension
        - This optimization is safe and improves storage efficiency

        Args:
            required_vars (list): Variables that should keep their time dimension
        """
        # Identify all variables that have gained spurious time dimensions
        # Include bounds variables and any other coordinate variables
        problematic_vars = [
            name
            for name in self.ds.variables
            if "time" not in name  # Don't touch time_bnds or time coordinate
            and name not in required_vars  # Don't touch required data variables
            and name in self.ds
            and "time" in self.ds[name].coords
            and self.ds[name].dims != ("time",)  # Skip pure time variables
        ]

        if problematic_vars:
            # Process all problematic variables efficiently in a single operation
            corrections = {
                name: self.ds[name].isel(time=0).drop_vars("time")
                for name in problematic_vars
            }
            self.ds = self.ds.assign(corrections)

    def select_and_process_variables(self):
        # Check if this is an internal calculation that doesn't need input variables
        calc = self.mapping[self.cmor_name]["calculation"]

        if calc["type"] == "internal":
            # For internal calculations, we don't need to load any input data
            # Call the internal calculation function directly
            func_name = calc["function"]
            if func_name not in custom_functions:
                raise ValueError(
                    f"Internal calculation function '{func_name}' not found in custom_functions"
                )

            # Execute the internal function to generate the variable data
            self.ds = custom_functions[func_name](**calc.get("kwargs", {}))

            self.vocab._get_axes(
                self.mapping
            )  # Ensure axes are loaded for renaming later

            # Ensure the CMOR variable exists
            if self.cmor_name not in self.ds:
                raise ValueError(
                    f"Internal calculation function '{func_name}' did not generate variable '{self.cmor_name}'"
                )

            return

        # Original logic for other calculation types
        # Select input variables required for the CMOR variable
        required_vars = self.mapping[self.cmor_name]["model_variables"]

        required_axes, axes_rename_map = self.vocab._get_axes(self.mapping)
        required_bounds, bounds_rename_map = self.vocab._get_required_bounds_variables(
            self.mapping
        )

        required = set(
            required_vars
            + list(axes_rename_map.keys())
            + list(bounds_rename_map.keys())
        )
        self.load_dataset(required_vars=required)

        # Remove spurious time dimensions from spatial bounds and coordinates
        self.remove_spurious_time_dimensions(required_vars)

        # Ensure time dimension is sorted
        self.sort_time_dimension()

        ## Calculate missing bounds variables
        ##self.calculate_missing_bounds_variables(required_bounds)

        # Handle the calculation type
        if calc["type"] == "direct":
            # If the calculation is direct, just rename the variable
            self.ds = self.ds.rename({required_vars[0]: self.cmor_name})
        elif calc["type"] == "formula":
            # If the calculation is a formula, evaluate it
            context = {var: self.ds[var] for var in required_vars}
            context.update(custom_functions)
            self.ds[self.cmor_name] = evaluate_expression(calc, context)
            # Drop unit after calculation. update_attributes() will add the right units later on.
            self.ds[self.cmor_name].attrs.pop("units", None)
            # Drop the original input variables, except the CMOR variable and keep bounds
            self.ds = self.ds.drop_vars(
                [
                    var
                    for var in required_vars
                    if var != self.cmor_name and var not in required_bounds.keys()
                ],
                errors="ignore",
            )
        elif calc["type"] == "dataset_function":
            # Function that operates on the full dataset
            func_name = calc["function"]
            self.ds = self.ds.rename({required_vars[0]: self.cmor_name})
            self.ds = custom_functions[func_name](self.ds, **calc.get("kwargs", {}))
        else:
            raise ValueError(f"Unsupported calculation type: {calc['type']}")

        # Rename axes and bounds variables
        rename_map = {
            k: v
            for k, v in {**bounds_rename_map, **axes_rename_map}.items()
            if k in self.ds
        }

        # Drop any existing variables that have the same names as our target names
        conflicting_vars = [
            v
            for v in rename_map.values()
            if v in self.ds and v not in rename_map.keys()
        ]
        if conflicting_vars:
            self.ds = self.ds.drop_vars(conflicting_vars, errors="ignore")

        self.ds = self.ds.rename(rename_map)

        # Transpose the data variable according to the CMOR dimensions
        # Handle both string and list dimension formats
        dimensions = self.vocab.variable["dimensions"]
        try:
            # Try treating as string (space-separated)
            cmor_dims = re.sub(r"\w*level", "lev", dimensions).split()
        except TypeError:
            # If re.sub() fails (TypeError for list input), it's already a list
            cmor_dims = [re.sub(r"\w*level", "lev", dim) for dim in dimensions]

        transpose_order = [
            self.vocab.axes[dim]["out_name"]
            for dim in cmor_dims
            if "value" not in self.vocab.axes[dim]
        ]

        # Squeeze singleton time dimensions not needed in output
        for dim in ("time_0", "time_1"):
            if dim in self.ds[self.cmor_name].dims and dim not in transpose_order:
                self.ds = self.ds.isel({dim: 0}, drop=True)
        # Squeeze singleton dimensions if they are not in the transpose order
        for dim in self.ds[self.cmor_name].dims:
            if dim not in transpose_order and self.ds[self.cmor_name][dim].size == 1:
                self.ds[self.cmor_name] = self.ds[self.cmor_name].squeeze(dim)

        # Enforce dimension order: time first, lat/lon last (lat before lon),
        # with any remaining dimensions (e.g. lev) in between.
        time_dims = [dim for dim in transpose_order if dim == "time"]
        middle_dims = [
            dim for dim in transpose_order if dim not in ("time", "lat", "lon")
        ]
        lat_lon_dims = [dim for dim in ("lat", "lon") if dim in transpose_order]
        transpose_order = time_dims + middle_dims + lat_lon_dims

        self.ds[self.cmor_name] = self.ds[self.cmor_name].transpose(*transpose_order)

    def update_attributes(self):
        self.ds.attrs = {
            k: v
            for k, v in self.vocab.get_required_global_attributes().items()
            if v not in (None, "")
        }

        required_coords = {
            v["out_name"] for v in self.vocab.axes.values() if "value" in v
        }.union({v["out_name"] for v in self.vocab.axes.values()})
        self.ds = self.ds.drop_vars(
            [c for c in self.ds.coords if c not in required_coords], errors="ignore"
        )

        cmor_attrs = self.vocab.variable
        self._check_units(self.cmor_name, cmor_attrs.get("units"))

        self.ds[self.cmor_name].attrs.update(
            {k: v for k, v in cmor_attrs.items() if v not in (None, "")}
        )
        var_type = cmor_attrs.get("type", "double")
        self.ds[self.cmor_name] = self.ds[self.cmor_name].astype(
            self.type_mapping.get(var_type, np.float64)
        )

        try:
            if cmor_attrs.get("valid_min") not in (None, "") and cmor_attrs.get(
                "valid_max"
            ) not in (None, ""):
                vmin = self.type_mapping.get(var_type, np.float64)(
                    cmor_attrs["valid_min"]
                )
                vmax = self.type_mapping.get(var_type, np.float64)(
                    cmor_attrs["valid_max"]
                )
                self._check_range(self.cmor_name, vmin, vmax)
        except ValueError as e:
            raise ValueError(
                f"Failed to validate value range for {self.cmor_name}: {e}"
            )

        for dim, meta in self.vocab.axes.items():
            name = meta["out_name"]
            dtype = self.type_mapping.get(meta.get("type", "double"), np.float64)
            if name in self.ds:
                self._check_units(name, meta.get("units", ""))
                if meta.get("standard_name") == "time":
                    self._check_calendar(name)
                original_units = self.ds[name].attrs.get("units", "")
                coord_attrs = {
                    k: v
                    for k, v in {
                        "standard_name": meta.get("standard_name"),
                        "long_name": meta.get("long_name"),
                        "units": meta.get("units"),
                        "axis": meta.get("axis"),
                        "positive": meta.get("positive"),
                        "valid_min": dtype(meta["valid_min"])
                        if "valid_min" in meta
                        else None,
                        "valid_max": dtype(meta["valid_max"])
                        if "valid_max" in meta
                        else None,
                    }.items()
                    if v is not None
                }
                if coord_attrs.get(
                    "units"
                ) == "days since ?" and original_units.lower().startswith("days since"):
                    coord_attrs["units"] = original_units
                updated = self.ds[name].astype(dtype)
                updated.attrs.update(coord_attrs)
                self.ds[name] = updated
            elif "value" in meta:
                val = meta["value"]
                # Handle character type (e.g., string coordinate)
                if meta["type"] == "character":
                    arr = xr.DataArray(
                        np.array(
                            val, dtype="S"
                        ),  # ensure type is character (byte string)
                        dims=(),
                        attrs={
                            k: v
                            for k, v in {
                                "standard_name": meta.get("standard_name"),
                                "long_name": meta.get("long_name"),
                                "units": meta.get("units"),
                                "axis": meta.get("axis"),
                                "positive": meta.get("positive"),
                                "valid_min": meta.get("valid_min"),
                                "valid_max": meta.get("valid_max"),
                            }.items()
                            if v is not None
                        },
                    )
                else:
                    arr = xr.DataArray(
                        dtype(val),
                        dims=(),
                        attrs={
                            k: v
                            for k, v in {
                                "standard_name": meta.get("standard_name"),
                                "long_name": meta.get("long_name"),
                                "units": meta.get("units"),
                                "axis": meta.get("axis"),
                                "positive": meta.get("positive"),
                                "valid_min": dtype(meta["valid_min"])
                                if "valid_min" in meta
                                else None,
                                "valid_max": dtype(meta["valid_max"])
                                if "valid_max" in meta
                                else None,
                            }.items()
                            if v is not None
                        },
                    )
                self.ds = self.ds.assign_coords({name: arr})
