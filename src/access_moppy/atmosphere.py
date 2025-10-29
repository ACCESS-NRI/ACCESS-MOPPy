import numpy as np
import xarray as xr

from access_moppy.base import CMIP6_CMORiser
from access_moppy.derivations import custom_functions, evaluate_expression


class CMIP6_Atmosphere_CMORiser(CMIP6_CMORiser):
    """
    Handles CMORisation of NetCDF datasets using CMIP6 metadata (Atmosphere/Land).
    """

    def select_and_process_variables(self):
        # Find all required bounds variables
        bnds_required = []
        bounds_rename_map = {}
        for dim, v in self.vocab.axes.items():
            if v.get("must_have_bounds") == "yes":
                # Find the input dimension name that maps to this output name
                input_dim = None
                for k, val in self.mapping[self.cmor_name]["dimensions"].items():
                    if val == v["out_name"]:
                        input_dim = k
                        break
                if input_dim is None:
                    raise KeyError(
                        f"Can't find input dimension mapping for output dimension '{v['out_name']}'."
                    )
                bnds_var = input_dim + "_bnds"
                bounds_rename_map[bnds_var] = v["out_name"] + "_bnds"
                bnds_required.append(bnds_var)

        # Select input variables
        input_vars = self.mapping[self.cmor_name]["model_variables"]
        calc = self.mapping[self.cmor_name]["calculation"]

        required_vars = set(input_vars + bnds_required)
        self.load_dataset(required_vars=required_vars)
        self.sort_time_dimension()

        # Handle the calculation type
        if calc["type"] == "direct":
            # If the calculation is direct, just rename the variable
            self.ds = self.ds.rename({input_vars[0]: self.cmor_name})
        elif calc["type"] == "formula":
            # If the calculation is a formula, evaluate it
            context = {var: self.ds[var] for var in input_vars}
            context.update(custom_functions)
            self.ds[self.cmor_name] = evaluate_expression(calc, context)
            # Drop the original input variables, except the CMOR variable and keep bounds
            self.ds = self.ds.drop_vars(
                [
                    var
                    for var in input_vars
                    if var != self.cmor_name and var not in bnds_required
                ],
                errors="ignore",
            )
        else:
            raise ValueError(f"Unsupported calculation type: {calc['type']}")

        # Rename dimensions according to the CMOR vocabulary
        dim_rename = self.mapping[self.cmor_name]["dimensions"]
        dims_to_rename = {k: v for k, v in dim_rename.items() if k in self.ds.dims}
        self.ds = self.ds.rename(dims_to_rename)

        # Also rename coordinates if needed
        coords_to_rename = {k: v for k, v in dim_rename.items() if k in self.ds.coords}
        if coords_to_rename:
            self.ds = self.ds.rename(coords_to_rename)

        # Rename bounds variables
        for bnds_var, out_bnds_name in bounds_rename_map.items():
            if bnds_var in self.ds:
                self.ds = self.ds.rename({bnds_var: out_bnds_name})
            elif bnds_var in self.ds.coords:
                self.ds = self.ds.rename({bnds_var: out_bnds_name})
            # trim 'time' dimention of lat_bnds and lon_bnds
            if "time" not in out_bnds_name and "time" in self.ds[out_bnds_name].coords:
                self.ds[out_bnds_name] = (
                    self.ds[out_bnds_name].isel(time=0).drop_vars("time")
                )

        # Update "bounds" attribute in all variables and coordinates
        for var in list(self.ds.variables) + list(self.ds.coords):
            bounds_attr = self.ds[var].attrs.get("bounds")
            if bounds_attr and bounds_attr in bounds_rename_map:
                self.ds[var].attrs["bounds"] = bounds_rename_map[bounds_attr]

        # Transpose the data variable according to the CMOR dimensions
        cmor_dims = self.vocab.variable["dimensions"].split()
        transpose_order = [
            self.vocab.axes[dim]["out_name"]
            for dim in cmor_dims
            if "value" not in self.vocab.axes[dim]
        ]
        # Squeeze singleton dimensions if they are not in the transpose order
        for dim in self.ds[self.cmor_name].dims:
            if dim not in transpose_order and self.ds[self.cmor_name][dim].size == 1:
                self.ds[self.cmor_name] = self.ds[self.cmor_name].squeeze(dim)

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
