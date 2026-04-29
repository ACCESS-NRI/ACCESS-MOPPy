from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import numpy as np
import xarray as xr

from access_moppy.derivations import custom_functions, evaluate_expression
from access_moppy.ocean import Ocean_CMORiser
from access_moppy.ocean_supergrid import Supergrid
from access_moppy.vocabulary_processors import CMIP6Vocabulary


class SeaIce_CMORiser(Ocean_CMORiser):
    """
    CMORiser subclass for sea-ice variables that infers grid type from coordinate attributes.

    Sea-ice models often specify grid coordinates as variable attributes rather than
    dimension coordinates (e.g., coordinates="ULON ULAT" for U-grid variables).
    """

    def __init__(
        self,
        input_data: Optional[Union[str, List[str], xr.Dataset, xr.DataArray]] = None,
        *,
        output_path: str,
        vocab: CMIP6Vocabulary,
        variable_mapping: Dict[str, Any],
        compound_name: str,
        drs_root: Optional[Path] = None,
        validate_frequency: bool = True,
        enable_resampling: bool = False,
        resampling_method: str = "auto",
        # Backward compatibility
        input_paths: Optional[Union[str, List[str]]] = None,
    ):
        super().__init__(
            input_data=input_data,
            input_paths=input_paths,
            output_path=output_path,
            vocab=vocab,
            variable_mapping=variable_mapping,
            compound_name=compound_name,
            drs_root=drs_root,
            validate_frequency=validate_frequency,
            enable_resampling=enable_resampling,
            resampling_method=resampling_method,
        )

        nominal_resolution = vocab._get_nominal_resolution(target_realm="seaIce")
        self.supergrid = Supergrid(nominal_resolution)
        self.grid_info = None
        self.grid_type = None
        self.symmetric = None
        self.arakawa = "B"  # Sea-ice typically uses B-grid

    def infer_grid_type(self):
        """
        Infer the grid type from coordinate attributes in sea-ice variables.

        Sea-ice models often store coordinate information in variable attributes:
        - coordinates="TLON TLAT" indicates T-grid
        - coordinates="ULON ULAT" indicates U-grid
        - coordinates="VLON VLAT" indicates V-grid

        Falls back to standard coordinate-based detection if attribute method fails.
        """
        # First check variable coordinate attributes (sea-ice specific)
        for var_name, var in self.ds.data_vars.items():
            if hasattr(var, "attrs") and "coordinates" in var.attrs:
                coord_attr = var.attrs["coordinates"]
                if isinstance(coord_attr, str):
                    coord_attr = coord_attr.upper()  # Handle case variations

                    # Check for sea-ice coordinate patterns
                    if "ULON" in coord_attr and "ULAT" in coord_attr:
                        return "U", None
                    elif "TLON" in coord_attr and "TLAT" in coord_attr:
                        return "T", None
                    elif "VLON" in coord_attr and "VLAT" in coord_attr:
                        return "V", None

        raise ValueError(
            "Could not infer grid type from coordinate attributes or dataset coordinates. "
            "Expected coordinate attributes like 'ULON ULAT', 'TLON TLAT', or 'VLON VLAT'"
        )

    def _get_dim_rename(self):
        """Get dimension renaming for sea-ice variables."""
        # Common sea-ice dimension mappings
        if "ACCESS" in self.vocab.source_id:
            return {
                "ni": "i",  # x-dimension for sea-ice
                "nj": "j",  # y-dimension for sea-ice
                "ncat": "ncat",  # sea-ice categories (if present)
                # Add other sea-ice specific dimensions as needed
            }
        else:
            raise ValueError(
                f"Unsupported source_id for sea-ice: {self.vocab.source_id}"
            )

    def select_and_process_variables(self):
        """Select and process variables for the CMOR output."""
        calc = self.mapping[self.cmor_name]["calculation"]

        if calc["type"] == "internal":
            # For internal calculations, we don't need to load any input data
            # Create empty dataset and let the internal function handle everything
            self.load_dataset(required_vars=[])

            # Call the internal calculation function
            func_name = calc["function"]
            if func_name not in custom_functions:
                raise ValueError(
                    f"Internal calculation function '{func_name}' not found in custom_functions"
                )

            # Execute the internal function to generate the variable data
            self.ds = custom_functions[func_name](self.ds, **calc.get("kwargs", {}))

            self.vocab._get_axes(
                self.mapping
            )  # Ensure axes are loaded for renaming later

            # Ensure the CMOR variable exists
            if self.cmor_name not in self.ds:
                raise ValueError(
                    f"Internal calculation function '{func_name}' did not generate variable '{self.cmor_name}'"
                )

            return

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

        # Ensure time dimension is sorted
        self.sort_time_dimension()

        # Handle the calculation type
        if calc["type"] == "direct":
            # If the calculation is direct, just rename the variable
            self.ds[self.cmor_name] = self.ds[required_vars[0]]
        elif calc["type"] == "formula":
            # If the calculation is a formula, evaluate it
            context = {var: self.ds[var] for var in required_vars}
            context.update(custom_functions)
            self.ds[self.cmor_name] = evaluate_expression(calc, context)
        elif calc["type"] == "dataset_function":
            # Function that operates on the full dataset
            func_name = calc["function"]
            self.ds = self.ds.rename({required_vars[0]: self.cmor_name})
            self.ds = custom_functions[func_name](self.ds, **calc.get("kwargs", {}))
        else:
            raise ValueError(f"Unsupported calculation type: {calc['type']}")

        self.grid_type, self.symmetric = self.infer_grid_type()

        # Get sea-ice dimension rename map
        seaice_dim_rename = self._get_dim_rename()

        # Rename axes and bounds variables
        rename_map = {
            k: v
            for k, v in {
                **bounds_rename_map,
                **axes_rename_map,
                **seaice_dim_rename,
            }.items()
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

        # Determine transpose order based on available dimensions
        dims = list(self.ds[self.cmor_name].dims)

        # Define the preferred dimension order for sea-ice
        preferred_order = ["time", "ncat", "j", "i"]  # ncat for sea-ice categories

        # Create transpose order from available dimensions following preferred order
        transpose_order = [dim for dim in preferred_order if dim in dims]

        # Add any remaining dimensions not in preferred_order at the end
        remaining_dims = [dim for dim in dims if dim not in transpose_order]
        transpose_order.extend(remaining_dims)

        # Only transpose if the current order differs from desired order
        if transpose_order != dims:
            self.ds[self.cmor_name] = self.ds[self.cmor_name].transpose(
                *transpose_order
            )

    def update_attributes(self):
        """Update attributes for sea-ice variables."""
        grid_type = self.grid_type
        arakawa = self.arakawa
        symmetric = self.symmetric
        self.grid_info = self.supergrid.extract_grid(grid_type, arakawa, symmetric)

        self.ds = self.ds.assign_coords(
            {
                "i": self.grid_info["i"],
                "j": self.grid_info["j"],
                "vertices": self.grid_info["vertices"],
            }
        )

        self.ds["latitude"] = self.grid_info["latitude"]
        self.ds["longitude"] = self.grid_info["longitude"]
        self.ds["vertices_latitude"] = self.grid_info["vertices_latitude"]
        self.ds["vertices_longitude"] = self.grid_info["vertices_longitude"]

        self.ds["latitude"].attrs.update(
            {
                "standard_name": "latitude",
                "units": "degrees_north",
                "bounds": "vertices_latitude",
            }
        )
        self.ds["longitude"].attrs.update(
            {
                "standard_name": "longitude",
                "units": "degrees_east",
                "bounds": "vertices_longitude",
            }
        )
        self.ds["vertices_latitude"].attrs.update(
            {"standard_name": "latitude", "units": "degrees_north"}
        )
        self.ds["vertices_longitude"].attrs.update(
            {"standard_name": "longitude", "units": "degrees_east"}
        )

        self.ds.attrs = {
            k: v
            for k, v in self.vocab.get_required_global_attributes().items()
            if v not in (None, "")
        }

        if "nv" in self.ds.dims:
            self.ds = self.ds.rename_dims({"nv": "bnds"}).rename_vars({"nv": "bnds"})
            self.ds["bnds"].attrs.update(
                {"long_name": "vertex number of the bounds", "units": "1"}
            )

        cmor_attrs = self.vocab.variable
        self.ds[self.cmor_name].attrs.update(
            {k: v for k, v in cmor_attrs.items() if v not in (None, "")}
        )
        var_type = cmor_attrs.get("type", "double")
        self.ds[self.cmor_name] = self.ds[self.cmor_name].astype(
            self.type_mapping.get(var_type, np.float64)
        )

        # Check calendar and units
        if "time" in self.ds.dims:
            self._check_calendar("time")
