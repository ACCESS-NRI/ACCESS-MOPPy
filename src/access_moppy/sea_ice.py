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
        staging_path: Optional[Path] = None,
        validate_frequency: bool = True,
        enable_resampling: bool = False,
        resampling_method: str = "auto",
        enable_chunking: bool = True,
        chunk_size_mb: float = 4.0,
        max_chunk_size_mb: float = 128.0,
        write_prefetch: int = 4,
        split_years="auto",
        enable_qc_plots: bool = False,
        cmip7_grid_labels: Optional[Dict[str, Any]] = None,
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
            staging_path=staging_path,
            validate_frequency=validate_frequency,
            enable_resampling=enable_resampling,
            resampling_method=resampling_method,
            enable_chunking=enable_chunking,
            chunk_size_mb=chunk_size_mb,
            max_chunk_size_mb=max_chunk_size_mb,
            write_prefetch=write_prefetch,
            split_years=split_years,
            enable_qc_plots=enable_qc_plots,
            cmip7_grid_labels=cmip7_grid_labels,
        )

        nominal_resolution = vocab._get_nominal_resolution(target_realm="seaIce")
        self.supergrid = Supergrid(nominal_resolution)
        self.grid_info = None
        self.grid_type = None
        self.symmetric = None
        self.arakawa = "B"  # Sea-ice typically uses B-grid
        self._cmip7_component = "sea_ice"

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

        coord_attrs_found = {
            v: self.ds[v].attrs.get("coordinates", "<missing>")
            for v in self.ds.data_vars
        }
        raise ValueError(
            f"Could not infer grid type from coordinate attributes. "
            f"Expected a 'coordinates' attribute containing 'ULON ULAT' (U-grid), "
            f"'TLON TLAT' (T-grid), or 'VLON VLAT' (V-grid) on at least one data variable. "
            f"Found data_vars with their 'coordinates' attrs: {coord_attrs_found}"
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
                f"Unsupported source_id '{self.vocab.source_id}' for SeaIce_CMORiser. "
                f"source_id must start with 'ACCESS-'."
            )

    def _normalise_time_bounds(self):
        """Rename the model time-bounds variable to the CMOR ``time_bnds``/``bnds``.

        CICE writes the time-averaging interval as ``time_bounds`` on a ``d2``
        dimension; CMIP expects ``time_bnds`` on a ``bnds`` dimension. Follow the
        time coordinate's ``bounds`` attribute to whatever variable it names and
        rename it (and its non-time dimension) so the reference resolves. No-op
        when the bounds variable is absent or already canonically named.
        """
        if "time" not in self.ds:
            return
        bnds_name = self.ds["time"].attrs.get("bounds")
        if not bnds_name or bnds_name not in self.ds:
            return
        rename = {}
        if bnds_name != "time_bnds":
            rename[bnds_name] = "time_bnds"
        bnd_dim = next((d for d in self.ds[bnds_name].dims if d != "time"), None)
        if bnd_dim and bnd_dim != "bnds":
            rename[bnd_dim] = "bnds"
        if rename:
            self.ds = self.ds.rename(rename)
        self.ds["time"].attrs["bounds"] = "time_bnds"

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
        # CICE writes the time-averaging interval as `time_bounds` (on a `d2`
        # dimension), which does not match the `<dim>_bnds` name the bounds map
        # expects. Pull it in explicitly, otherwise it is dropped at load and the
        # `time:bounds` attribute is left pointing at a missing variable.
        required.add("time_bounds")
        self.load_dataset(required_vars=required)

        # Ensure time dimension is sorted
        self.sort_time_dimension()

        # Normalise the model time-bounds variable to the CMOR name/dim.
        self._normalise_time_bounds()

        # Handle the calculation type
        if calc["type"] in ("direct", "dataset_function") and not required_vars:
            raise ValueError(
                f"Calculation type '{calc['type']}' for '{self.cmor_name}' requires at least "
                f"one model_variable, but 'model_variables' is empty in the mapping."
            )
        if calc["type"] == "direct":
            # If the calculation is direct, just rename the variable
            self.ds[self.cmor_name] = self.ds[required_vars[0]]
        elif calc["type"] == "formula":
            # If the calculation is a formula, evaluate it
            missing_model_vars = [v for v in required_vars if v not in self.ds]
            if missing_model_vars:
                raise KeyError(
                    f"Required model variable(s) {missing_model_vars} not found in input "
                    f"files for '{self.cmor_name}'. "
                    f"Available data variables: {sorted(self.ds.data_vars)}. "
                    f"Check 'model_variables' in the mapping."
                )
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

        # Rename axes and bounds variables. The grid dimensions ni/nj are pure
        # dimensions (no same-named coordinate variable), so `k in self.ds` alone
        # would drop them and leave the data variable on (nj, ni) while the
        # supergrid coordinates are built on (j, i); include dimension names too.
        rename_map = {
            k: v
            for k, v in {
                **bounds_rename_map,
                **axes_rename_map,
                **seaice_dim_rename,
            }.items()
            if k in self.ds or k in self.ds.dims
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

        if self.cmip7_grid_labels is not None:
            grid_labels = self.cmip7_grid_labels.get(self._cmip7_component, {})
            self.vocab.grid_label = (
                grid_labels.get(grid_type)
                or grid_labels.get("default")
                or self.vocab.grid_label
            )

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
        # Bounds variables must not carry standard_name (CF §7.1); the
        # published reference keeps only units on vertices_latitude/longitude.
        self.ds["vertices_latitude"].attrs.update({"units": "degrees_north"})
        self.ds["vertices_longitude"].attrs.update({"units": "degrees_east"})

        # The supergrid latitude/longitude replace the model's curvilinear
        # coordinates (renamed from TLAT/TLON to lat/lon). Drop the redundant
        # originals — they carry no standard_name and would otherwise be
        # mis-detected as data variables — and point the data variable at the
        # supergrid auxiliary coordinates, clearing the stale
        # `coordinates = "TLON TLAT time"` inherited from the model file.
        self.ds = self.ds.drop_vars(
            ["lat", "lon", "lat_bnds", "lon_bnds"], errors="ignore"
        )
        self.ds[self.cmor_name].attrs["coordinates"] = "latitude longitude"

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

        # Keep `vertices` as a pure dimension, as the ocean path does: an
        # attribute-less `vertices` coordinate variable is not part of the CMOR
        # data model and fails CF §3.3.
        if "vertices" in self.ds.coords:
            self.ds = self.ds.drop_vars("vertices")

        cmor_attrs = self.vocab.variable
        self.ds[self.cmor_name].attrs.update(
            {k: v for k, v in cmor_attrs.items() if v not in (None, "")}
        )
        # CMIP7 tables don't carry a per-variable "type" (unlike CMIP6), so
        # falling back to a hardcoded "double" here silently upcasts every
        # CMIP7 variable and drifts its _FillValue precision in the process.
        # Preserve the source dtype when the table is silent instead.
        var_type = cmor_attrs.get("type")
        target_dtype = (
            np.dtype(self.type_mapping[var_type])
            if var_type in self.type_mapping
            else self.ds[self.cmor_name].dtype
        )
        self.ds[self.cmor_name] = self.ds[self.cmor_name].astype(target_dtype)
        # Re-cast the fill/missing value to the final dtype: they were
        # computed against the pre-cast dtype in standardize_missing_values(),
        # so a dtype change here would otherwise leave a mismatched sentinel.
        for attr in ("_FillValue", "missing_value"):
            if attr in self.ds[self.cmor_name].attrs:
                self.ds[self.cmor_name].attrs[attr] = target_dtype.type(
                    self.ds[self.cmor_name].attrs[attr]
                )

        # Apply CF time-coordinate attributes (standard_name, axis, long_name)
        # from the CMOR table; the manual coordinate build above does not.
        self._apply_time_coordinate_attributes()

        # Check calendar and units
        if "time" in self.ds.dims:
            self._check_calendar("time")
