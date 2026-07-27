from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import numpy as np
import xarray as xr

from access_moppy.base import CMORiser
from access_moppy.derivations import custom_functions, evaluate_expression
from access_moppy.ocean_supergrid import Supergrid
from access_moppy.vocabulary_processors import CMIP6Vocabulary


class Ocean_CMORiser(CMORiser):
    """
    CMORiser subclass for ocean variables using curvilinear supergrid coordinates.
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
            validate_frequency=validate_frequency,
            enable_resampling=enable_resampling,
            resampling_method=resampling_method,
            enable_chunking=enable_chunking,
            chunk_size_mb=chunk_size_mb,
            max_chunk_size_mb=max_chunk_size_mb,
            write_prefetch=write_prefetch,
            split_years=split_years,
            enable_qc_plots=enable_qc_plots,
        )
        self.supergrid = None  # To be defined in subclasses
        self.grid_info = None
        self.grid_type = None
        self.symmetric = None
        self.arakawa = None
        self.cmip7_grid_labels = cmip7_grid_labels
        self._cmip7_component = "ocean"  # overridden by SeaIce_CMORiser

    def infer_grid_type(self):
        """A abstract method to infer the grid type and memory mode based on present coordinates."""
        raise NotImplementedError("Subclasses must implement infer_grid_type.")

    def _expected_dim_names(self) -> set:
        """Return the set of dim names the active CMOR table expects for this variable.

        Handles the format difference between CMIP6 (space-separated string) and
        CMIP7 (list).
        """
        dims = self.vocab.variable.get("dimensions", "")
        if isinstance(dims, str):
            dims = dims.split()
        return set(dims)

    def _align_main_var_dims_with_vocab(self):
        """Drop the time axis from the main variable when the CMOR table does not request it.

        Lets a single mapping entry serve both a fixed-table (Ofx/fx) variant
        and a time-dependent (Omon/Oday/Odec) variant of the same variable:
        the mapping describes the time-aware form, and the time axis is
        stripped only when the CMOR table requires no time.

        Scope is intentionally minimal — only the main CMOR variable is
        touched; orphan time coords on the dataset (if any) are left alone to
        preserve existing behaviour of intermediate cleanup steps.
        """
        if self.cmor_name not in self.ds:
            return
        main_var = self.ds[self.cmor_name]
        if "time" not in main_var.dims:
            return
        if "time" in self._expected_dim_names():
            return
        self.ds[self.cmor_name] = main_var.isel(time=0, drop=True)

    def _get_dim_rename(self):
        """A abstract method to get the dimension renaming mapping for the grid type."""
        raise NotImplementedError("Subclasses must implement _get_dim_rename.")

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

        # Remove spurious time dimensions from spatial bounds and coordinates
        # Note sure this is required for ocean data, but we had some issues with this for some variables in the past, so we'll keep it here for now.
        # self.remove_spurious_time_dimensions(required_vars)

        # Ensure time dimension is sorted
        self.sort_time_dimension()

        # Calculate missing bounds variables. For ocean variables this only ever
        # covers time_bnds: the 2-D curvilinear lat/lon use vertices_* bounds and
        # are excluded from required_bounds by _get_required_bounds_variables.
        self.calculate_missing_bounds_variables(required_bounds)

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
            # Variables listed in model_variables that are absent from the
            # loaded dataset (e.g. optional frazil fields) are silently
            # omitted from the context; individual derivation functions
            # handle the None case via {"optional": ...} expressions.
            context = {var: self.ds[var] for var in required_vars if var in self.ds}
            context.update(custom_functions)
            self.ds[self.cmor_name] = evaluate_expression(calc, context)
        elif calc["type"] == "dataset_function":
            # Function that operates on the full dataset
            func_name = calc["function"]
            self.ds = self.ds.rename({required_vars[0]: self.cmor_name})
            self.ds = custom_functions[func_name](self.ds, **calc.get("kwargs", {}))
        else:
            raise ValueError(f"Unsupported calculation type: {calc['type']}")

        # Strip the time axis when the active CMOR table does not request it.
        # Lets a single ocean mapping entry serve both fx and time-dependent
        # tables of the same variable (e.g. Ofx.masscello vs Omon.masscello).
        self._align_main_var_dims_with_vocab()

        self.grid_type, self.symmetric = self.infer_grid_type()

        # Get ocean rename map
        ocean_dim_rename = self._get_dim_rename()

        # Rename axes and bounds variables
        rename_map = {
            k: v
            for k, v in {
                **bounds_rename_map,
                **axes_rename_map,
                **ocean_dim_rename,
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

        # Define the preferred dimension order
        preferred_order = ["time", "lev", "j", "i"]

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
        grid_type = self.grid_type
        arakawa = self.arakawa
        symmetric = self.symmetric

        # Resolve the CMIP7 grid label from the inferred grid type when the
        # caller did not supply an explicit label (indicated by cmip7_grid_labels
        # being set on this instance).
        if self.cmip7_grid_labels is not None:
            _cfg = self.cmip7_grid_labels.get(self._cmip7_component, {})
            resolved = (
                _cfg.get(grid_type) or _cfg.get("default") or self.vocab.grid_label
            )
            self.vocab.grid_label = resolved

        self.grid_info = self.supergrid.extract_grid(grid_type, arakawa, symmetric)

        # Scalar time-series variables (e.g. zostoga) have no spatial (i/j)
        # dimensions and must not carry 2-D grid coordinates.  When spatial
        # dims are absent, drop any orphaned dimension coordinates left behind
        # after intermediate variables were removed.
        cmor_dims = set(self.ds[self.cmor_name].dims)
        has_spatial_dims = bool(cmor_dims & {"i", "j"})

        if has_spatial_dims:
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

            # Point the data variable at the curvilinear auxiliary coordinates we
            # just built. The model file's `coordinates` attribute (e.g.
            # "geolon_t geolat_t") names the native grid variables, which are not
            # carried into the CMORised output — leaving it stale makes the WCRP
            # ATTR004 "coordinates as-variable" check fail on missing references.
            # (self.cmor_name is guaranteed present: its dims were read above.)
            self.ds[self.cmor_name].attrs["coordinates"] = "latitude longitude"
        else:
            # Drop dimensions that are no longer referenced by any data variable.
            used_dims = set()
            for var in self.ds.data_vars:
                used_dims.update(self.ds[var].dims)
            orphaned = [dim for dim in self.ds.dims if dim not in used_dims]
            if orphaned:
                self.ds = self.ds.drop_dims(orphaned)

        self.ds.attrs = {
            k: v
            for k, v in self.vocab.get_required_global_attributes().items()
            if v not in (None, "")
        }

        if "nv" in self.ds.dims:
            self.ds = self.ds.rename_dims({"nv": "bnds"})
            # Drop the nv coordinate variable so bnds remains a pure dimension
            # with no index values, as required by CMIP6.
            if "nv" in self.ds.coords:
                self.ds = self.ds.drop_vars("nv")

        # calculate_missing_bounds_variables attaches a [0, 1] index coordinate to
        # the bnds dimension; drop it so bnds stays a pure dimension as above.
        if "bnds" in self.ds.coords:
            self.ds = self.ds.drop_vars("bnds")

        # Keep `vertices` as a pure dimension (the published reference has no
        # `vertices` coordinate variable; a present one fails CF §2.2/§3.3).
        if "vertices" in self.ds.coords:
            self.ds = self.ds.drop_vars("vertices")

        # Bounds variables inherit units/calendar from their parent coordinate
        # (CF §7.1); the published reference leaves time_bnds attribute-free.
        if "time_bnds" in self.ds:
            self.ds["time_bnds"].attrs = {}

        # The vertical coordinate needs a CF `axis` attribute (WCRP ATTR001); the
        # model file only carries the non-CF `cartesian_axis`.
        if "lev" in self.ds.coords:
            self.ds["lev"].attrs["axis"] = "Z"

        cmor_attrs = self.vocab.variable
        self.ds[self.cmor_name].attrs.update(
            {k: v for k, v in cmor_attrs.items() if v not in (None, "")}
        )
        var_type = cmor_attrs.get("type", "double")
        self.ds[self.cmor_name] = self.ds[self.cmor_name].astype(
            self.type_mapping.get(var_type, np.float64)
        )

        # Apply CF time-coordinate attributes (standard_name, axis, long_name)
        # from the CMOR table; the manual coordinate build above does not.
        self._apply_time_coordinate_attributes()

        # Check calendar and units
        if "time" in self.ds.dims:
            self._check_calendar("time")


class Ocean_CMORiser_OM2(Ocean_CMORiser):
    """CMORiser for ocean variables on the ACCESS-OM2 model using B-grid supergrid coordinates."""

    def __init__(
        self,
        input_data: Optional[Union[str, List[str], xr.Dataset, xr.DataArray]] = None,
        *,
        output_path: str,
        compound_name: str,
        vocab: CMIP6Vocabulary,
        variable_mapping: Dict[str, Any],
        drs_root: Optional[Path] = None,
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
            compound_name=compound_name,
            vocab=vocab,
            variable_mapping=variable_mapping,
            drs_root=drs_root,
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

        nominal_resolution = vocab._get_nominal_resolution(target_realm="ocean")
        self.supergrid = Supergrid(nominal_resolution)
        self.grid_info = None
        self.grid_type = None
        self.symmetric = None  # MOM5 does not have configurable memory modes
        self.arakawa = "B"  # ACCESS-OM2 MOM5 uses B-grid

    def infer_grid_type(self):
        """Infer the grid type (T, U, V, C) and memory mode based on present coordinates."""
        grid_types = {
            "T": {"xt_ocean", "yt_ocean"},
            "U": {"xu_ocean", "yt_ocean"},
            "V": {"xt_ocean", "yu_ocean"},
            "C": {"xu_ocean", "yu_ocean"},
        }
        present_coords = set(self.ds.coords)

        for type_, coords in grid_types.items():
            if coords.issubset(present_coords):
                return type_, None

        expected = {t: sorted(c) for t, c in grid_types.items()}
        raise ValueError(
            f"Could not infer grid type from dataset coordinates (MOM5/OM2). "
            f"Expected one of {expected}. "
            f"Found coordinates: {sorted(present_coords)}"
        )

    def _get_dim_rename(self):
        """Get the dimension renaming mapping for the grid type."""

        supported_sources = [
            "ACCESS-OM2",
            "ACCESS-CM",
            "ACCESS-ESM1-5",
            "ACCESS-ESM1-6",
        ]
        if self.vocab.source_id in supported_sources:
            return {
                "xt_ocean": "i",
                "yt_ocean": "j",
                "xu_ocean": "i",
                "yu_ocean": "j",
                "st_ocean": "lev",  # depth level
            }
        else:
            raise ValueError(
                f"Unsupported source_id '{self.vocab.source_id}' for Ocean_CMORiser_OM2. "
                f"Supported: {supported_sources}"
            )


class Ocean_CMORiser_OM3(Ocean_CMORiser):
    """CMORiser subclass for ocean variables on the ACCESS-OM3 model using C-grid supergrid coordinates."""

    def __init__(
        self,
        input_data: Optional[Union[str, List[str], xr.Dataset, xr.DataArray]] = None,
        *,
        output_path: str,
        compound_name: str,
        vocab: CMIP6Vocabulary,
        variable_mapping: Dict[str, Any],
        drs_root: Optional[Path] = None,
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
            compound_name=compound_name,
            vocab=vocab,
            variable_mapping=variable_mapping,
            drs_root=drs_root,
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

        nominal_resolution = vocab._get_nominal_resolution(target_realm="ocean")
        self.supergrid = Supergrid(nominal_resolution)
        self.grid_info = None
        self.grid_type = None
        self.symmetric = None
        self.arakawa = "C"  # ACCESS-OM3 MOM6 uses C-grid

    def infer_grid_type(self):
        """Infer the grid type (T, U, V, C) and memory mode based on present coordinates."""
        grid_types = {
            "T": {"xh", "yh"},
            "U": {"xq", "yh"},
            "V": {"xh", "yq"},
            "C": {"xq", "yq"},
        }
        present_coords = set(self.ds.coords)

        # TODO: Currently assume MOM6 always uses symmetric memory mode.
        # We may need to revisit this.
        symmetric = True
        for type_, coords in grid_types.items():
            if coords.issubset(present_coords):
                return type_, symmetric

        expected = {t: sorted(c) for t, c in grid_types.items()}
        raise ValueError(
            f"Could not infer grid type from dataset coordinates (MOM6/OM3). "
            f"Expected one of {expected}. "
            f"Found coordinates: {sorted(present_coords)}"
        )

    def _get_dim_rename(self):
        """Get the dimension renaming mapping for the grid type."""
        if "ACCESS-OM3" in self.vocab.source_id or "ACCESS-CM" in self.vocab.source_id:
            return {
                "xh": "i",
                "yh": "j",
                "xq": "i",
                "yq": "j",
                "zl": "lev",  # depth level
            }
        else:
            raise ValueError(
                f"Unsupported source_id '{self.vocab.source_id}' for Ocean_CMORiser_OM3. "
                f"source_id must contain 'ACCESS-OM3' or 'ACCESS-CM'."
            )
