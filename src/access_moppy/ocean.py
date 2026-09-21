import warnings
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import numpy as np
import xarray as xr

from access_moppy.base import CMORiser
from access_moppy.derivations import custom_functions, evaluate_expression
from access_moppy.ocean_supergrid import Supergrid
from access_moppy.utilities import calculate_latitude_bounds
from access_moppy.vocabulary_processors import (
    CMIP6Vocabulary,
    apply_cell_measures_override,
)

#: CMOR table dimensions that stand for a latitude axis, and for a longitude
#: axis. A variable that asks for the first without the second has been summed
#: along longitude (the overturning streamfunctions) and keeps a 1-D latitude
#: coordinate rather than the model's 2-D curvilinear grid.
_LATITUDE_DIMS = frozenset({"latitude", "gridlatitude"})
_LONGITUDE_DIMS = frozenset({"longitude", "gridlongitude"})

#: Bounds the ocean CMORiser builds itself, from the model's own vertical cell
#: edges or its native y axis. The generic calculator in the base class cannot
#: reach them: it looks a coordinate up by its CMOR name, and the rename from
#: st_ocean/yu_ocean happens after that call.
_NATIVE_BOUNDS = frozenset({"lev_bnds", "rho_bnds", "lat_bnds", "rlat_bnds"})


class Ocean_CMORiser(CMORiser):
    """
    CMORiser subclass for ocean variables using curvilinear supergrid coordinates.
    """

    #: Model vertical dimension -> the variable holding that dimension's cell
    #: edges. The edges are the only faithful source for lev_bnds: MOM cell
    #: centres are not midway between their edges (up to ~6 m off in the ACCESS
    #: z* grid), so interpolating bounds from the centres would be wrong.
    #: Subclasses that know their model's naming override this.
    depth_edges: Dict[str, str] = {}

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
        cell_measures_overrides: Optional[Dict[str, Any]] = None,
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
        )
        self.supergrid = None  # To be defined in subclasses
        self.grid_info = None
        self.grid_type = None
        self.symmetric = None
        self.arakawa = None
        self.cmip7_grid_labels = cmip7_grid_labels
        self.cell_measures_overrides = cell_measures_overrides
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

    def _is_zonal_variable(self) -> bool:
        """Whether the CMOR table asks for a latitude axis and no longitude one.

        True for the overturning streamfunctions (msftmz, msftyz, msftmrho,
        msftyrho), which are summed along longitude within a basin. They keep
        the model's own 1-D y axis as ``lat`` (or ``rlat``) instead of being
        placed on the 2-D curvilinear ``i``/``j`` grid every other ocean
        variable uses.

        For the ``gridlatitude`` (rlat) variants that axis is exactly what CMIP
        asks for. For the ``latitude`` ones it is the model's nominal latitude,
        which north of ~65°N labels a row of the tripolar grid rather than a
        true circle of latitude — the same approximation every tripolar-grid
        model publishes msftmz under.
        """
        expected = self._expected_dim_names()
        return bool(expected & _LATITUDE_DIMS) and not (expected & _LONGITUDE_DIMS)

    def _mapped_dimensions(self) -> Dict[str, str]:
        """Return the mapping's model dimension -> CMOR out_name dict."""
        return self.mapping.get(self.cmor_name, {}).get("dimensions", {})

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

    def _bounds_dimension(self) -> str:
        """Return the bounds dimension a new bounds variable should use.

        Matching whichever one the time bounds already use keeps the later
        nv -> bnds rename in update_attributes from colliding.
        """
        return "nv" if "nv" in self.ds.dims else "bnds"

    def _add_depth_bounds_from_edges(self, required_bounds):
        """Build ``lev_bnds`` (or ``rho_bnds``) from the model's own cell edges.

        Called before the dimension rename, so the bounds are attached to the
        model's own vertical dimension (e.g. ``st_ocean``, ``potrho``) and are
        carried over to ``lev``/``rho`` by that rename.
        """
        dimensions = self._mapped_dimensions()

        for depth_dim, edges_name in self.depth_edges.items():
            if depth_dim not in self.ds.dims:
                continue

            bnds_var = f"{dimensions.get(depth_dim, 'lev')}_bnds"
            if bnds_var not in required_bounds or bnds_var in self.ds:
                continue

            if edges_name not in self.ds:
                warnings.warn(
                    f"'{edges_name}' not found in raw data; '{depth_dim}' cell bounds "
                    f"cannot be derived and {bnds_var} will be missing from the output.",
                    UserWarning,
                    stacklevel=2,
                )
                continue

            edges = self.ds[edges_name].values
            if edges.ndim != 1 or edges.size != self.ds.sizes[depth_dim] + 1:
                warnings.warn(
                    f"'{edges_name}' has {edges.shape} values, expected "
                    f"{self.ds.sizes[depth_dim] + 1} contiguous edges for "
                    f"'{depth_dim}'; skipping {bnds_var}.",
                    UserWarning,
                    stacklevel=2,
                )
                continue

            self.ds[bnds_var] = (
                (depth_dim, self._bounds_dimension()),
                np.stack([edges[:-1], edges[1:]], axis=-1),
            )
            self.ds = self.ds.drop_vars(edges_name)

    def _add_latitude_bounds_from_coordinate(self, required_bounds):
        """Build ``lat_bnds``/``rlat_bnds`` for the zonally summed variables.

        The overturning streamfunctions keep the model's 1-D y axis as their
        latitude coordinate, and MOM writes no bounds for it. Called before the
        dimension rename, so the bounds are attached to the model's own name
        (``yu_ocean_bnds``) and are carried over to ``lat_bnds`` by that
        rename.

        The y axis is a row of cell *faces*, not centres — the meridional
        transport is reported on the northern face of each tracer cell — so
        there are no model-supplied edges to use, and the midpoints between
        successive rows are the best available bounds.
        """
        if not self._is_zonal_variable():
            return

        for model_dim, cmor_name in self._mapped_dimensions().items():
            if cmor_name not in ("lat", "rlat"):
                continue
            if f"{cmor_name}_bnds" not in required_bounds:
                continue
            bnds_var = f"{model_dim}_bnds"
            if bnds_var in self.ds or model_dim not in self.ds.coords:
                continue
            self.ds[bnds_var] = calculate_latitude_bounds(
                self.ds, model_dim, bnds_name=self._bounds_dimension()
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
            + [
                edges
                for dim, edges in self.depth_edges.items()
                if dim in axes_rename_map
            ]
        )
        self.load_dataset(required_vars=required)

        # Remove spurious time dimensions from spatial bounds and coordinates
        # Note sure this is required for ocean data, but we had some issues with this for some variables in the past, so we'll keep it here for now.
        # self.remove_spurious_time_dimensions(required_vars)

        # Ensure time dimension is sorted
        self.sort_time_dimension()

        self._add_depth_bounds_from_edges(required_bounds)
        self._add_latitude_bounds_from_coordinate(required_bounds)

        # Calculate missing bounds variables. For ocean variables this only ever
        # covers time_bnds: the 2-D curvilinear lat/lon use vertices_* bounds and
        # are excluded from required_bounds by _get_required_bounds_variables.
        # The vertical and zonal-latitude bounds are handled above instead — those
        # coordinates are still under their model names at this point, so the
        # generic calculator (which looks a coordinate up by its CMOR name) cannot
        # see them.
        self.calculate_missing_bounds_variables(
            {k: v for k, v in required_bounds.items() if k not in _NATIVE_BOUNDS}
        )

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

        if self._is_zonal_variable():
            # A zonally summed variable has no i dimension, so its y axis is a
            # latitude coordinate in its own right rather than the j index of a
            # curvilinear grid. Let the CMOR table's own name for it (lat, rlat)
            # win over the blanket yu_ocean -> j rename.
            ocean_dim_rename = {
                k: v for k, v in ocean_dim_rename.items() if k not in axes_rename_map
            }

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

        # Define the preferred dimension order. CMOR writes a variable's
        # dimensions in the reverse of the order the table lists them, which for
        # the basin-split streamfunctions is (time, basin, lev, lat).
        preferred_order = ["time", "basin", "lev", "rho", "j", "lat", "rlat", "i"]

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

        # Answer a "--MODEL" cell_measures with the measure this model
        # publishes for the point the field sits on, when the config names one.
        # Runs before the global attributes are built so external_variables
        # picks it up.
        if self.cell_measures_overrides is not None:
            _measures = self.cell_measures_overrides.get(self._cmip7_component, {})
            apply_cell_measures_override(
                self.vocab, _measures.get(grid_type) or _measures.get("default")
            )

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
            # CF §7.1 — a bounds variable inherits units and standard_name from
            # its parent and must not repeat them. CMOR's published CMIP6 output
            # does keep units here, but CF-1.11 tightened §7.1 against it and
            # these files declare CF-1.12, so the vertices carry no attributes.
            self.ds["vertices_latitude"].attrs = {}
            self.ds["vertices_longitude"].attrs = {}

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
            self._preserve_bounds_time_encoding("time_bnds")
            self.ds["time_bnds"].attrs = {}

        # Ocean builds its coordinate set manually rather than through the
        # atmosphere's axis loop, so a coordinate carried straight over from the
        # model would keep its native metadata ("tcell zstar depth" / "meters" /
        # the non-CF cartesian_axis and edges attributes for `lev`, and the
        # equivalents for `rho` and for the `lat`/`rlat` of a zonally summed
        # variable). Replace the lot with the CMOR axis definition.
        #
        # Only 1-D coordinates: `time` has its own pass below, and the scalar
        # table-defined ones (mlotst's `deltasigt`, fgco2's `depth0m`) are
        # synthesized with their attributes further down.
        for meta in self.vocab.axes.values():
            name = meta.get("out_name")
            if name in (None, "time") or name not in self.ds.coords:
                continue
            if self.ds[name].ndim != 1:
                continue
            axis_attrs = {
                k: meta[k]
                for k in ("standard_name", "long_name", "units", "axis", "positive")
                if meta.get(k) not in (None, "")
            }
            if f"{name}_bnds" in self.ds:
                axis_attrs["bounds"] = f"{name}_bnds"
            self.ds[name].attrs = axis_attrs

        cmor_attrs = self.vocab.variable
        self._apply_cmor_variable_attributes(cmor_attrs)
        self._drop_stale_range_attributes(cmor_attrs)
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

        # Some CMOR dimensions (e.g. mlotst's `deltasigt`, fgco2's `depth0m`) are
        # fixed scalar coordinates defined entirely by the table rather than
        # carried in the model output. Synthesize any such coordinate that the
        # table requires but the dataset doesn't already have.
        for meta in self.vocab.axes.values():
            name = meta.get("out_name")
            if name in self.ds or "value" not in meta:
                continue
            dtype = self.type_mapping.get(meta.get("type", "double"), np.float64)
            coord_attrs = {
                k: v
                for k, v in {
                    "standard_name": meta.get("standard_name"),
                    "long_name": meta.get("long_name"),
                    "units": meta.get("units"),
                }.items()
                if v
            }
            self.ds = self.ds.assign_coords(
                {name: xr.DataArray(dtype(meta["value"]), dims=(), attrs=coord_attrs)}
            )

        # Apply CF time-coordinate attributes (standard_name, axis, long_name)
        # from the CMOR table; the manual coordinate build above does not.
        self._apply_time_coordinate_attributes()

        # Check calendar and units
        if "time" in self.ds.dims:
            self._check_calendar("time")

        # CF-1.11 units_metadata for the temperature and time units, last so it
        # sees the final variable units and the normalized calendar.
        self._apply_units_metadata()

        # Last of all: strip the ACCESS-native attributes the raw files carry.
        # After _check_calendar, which reads and rewrites calendar_type, and
        # after every step above that sets attributes of its own.
        self._drop_model_native_attributes()


class Ocean_CMORiser_OM2(Ocean_CMORiser):
    """CMORiser for ocean variables on the ACCESS-OM2 model using B-grid supergrid coordinates."""

    depth_edges = {
        "st_ocean": "st_edges_ocean",
        "sw_ocean": "sw_edges_ocean",
        # The overturning streamfunctions in density space are on potential
        # density rather than depth, but their bounds come from edges the same
        # way.
        "potrho": "potrho_edges",
    }

    def __init__(
        self,
        input_data: Optional[Union[str, List[str], xr.Dataset, xr.DataArray]] = None,
        *,
        output_path: str,
        compound_name: str,
        vocab: CMIP6Vocabulary,
        variable_mapping: Dict[str, Any],
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
        cell_measures_overrides: Optional[Dict[str, Any]] = None,
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
            cell_measures_overrides=cell_measures_overrides,
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
        # MOM5 prefixes the horizontal axes of its density-space diagnostics
        # (ty_trans_rho and friends) with "grid_", on the same points.
        points = {str(coord).removeprefix("grid_") for coord in present_coords}

        for type_, coords in grid_types.items():
            if coords.issubset(points):
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
                # Density-space diagnostics (ty_trans_rho and friends) sit on
                # the same points under a "grid_" prefixed name.
                "grid_xt_ocean": "i",
                "grid_yt_ocean": "j",
                "grid_xu_ocean": "i",
                "grid_yu_ocean": "j",
                "st_ocean": "lev",  # depth level
                "sw_ocean": "lev",  # depth level at w-points (wo, wmo)
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
        cell_measures_overrides: Optional[Dict[str, Any]] = None,
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
            cell_measures_overrides=cell_measures_overrides,
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
