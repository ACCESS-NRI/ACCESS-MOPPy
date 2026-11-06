from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import numpy as np
import xarray as xr

from access_moppy.derivations import custom_functions, evaluate_expression
from access_moppy.ocean import (
    Ocean_CMORiser,
    validate_calendar_month_bounds,
    validated_wet_mask,
)
from access_moppy.ocean_supergrid import Supergrid
from access_moppy.vocabulary_processors import (
    CMIP6Vocabulary,
    apply_cell_measures_override,
)


def _clean_cice_angles(values, wet, *, latitude, label):
    """Keep supplied land geometry where valid; clear only inactive fill cells."""
    array = np.asarray(values, dtype=float)
    if latitude:
        valid = np.isfinite(array) & (array >= -90) & (array <= 90)
    else:
        # CICE longitudes may cross a seam, but netCDF default fill values are
        # enormous. Permit one unwrapped revolution on either side.
        valid = np.isfinite(array) & (array >= -720) & (array <= 720)
    if not np.all(valid[wet]):
        raise ValueError(f"CICE wet {label} contains missing or invalid positions")
    return np.where(valid, array, np.nan)


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
        cell_measures_overrides: Optional[Dict[str, Any]] = None,
        grid_data: xr.Dataset | None = None,
        supergrid_data: xr.Dataset | None = None,
        grid_identity_verified: bool = False,
        grid_reference: str | None = None,
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
            cell_measures_overrides=cell_measures_overrides,
            grid_data=grid_data,
            supergrid_data=supergrid_data,
            grid_identity_verified=grid_identity_verified,
            grid_reference=grid_reference,
        )

        self._uses_line_axis = (
            "line" in self.mapping[self.cmor_name].get("dimensions", {}).values()
        )
        if not self._uses_line_axis:
            if grid_data is not None and supergrid_data is None:
                self.supergrid = None
            else:
                nominal_resolution = (
                    None
                    if supergrid_data is not None
                    else vocab._get_nominal_resolution(target_realm="seaIce")
                )
                self.supergrid = Supergrid(nominal_resolution, dataset=supergrid_data)
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
        if self.grid_data is not None and self.cmor_name in {"siconc", "areacello"}:
            return "T", None
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
        bnds_name = self.ds["time"].attrs.get("bounds") or self.ds["time"].encoding.get(
            "bounds"
        )
        if not bnds_name or bnds_name not in self.ds:
            if getattr(self, "grid_data", None) is not None and "time" in self.ds.dims:
                raise ValueError(
                    "Native CICE monthly input requires explicit time bounds"
                )
            return
        if getattr(self, "grid_data", None) is not None:
            bounds = self.ds[bnds_name]
            extra = [dim for dim in bounds.dims if dim != "time"]
            if bounds.ndim != 2 or len(extra) != 1 or bounds.sizes[extra[0]] != 2:
                raise ValueError("Native CICE time bounds require two endpoints")
            bounds = bounds.transpose("time", extra[0])
            validate_calendar_month_bounds(bounds.values, self.ds["time"], "CICE")
            midpoint = (
                bounds.isel({extra[0]: 0}, drop=True)
                + (
                    bounds.isel({extra[0]: 1}, drop=True)
                    - bounds.isel({extra[0]: 0}, drop=True)
                )
                / 2
            )
            original_attrs = dict(self.ds["time"].attrs)
            original_encoding = dict(self.ds["time"].encoding)
            self.ds = self.ds.assign_coords(time=("time", midpoint.values))
            self.ds["time"].attrs = original_attrs
            self.ds["time"].encoding = original_encoding
        rename = {}
        if bnds_name != "time_bnds":
            rename[bnds_name] = "time_bnds"
        bnd_dim = next((d for d in self.ds[bnds_name].dims if d != "time"), None)
        if bnd_dim and bnd_dim != "bnds":
            rename[bnd_dim] = "bnds"
        if rename:
            self.ds = self.ds.rename(rename)
        self.ds["time"].attrs["bounds"] = "time_bnds"

    def _validate_native_grid(self):
        if self.grid_data is None or self._uses_line_axis:
            return
        if self.cmor_name not in {"siconc", "areacello"}:
            raise ValueError(
                "Explicit CICE T-grid handling is supported for siconc/areacello only"
            )
        grid = self.grid_data
        required = ("TLAT", "TLON", "tarea", "tmask")
        if any(name not in grid for name in required):
            raise ValueError(
                "Explicit CICE T grid requires TLAT, TLON, tarea, and tmask"
            )
        lat, lon, area, mask = (grid[name] for name in required)
        field = self.ds[self.cmor_name]
        dims = area.dims
        if len(dims) != 2 or any(a.dims != dims for a in (lat, lon, mask)):
            raise ValueError(
                "CICE T grid positions, area, and mask must share two dimensions"
            )
        if not all(
            dim in field.dims and field.sizes[dim] == area.sizes[dim] for dim in dims
        ):
            raise ValueError("CICE field/grid dimension or size mismatch")
        source = self._grid_identity_source
        if source is not None:
            for dim in dims:
                if dim in source.coords and dim in grid.coords:
                    if not np.array_equal(source[dim], grid[dim]):
                        raise ValueError(
                            f"CICE field/grid coordinate mismatch on {dim}"
                        )
            for name in ("TLAT", "TLON"):
                if name in source and not np.allclose(
                    source[name].values, grid[name].values, equal_nan=True
                ):
                    raise ValueError(f"CICE field/grid {name} mismatch")
        has_positions = source is not None and all(
            name in source for name in ("TLAT", "TLON")
        )
        if not (
            has_positions
            or (self.grid_identity_verified is True and self.grid_reference)
        ):
            raise ValueError(
                "CICE input lacks native grid positions; verified grid_reference required"
            )
        if (
            lat.attrs.get("units") != "degrees_north"
            or lon.attrs.get("units") != "degrees_east"
        ):
            raise ValueError("CICE T-grid positions require degree units")
        wet = validated_wet_mask(mask, "CICE")
        _clean_cice_angles(lat.values, wet.values, latitude=True, label="TLAT")
        _clean_cice_angles(lon.values, wet.values, latitude=False, label="TLON")
        factors = {
            "m2": 1.0,
            "m^2": 1.0,
            "m**2": 1.0,
            "cm2": 1e-4,
            "cm^2": 1e-4,
            "cm**2": 1e-4,
            "km2": 1e6,
        }
        unit = area.attrs.get("units")
        if unit not in factors or not bool(
            ((np.isfinite(area) & (area > 0)) | ~wet).all()
        ):
            raise ValueError(
                "CICE wet tarea must have supported area units and positive values"
            )
        if self.cmor_name == "areacello":
            expected = area * factors[unit]
            matched = xr.apply_ufunc(
                np.isclose, field, expected, kwargs={"rtol": 1e-6}, dask="allowed"
            )
            if not bool((matched | ~wet).all()):
                raise ValueError(
                    "CICE areacello mapping must convert native tarea to m2"
                )
        self.ds[self.cmor_name] = field.where(wet)

    def _native_grid_info(self, grid_type):
        if self.grid_data is None:
            return self.supergrid.extract_grid(grid_type, self.arakawa, self.symmetric)
        if grid_type != "T":
            raise ValueError(
                "Explicit CICE static geometry currently supports T cells only"
            )
        grid = self.grid_data
        lat, lon = grid["TLAT"], grid["TLON"]
        dims = lat.dims
        wet = validated_wet_mask(grid["tmask"], "CICE").values
        center_lat = _clean_cice_angles(lat.values, wet, latitude=True, label="TLAT")
        center_lon = _clean_cice_angles(lon.values, wet, latitude=False, label="TLON")
        bounds = {}
        for name, output in (("TLAT", "latitude"), ("TLON", "longitude")):
            source = grid[name]
            candidates = (
                source.attrs.get("bounds"),
                f"{name}_bnds",
                {"TLAT": "latt_bounds", "TLON": "lont_bounds"}[name],
                f"vertices_{output}",
            )
            bound_name = next(
                (candidate for candidate in candidates if candidate in grid), None
            )
            if bound_name is None:
                if self.supergrid is None:
                    raise ValueError(
                        f"CICE {name} requires native four-corner bounds or an explicitly supplied supergrid"
                    )
                generated = self.supergrid.extract_grid("T", "B")
                values = generated[f"vertices_{output}"].values
            else:
                native = grid[bound_name]
                extra = [dim for dim in native.dims if dim not in dims]
                if native.ndim != 3 or len(extra) != 1 or native.sizes[extra[0]] != 4:
                    raise ValueError(
                        f"CICE {bound_name} must have four corners per T cell"
                    )
                if native.attrs.get(
                    "units", source.attrs.get("units")
                ) != source.attrs.get("units"):
                    raise ValueError(f"CICE {name} center/corner units disagree")
                values = native.transpose(*dims, extra[0]).values
            if values.shape != (*lat.shape, 4):
                raise ValueError(f"CICE {name} corners are misaligned")
            bounds[output] = _clean_cice_angles(
                values,
                wet,
                latitude=output == "latitude",
                label=f"{name} corners",
            )
        if self.supergrid is not None:
            generated = self.supergrid.extract_grid("T", "B")
            if generated["latitude"].shape != lat.shape:
                raise ValueError("CICE T-grid/supergrid shape mismatch")
            for native, derived, longitude in (
                (center_lat, generated["latitude"].values, False),
                (center_lon, generated["longitude"].values, True),
            ):
                difference = derived - native
                if longitude:
                    difference = (difference + 180.0) % 360.0 - 180.0
                if not np.all(np.abs(difference[wet]) <= 1e-3):
                    raise ValueError("CICE T-grid/supergrid wet centers disagree")
        shape = lat.shape
        return {
            "i": xr.DataArray(np.arange(shape[1], dtype=np.int32), dims="i"),
            "j": xr.DataArray(np.arange(shape[0], dtype=np.int32), dims="j"),
            "vertices": xr.DataArray(np.arange(4), dims="vertices"),
            "latitude": xr.DataArray(center_lat, dims=("j", "i")),
            "longitude": xr.DataArray((center_lon + 360) % 360, dims=("j", "i")),
            "vertices_latitude": xr.DataArray(
                bounds["latitude"], dims=("j", "i", "vertices")
            ),
            "vertices_longitude": xr.DataArray(
                (bounds["longitude"] + 360) % 360, dims=("j", "i", "vertices")
            ),
        }

    def _discard_staggered_native_coords(self):
        """Keep only the selected T-cell geolocation on explicit CICE output."""
        if self.grid_data is None or self.grid_type != "T":
            return
        staggered = [
            name
            for name in self.ds.variables
            if name.isupper() and name.endswith(("LAT", "LON"))
        ]
        staggered_bounds = [self.ds[name].attrs.get("bounds") for name in staggered]
        self.ds = self.ds.drop_vars(
            [*staggered, *[name for name in staggered_bounds if name]],
            errors="ignore",
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
        if self._uses_line_axis:
            # This coordinate is generated by the reduction, not read from CICE.
            required.discard("siline")
        # CICE writes the time-averaging interval as `time_bounds` (on a `d2`
        # dimension), which does not match the `<dim>_bnds` name the bounds map
        # expects. Pull it in explicitly, otherwise it is dropped at load and the
        # `time:bounds` attribute is left pointing at a missing variable.
        if "time" in self._expected_dim_names():
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

        self._validate_native_grid()

        if self._uses_line_axis:
            self.grid_type, self.symmetric = "line", None
        else:
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
        preferred_order = ["time", "line", "ncat", "j", "i"]

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

        if self.cmip7_grid_labels is not None:
            grid_labels = self.cmip7_grid_labels.get(self._cmip7_component, {})
            self.vocab.grid_label = (
                grid_labels.get(grid_type)
                or grid_labels.get("default")
                or self.vocab.grid_label
            )

        # Answer a "--MODEL" cell_measures with the measure this model
        # publishes for the point the field sits on (siu/siv and the drift
        # stresses sit on the B-grid corner), when the config names one.
        if self.cell_measures_overrides is not None:
            measures = self.cell_measures_overrides.get(self._cmip7_component, {})
            apply_cell_measures_override(
                self.vocab, measures.get(grid_type) or measures.get("default")
            )
        # A placeholder the config left unanswered must not reach the file
        # (CMOR's own behaviour). Popping it from the table entry above isn't
        # enough on its own: the variable can still carry CICE's native
        # cell_measures (e.g. "area: uarea") inherited from the source file
        # untouched, which then dangles — the attribute reaches the output
        # while external_variables, built from the table entry, never
        # registers the measure it names (CF §7.2). Clear it explicitly here.
        if self.vocab.cell_measures_placeholder and not self.vocab.variable.get(
            "cell_measures"
        ):
            self.ds[self.cmor_name].attrs.pop("cell_measures", None)

        # Hemispheric totals (siarean, siextents, ...) are time series with no
        # spatial (i/j) dimensions and must not carry 2-D grid coordinates
        # (CF §5: auxiliary coordinate dims must be a subset of the variable's).
        has_spatial_dims = bool(set(self.ds[self.cmor_name].dims) & {"i", "j"})

        if has_spatial_dims:
            self.grid_info = self._native_grid_info(grid_type)
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
            # these files declare CF-1.12, so the vertices carry no attributes
            # (as in ocean.py).
            self.ds["vertices_latitude"].attrs = {}
            self.ds["vertices_longitude"].attrs = {}

        # This method overrides Ocean_CMORiser.update_attributes, so the parent's
        # time_bnds clearing never runs here. Repeat it: a bounds variable inherits
        # units/calendar from its parent (CF §7.1) and the published reference
        # leaves time_bnds attribute-free.
        if "time_bnds" in self.ds:
            self._preserve_bounds_time_encoding("time_bnds")
            self.ds["time_bnds"].attrs = {}

        # The supergrid latitude/longitude replace the model's curvilinear
        # coordinates (renamed from TLAT/TLON to lat/lon). Drop the redundant
        # originals — they carry no standard_name and would otherwise be
        # mis-detected as data variables — and point the data variable at the
        # supergrid auxiliary coordinates, clearing the stale
        # `coordinates = "TLON TLAT time"` inherited from the model file.
        self.ds = self.ds.drop_vars(
            ["lat", "lon", "lat_bnds", "lon_bnds"], errors="ignore"
        )
        # CICE input files may attach U/N/E geolocations to T-cell fields.
        # They would become extra Iris latitude/longitude coordinates.
        self._discard_staggered_native_coords()
        if has_spatial_dims:
            self.ds[self.cmor_name].attrs["coordinates"] = "latitude longitude"
        else:
            # Neither the native grid coordinates nor CICE's own
            # `cell_measures = "area: tarea"` apply to a hemispheric total.
            self.ds[self.cmor_name].attrs.pop("coordinates", None)
            self.ds[self.cmor_name].attrs.pop("cell_measures", None)
            # Drop dimensions no longer referenced by any data variable.
            used_dims = set()
            for var in self.ds.data_vars:
                used_dims.update(self.ds[var].dims)
            orphaned = [dim for dim in self.ds.dims if dim not in used_dims]
            if orphaned:
                self.ds = self.ds.drop_dims(orphaned)
            if "line" in self.ds[self.cmor_name].dims:
                self.ds["line"].attrs = {
                    "standard_name": "region",
                    "long_name": "ocean passage",
                }
                # In NetCDF this string label becomes line(line, strlen),
                # an auxiliary coordinate that must be named explicitly (CF 6.1).
                self.ds[self.cmor_name].attrs["coordinates"] = "line"

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

        dimensions = self.vocab.variable.get("dimensions", "")
        if isinstance(dimensions, str):
            dimensions = dimensions.split()
        if "typesi" in dimensions and "type" not in self.ds:
            self.ds = self.ds.assign_coords(
                type=xr.DataArray(
                    "sea_ice",
                    attrs={
                        "standard_name": "area_type",
                        "long_name": "Sea Ice area type",
                    },
                )
            )

        cmor_attrs = self.vocab.variable
        reconstruction_comment = (
            self.ds[self.cmor_name].attrs.get("comment")
            if "line" in self.ds[self.cmor_name].dims
            else None
        )
        self._apply_cmor_variable_attributes(cmor_attrs)
        if reconstruction_comment:
            table_comment = cmor_attrs.get("comment", "")
            self.ds[self.cmor_name].attrs["comment"] = " ".join(
                dict.fromkeys(c for c in (table_comment, reconstruction_comment) if c)
            )
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
