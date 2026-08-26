import re

import dask.array as da
import numpy as np
import xarray as xr

from access_moppy.base import CMORiser
from access_moppy.derivations import (
    TIME_REDUCTION_OPERATIONS,
    custom_functions,
    evaluate_expression,
)
from access_moppy.utilities import calculate_time_bounds


class Atmosphere_CMORiser(CMORiser):
    """
    Handles CMORisation of NetCDF datasets for Atmosphere/Land variables across CMIP versions.
    """

    def _is_daily_extrema_target(self, calc):
        """Return whether a monthly-reduction formula targets daily tasmin/tasmax.

        The tasmin/tasmax mapping entries read the model's daily extrema
        (e.g. fld_s03i236_min/_max) and reduce them to monthly with a formula.
        For daily targets the input already *is* the requested field, so the
        formula must be bypassed and the variable renamed directly.
        """
        if self.cmor_name not in {"tasmin", "tasmax"} or not self.compound_name:
            return False

        operation = calc.get("operation", "")
        formula = calc.get("formula", "")
        uses_monthly_extrema = operation in TIME_REDUCTION_OPERATIONS or any(
            f"{name}(" in formula for name in TIME_REDUCTION_OPERATIONS
        )
        if not uses_monthly_extrema:
            return False

        parts = self.compound_name.split(".")
        frequency = parts[0] if len(parts) == 2 else parts[-2]
        return frequency.lower().endswith("day")

    def _replace_time_bounds_with_computed(self):
        """Replace a raw, source-file 'time_bnds' with one computed directly
        from the time coordinate already resident in self.ds.

        The UM archive packs every atmosphere/land variable for a month into
        one file, so a raw ``time_bnds`` opened via ``open_mfdataset`` carries
        a Dask graph tying every timestep's bounds to a single multi-file
        merge step. Unlike the main data variable, that graph cannot be
        pruned down to one output chunk's own files by
        ``_write_dask_slices()``'s ``dask.cull()`` -- so every chunked write
        re-reads the *entire* input file list just for these two numbers per
        timestep, dominating wall time on multi-century runs (see
        docs/investigations/time_bnds_culling.md -- 56% of total task time
        measured on a 348-file/3-chunk run).

        ``calculate_time_bounds()`` derives numerically identical values
        (verified byte-for-byte against this model's native monthly output,
        including leap years) directly from the time coordinate, which is
        already fully resident by this point -- at a fraction of the cost.
        Attrs are cleared to match the empty-attrs convention of the raw
        model output, so published files are unchanged by this fix.
        """
        if "time" not in self.ds.coords:
            return
        bounds_name = self.ds["time"].attrs.get("bounds")
        if not bounds_name or bounds_name not in self.ds:
            return  # nothing to replace; calculate_missing_bounds_variables() already ran
        if not isinstance(self.ds[bounds_name].data, da.Array):
            return  # already eager (e.g. just synthesized above) -- nothing to gain
        synthetic = calculate_time_bounds(
            self.ds,
            time_coord="time",
            bnds_name="bnds",
            freq_hint=self._target_frequency_hint(),
        )
        synthetic.attrs = {}
        self.ds[bounds_name] = synthetic

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

    def _retarget_renamed_references(self, rename_map):
        """Rewrite ``coordinates`` / ``formula_terms`` attribute strings to the
        post-rename variable names.

        ``Dataset.rename`` relabels variables but leaves attribute strings that
        reference them untouched. Only tokens that are keys in ``rename_map`` are
        rewritten, so variables whose references were not renamed are unaffected.

        - ``coordinates``: a space-separated list of variable names.
        - ``formula_terms``: ``"<term>: <variable> ..."`` — only the variable
          tokens (those not ending in ``:``) are remapped; the term names are
          left as-is.
        """
        if not rename_map:
            return
        for var in self.ds.variables:
            attrs = self.ds[var].attrs

            coords = attrs.get("coordinates")
            if isinstance(coords, str) and coords:
                attrs["coordinates"] = " ".join(
                    rename_map.get(tok, tok) for tok in coords.split()
                )

            terms = attrs.get("formula_terms")
            if isinstance(terms, str) and terms:
                attrs["formula_terms"] = " ".join(
                    tok if tok.endswith(":") else rename_map.get(tok, tok)
                    for tok in terms.split()
                )

    def _normalize_hybrid_bounds(self):
        """Normalize hybrid-coordinate bounds to ascending [min, max] pairs.

        Some UM files encode ``b_bnds`` as descending ``[upper, lower]`` pairs.
        WCRP VAR012 expects each pair to bracket ``b`` with ordered bounds.
        """
        if "b_bnds" not in self.ds or "b" not in self.ds:
            return

        b_bnds = self.ds["b_bnds"]
        if b_bnds.ndim < 1 or b_bnds.shape[-1] != 2:
            return

        self.ds["b_bnds"] = xr.DataArray(
            np.sort(b_bnds.values, axis=-1),
            dims=b_bnds.dims,
            coords=b_bnds.coords,
            attrs=b_bnds.attrs,
        )

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

            # An internal calculation builds its own grid, so the coordinate bounds
            # that the discovery path inherits from the source files are never
            # present. The axes declare must_have_bounds, and both reorder() and the
            # CF/WCRP checks expect the bounds variables to exist, so fill them in
            # before returning past the code that would otherwise do it.
            bnds_required, _ = self.vocab._get_required_bounds_variables(self.mapping)
            self.calculate_missing_bounds_variables(bnds_required)

            return

        # Original logic for other calculation types
        # Select input variables required for the CMOR variable
        required_vars = self.mapping[self.cmor_name]["model_variables"]

        required_axes, axes_rename_map = self.vocab._get_axes(self.mapping)
        required_bounds, bounds_rename_map = self.vocab._get_required_bounds_variables(
            self.mapping
        )

        required = set(
            list(required_vars)
            + list(axes_rename_map.keys())
            + list(bounds_rename_map.keys())
        )
        self.load_dataset(required_vars=required)

        # Validate that all required model variables were actually loaded.
        # Without this, a missing variable is only caught at the rename/formula
        # step, producing a cryptic ValueError. Raise early with actionable context.
        missing_model_vars = [v for v in required_vars if v not in self.ds]
        if missing_model_vars:
            available = sorted(self.ds.data_vars)
            raise KeyError(
                f"Required model variable(s) {missing_model_vars} not found in the "
                f"input files for '{self.cmor_name}'. "
                f"Available data variables: {available}. "
                f"Check the 'model_variables' entry in the mapping."
            )

        # Remove spurious time dimensions from spatial bounds and coordinates
        self.remove_spurious_time_dimensions(required_vars)

        # Ensure time dimension is sorted
        self.sort_time_dimension()

        # Handle the calculation type
        if calc["type"] == "direct" or self._is_daily_extrema_target(calc):
            # If the calculation is direct, just rename the variable
            if required_vars[0] != self.cmor_name:
                self.ds = self.ds.rename({required_vars[0]: self.cmor_name})
        elif calc["type"] == "formula":
            context = {var: self.ds[var] for var in required_vars}
            context.update(custom_functions)

            # Save original time attrs before formula (decode_cf moves them to encoding)
            orig_time_attrs = self.ds["time"].attrs.copy() if "time" in self.ds else {}
            result = evaluate_expression(calc, context)

            # Check whether the time interval/frequency has changed (e.g. daily → monthly)
            result_has_time = "time" in result.dims
            time_size_changed = result_has_time and result.sizes[
                "time"
            ] != self.ds.sizes.get("time", result.sizes["time"])
            # Even when sizes match, assignment can align by coordinate labels.
            # If formula changes time labels (e.g. month-start -> month-midpoint),
            # direct assignment would reindex to NaN and later become 1e20.
            time_coord_changed = False
            if result_has_time and not time_size_changed and "time" in self.ds.coords:
                try:
                    time_coord_changed = not np.array_equal(
                        result["time"].values, self.ds["time"].values
                    )
                except Exception:
                    time_coord_changed = True

            if time_size_changed or time_coord_changed:
                # If the temporal resolution changes, rebuild self.ds while preserving variables that are not time-dependent
                time_indep = {
                    v: self.ds[v]
                    for v in self.ds.data_vars
                    if "time" not in self.ds[v].dims and v not in required_vars
                }
                time_indep_coords = {
                    c: self.ds[c]
                    for c in self.ds.coords
                    if "time" not in self.ds[c].dims and c != "time"
                }
                self.ds = result.to_dataset(name=self.cmor_name)
                for v, da in time_indep.items():
                    self.ds[v] = da
                self.ds = self.ds.assign_coords(
                    {
                        c: v
                        for c, v in time_indep_coords.items()
                        if c not in self.ds.coords
                    }
                )
                # Restore original time attrs so generate_filename can read units/calendar
                if orig_time_attrs and "time" in self.ds:
                    self.ds["time"].attrs.update(orig_time_attrs)
                self.calculate_missing_bounds_variables(required_bounds)
            else:
                # If the temporal resolution remains unchanged, assign directly
                self.ds[self.cmor_name] = result

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
            raise ValueError(
                f"Unsupported calculation type '{calc['type']}' for '{self.cmor_name}'. "
                f"Supported: 'direct', 'formula', 'dataset_function', 'internal'."
            )

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
        # rename() relabels the variables but not the attribute *strings* that
        # reference them. Re-point any `coordinates` / `formula_terms` references
        # at the new names so hybrid-height terms (e.g. sigma_theta -> b,
        # surface_altitude -> orog, theta_level_height -> lev) resolve instead of
        # dangling on the pre-rename input names. Use the full intended rename
        # (not the filtered `rename_map`): a dataset_function such as
        # cl_level_to_height renames theta_level_height -> lev itself, so that
        # key is absent from `rename_map` yet still referenced in the attrs.
        self._retarget_renamed_references({**bounds_rename_map, **axes_rename_map})
        # Drop stale units from renamed coordinates; update_attributes will
        # assign the correct CMIP units from the vocabulary.
        for old_name, new_name in rename_map.items():
            if old_name != new_name and new_name in self.ds.coords:
                self.ds[new_name].attrs.pop("units", None)

        # Calculate missing bounds variables after renaming so that
        # coordinate names in self.ds match the output names in required_bounds
        self.calculate_missing_bounds_variables(required_bounds)
        self._replace_time_bounds_with_computed()

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
        self._drop_stale_range_attributes(cmor_attrs)

        # A geophysical data variable must never carry an `axis` attribute: the
        # WCRP "Geophysical Variable Detection" check classifies any variable
        # with `axis` as a coordinate and excludes it (mrsos, derived from a
        # soil-layer field, otherwise inherits axis='Z'/positive='down' from the
        # source vertical coordinate). Drop `axis` unconditionally, and `positive`
        # unless the CMOR table actually declares one for this variable.
        self.ds[self.cmor_name].attrs.pop("axis", None)
        if not cmor_attrs.get("positive"):
            self.ds[self.cmor_name].attrs.pop("positive", None)

        # Drop model-native attributes inherited from the source variable via the
        # rename above that have no place in CMIP6 output:
        #  - grid_mapping: a regular lat-lon CMIP6 grid carries none, and its
        #    container variable is not carried into the output, so the attribute
        #    is a dangling reference that fails the CF grid-mapping check.
        #  - um_stash_source: a UM STASH provenance code, absent from the
        #    published reference.
        self.ds[self.cmor_name].attrs.pop("grid_mapping", None)
        self.ds[self.cmor_name].attrs.pop("um_stash_source", None)

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

        try:
            if cmor_attrs.get("valid_min") not in (None, "") and cmor_attrs.get(
                "valid_max"
            ) not in (None, ""):
                vmin = target_dtype.type(cmor_attrs["valid_min"])
                vmax = target_dtype.type(cmor_attrs["valid_max"])
                self._check_range(self.cmor_name, vmin, vmax)
        except ValueError as e:
            raise ValueError(
                f"Failed to validate value range for {self.cmor_name}: {e}"
            )

        for dim, meta in self.vocab.axes.items():
            name = meta["out_name"]
            dtype = self.type_mapping.get(meta.get("type", "double"), np.float64)
            if name in self.ds:
                if meta.get("standard_name") == "time":
                    self._check_calendar(name)
                original_units = self.ds[name].attrs.get("units") or self.ds[
                    name
                ].encoding.get("units", "")
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
                # Skip astype for time coordinates containing datetime/cftime objects,
                # and for character-type coordinates (string arrays like vegtype)
                if (
                    meta.get("standard_name") == "time"
                    and (
                        self.ds[name].dtype == object
                        or np.issubdtype(self.ds[name].dtype, np.datetime64)
                    )
                ) or meta.get("type") == "character":
                    updated = self.ds[name].copy()
                else:
                    updated = self.ds[name].astype(dtype)
                updated.attrs.update(coord_attrs)
                updated.attrs.pop("_FillValue", None)
                self.ds[name] = updated
                if meta.get("type") != "character":
                    self._match_bounds_dtype(name, dtype)
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

        # CF §7.1 — a bounds variable inherits its parent coordinate's semantics
        # and must not repeat them: units, standard_name, axis, positive and
        # calendar all belong on the parent alone. Published CMOR output agrees —
        # lat_bnds/lon_bnds/time_bnds in the reference files carry no attributes at
        # all — so the source attrs left over from renaming (e.g. sigma_theta_bnds
        # → b_bnds) are cleared rather than replaced with the parent's.
        #
        # One exception matches what CMOR itself writes: a parametric vertical
        # coordinate's bounds keeps the metadata needed to evaluate the formula --
        # standard_name and units, plus the formula_terms added below -- but not
        # axis, positive or long_name.
        PARAMETRIC_KEEP = ("standard_name", "units")
        parametric_bnds = {
            f"{meta.get('out_name')}_bnds"
            for meta in self.vocab.axes.values()
            if meta.get("z_bounds_factors")
        }

        all_ds_vars = list(self.ds.data_vars) + list(self.ds.coords)
        for var in all_ds_vars:
            if not var.endswith("_bnds"):
                continue
            self._preserve_bounds_time_encoding(var)
            if var in parametric_bnds:
                parent = self.ds.get(var[: -len("_bnds")])
                parent_attrs = parent.attrs if parent is not None else {}
                self.ds[var].attrs = {
                    k: parent_attrs[k] for k in PARAMETRIC_KEEP if k in parent_attrs
                }
            else:
                self.ds[var].attrs = {}

        # CF §4.3.3 — a parametric vertical coordinate's bounds variable carries its
        # own formula_terms, referencing the *bounds* of each term. The coordinate
        # table supplies that string as `z_bounds_factors`; the parent's own
        # formula_terms points at the coordinates, so it must not be inherited above.
        # Runs after the loop, which replaces bounds attrs wholesale.
        for meta in self.vocab.axes.values():
            terms = meta.get("z_bounds_factors", "")
            bnds_var = f"{meta.get('out_name')}_bnds"
            if not terms or bnds_var not in self.ds:
                continue
            # A term whose variable is absent would leave a dangling reference,
            # which fails a different CF check than the one being fixed.
            referenced = [tok for tok in terms.split() if not tok.endswith(":")]
            if all(name in self.ds for name in referenced):
                self.ds[bnds_var].attrs["formula_terms"] = terms

        # CF §4.3.3 — the parent parametric coordinate names what its
        # formula_terms compute. Runs after the bounds loop above, which would
        # otherwise be free to copy the attribute onto lev_bnds.
        self._apply_computed_standard_names()

        self._normalize_hybrid_bounds()

        # CF-1.11 units_metadata for the temperature and time units, last so it
        # sees the final variable units and the normalized calendar.
        self._apply_units_metadata()
