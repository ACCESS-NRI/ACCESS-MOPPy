import json
import re
import uuid
from datetime import datetime, timezone
from importlib.resources import as_file, files
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import xarray as xr

from access_moppy import _creator

# Module-level cache so that controlled-vocabulary JSON files are read from disk
# only once per cv_dir, regardless of how many vocabulary objects are created.
_CV_CACHE: Dict[str, Dict[str, Any]] = {}


class VariableNotFoundError(ValueError):
    """
    Exception raised when a requested variable is not found in the specified CMIP6 table.

    Provides helpful suggestions for alternative tables or similar variables.
    """

    def __init__(
        self,
        variable_name: str,
        table_name: str,
        suggestions: Optional[List[str]] = None,
    ):
        self.variable_name = variable_name
        self.table_name = table_name
        self.suggestions = suggestions or []

        message = f"Variable '{variable_name}' not found in CMIP6 table '{table_name}'."

        if self.suggestions:
            message += "\n\nSuggestions:\n" + "\n".join(
                f"  • {s}" for s in self.suggestions
            )

        super().__init__(message)


class CMIP6Vocabulary:
    cv_dir = "access_moppy.vocabularies.CMIP6_CVs"
    table_dir = "access_moppy.vocabularies.cmip6_cmor_tables.Tables"
    cv_prefix = "CMIP6"
    table_prefix = "CMIP6"
    mip_era = "CMIP6"

    def __init__(
        self,
        compound_name: str,
        experiment_id: str,
        source_id: str,
        variant_label: str,
        grid_label: str,
        activity_id: Optional[str] = None,
        parent_info: Optional[Dict[str, Dict[str, Any]]] = None,
    ):
        self.compound_name = compound_name
        self.experiment_id = experiment_id
        self.source_id = source_id
        self.variant_label = variant_label
        self.grid_label = grid_label
        self.activity_id = activity_id
        self.user_defined_parents = parent_info or {}

        self.vocab: Dict[str, Any] = self._load_controlled_vocab()
        self.experiment: Dict[str, Any] = self._get_experiment()
        self.source: Dict[str, Any] = self._get_source()
        self.table, self.cmor_name = self.compound_name.split(".")
        self.variable: Dict[str, Any] = self._get_variable_entry()
        self.cmip_table: Dict[str, Any] = self._load_table()

    def _load_controlled_vocab(self) -> Dict[str, Any]:
        if self.cv_dir not in _CV_CACHE:
            vocab: Dict[str, Any] = {}
            for entry in files(self.cv_dir).iterdir():
                if entry.name.endswith(".json"):
                    with as_file(entry) as path:
                        with open(path, "r", encoding="utf-8") as jf:
                            vocab.update(json.load(jf))
            _CV_CACHE[self.cv_dir] = vocab
        return _CV_CACHE[self.cv_dir]

    def _get_experiment(self) -> Dict[str, Any]:
        try:
            return self.vocab["experiment_id"][self.experiment_id]
        except KeyError:
            raise ValueError(
                f"Experiment '{self.experiment_id}' not found in controlled vocabularies."
            )

    def _get_parent_metadata(self) -> Dict[str, Any]:
        if not self.parent_experiment_id:
            return {}

        parent_cv = self.vocab.get("experiment_id", {})
        if self.parent_experiment_id not in parent_cv:
            raise ValueError(
                f"Parent experiment '{self.parent_experiment_id}' not found in controlled vocabularies."
            )
        return parent_cv[self.parent_experiment_id]

    def _get_source(self) -> Dict[str, Any]:
        try:
            return self.vocab["source_id"][self.source_id]
        except KeyError:
            raise ValueError(
                f"Source '{self.source_id}' not found in controlled vocabularies."
            )

    def get_parent_experiment_attrs(self) -> Dict[str, Any]:
        """
        Return and validate parent experiment attributes if required.
        """
        parent_attrs = self.user_defined_parents

        # Required fields
        required_keys = [
            "parent_experiment_id",
            "parent_activity_id",
            "parent_mip_era",
            "parent_source_id",
            "parent_variant_label",
            "parent_time_units",
            "branch_time_in_child",
            "branch_time_in_parent",
            "branch_method",
        ]
        for key in required_keys:
            if key not in parent_attrs:
                raise ValueError(
                    f"Missing required parent key '{key}' for experiment '{self.experiment_id}'"
                )

        # Validate against CV where applicable
        if parent_attrs["parent_experiment_id"] not in self.vocab["experiment_id"]:
            raise ValueError(
                f"Invalid parent_experiment_id: {parent_attrs['parent_experiment_id']}"
            )

        if parent_attrs["parent_activity_id"] not in self.vocab["activity_id"]:
            raise ValueError(
                f"Invalid parent_activity_id: {parent_attrs['parent_activity_id']}"
            )

        if parent_attrs["parent_source_id"] not in self.vocab["source_id"]:
            raise ValueError(
                f"Invalid parent_source_id: {parent_attrs['parent_source_id']}"
            )

        return parent_attrs

    def _load_table(self) -> Dict[str, Any]:
        # Resolve the file from the module path
        entry = files(self.table_dir) / self._table_filename(self.table)

        if not entry.exists():
            raise FileNotFoundError(f"Table file not found: {entry}")

        with as_file(entry) as path:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)

    def _get_variable_entry(self) -> Dict[str, Any]:
        try:
            var_entry = self._load_table()["variable_entry"][self.cmor_name]

            # Ensure fill values are included if present in the CMOR table
            for key in ("missing_value", "_FillValue"):
                if key in var_entry:
                    var_entry[key] = var_entry[key]
                else:
                    var_entry[key] = 1e20  # default fallback

            return var_entry
        except KeyError:
            # Generate helpful suggestions
            suggestions = self._get_variable_suggestions()
            raise VariableNotFoundError(self.cmor_name, self.table, suggestions)

    def _get_variable_suggestions(self) -> List[str]:
        """
        Generate helpful suggestions when a variable is not found.

        Returns:
            List of suggestion strings for the user.
        """
        suggestions = []

        # Check if variable exists in other CMIP6 tables
        common_tables = [
            "Amon",
            "Lmon",
            "Omon",
            "Emon",
            "CFmon",
            "day",
            "6hrLev",
            "3hr",
            "Ofx",
            "fx",
            "day",
            "6hrPlev",
            "3hr",
            "CFmon",
            "Eday",
            "AERmon",
        ]
        found_in_tables = []

        for table in common_tables:
            if table == self.table:
                continue  # Skip current table

            try:
                table_file = self._table_filename(table)
                table_resource = files(self.table_dir) / table_file

                with as_file(table_resource) as table_path:
                    with open(table_path, "r", encoding="utf-8") as f:
                        table_data = json.load(f)

                    if self.cmor_name in table_data.get("variable_entry", {}):
                        found_in_tables.append(table)

            except (FileNotFoundError, KeyError):
                continue  # Table doesn't exist or has no variable_entry

        if found_in_tables:
            table_list = ", ".join(found_in_tables)
            suggestions.append(
                f"Variable '{self.cmor_name}' is available in table(s): {table_list}"
            )
            suggestions.append(f"Try using: {found_in_tables[0]}.{self.cmor_name}")

        # Check for similar variable names in current table
        try:
            current_table_data = self._load_table()
            available_vars = list(current_table_data.get("variable_entry", {}).keys())

            # Find variables with similar names (simple string similarity)
            similar_vars = []
            for var in available_vars:
                if len(var) > 2 and (
                    self.cmor_name.lower() in var.lower()
                    or var.lower() in self.cmor_name.lower()
                    or
                    # Check for common root (first 3 characters)
                    (
                        len(self.cmor_name) >= 3
                        and len(var) >= 3
                        and self.cmor_name[:3].lower() == var[:3].lower()
                    )
                ):
                    similar_vars.append(var)

            if similar_vars:
                similar_list = ", ".join(similar_vars[:5])  # Limit to 5 suggestions
                suggestions.append(
                    f"Similar variables in {self.table} table: {similar_list}"
                )

            # Show a sample of available variables if no similar ones found
            elif available_vars:
                sample_vars = ", ".join(available_vars[:10])  # Show first 10
                total_count = len(available_vars)
                if total_count > 10:
                    sample_vars += f" (and {total_count - 10} more)"
                suggestions.append(
                    f"Available variables in {self.table} table: {sample_vars}"
                )

        except Exception:
            pass  # Don't fail if we can't load suggestions

        # Add general guidance
        suggestions.append(
            "Visit https://clipc-services.ceda.ac.uk/dreq/index.html to browse CMIP6 variables"
        )

        return suggestions

    def _get_axes(self, mapping) -> Dict[str, Any]:
        # Resolve resource inside the module path
        coord_entry = files(self.table_dir) / self._table_filename("coordinate")

        with as_file(coord_entry) as path:
            with open(path, "r", encoding="utf-8") as f:
                axes = json.load(f)["axis_entry"]

        dims = self.variable["dimensions"].split()
        vars_required = {}

        for dim in dims:
            if dim in axes and dim not in ["alevel"]:
                coord = axes[dim]
                vars_required[dim] = {k: v for k, v in coord.items() if v != ""}

        # Get the single variable mapping (assuming mapping has only one key)
        var_mapping = list(mapping.values())[0]  # Get the first (and only) value

        # Add z-axis coordinate variables if applicable
        if "zaxis" in var_mapping:
            # Get z-axis type from mapping
            zaxis_type = var_mapping["zaxis"].get("type", {})

            # Process main z-axis coordinate
            zcoord = axes.get(zaxis_type, {})["out_name"]
            vars_required[zcoord] = {
                k: v for k, v in axes[zaxis_type].items() if v != ""
            }

            # Process z_factors
            zfactors_str = axes.get(zaxis_type, {}).get("z_factors", "")

            zfactors = {}
            if zfactors_str:
                parts = zfactors_str.split()
                zfactors = {
                    parts[i].rstrip(":"): parts[i + 1]
                    for i in range(0, len(parts), 2)
                    if i + 1 < len(parts)
                }

            formula_entry = files(self.table_dir) / self._table_filename(
                "formula_terms"
            )
            with as_file(formula_entry) as fpath:
                with open(fpath, "r", encoding="utf-8") as ff:
                    formula_terms = json.load(ff)["formula_entry"]

            for factor_name, _ in zfactors.items():
                if factor_name in formula_terms:
                    zcoord = formula_terms[factor_name]
                    vars_required[factor_name] = {
                        k: v for k, v in zcoord.items() if v != ""
                    }

        # Let's map the axis and formula terms to the inputs
        vars_rename_map = {}
        extended_mapping = var_mapping["dimensions"] | var_mapping.get("zaxis", {}).get(
            "coordinate_variables", {}
        )
        inverted_extended_mapping = {v: k for k, v in extended_mapping.items()}

        for _, v in vars_required.items():
            input_dim = inverted_extended_mapping.get(v["out_name"])
            if input_dim:
                vars_rename_map[input_dim] = v["out_name"]

        self.axes = vars_required

        return vars_required, vars_rename_map

    def _get_required_bounds_variables(self, mapping: Dict[str, Any]) -> tuple:
        """
        Get required bounds variables based on CMOR vocabulary axes.

        Args:
            mapping: Variable mapping dictionary containing dimensions

        Returns:
            tuple: (bnds_required, bounds_rename_map) where
                - bnds_required: list of required bounds variable names
                - bounds_rename_map: dict mapping input bounds names to output bounds names
        """
        bnds_required = {}
        bounds_rename_map = {}

        # Get the single variable mapping (assuming mapping has only one key)
        var_mapping = list(mapping.values())[0]  # Get the first (and only) value

        extended_mapping = var_mapping["dimensions"] | var_mapping.get("zaxis", {}).get(
            "coordinate_variables", {}
        )
        inverted_extended_mapping = {v: k for k, v in extended_mapping.items()}

        axes, _ = self._get_axes(mapping)
        for _, v in axes.items():
            if v.get("must_have_bounds") == "yes":
                # Find the input dimension name that maps to this output name
                input_dim = inverted_extended_mapping.get(v["out_name"])
                if input_dim:
                    input_bounds = input_dim + "_bnds"
                    output_bounds = v["out_name"] + "_bnds"
                    bounds_rename_map[input_bounds] = output_bounds
                    bnds_required[output_bounds] = {
                        key: val for key, val in v.items() if val != ""
                    }

        # Also handle bounds of z-axis formula terms (e.g. b_bnds for hybrid_height).
        # These are listed in z_bounds_factors of the coordinate table entry.
        for _, v in axes.items():
            z_bounds_factors_str = v.get("z_bounds_factors", "")
            if not z_bounds_factors_str:
                continue
            parts = z_bounds_factors_str.split()
            z_bounds_factors = {
                parts[i].rstrip(":"): parts[i + 1]
                for i in range(0, len(parts), 2)
                if i + 1 < len(parts)
            }
            for factor_name, output_bnds_name in z_bounds_factors.items():
                if not output_bnds_name.endswith("_bnds"):
                    continue
                input_factor = inverted_extended_mapping.get(factor_name)
                if input_factor:
                    input_bnds = input_factor + "_bnds"
                    if input_bnds not in bounds_rename_map:
                        bounds_rename_map[input_bnds] = output_bnds_name
                        bnds_required[output_bnds_name] = {
                            key: val for key, val in v.items() if val != ""
                        }

        return bnds_required, bounds_rename_map

    def get_variant_components(self) -> Dict[str, int]:
        pattern = re.compile(
            r"r(?P<realization_index>\d+)"
            r"i(?P<initialization_index>\d+)"
            r"p(?P<physics_index>\d+)"
            r"f(?P<forcing_index>\d+)$"
        )
        match = pattern.match(self.variant_label)
        if not match:
            raise ValueError(f"Invalid variant_label format: {self.variant_label}")
        return {k: int(v) for k, v in match.groupdict().items()}

    def get_cmip_missing_value(self) -> float:
        """
        Get the CMIP6-compliant missing value for this variable.

        Returns the missing value as specified in the CMOR table for this variable,
        with fallback to table default or global default.

        Returns:
            float: The CMIP6-compliant missing value
        """
        # Check if variable has specific missing value
        if "missing_value" in self.variable:
            return float(self.variable["missing_value"])

        # Check variable type and use appropriate table default
        var_type = self.variable.get("type", "real")
        if var_type == "integer":
            # Use integer missing value from table header
            return float(self.cmip_table["Header"].get("int_missing_value", -999))
        else:
            # Use real missing value from table header
            return float(self.cmip_table["Header"].get("missing_value", 1e20))

    def get_cmip_fill_value(self) -> float:
        """
        Get the CMIP6-compliant _FillValue for this variable.

        For CMIP6, _FillValue should be the same as missing_value.

        Returns:
            float: The CMIP6-compliant _FillValue
        """
        return self.get_cmip_missing_value()

    def normalize_missing_values_to_nan(self, data_array):
        """
        Normalize various missing value representations to NaN for consistent processing.

        This method converts different missing value conventions (e.g., -999, -1e20)
        to NaN, enabling XArray's built-in missing value handling to work properly
        during derivation calculations.

        Parameters:
            data_array: xarray.DataArray
                The data array to normalize

        Returns:
            xarray.DataArray: Data array with missing values converted to NaN
        """
        # Create a shallow copy to preserve lazy evaluation
        result = data_array.copy(deep=False)

        # Get current missing/fill values from attributes
        current_missing = data_array.attrs.get("missing_value")
        current_fill = data_array.attrs.get("_FillValue")

        # Build conditions for values that should become NaN
        nan_conditions = []

        # Check for current missing_value
        if current_missing is not None:
            try:
                current_missing = float(current_missing)
                if not np.isnan(current_missing):  # Don't double-convert NaN
                    nan_conditions.append(result == current_missing)
            except (ValueError, TypeError):
                pass

        # Check for current _FillValue
        if current_fill is not None:
            try:
                current_fill = float(current_fill)
                if not np.isnan(current_fill):  # Don't double-convert NaN
                    nan_conditions.append(result == current_fill)
            except (ValueError, TypeError):
                pass

        # Apply conversions using lazy operations
        if nan_conditions:
            combined_mask = nan_conditions[0]
            for condition in nan_conditions[1:]:
                combined_mask = combined_mask | condition

            # Convert to NaN using xarray.where (preserves lazy evaluation)
            result = result.where(~combined_mask, np.nan)

        # Update attributes to reflect NaN as the missing value
        result.attrs["missing_value"] = np.nan
        result.attrs["_FillValue"] = np.nan

        return result

    @staticmethod
    def normalize_dataset_missing_values(dataset):
        """
        Normalize missing values to NaN across all data variables in a dataset.

        This static method can be used to normalize missing values early in the
        processing pipeline, before any derivation calculations are performed.
        This enables XArray's built-in missing value propagation to handle
        everything correctly.

        Parameters:
            dataset: xarray.Dataset
                The dataset to normalize

        Returns:
            xarray.Dataset: Dataset with all missing values converted to NaN
        """
        # Create a shallow copy to preserve lazy evaluation
        result = dataset.copy(deep=False)

        for var_name in result.data_vars:
            var = result[var_name]

            # Get current missing/fill values from attributes
            current_missing = var.attrs.get("missing_value")
            current_fill = var.attrs.get("_FillValue")

            # Build conditions for values that should become NaN
            nan_conditions = []

            # Check for current missing_value
            if current_missing is not None:
                try:
                    current_missing = float(current_missing)
                    if not np.isnan(current_missing):  # Don't double-convert NaN
                        nan_conditions.append(
                            np.isclose(var, current_missing, rtol=0, atol=1e-3)
                        )
                except (ValueError, TypeError):
                    pass

            # Check for current _FillValue
            if current_fill is not None:
                try:
                    current_fill = float(current_fill)
                    if not np.isnan(current_fill):  # Don't double-convert NaN
                        nan_conditions.append(
                            np.isclose(var, current_fill, rtol=0, atol=1e-3)
                        )
                except (ValueError, TypeError):
                    pass

            # Apply conversions using lazy operations
            if nan_conditions:
                combined_mask = nan_conditions[0]
                for condition in nan_conditions[1:]:
                    combined_mask = combined_mask | condition

                # Convert to NaN using xarray.where (preserves lazy evaluation)
                result[var_name] = var.where(~combined_mask, np.nan)

                # Update attributes to reflect NaN as the missing value
                result[var_name].attrs["missing_value"] = np.nan
                result[var_name].attrs["_FillValue"] = np.nan

        return result

    def standardize_missing_values(self, data_array, convert_existing: bool = True):
        """
        Standardize missing values in a data array to CMIP6 requirements.

        This method ensures that:
        1. All missing/NaN values use the CMIP6-specified missing value
        2. Data with different missing values from derived calculations are standardized
        3. Attributes are updated with correct missing_value and _FillValue
        4. Lazy evaluation is preserved for dask arrays

        Parameters:
            data_array: xarray.DataArray
                The data array to standardize
            convert_existing: bool
                If True, convert existing missing values to CMIP6 standard.
                If False, only standardize NaN values and update attributes.

        Returns:
            xarray.DataArray: Data array with standardized missing values
        """
        # Get the correct CMIP6 missing value
        cmip_missing_value = self.get_cmip_missing_value()
        cmip_fill_value = self.get_cmip_fill_value()

        # Create a shallow copy to avoid modifying the original (preserves dask arrays)
        result = data_array.copy(deep=False)

        if convert_existing:
            # Get current missing/fill values from attributes
            current_missing = data_array.attrs.get("missing_value")
            current_fill = data_array.attrs.get("_FillValue")

            # Build conditions for missing values using xarray operations (lazy)
            missing_conditions = []

            # Check for NaN values
            missing_conditions.append(np.isnan(result))

            # Check for current missing_value
            if current_missing is not None:
                try:
                    current_missing = float(current_missing)
                    missing_conditions.append(result == current_missing)
                except (ValueError, TypeError):
                    pass

            # Check for current _FillValue
            if current_fill is not None:
                try:
                    current_fill = float(current_fill)
                    missing_conditions.append(result == current_fill)
                except (ValueError, TypeError):
                    pass

            # Combine all missing value conditions (this stays lazy with dask)
            if missing_conditions:
                combined_mask = missing_conditions[0]
                for condition in missing_conditions[1:]:
                    combined_mask = combined_mask | condition

                # Use xarray.where to preserve lazy evaluation
                result = result.where(~combined_mask, cmip_missing_value)
        else:
            # Only convert NaN values to CMIP6 missing value (lazy operation)
            result = result.where(~np.isnan(result), cmip_missing_value)

        # Update attributes with correct CMIP6 values (this doesn't affect lazy evaluation)
        result.attrs["missing_value"] = cmip_missing_value
        result.attrs["_FillValue"] = cmip_fill_value

        return result

    def _get_external_variables(self) -> Optional[str]:
        """
        Derive the list of external variables required for this CMOR variable.
        These variables are not in the file but must be declared so tools know they are needed.
        """
        externals: set[str] = set()

        # Known common external vars
        known_external_vars = {
            "areacella",
            "areacello",
            "volcello",
            "sftlf",
            "sftof",
            "deptho",
            "orog",
            "siconc",
            "landMask",
            "climofactor",
        }

        # 1. From cell_measures e.g., "area: areacella volume: volcello"
        cell_measures = self.variable.get("cell_measures", "")
        if cell_measures:
            tokens = cell_measures.strip().split()
            for i in range(1, len(tokens), 2):
                externals.add(tokens[i])

        # 2. From cell_methods (heuristic)
        cell_methods = self.variable.get("cell_methods", "")
        for ext in known_external_vars:
            if ext in cell_methods:
                externals.add(ext)

        # 3. Add known required ones based on variable name (heuristic)
        if self.cmor_name in {"evspsbl", "mrro", "mrso"}:
            externals.add("sftlf")
        if self.cmor_name in {"thetao", "so", "hfds", "ocean_heat_content"}:
            externals.update({"areacello", "volcello", "deptho"})

        return " ".join(sorted(externals)) if externals else None

    def _cv_filename(self, key: str) -> str:
        return f"{self.cv_prefix}_{key}.json"

    def _table_filename(self, key: str) -> str:
        return f"{self.table_prefix}_{key}.json"

    def get_required_attribute_names(self) -> List[str]:
        """
        Get the list of required global attribute names from CMIP6 controlled vocabulary.

        Returns:
            List[str]: List of required global attribute names
        """
        # Load the CMIP6 required global attributes CV file
        cv_file = files(self.cv_dir) / self._cv_filename("required_global_attributes")

        with as_file(cv_file) as path:
            with open(path, "r", encoding="utf-8") as f:
                cv_data = json.load(f)

        return cv_data["required_global_attributes"]

    def _load_drs_templates(self) -> Dict[str, str]:
        """
        Load directory and filename templates from CMIP6_DRS.json.

        Returns:
            Dict[str, str]: Mapping containing directory_path_template and
            filename_template when available.
        """
        drs_file = files(self.cv_dir) / self._cv_filename("DRS")

        with as_file(drs_file) as path:
            with open(path, "r", encoding="utf-8") as f:
                drs_data = json.load(f)

        return drs_data.get("DRS", {})

    @staticmethod
    def _render_template(template: str, values: Dict[str, Any]) -> str:
        """
        Render a template containing <placeholders> and optional [segments].
        """

        def replace_optional_segment(match: re.Match) -> str:
            segment = match.group(1)
            keys = re.findall(r"<([^>]+)>", segment)
            if keys and all(values.get(key) not in (None, "") for key in keys):
                rendered = segment
                for key in keys:
                    rendered = rendered.replace(f"<{key}>", str(values.get(key, "")))
                return rendered
            return ""

        rendered = re.sub(r"\[([^\]]+)\]", replace_optional_segment, template)

        for key, value in values.items():
            rendered = rendered.replace(f"<{key}>", "" if value is None else str(value))

        return re.sub(r"<[^>]+>", "", rendered)

    def generate_filename(
        self,
        attrs: Dict[str, Any],
        ds: xr.Dataset,
        cmor_name: str,
        compound_name: str,
    ) -> str:
        """
        Generate CMIP6-compliant filename using official DRS template.

        Args:
            attrs: Dataset global attributes
            ds: xarray Dataset
            cmor_name: CMOR variable name
            compound_name: Compound name for frequency detection

        Returns:
            str: CMIP6-compliant filename
        """

        # Create mapping of template variables to actual values
        template_vars = {
            "variable_id": attrs.get("variable_id", cmor_name),
            "table_id": attrs.get("table_id", ""),
            "source_id": attrs.get("source_id", ""),
            "experiment_id": attrs.get("experiment_id", ""),
            "member_id": attrs.get(
                "variant_label", ""
            ),  # member_id maps to variant_label
            "grid_label": attrs.get("grid_label", ""),
        }

        # Handle time range if time coordinate exists
        if "time" in ds[cmor_name].coords:
            from cftime import num2date

            time_var = ds[cmor_name].coords["time"]
            units = time_var.attrs.get("units", "")
            calendar = time_var.attrs.get("calendar", "standard").lower()

            sample = time_var.values[0]
            if hasattr(sample, "year"):
                times = time_var.values[[0, -1]]
            elif np.issubdtype(time_var.dtype, np.datetime64):
                # numpy datetime64 to pandas Timestamp
                import pandas as pd

                times = [pd.Timestamp(t) for t in time_var.values[[0, -1]]]
            else:
                from cftime import num2date

                times = num2date(
                    time_var.values[[0, -1]], units=units, calendar=calendar
                )

            # Check frequency for time formatting
            table_name = compound_name.split(".")[0]
            table_lower = table_name.lower()
            is_subdaily_data = any(freq in table_lower for freq in ["3hr", "6hr", "hr"])
            is_daily_data = "day" in table_lower

            # Format time range based on frequency
            if is_subdaily_data:
                # Sub-daily data: include hour and minute (YYYYMMDDHHMM)
                start, end = [
                    f"{t.year:04d}{t.month:02d}{t.day:02d}{t.hour:02d}{t.minute:02d}"
                    for t in times
                ]
            elif is_daily_data:
                # Daily data: include day (YYYYMMDD)
                start, end = [f"{t.year:04d}{t.month:02d}{t.day:02d}" for t in times]
            else:
                # Monthly or other data: year and month only (YYYYMM)
                start, end = [f"{t.year:04d}{t.month:02d}" for t in times]

            template_vars["time_range"] = f"{start}-{end}"
        else:
            # Time-independent variable - no time_range
            template_vars["time_range"] = None

        drs_templates = self._load_drs_templates()
        filename_template = drs_templates["filename_template"]
        ordered_keys = re.findall(r"<([^>]+)>", filename_template)
        template_without_placeholders = re.sub(r"<[^>]+>", "", filename_template)

        is_compact_placeholder_template = (
            len(ordered_keys) > 1
            and template_without_placeholders.strip() == ""
            and "[" not in filename_template
            and "]" not in filename_template
        )

        if is_compact_placeholder_template:
            rendered_parts = []
            for key in ordered_keys:
                value = template_vars.get(key)
                if value not in (None, ""):
                    rendered_parts.append(str(value))
            # Append time_range if available but absent from template
            if "time_range" not in ordered_keys and template_vars.get("time_range"):
                rendered_parts.append(str(template_vars["time_range"]))
            rendered_filename = "_".join(rendered_parts)
        else:
            rendered_filename = self._render_template(filename_template, template_vars)

        if not rendered_filename.endswith(".nc"):
            rendered_filename += ".nc"

        return rendered_filename

    def get_required_global_attributes(self) -> Dict[str, Any]:
        now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        variant = self.get_variant_components()

        attrs = {
            "Conventions": self.cmip_table["Header"].get("Conventions"),
            "activity_id": self._resolve_activity_id(),
            "creation_date": now,
            "data_specs_version": self.cmip_table["Header"].get("data_specs_version"),
            "experiment": self.experiment["experiment"],
            "experiment_id": self.experiment_id,
            "forcing_index": variant["forcing_index"],
            "frequency": self.variable["frequency"],
            "further_info_url": self._get_further_info_url(),
            "grid": "native atmosphere N96 grid (145x192 latxlon)",
            "grid_label": self.grid_label,
            "initialization_index": variant["initialization_index"],
            "institution": self._get_institution(),
            "institution_id": ",".join(self.source["institution_id"]),
            "license": self._get_license(),
            "mip_era": self.mip_era,
            "nominal_resolution": self._get_nominal_resolution(),
            "physics_index": variant["physics_index"],
            "product": self.cmip_table["Header"].get("product"),
            "realization_index": variant["realization_index"],
            "realm": self.variable["modeling_realm"],
            "source": self._format_source_string(),
            "source_id": self.source_id,
            "source_type": self._get_source_type(),
            "sub_experiment": self._get_sub_experiment(),
            "sub_experiment_id": self._get_sub_experiment_id(),
            "table_id": self.table,
            "tracking_id": f"hdl:21.14100/{uuid.uuid4()}",
            "variable_id": self.cmor_name,
            "variant_label": self.variant_label,
        }

        parent_attrs = self.get_parent_experiment_attrs()
        if parent_attrs:
            for k, v in parent_attrs.items():
                attrs[k] = v

        external_vars = self._get_external_variables()
        if external_vars:
            attrs["external_variables"] = external_vars

        # Initialise creator information for all experiments
        attrs["creator_name"] = _creator.creator_name
        attrs["creator_organisation"] = _creator.organisation
        attrs["creator_email"] = _creator.creator_email
        attrs["creator_url"] = _creator.creator_url

        return attrs

    def _get_institution(self) -> str:
        institution_ids = self.source.get("institution_id", [])
        if not institution_ids:
            return ""

        institution_map = self.vocab.get("institution_id")
        if isinstance(institution_map, dict):
            first_id = institution_ids[0]
            return institution_map.get(first_id, first_id)

        return ",".join(institution_ids)

    def _get_nominal_resolution(self) -> Optional[str]:
        realm = self.variable.get("modeling_realm")
        try:
            return self.source["model_component"][realm]["native_nominal_resolution"]
        except KeyError:
            return None

    def _resolve_activity_id(self) -> str:
        available = self.experiment["activity_id"]
        if len(available) == 1:
            return available[0]
        if self.activity_id and self.activity_id in available:
            return self.activity_id
        raise ValueError(
            f"Multiple activity IDs: {available}. Please specify one explicitly."
        )

    def _get_sub_experiment_id(self) -> str:
        return self.experiment.get("sub_experiment_id", "none")

    def _get_sub_experiment(self) -> str:
        return (
            "none"
            if self._get_sub_experiment_id() == "none"
            else self._get_sub_experiment_id()[0]
        )

    def _get_source_type(self) -> str:
        required = self.experiment["required_model_components"]
        return " ".join(required)

    def _format_source_string(self) -> str:
        label = self.source["label"]
        year = self.source["release_year"]
        components = self.source["model_component"]
        return f"{label} ({year}): \n" + "\n".join(
            f"{comp}: {desc.get('description', 'none')}"
            for comp, desc in components.items()
        )

    def _get_further_info_url(self) -> str:
        mip_era = self.mip_era
        institution_id = self.source["institution_id"][0]
        source_id = self.source_id
        experiment_id = self.experiment_id
        sub_experiment_id = self._get_sub_experiment_id()[0]
        variant_label = self.variant_label

        return (
            f"https://furtherinfo.es-doc.org/"
            f"{mip_era}.{institution_id}.{source_id}.{experiment_id}.{sub_experiment_id}.{variant_label}"
        )

    def _get_license(self) -> str:
        """
        Construct the CMIP6 license string by filling placeholders in the template from CMIP6_license.json.
        """
        license_info = self.source.get("license_info", {})
        institution = self.source["institution_id"][0]
        license = license_info.get("license")
        license_url = license_info.get(
            "url", "https://creativecommons.org/licenses/by/4.0/"
        )
        return (
            f"CMIP6 model data produced by {institution} is licensed under a "
            f"{license} License ({license_url}). Consult "
            "https://pcmdi.llnl.gov/CMIP6/TermsOfUse for terms of use governing "
            "CMIP6 output, including citation requirements and proper "
            "acknowledgment. Further information about this data, including some "
            "limitations, can be found via the further_info_url (recorded as a "
            "global attribute in this file). The data producers and data providers "
            "make no warranty, either express or implied, including, but not "
            "limited to, warranties of merchantability and fitness for a "
            "particular purpose. All liabilities arising from the supply of the "
            "information (including any liability arising in negligence) are "
            "excluded to the fullest extent permitted by law."
        )

    def build_drs_path(self, drs_root: Path, version_date: str) -> Path:
        """
        Build DRS (Data Reference Syntax) path according to CMIP6 specifications.

        Args:
            drs_root: Root directory for DRS structure
            version_date: Version date in YYYYMMDD format

        Returns:
            Complete DRS path following CMIP6 template:
            <mip_era>/<activity_id>/<institution_id>/<source_id>/<experiment_id>/<member_id>/<table_id>/<variable_id>/<grid_label>/<version>
        """
        template_vars = {
            "mip_era": self.mip_era,
            "activity_id": self._resolve_activity_id(),
            "institution_id": ",".join(self.source["institution_id"]),
            "source_id": self.source_id,
            "experiment_id": self.experiment_id,
            "member_id": self.variant_label,
            "table_id": self.table,
            "variable_id": self.cmor_name,
            "grid_label": self.grid_label,
            "version": f"v{version_date}",
        }

        drs_templates = self._load_drs_templates()
        directory_template = drs_templates["directory_path_template"]

        ordered_keys = re.findall(r"<([^>]+)>", directory_template)
        rendered_components = [str(template_vars[key]).strip() for key in ordered_keys]

        return drs_root.joinpath(*rendered_components)

    def __repr__(self) -> str:
        return f"<CMIP6Vocabulary variable={self.cmor_name} experiment={self.experiment_id} source={self.source_id}>"


class CMIP6PlusVocabulary(CMIP6Vocabulary):
    cv_dir = "access_moppy.vocabularies.CMIP6Plus_CVs"
    cv_prefix = "CMIP6Plus"
    table_prefix = "CMIP6"
    mip_era = "CMIP6Plus"

    def _get_license(self) -> str:
        license_info = self.source.get("license_info", {})
        institution = self.source["institution_id"][0]
        license_id = license_info.get("id", "CC BY 4.0")
        license_url = license_info.get(
            "url", "https://creativecommons.org/licenses/by/4.0/"
        )

        return (
            f"CMIP6Plus model data produced by {institution} is licensed under a "
            f"Creative Commons {license_id} License ({license_url}). "
            "Consult https://pcmdi.llnl.gov/CMIP6Plus/TermsOfUse for terms of use "
            "governing CMIP6Plus output, including citation requirements and proper "
            "acknowledgment. The data producers and data providers make no warranty, "
            "either express or implied, including, but not limited to, warranties of "
            "merchantability and fitness for a particular purpose. All liabilities "
            "arising from the supply of the information (including any liability "
            "arising in negligence) are excluded to the fullest extent permitted by law."
        )


class CMIP7Vocabulary:
    cv_dir = "access_moppy.vocabularies.CMIP7_CVs"
    table_dir = "access_moppy.vocabularies.cmip7-cmor-tables.tables"

    def __init__(
        self,
        compound_name: str,
        experiment_id: str,
        source_id: str,
        variant_label: str,
        grid_label: str,
        activity_id: Optional[str] = None,
        parent_info: Optional[Dict[str, Dict[str, Any]]] = None,
    ):
        self.compound_name = compound_name
        self.experiment_id = experiment_id
        self.source_id = source_id
        self.variant_label = variant_label
        self.grid_label = grid_label
        self.activity_id = activity_id
        self.user_defined_parents = parent_info or {}

        self.experiment: Dict[str, Any] = self._get_experiment()
        self.source: Dict[str, Any] = self._get_source()

        # Parse the CMIP7 compound name format
        compound_parts = self._parse_compound_name(compound_name)
        self.table = compound_parts["table"]
        self.physical_parameter = compound_parts["physical_parameter"]
        self.processing_info = compound_parts["processing_info"]
        self.branded_name = compound_parts["branded_name"]
        self.cmor_name = compound_parts["cmor_name"]
        self.frequency = compound_parts["frequency"]
        self.region = compound_parts["region"]

        self.variable: Dict[str, Any] = self._get_variable_entry()
        self.cmip_table: Dict[str, Any] = self._load_table()

    def _parse_compound_name(self, compound_name: str) -> Dict[str, str]:
        """
        Parse CMIP7 compound name format: table.physical_parameter.processing_info.frequency.region

        Example: atmos.aod550volso4.tavg-u-hxy-u.mon.GLB
        - physical_parameter: aod550volso4
        - branded_name: aod550volso4_tavg-u-hxy-u (combination of physical_parameter + processing_info with underscore)
        - cmor_name: same as branded_name

        Returns:
            Dict with keys: table, physical_parameter, processing_info, branded_name, cmor_name, frequency, region
        """
        parts = compound_name.split(".")

        if len(parts) < 2:
            raise ValueError(
                f"Invalid CMIP7 compound name format: '{compound_name}'. Expected at least 'table.physical_parameter'"
            )

        # Basic format: table.physical_parameter[.processing_info][.frequency][.region]
        table = parts[0]
        physical_parameter = parts[1]

        # Initialize optional components
        processing_info = ""
        frequency = ""
        region = ""

        # Parse remaining parts based on length
        if len(parts) == 2:
            # Simple format: table.physical_parameter
            pass
        elif len(parts) == 3:
            # Could be table.physical_parameter.processing_info OR table.physical_parameter.frequency
            # We'll assume it's processing_info for now
            processing_info = parts[2]
        elif len(parts) == 4:
            # table.physical_parameter.processing_info.frequency
            processing_info = parts[2]
            frequency = parts[3]
        elif len(parts) == 5:
            # Full format: table.physical_parameter.processing_info.frequency.region
            processing_info = parts[2]
            frequency = parts[3]
            region = parts[4]
        else:
            raise ValueError(
                f"Invalid CMIP7 compound name format: '{compound_name}'. Too many parts: {len(parts)}"
            )

        # The branded name is the combination of physical_parameter and processing_info with underscore
        if processing_info:
            branded_name = f"{physical_parameter}_{processing_info}"
        else:
            branded_name = physical_parameter

        # CMOR name is essentially the branded name
        cmor_name = branded_name

        return {
            "table": table,
            "physical_parameter": physical_parameter,
            "processing_info": processing_info,
            "branded_name": branded_name,
            "cmor_name": cmor_name,
            "frequency": frequency,
            "region": region,
        }

    def _get_experiment(self) -> Dict[str, Any]:
        """Load experiment metadata from individual JSON file"""
        try:
            experiment_file = (
                files(self.cv_dir) / "experiment" / f"{self.experiment_id}.json"
            )
            with as_file(experiment_file) as path:
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
        except FileNotFoundError:
            raise ValueError(
                f"Experiment '{self.experiment_id}' not found in CMIP7 controlled vocabularies."
            )

    def _get_source(self) -> Dict[str, Any]:
        """Load source metadata from individual JSON file"""
        try:
            source_file = files(self.cv_dir) / "source" / f"{self.source_id}.json"
            with as_file(source_file) as path:
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
        except FileNotFoundError:
            raise ValueError(
                f"Source '{self.source_id}' not found in CMIP7 controlled vocabularies."
            )

    def get_parent_experiment_attrs(self) -> Dict[str, Any]:
        """
        Return and validate parent experiment attributes if required.
        """
        parent_attrs = self.user_defined_parents

        # Check if experiment requires parent information
        if self.experiment.get("parent_experiment", ["none"])[0] == "none":
            return {}

        # Required fields for CMIP7
        required_keys = [
            "parent_experiment_id",
            "parent_activity_id",
            "parent_mip_era",
            "parent_source_id",
            "parent_variant_label",
            "parent_time_units",
            "branch_time_in_child",
            "branch_time_in_parent",
            "branch_method",
        ]

        for key in required_keys:
            if key not in parent_attrs:
                raise ValueError(
                    f"Missing required parent key '{key}' for experiment '{self.experiment_id}'"
                )

        # Validate parent experiment exists
        try:
            parent_exp_file = (
                files(self.cv_dir)
                / "experiment"
                / f"{parent_attrs['parent_experiment_id']}.json"
            )
            with as_file(parent_exp_file) as path:
                with open(path, "r", encoding="utf-8") as f:
                    json.load(f)  # Just validate it exists and is valid JSON
        except FileNotFoundError:
            raise ValueError(
                f"Invalid parent_experiment_id: {parent_attrs['parent_experiment_id']}"
            )

        # Validate parent source exists
        try:
            parent_source_file = (
                files(self.cv_dir)
                / "source"
                / f"{parent_attrs['parent_source_id']}.json"
            )
            with as_file(parent_source_file) as path:
                with open(path, "r", encoding="utf-8") as f:
                    json.load(f)  # Just validate it exists and is valid JSON
        except FileNotFoundError:
            raise ValueError(
                f"Invalid parent_source_id: {parent_attrs['parent_source_id']}"
            )

        return parent_attrs

    def _load_table(self) -> Dict[str, Any]:
        """Load CMIP7 table file"""
        entry = files(self.table_dir) / f"CMIP7_{self.table}.json"

        if not entry.exists():
            raise FileNotFoundError(f"Table file not found: {entry}")

        with as_file(entry) as path:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)

    def _get_variable_entry(self) -> Dict[str, Any]:
        """Get variable entry from CMIP7 table"""
        try:
            table_data = self._load_table()
            var_entry = table_data["variable_entry"][self.cmor_name]

            # Ensure fill values are included if present in the CMOR table
            for key in ("missing_value", "_FillValue"):
                if key in var_entry:
                    var_entry[key] = var_entry[key]
                else:
                    var_entry[key] = 1e20  # default fallback

            return var_entry
        except KeyError:
            # Generate helpful suggestions
            suggestions = self._get_variable_suggestions()
            raise VariableNotFoundError(self.cmor_name, self.table, suggestions)

    def _get_variable_suggestions(self) -> List[str]:
        """
        Generate helpful suggestions when a variable is not found in CMIP7.
        """
        suggestions = []

        # Check if variable exists in other CMIP7 tables
        common_tables = [
            "atmos",
            "ocean",
            "land",
            "seaIce",
            "landIce",
            "aerosol",
            "atmosChem",
            "ocnBgchem",
        ]
        found_in_tables = []

        for table in common_tables:
            if table == self.table:
                continue  # Skip current table

            try:
                table_file = f"CMIP7_{table}.json"
                table_resource = files(self.table_dir) / table_file

                with as_file(table_resource) as table_path:
                    with open(table_path, "r", encoding="utf-8") as f:
                        table_data = json.load(f)

                    if self.cmor_name in table_data.get("variable_entry", {}):
                        found_in_tables.append(table)

            except (FileNotFoundError, KeyError):
                continue  # Table doesn't exist or has no variable_entry

        if found_in_tables:
            table_list = ", ".join(found_in_tables)
            suggestions.append(
                f"Variable '{self.cmor_name}' is available in table(s): {table_list}"
            )
            suggestions.append(f"Try using: {found_in_tables[0]}.{self.cmor_name}")

        # Check for similar variable names in current table
        try:
            current_table_data = self._load_table()
            available_vars = list(current_table_data.get("variable_entry", {}).keys())

            # Find variables with similar names (simple string similarity)
            similar_vars = []
            for var in available_vars:
                if len(var) > 2 and (
                    self.cmor_name.lower() in var.lower()
                    or var.lower() in self.cmor_name.lower()
                    or
                    # Check for common root (first 3 characters)
                    (
                        len(self.cmor_name) >= 3
                        and len(var) >= 3
                        and self.cmor_name[:3].lower() == var[:3].lower()
                    )
                ):
                    similar_vars.append(var)

            if similar_vars:
                similar_list = ", ".join(similar_vars[:5])  # Limit to 5 suggestions
                suggestions.append(
                    f"Similar variables in {self.table} table: {similar_list}"
                )

            # Show a sample of available variables if no similar ones found
            elif available_vars:
                sample_vars = ", ".join(available_vars[:10])  # Show first 10
                total_count = len(available_vars)
                if total_count > 10:
                    sample_vars += f" (and {total_count - 10} more)"
                suggestions.append(
                    f"Available variables in {self.table} table: {sample_vars}"
                )

        except Exception:
            pass  # Don't fail if we can't load suggestions

        # Add general guidance for CMIP7
        suggestions.append(
            "Visit the CMIP7 data request for more information on available variables"
        )

        return suggestions

    def _get_axes(self, mapping) -> Dict[str, Any]:
        # Resolve resource inside the module path
        coord_entry = files(self.table_dir) / "CMIP7_coordinate.json"

        with as_file(coord_entry) as path:
            with open(path, "r", encoding="utf-8") as f:
                axes = json.load(f)["axis_entry"]

        dims = self.variable["dimensions"]  # It is a list for CMIP7
        vars_required = {}

        for dim in dims:
            if dim in axes and dim not in ["alevel"]:
                coord = axes[dim]
                vars_required[dim] = {k: v for k, v in coord.items() if v != ""}

        # Get the single variable mapping (assuming mapping has only one key)
        var_mapping = list(mapping.values())[0]  # Get the first (and only) value

        # Add z-axis coordinate variables if applicable
        if "zaxis" in var_mapping:
            # Get z-axis type from mapping
            zaxis_type = var_mapping["zaxis"].get("type", {})

            # Process main z-axis coordinate
            zcoord = axes.get(zaxis_type, {})["out_name"]
            vars_required[zcoord] = {
                k: v for k, v in axes[zaxis_type].items() if v != ""
            }

            # Process z_factors
            zfactors_str = axes.get(zaxis_type, {}).get("z_factors", "")

            zfactors = {}
            if zfactors_str:
                parts = zfactors_str.split()
                zfactors = {
                    parts[i].rstrip(":"): parts[i + 1]
                    for i in range(0, len(parts), 2)
                    if i + 1 < len(parts)
                }

            formula_entry = files(self.table_dir) / "CMIP7_formula_terms.json"
            with as_file(formula_entry) as fpath:
                with open(fpath, "r", encoding="utf-8") as ff:
                    formula_terms = json.load(ff)["formula_entry"]

            for factor_name, _ in zfactors.items():
                if factor_name in formula_terms:
                    zcoord = formula_terms[factor_name]
                    vars_required[factor_name] = {
                        k: v for k, v in zcoord.items() if v != ""
                    }

        # Let's map the axis and formula terms to the inputs
        vars_rename_map = {}
        extended_mapping = var_mapping["dimensions"] | var_mapping.get("zaxis", {}).get(
            "coordinate_variables", {}
        )
        inverted_extended_mapping = {v: k for k, v in extended_mapping.items()}

        for _, v in vars_required.items():
            input_dim = inverted_extended_mapping.get(v["out_name"])
            if input_dim:
                vars_rename_map[input_dim] = v["out_name"]

        self.axes = vars_required

        return vars_required, vars_rename_map

    def _get_required_bounds_variables(self, mapping: Dict[str, Any]) -> tuple:
        """
        Get required bounds variables based on CMOR vocabulary axes.

        Args:
            mapping: Variable mapping dictionary containing dimensions

        Returns:
            tuple: (bnds_required, bounds_rename_map) where
                - bnds_required: list of required bounds variable names
                - bounds_rename_map: dict mapping input bounds names to output bounds names
        """
        bnds_required = {}
        bounds_rename_map = {}

        # Get the single variable mapping (assuming mapping has only one key)
        var_mapping = list(mapping.values())[0]  # Get the first (and only) value

        extended_mapping = var_mapping["dimensions"] | var_mapping.get("zaxis", {}).get(
            "coordinate_variables", {}
        )
        inverted_extended_mapping = {v: k for k, v in extended_mapping.items()}

        axes, _ = self._get_axes(mapping)
        for _, v in axes.items():
            if v.get("must_have_bounds") == "yes":
                # Find the input dimension name that maps to this output name
                input_dim = inverted_extended_mapping.get(v["out_name"])
                if input_dim:
                    input_bounds = input_dim + "_bnds"
                    output_bounds = v["out_name"] + "_bnds"
                    bounds_rename_map[input_bounds] = output_bounds
                    bnds_required[output_bounds] = {
                        key: val for key, val in v.items() if val != ""
                    }

        # Also handle bounds of z-axis formula terms (e.g. b_bnds for hybrid_height).
        # These are listed in z_bounds_factors of the coordinate table entry.
        for _, v in axes.items():
            z_bounds_factors_str = v.get("z_bounds_factors", "")
            if not z_bounds_factors_str:
                continue
            parts = z_bounds_factors_str.split()
            z_bounds_factors = {
                parts[i].rstrip(":"): parts[i + 1]
                for i in range(0, len(parts), 2)
                if i + 1 < len(parts)
            }
            for factor_name, output_bnds_name in z_bounds_factors.items():
                if not output_bnds_name.endswith("_bnds"):
                    continue
                input_factor = inverted_extended_mapping.get(factor_name)
                if input_factor:
                    input_bnds = input_factor + "_bnds"
                    if input_bnds not in bounds_rename_map:
                        bounds_rename_map[input_bnds] = output_bnds_name
                        bnds_required[output_bnds_name] = {
                            key: val for key, val in v.items() if val != ""
                        }

        return bnds_required, bounds_rename_map

    def get_variant_components(self) -> Dict[str, int]:
        """Parse variant label components (same as CMIP6)"""
        pattern = re.compile(
            r"r(?P<realization_index>\d+)"
            r"i(?P<initialization_index>\d+)"
            r"p(?P<physics_index>\d+)"
            r"f(?P<forcing_index>\d+)$"
        )
        match = pattern.match(self.variant_label)
        if not match:
            raise ValueError(f"Invalid variant_label format: {self.variant_label}")
        return {k: int(v) for k, v in match.groupdict().items()}

    def _get_external_variables(self) -> Optional[str]:
        """
        Derive the list of external variables required for this CMOR variable.
        """
        externals: set[str] = set()

        # Known common external vars (similar to CMIP6)
        known_external_vars = {
            "areacella",
            "areacello",
            "volcello",
            "sftlf",
            "sftof",
            "deptho",
            "orog",
            "siconc",
            "landMask",
            "climofactor",
        }

        # 1. From cell_measures
        cell_measures = self.variable.get("cell_measures", "")
        if cell_measures:
            tokens = cell_measures.strip().split()
            for i in range(1, len(tokens), 2):
                externals.add(tokens[i])

        # 2. From cell_methods (heuristic)
        cell_methods = self.variable.get("cell_methods", "")
        for ext in known_external_vars:
            if ext in cell_methods:
                externals.add(ext)

        # 3. Add known required ones based on variable name (heuristic)
        if self.cmor_name in {"evspsbl", "mrro", "mrso"}:
            externals.add("sftlf")
        if self.cmor_name in {"thetao", "so", "hfds", "ocean_heat_content"}:
            externals.update({"areacello", "volcello", "deptho"})

        return " ".join(sorted(externals)) if externals else None

    def get_required_global_attributes(self) -> Dict[str, Any]:
        """Generate CMIP7-compliant global attributes"""
        now = datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        variant = self.get_variant_components()

        attrs = {
            "Conventions": self.cmip_table["Header"].get("Conventions"),
            "activity_id": self._resolve_activity_id(),
            "area_label": self._get_area_label(),
            "branded_variable": self.branded_name,
            "branding_suffix": self._get_branding_suffix(),
            "creation_date": now,
            "data_specs_version": self.cmip_table["Header"].get("data_specs_version"),
            "drs_specs": self._get_drs_specs(),
            "experiment_id": self.experiment_id,
            "forcing_index": variant["forcing_index"],
            "frequency": self.frequency,
            "grid_label": self.grid_label,
            "horizontal_label": self._get_horizontal_label(),
            "initialization_index": variant["initialization_index"],
            "institution_id": ",".join(self.source["institution_id"]),
            "license_id": self._get_license_id(),
            "mip_era": "CMIP7",
            "nominal_resolution": self._get_nominal_resolution(),
            "physics_index": variant["physics_index"],
            "product": self.cmip_table["Header"].get("product"),
            "realization_index": variant["realization_index"],
            "realm": self.variable["modeling_realm"],
            "region": self._get_validated_region(),
            "source_id": self.source_id,
            "temporal_label": self._get_temporal_label(),
            "tracking_id": f"hdl:21.14100/{uuid.uuid4()}",
            "variable_id": self.cmor_name,
            "variant_label": self.variant_label,
            "vertical_label": self._get_vertical_label(),
        }

        # Add parent experiment attributes if needed
        parent_attrs = self.get_parent_experiment_attrs()
        if parent_attrs:
            for k, v in parent_attrs.items():
                attrs[k] = v

        # Add external variables if any
        external_vars = self._get_external_variables()
        if external_vars:
            attrs["external_variables"] = external_vars

        # Add creator information
        attrs["creator_name"] = _creator.creator_name
        attrs["creator_organisation"] = _creator.organisation
        attrs["creator_email"] = _creator.creator_email
        attrs["creator_url"] = _creator.creator_url

        return attrs

    def _get_variable_frequency(self) -> str:
        """Get variable frequency from CMIP7 table or variable definition"""
        # In CMIP7, frequency might be in the variable entry or table header
        return self.variable.get(
            "frequency", self.cmip_table["Header"].get("frequency", "")
        )

    def _get_nominal_resolution(self) -> Optional[str]:
        """Get nominal resolution from source metadata"""
        realm = self.variable.get("modeling_realm")
        try:
            model_components = self.source.get("model_component", {})
            return model_components.get(realm, {}).get("native_nominal_resolution")
        except (KeyError, AttributeError):
            return None

    def _resolve_activity_id(self) -> str:
        """Resolve activity ID from experiment metadata"""
        available = self.experiment.get("activity", [])
        if len(available) == 1:
            return available[0]
        if self.activity_id and self.activity_id in available:
            return self.activity_id
        if available:
            return available[0]  # Default to first if multiple
        raise ValueError(
            f"No activity IDs found for experiment '{self.experiment_id}'. "
            f"Available: {available}. Please specify one explicitly."
        )

    def _get_sub_experiment_id(self) -> str:
        """Get sub-experiment ID (CMIP7 might handle this differently)"""
        return self.experiment.get("sub_experiment_id", "none")

    def _get_sub_experiment(self) -> str:
        """Get sub-experiment description"""
        sub_exp_id = self._get_sub_experiment_id()
        return "none" if sub_exp_id == "none" else sub_exp_id

    def _get_source_type(self) -> str:
        """Get source type from experiment requirements"""
        required = self.experiment.get("model_realms_required", [])
        return " ".join(required)

    def _get_institution_name(self) -> str:
        """Get institution name from source metadata"""
        institution_ids = self.source.get("institution_id", [])
        if institution_ids:
            # For now, return the first institution ID
            # In a full implementation, you'd load institution metadata
            return institution_ids[0]
        return ""

    def _format_source_string(self) -> str:
        """Format source string with model components"""
        label = self.source.get("label", "")
        components = self.source.get("model_component", {})

        if not components:
            return label

        component_descriptions = []
        for comp, desc in components.items():
            comp_desc = desc.get("description", "none")
            component_descriptions.append(f"{comp}: {comp_desc}")

        return f"{label}: \n" + "\n".join(component_descriptions)

    def _get_license(self) -> str:
        """
        Get CMIP7 license information from license.json controlled vocabulary.
        """
        # Get institution name for license template
        institution_ids = self.source.get("institution_id", [])
        institution = institution_ids[0] if institution_ids else "<institution>"

        # Use the CMIP7 license template
        return (
            f"CMIP7 model data produced by {institution} is licensed under a "
            "Creative Commons Attribution 4.0 International License "
            "(https://creativecommons.org/licenses/by/4.0/). Consult "
            "https://pcmdi.llnl.gov/CMIP7/TermsOfUse for terms of use governing "
            "CMIP7 output, including citation requirements and proper acknowledgment. "
            "The data producers and data providers make no warranty, either express or implied, "
            "including, but not limited to, warranties of merchantability and fitness for a "
            "particular purpose. All liabilities arising from the supply of the information "
            "(including any liability arising in negligence) are excluded to the fullest "
            "extent permitted by law."
        )

    def _get_license_id(self) -> str:
        """
        Get CMIP7 license ID from source metadata or CMIP7 controlled vocabulary.
        """
        license_info = self.source.get("license_info", {})

        # Return the license ID if available in source metadata
        if "id" in license_info:
            return license_info["id"]

        # Default CMIP7 license ID - this should be updated based on CMIP7 requirements
        return "CC BY 4.0"

    def _load_project_cv(self, cv_name: str) -> Dict[str, Any]:
        """Load a project controlled vocabulary JSON file"""
        try:
            cv_file = files(self.cv_dir) / "project" / f"{cv_name}.json"
            with as_file(cv_file) as path:
                with open(path, "r", encoding="utf-8") as f:
                    return json.load(f)
        except FileNotFoundError:
            raise ValueError(
                f"Project CV '{cv_name}' not found in CMIP7 controlled vocabularies."
            )

    def _get_drs_specs(self) -> str:
        """Get DRS specifications from CMIP7 controlled vocabularies"""
        drs_cv = self._load_project_cv("drs")
        drs_specs_list = drs_cv["drs"]["drs_specs"]
        return drs_specs_list[0] if drs_specs_list else "MIP-DRS7"

    def _get_horizontal_label(self) -> Optional[str]:
        """Extract horizontal label from processing info using CMIP7 controlled vocabulary"""
        if not self.processing_info:
            return None

        # Load CMIP7 horizontal label controlled vocabulary
        horizontal_cv = self._load_project_cv("horizontal_label")
        valid_labels = horizontal_cv["horizontal_label"]

        # Check processing_info against valid CMIP7 horizontal labels
        processing_lower = self.processing_info.lower()

        for label in valid_labels:
            if label.lower() in processing_lower:
                return label

        return None

    def _get_vertical_label(self) -> Optional[str]:
        """Extract vertical label from processing info using CMIP7 controlled vocabulary"""
        if not self.processing_info:
            return None

        # For now, there doesn't seem to be a vertical_label.json in the project CVs
        # We'll implement this when CMIP7 defines the controlled vocabulary
        # Return None until vertical labels are defined in CMIP7 CVs
        return None

    def _get_temporal_label(self) -> Optional[str]:
        """Extract temporal label from processing info using CMIP7 controlled vocabulary"""
        if not self.processing_info:
            return None

        # Load CMIP7 temporal label controlled vocabulary
        temporal_cv = self._load_project_cv("temporal_label")
        valid_labels = temporal_cv["temporal_label"]

        # Check processing_info against valid CMIP7 temporal labels
        processing_lower = self.processing_info.lower()

        for label in valid_labels:
            if label.lower() in processing_lower:
                return label

        return None

    def _get_area_label(self) -> Optional[str]:
        """Extract area label from processing info using CMIP7 controlled vocabulary"""
        if not self.processing_info:
            return None

        # Load CMIP7 area label controlled vocabulary
        area_cv = self._load_project_cv("area_label")
        valid_labels = area_cv["area_label"]

        # Check processing_info against valid CMIP7 area labels
        processing_lower = self.processing_info.lower()

        for label in valid_labels:
            if label.lower() in processing_lower:
                return label

        return None

    def _get_validated_region(self) -> str:
        """Get validated region from CMIP7 controlled vocabulary"""
        # Load CMIP7 region controlled vocabulary
        region_cv = self._load_project_cv("region")
        valid_regions = region_cv["region"]

        # Check if parsed region is valid
        if self.region:
            region_lower = self.region.lower()
            for valid_region in valid_regions:
                if valid_region.lower() == region_lower:
                    return valid_region

        # Default to global if no valid region found
        return "glb"

    def build_drs_path(self, drs_root: Path, version_date: str) -> Path:
        """
        Build DRS (Data Reference Syntax) path according to CMIP7 specifications.

        Args:
            drs_root: Root directory for DRS structure
            version_date: Version date in YYYYMMDD format

        Returns:
            Complete DRS path following CMIP7 template:
            <drs_specs>/<mip_era>/<activity_id>/<institution_id>/<source_id>/<experiment_id>/<variant_label>/<region>/<frequency>/<variable_id>/<branding_suffix>/<grid_label>/<version>
        """
        # Load DRS template from CMIP7 controlled vocabulary
        drs_cv = self._load_project_cv("drs")
        drs_spec = drs_cv["drs"]

        # Build DRS components according to CMIP7 template
        drs_components = [
            drs_spec["drs_specs"][0],  # drs_specs (e.g., "MIP-DRS7")
            "CMIP7",  # mip_era
            self._resolve_activity_id(),  # activity_id
            ",".join(self.source["institution_id"]),  # institution_id
            self.source_id,  # source_id
            self.experiment_id,  # experiment_id
            self.variant_label,  # variant_label
            self._get_validated_region(),  # region
            self.frequency or "fx",  # frequency (use "fx" if not specified)
            self.cmor_name,  # variable_id
            # branding_suffix - this might need to be derived from processing_info or other metadata
            self._get_branding_suffix(),  # branding_suffix
            self.grid_label,  # grid_label
            f"v{version_date}",  # version
        ]

        return drs_root.joinpath(*drs_components)

    def _get_branding_suffix(self) -> str:
        """
        Get branding suffix for CMIP7 DRS structure.
        The branding suffix is the processing_info with an underscore prefix.
        """
        if self.processing_info:
            return f"{self.processing_info}"
        return ""

    def get_cmip_missing_value(self) -> float:
        """
        Get the CMIP7-compliant missing value for this variable.

        Returns the missing value as specified in the CMOR table for this variable,
        with fallback to table default or global default.

        Returns:
            float: The CMIP7-compliant missing value
        """
        # Check if variable has specific missing value
        if "missing_value" in self.variable:
            return float(self.variable["missing_value"])

        # Check variable type and use appropriate table default
        var_type = self.variable.get("type", "real")
        if var_type == "integer":
            # Use integer missing value from table header
            return float(self.cmip_table["Header"].get("int_missing_value", -999))
        else:
            # Use real missing value from table header
            return float(self.cmip_table["Header"].get("missing_value", 1e20))

    def get_cmip_fill_value(self) -> float:
        """
        Get the CMIP7-compliant _FillValue for this variable.

        For CMIP7, _FillValue should be the same as missing_value.

        Returns:
            float: The CMIP7-compliant _FillValue
        """
        return self.get_cmip_missing_value()

    def standardize_missing_values(self, data_array, convert_existing: bool = True):
        """
        Standardize missing values in a data array to CMIP7 requirements.

        This method ensures that:
        1. All missing/NaN values use the CMIP7-specified missing value
        2. Data with different missing values from derived calculations are standardized
        3. Attributes are updated with correct missing_value and _FillValue
        4. Lazy evaluation is preserved for dask arrays

        Parameters:
            data_array: xarray.DataArray
                The data array to standardize
            convert_existing: bool
                If True, convert existing missing values to CMIP7 standard.
                If False, only standardize NaN values and update attributes.

        Returns:
            xarray.DataArray: Data array with standardized missing values
        """
        # Get the correct CMIP7 missing value
        cmip_missing_value = self.get_cmip_missing_value()
        cmip_fill_value = self.get_cmip_fill_value()

        # Create a shallow copy to avoid modifying the original (preserves dask arrays)
        result = data_array.copy(deep=False)

        if convert_existing:
            # Get current missing/fill values from attributes
            current_missing = data_array.attrs.get("missing_value")
            current_fill = data_array.attrs.get("_FillValue")

            # Build conditions for missing values using xarray operations (lazy)
            missing_conditions = []

            # Check for NaN values
            missing_conditions.append(np.isnan(result))

            # Check for current missing_value
            if current_missing is not None:
                try:
                    current_missing = float(current_missing)
                    missing_conditions.append(result == current_missing)
                except (ValueError, TypeError):
                    pass

            # Check for current _FillValue
            if current_fill is not None:
                try:
                    current_fill = float(current_fill)
                    missing_conditions.append(result == current_fill)
                except (ValueError, TypeError):
                    pass

            # Combine all missing value conditions (this stays lazy with dask)
            if missing_conditions:
                combined_mask = missing_conditions[0]
                for condition in missing_conditions[1:]:
                    combined_mask = combined_mask | condition

                # Use xarray.where to preserve lazy evaluation
                result = result.where(~combined_mask, cmip_missing_value)
        else:
            # Only convert NaN values to CMIP7 missing value (lazy operation)
            result = result.where(~np.isnan(result), cmip_missing_value)

        # Update attributes with correct CMIP7 values (this doesn't affect lazy evaluation)
        result.attrs["missing_value"] = cmip_missing_value
        result.attrs["_FillValue"] = cmip_fill_value

        return result

    def get_required_attribute_names(self) -> List[str]:
        """
        Get the list of required global attribute names from CMIP7 controlled vocabulary.

        Returns:
            List[str]: List of required global attribute names
        """
        # Load the CMIP7 required global attributes CV file
        return self._load_project_cv("required_global_attributes")[
            "required_global_attributes"
        ]

    def generate_filename(
        self,
        attrs: Dict[str, Any],
        ds: xr.Dataset,
        cmor_name: str,
        compound_name: str,
    ) -> str:
        """
        Generate CMIP7-compliant filename using official DRS template.

        Args:
            attrs: Dataset global attributes
            ds: xarray Dataset
            cmor_name: CMOR variable name
            compound_name: Compound name for extracting components

        Returns:
            str: CMIP7-compliant filename
        """

        # Parse compound name to get components
        compound_parts = self._parse_compound_name(compound_name)

        # Create mapping of template variables to actual values
        template_vars = {
            "variable_id": compound_parts["physical_parameter"],
            "branding_suffix": self._get_branding_suffix(),
            "frequency": attrs.get("frequency", compound_parts["frequency"] or "fx"),
            "region": attrs.get("region", compound_parts["region"] or "glb"),
            "grid_label": attrs.get("grid_label", ""),
            "source_id": attrs.get("source_id", ""),
            "experiment_id": attrs.get("experiment_id", ""),
            "variant_label": attrs.get("variant_label", ""),
        }

        # Handle time range if time coordinate exists
        if "time" in ds[cmor_name].coords:
            from cftime import num2date

            time_var = ds[cmor_name].coords["time"]
            units = time_var.attrs.get("units", "")
            calendar = time_var.attrs.get("calendar", "standard").lower()

            sample = time_var.values[0]
            if hasattr(sample, "year"):
                times = time_var.values[[0, -1]]
            elif np.issubdtype(time_var.dtype, np.datetime64):
                import pandas as pd

                times = [pd.Timestamp(t) for t in time_var.values[[0, -1]]]
            else:
                from cftime import num2date

                times = num2date(
                    time_var.values[[0, -1]], units=units, calendar=calendar
                )

            # Use simple YYYYMM format for CMIP7 (can be updated as standards evolve)
            start, end = [f"{t.year:04d}{t.month:02d}" for t in times]
            time_range = f"_{start}-{end}"
        else:
            # Time-independent variable - no time_range
            time_range = ""

        print(compound_parts["physical_parameter"])

        # Build filename from template
        filename_parts = [
            template_vars["variable_id"],
            template_vars["branding_suffix"],
            template_vars["frequency"],
            template_vars["region"],
            template_vars["grid_label"],
            template_vars["source_id"],
            template_vars["experiment_id"],
            template_vars["variant_label"],
        ]

        # Join with underscores as per CMIP7 template, add time range and .nc extension
        filename = "_".join(filename_parts) + time_range + ".nc"

        return filename

    def __repr__(self) -> str:
        return f"<CMIP7Vocabulary table={self.table} physical_parameter={self.physical_parameter} branded_name={self.branded_name} frequency={self.frequency} region={self.region} experiment={self.experiment_id} source={self.source_id}>"
