import json
import re
import uuid
from datetime import datetime, timezone
from importlib.resources import as_file, files
from typing import Any, Dict, List, Optional

from access_moppy import _creator


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
    cv_dir = "access_moppy.vocabularies.cmip6_cmor_tables.CMIP6_CVs"
    table_dir = "access_moppy.vocabularies.cmip6_cmor_tables.Tables"

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
        self.axes: Dict[str, Any] = self._get_axes()

    def _load_controlled_vocab(self) -> Dict[str, Any]:
        vocab = {}
        for entry in files(self.cv_dir).iterdir():
            if entry.name.endswith(".json"):
                with as_file(entry) as path:
                    with open(path, "r", encoding="utf-8") as jf:
                        vocab.update(json.load(jf))
        return vocab

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
        entry = files(self.table_dir) / f"CMIP6_{self.table}.json"

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
        common_tables = ["Amon", "Lmon", "Omon", "Emon", "day", "6hrLev", "3hr"]
        found_in_tables = []

        for table in common_tables:
            if table == self.table:
                continue  # Skip current table

            try:
                table_file = f"CMIP6_{table}.json"
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

    def _get_axes(self) -> Dict[str, Any]:
        # Resolve resource inside the module path
        coord_entry = files(self.table_dir) / "CMIP6_coordinate.json"

        with as_file(coord_entry) as path:
            with open(path, "r", encoding="utf-8") as f:
                axes = json.load(f)["axis_entry"]

        dims = self.variable["dimensions"].split()
        result = {}
        # handle "olevel" specially as it maps to "depth_coord"
        for dim in dims:
            if dim == "olevel":
                coord = axes["depth_coord"]
            else:
                coord = axes[dim]
            result[dim] = {k: v for k, v in coord.items() if v != ""}
        return result

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
            "institution": self.vocab["institution_id"][
                self.source["institution_id"][0]
            ],
            "institution_id": ",".join(self.source["institution_id"]),
            "license": self._get_license(),
            "mip_era": "CMIP6",
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
        mip_era = "CMIP6"
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

        entry = files(self.cv_dir) / "CMIP6_license.json"

        if not entry.exists():
            raise FileNotFoundError(f"License CV file not found: {entry}")

        with as_file(entry) as path:
            with open(path, "r", encoding="utf-8") as f:
                license_template = json.load(f)

        # Perform placeholder substitutions
        license_text = license_template["license"]["license"]
        license_id = license_template["license"]["license_options"][
            license_info.get("id")
        ]["license_id"]
        license_url = license_template["license"]["license_options"][
            license_info.get("id")
        ]["license_url"]
        license_text = license_text.replace(
            "<Your Institution; see CMIP6_institution_id.json>", institution
        )
        license_text = license_text.replace(
            "<Creative Commons; select and insert a license_id; see below>", license_id
        )
        license_text = license_text.replace(
            "<insert the matching license_url; see below>", license_url
        )
        license_text = license_text.replace(
            "[ and at <some URL maintained by modeling group>]", ""
        )

        return license_text

    def __repr__(self) -> str:
        return f"<CMIP6Vocabulary variable={self.cmor_name} experiment={self.experiment_id} source={self.source_id}>"
