import warnings
from typing import Dict, Optional, Any

import numpy as np
import xarray as xr

from access_moppy.base import CMIP6_CMORiser
from access_moppy.derivations import custom_functions, evaluate_expression
from access_moppy.utilities import (
    calculate_latitude_bounds,
    calculate_longitude_bounds,
    calculate_time_bounds,
)


class HybridCoordinateHandler:
    """
    Handles hybrid coordinate systems for atmospheric model levels.
    
    Supports:
    - Hybrid sigma-pressure coordinates (ap + b*ps or a*p0 + b*ps)
    - Hybrid height coordinates (a + b*orog)
    """
    
    def __init__(self, vocab: Any):
        """
        Initialize handler with CMOR vocabulary.
        
        Args:
            vocab: CMOR vocabulary object containing coordinate definitions
        """
        self.vocab = vocab
        
    def identify_hybrid_coordinate(self, dimension: str, dataset: Optional[xr.Dataset] = None) -> Optional[str]:
        """
        Identify what type of hybrid coordinate a dimension uses.
        
        Args:
            dimension: Dimension name (e.g., 'alevel')
            dataset: Optional dataset to auto-detect coordinate type from available variables
            
        Returns:
            Coordinate type name or None if not hybrid
        """
        if dimension not in self.vocab.axes:
            return None
            
        axis_meta = self.vocab.axes[dimension]
        generic_level = axis_meta.get("generic_level_name")
        
        # Check if this is an alevel coordinate
        if generic_level == "alevel":
            try:
                coord_table = self.vocab.get_coordinate_entries()
            except AttributeError:
                # Fallback for older interface
                coord_table = getattr(self.vocab, 'coordinate_entries', {})
                if not coord_table:
                    cmor_table = getattr(self.vocab, 'cmor_table', {})
                    coord_table = cmor_table.get('coordinate', {}).get('axis_entry', {})
            
            # Get all hybrid coordinates that match alevel
            hybrid_coords = {}
            for coord_name, coord_def in coord_table.items():
                if coord_def.get("generic_level_name") == "alevel" and "formula" in coord_def:
                    hybrid_coords[coord_name] = coord_def
            
            if not hybrid_coords:
                return None
                
            # If dataset is provided, auto-detect based on available variables
            if dataset is not None:
                return self._auto_detect_coordinate_type(hybrid_coords, dataset)
            
            # Fallback: return the first one found
            return list(hybrid_coords.keys())[0]
        
        return None
    
    def _auto_detect_coordinate_type(self, hybrid_coords: Dict[str, Any], dataset: xr.Dataset) -> Optional[str]:
        """
        Auto-detect the appropriate hybrid coordinate type based on available variables in the dataset.
        
        Enhanced to parse actual formula_terms attributes from dataset coordinates and match
        formula structures rather than exact variable names.
        
        Args:
            hybrid_coords: Dictionary of available hybrid coordinate definitions
            dataset: Dataset to check for formula term variables
            
        Returns:
            Best matching coordinate type name
        """
        available_vars = set(dataset.variables.keys()) | set(dataset.coords.keys())
        
        # Extract formula_terms from dataset coordinates
        dataset_formula_patterns = []
        for var_name in dataset.coords:
            coord_var = dataset.coords[var_name]
            if hasattr(coord_var, 'attrs') and 'formula_terms' in coord_var.attrs:
                formula_terms = coord_var.attrs['formula_terms']
                standard_name = coord_var.attrs.get('standard_name', '')
                
                # Parse the formula_terms to extract structure
                import re
                pattern = r'(\w+):\s*(\w+)'
                matches = re.findall(pattern, formula_terms)
                formula_vars = {key: value for key, value in matches}
                
                dataset_formula_patterns.append({
                    'coord_name': var_name,
                    'standard_name': standard_name,
                    'formula_terms': formula_terms,
                    'formula_vars': formula_vars,
                    'available_vars': set(formula_vars.values()) & available_vars
                })
                
                print(f"🔍 Found coordinate formula: {var_name}")
                print(f"   Standard name: {standard_name}")
                print(f"   Formula terms: {formula_terms}")
                print(f"   Available variables: {sorted(set(formula_vars.values()) & available_vars)}")
        
        # Score each coordinate type against dataset patterns
        coord_scores = {}
        for coord_name, coord_def in hybrid_coords.items():
            best_score = 0
            best_match = None
            
            # Get expected formula structure from CMOR table
            z_factors = coord_def.get("z_factors", "")
            expected_standard_name = coord_def.get("standard_name", "")
            
            if z_factors:
                import re
                pattern = r'(\w+):\s*(\w+)'
                matches = re.findall(pattern, z_factors)
                expected_structure = set(key for key, value in matches)
                
                # Try to match against dataset formula patterns
                for dataset_pattern in dataset_formula_patterns:
                    score = 0
                    
                    # Check if standard names match (highest priority)
                    if (expected_standard_name and 
                        dataset_pattern['standard_name'] == expected_standard_name):
                        score += 20
                        print(f"   ✅ Standard name match: {expected_standard_name}")
                    
                    # Check if formula structure matches
                    actual_structure = set(dataset_pattern['formula_vars'].keys())
                    structure_overlap = expected_structure & actual_structure
                    if structure_overlap:
                        score += len(structure_overlap) * 5
                        print(f"   ✅ Formula structure overlap: {sorted(structure_overlap)}")
                    
                    # Check if all formula variables are available
                    available_formula_vars = dataset_pattern['available_vars']
                    if available_formula_vars:
                        score += len(available_formula_vars) * 2
                        print(f"   ✅ Available formula vars: {sorted(available_formula_vars)}")
                    
                    # Bonus for complete match
                    if (len(structure_overlap) == len(expected_structure) and 
                        len(available_formula_vars) == len(dataset_pattern['formula_vars'])):
                        score += 10
                        print(f"   🎯 Complete formula match!")
                    
                    if score > best_score:
                        best_score = score
                        best_match = dataset_pattern
            
            # Fallback: try legacy variable name matching
            if best_score == 0 and z_factors:
                import re
                pattern = r'(\w+):\s*(\w+)'
                matches = re.findall(pattern, z_factors)
                required_terms = set()
                for key, value in matches:
                    if value:  # Only count non-empty variable names
                        required_terms.add(value)
                
                # Count how many required terms are available
                available_terms = required_terms & available_vars
                legacy_score = len(available_terms)
                
                # Bonus points for having all required terms
                if len(available_terms) == len(required_terms) and len(required_terms) > 0:
                    legacy_score += 5  # Lower bonus than formula matching
                
                if legacy_score > best_score:
                    best_score = legacy_score
                    best_match = {'type': 'legacy', 'available_terms': available_terms}
            
            coord_scores[coord_name] = {
                'score': best_score,
                'match': best_match,
                'formula': coord_def.get('formula', ''),
                'standard_name': coord_def.get('standard_name', '')
            }
        
        if not coord_scores:
            return None
            
        # Find the coordinate type with the highest score
        best_coord = max(coord_scores.keys(), key=lambda x: coord_scores[x]['score'])
        best_score = coord_scores[best_coord]['score']
        
        # Only return if we found some matching terms
        if best_score > 0:
            match_info = coord_scores[best_coord]['match']
            print(f"🎯 Auto-detected coordinate type '{best_coord}' (score: {best_score})")
            print(f"   Standard name: {coord_scores[best_coord]['standard_name']}")
            print(f"   Formula: {coord_scores[best_coord]['formula']}")
            if match_info and isinstance(match_info, dict):
                if match_info.get('type') == 'legacy':
                    print(f"   Available terms: {match_info.get('available_terms', set())}")
                else:
                    print(f"   Matched coordinate: {match_info.get('coord_name', 'unknown')}")
                    print(f"   Available formula vars: {match_info.get('available_vars', set())}")
            return best_coord
        
        # If no variables match, fall back to first coordinate type
        fallback = list(hybrid_coords.keys())[0]
        print(f"⚠️  No formula terms found in dataset, using fallback: {fallback}")
        return fallback
    
    def get_formula_terms(self, coord_type: str) -> Dict[str, str]:
        """
        Get required formula terms for a coordinate type.
        
        Args:
            coord_type: Type of coordinate (e.g., 'standard_hybrid_sigma')
            
        Returns:
            Dictionary mapping term names to variable names
        """
        try:
            coord_table = self.vocab.get_coordinate_entries()
        except AttributeError:
            # Fallback for older interface
            coord_table = getattr(self.vocab, 'coordinate_entries', {})
            if not coord_table:
                # Try accessing through cmor_table
                cmor_table = getattr(self.vocab, 'cmor_table', {})
                coord_table = cmor_table.get('coordinate', {}).get('axis_entry', {})
            
        if coord_type not in coord_table:
            return {}
            
        coord_def = coord_table[coord_type]
        z_factors = coord_def.get("z_factors", "")
        
        formula_terms = {}
        
        # Parse z_factors string like "p0: p0 a: a b: b ps: ps"
        # This needs to be parsed as pairs: "key: value"
        if z_factors:
            # Use regex to match "key: value" patterns
            import re
            pattern = r'(\w+):\s*(\w*)'
            matches = re.findall(pattern, z_factors)
            for key, value in matches:
                formula_terms[key] = value
        
        return formula_terms
    
    def add_hybrid_coordinate_metadata(self, ds: xr.Dataset, coord_name: str, 
                                     coord_type: str) -> xr.Dataset:
        """
        Add proper hybrid coordinate metadata to dataset.
        
        Args:
            ds: Input dataset
            coord_name: Name of coordinate variable (usually 'lev')
            coord_type: Type of hybrid coordinate
            
        Returns:
            Dataset with updated coordinate metadata
        """
        try:
            coord_table = self.vocab.get_coordinate_entries()
        except AttributeError:
            # Fallback for older interface
            coord_table = getattr(self.vocab, 'coordinate_entries', {})
            if not coord_table:
                # Try accessing through cmor_table
                cmor_table = getattr(self.vocab, 'cmor_table', {})
                coord_table = cmor_table.get('coordinate', {}).get('axis_entry', {})
            
        if coord_type not in coord_table:
            warnings.warn(f"Unknown coordinate type: {coord_type}")
            return ds
            
        coord_def = coord_table[coord_type]
        
        # Update coordinate attributes
        if coord_name in ds:
            attrs_to_add = {
                "standard_name": coord_def.get("standard_name", ""),
                "long_name": coord_def.get("long_name", ""),
                "units": coord_def.get("units", ""),
                "axis": coord_def.get("axis", "Z"),
                "positive": coord_def.get("positive", ""),
            }
            
            # Add formula if present
            if "formula" in coord_def and coord_def["formula"]:
                attrs_to_add["formula"] = coord_def["formula"]
            
            # Add formula_terms if available
            formula_terms = self.get_formula_terms(coord_type)
            if formula_terms:
                # Convert to space-separated string format
                formula_terms_str = " ".join([f"{k}: {v}" for k, v in formula_terms.items()])
                attrs_to_add["formula_terms"] = formula_terms_str
            
            # Only add non-empty attributes
            for key, value in attrs_to_add.items():
                if value:
                    ds[coord_name].attrs[key] = value
        
        return ds
    
    def validate_formula_terms(self, ds: xr.Dataset, coord_type: str) -> bool:
        """
        Validate that required formula terms are present in dataset.
        
        Args:
            ds: Dataset to validate
            coord_type: Type of hybrid coordinate
            
        Returns:
            True if all required terms are present, False otherwise
        """
        required_terms = self.get_formula_terms(coord_type)
        missing_terms = []
        
        for term_name, var_name in required_terms.items():
            if var_name not in ds.variables and var_name not in ds.coords:
                missing_terms.append(var_name)
        
        if missing_terms:
            warnings.warn(
                f"Missing required formula terms for {coord_type}: {missing_terms}. "
                "Hybrid coordinate may not be properly defined."
            )
            return False
            
        return True
    
    def process_hybrid_coordinate(self, ds: xr.Dataset, dimension: str) -> xr.Dataset:
        """
        Process hybrid coordinate for a given dimension.
        
        Args:
            ds: Input dataset
            dimension: Dimension name that uses hybrid coordinates
            
        Returns:
            Dataset with processed hybrid coordinate
        """
        coord_type = self.identify_hybrid_coordinate(dimension, dataset=ds)
        if not coord_type:
            return ds
            
        print(f"🔧 Processing hybrid coordinate '{coord_type}' for dimension '{dimension}'")
            
        # Get the output coordinate name from vocabulary
        axis_meta = self.vocab.axes[dimension]
        coord_name = axis_meta["out_name"]
        
        # Add metadata
        ds = self.add_hybrid_coordinate_metadata(ds, coord_name, coord_type)
        
        # Validate formula terms (informational only)
        self.validate_formula_terms(ds, coord_type)
        
        return ds


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

        # Calculate missing bounds variables
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
                    stacklevel=2,
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
                        f"No automatic calculation available for '{bnds_var}'. This may cause CMIP6 compliance issues.",
                        UserWarning,
                        stacklevel=2,
                    )

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

        # Process hybrid coordinates after renaming
        hybrid_handler = HybridCoordinateHandler(self.vocab)
        cmor_dims = self.vocab.variable["dimensions"].split()
        
        for dim in cmor_dims:
            if dim in self.vocab.axes:
                axis_meta = self.vocab.axes[dim]
                # Check if this is a hybrid coordinate
                if axis_meta.get("generic_level_name") in ["alevel", "alevhalf"]:
                    self.ds = hybrid_handler.process_hybrid_coordinate(self.ds, dim)

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
