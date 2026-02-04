import warnings
from pathlib import Path
from typing import Any, Dict, Optional, Union

import xarray as xr

from access_moppy.atmosphere import CMIP6_Atmosphere_CMORiser
from access_moppy.defaults import _default_parent_info
from access_moppy.ocean import CMIP6_Ocean_CMORiser_OM2, CMIP6_Ocean_CMORiser_OM3
from access_moppy.utilities import load_model_mappings
from access_moppy.vocabulary_processors import CMIP6Vocabulary


class ACCESS_ESM_CMORiser:
    """
    Coordinates the CMORisation process using CMIP6Vocabulary and CMORiser.
    Handles DRS, versioning, and orchestrates the workflow.
    """

    def __init__(
        self,
        input_data: Optional[Union[str, list, xr.Dataset, xr.DataArray]] = None,
        *,
        compound_name: str,
        experiment_id: str,
        source_id: str,
        variant_label: str,
        grid_label: str,
        activity_id: str = None,
        output_path: Optional[Union[str, Path]] = ".",
        drs_root: Optional[Union[str, Path]] = None,
        parent_info: Optional[Dict[str, Dict[str, Any]]] = None,
        model_id: Optional[str] = None,
        validate_frequency: bool = True,
        enable_resampling: bool = False,
        enable_chunking: bool = False,
        resampling_method: str = "auto",
        # Backward compatibility
        input_paths: Optional[Union[str, list]] = None,
    ):
        """
        Initializes the CMORiser with necessary parameters.
        :param input_data: Path(s) to input NetCDF files, xarray Dataset, or xarray DataArray.
        :param compound_name: CMOR variable name (e.g., 'Amon.tas').
        :param experiment_id: CMIP6 experiment ID (e.g., 'historical').
        :param source_id: CMIP6 source ID (e.g., 'ACCESS-ESM1-5').
        :param variant_label: CMIP6 variant label (e.g., 'r1i1p1f1').
        :param grid_label: CMIP6 grid label (e.g., 'gn').
        :param activity_id: CMIP6 activity ID (e.g., 'CMIP').
        :param output_path: Path to write the CMORised output.
        :param drs_root: Optional root path for DRS structure.
        :param parent_info: Optional dictionary with parent experiment metadata.
        :param model_id: Optional model identifier for model-specific mappings (e.g., 'ACCESS-ESM1.6').
        :param validate_frequency: Whether to validate temporal frequency consistency across input files (default: True).
        :param enable_resampling: Whether to enable automatic temporal resampling when frequency mismatches occur (default: False).
        :param resampling_method: Method for temporal resampling ('auto', 'mean', 'sum', 'min', 'max', 'first', 'last') (default: 'auto').
        :param input_paths: [DEPRECATED] Use input_data instead. Kept for backward compatibility.
        """

        # Handle backward compatibility and validation
        if input_paths is not None and input_data is None:
            warnings.warn(
                "The 'input_paths' parameter is deprecated. Use 'input_data' instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            input_data = input_paths
        elif input_paths is not None and input_data is not None:
            raise ValueError(
                "Cannot specify both 'input_data' and 'input_paths'. Use 'input_data'."
            )

        # Load variable mapping to check if this is an internal calculation
        self.variable_mapping = load_model_mappings(compound_name, model_id)
        table, cmor_name = compound_name.split(".")

        # Check if this is an internal calculation that doesn't need input data
        is_internal_calc = False
        if cmor_name in self.variable_mapping:
            calc = self.variable_mapping[cmor_name].get("calculation", {})
            is_internal_calc = calc.get("type") == "internal"

        if input_paths is None and input_data is None:
            if not is_internal_calc:
                raise ValueError(
                    "Must specify either 'input_data' or 'input_paths' for non-internal calculations."
                )
            else:
                print(f"✓ No input data required for internal calculation: {cmor_name}")

        # Determine input type and store appropriately
        self.input_is_xarray = isinstance(input_data, (xr.Dataset, xr.DataArray))

        if self.input_is_xarray:
            # For xarray inputs, convert DataArray to Dataset if needed
            if isinstance(input_data, xr.DataArray):
                self.input_dataset = input_data.to_dataset()
            else:
                self.input_dataset = input_data
            self.input_paths = []  # Empty list for compatibility
            # Disable frequency validation for xarray inputs (already loaded)
            if validate_frequency:
                warnings.warn(
                    "Disabling frequency validation for xarray input (data is already loaded).",
                    UserWarning,
                )
            validate_frequency = False
        else:
            # For file paths, store as before
            self.input_paths = (
                input_data
                if isinstance(input_data, list)
                else [input_data]
                if input_data
                else []
            )
            self.input_dataset = None
        self.validate_frequency = validate_frequency
        self.enable_resampling = enable_resampling
        self.enable_chunking = enable_chunking
        self.resampling_method = resampling_method
        self.output_path = Path(output_path)
        self.compound_name = compound_name
        self.experiment_id = experiment_id
        self.source_id = source_id
        self.variant_label = variant_label
        self.grid_label = grid_label
        self.activity_id = activity_id
        self.model_id = model_id
        self.drs_root = Path(drs_root) if isinstance(drs_root, str) else drs_root
        if not parent_info:
            warnings.warn(
                "No parent_info provided. Defaulting to piControl parent experiment metadata. "
                "You should verify this is appropriate. Incorrect parent settings may lead to invalid CMIP submission."
            )

        self.parent_info = {**_default_parent_info, **(parent_info or {})}

        # Create the CMIP6Vocabulary instance with error handling
        try:
            self.vocab = CMIP6Vocabulary(
                compound_name=compound_name,
                experiment_id=experiment_id,
                source_id=source_id,
                variant_label=variant_label,
                grid_label=grid_label,
                activity_id=activity_id,
                parent_info=self.parent_info,
            )
        except Exception as e:
            # For VariableNotFoundError, just re-raise as-is (it already has good messaging)
            # For other exceptions, add context about the compound name
            if "VariableNotFoundError" in str(type(e)):
                raise
            else:
                raise type(e)(f"Error processing '{compound_name}': {str(e)}") from e

        # Initialize the CMORiser based on the compound name
        table, _ = compound_name.split(".")  # cmor_name now extracted internally
        if table in (
            "Amon",
            "Lmon",
            "Emon",
            "AERmon",
            "AERday",
            "day",
            "CFmon",
            "3hr",
            "6hrPlev",
            "Eday",
            "fx",
        ):
            self.cmoriser = CMIP6_Atmosphere_CMORiser(
                input_data=self.input_dataset
                if self.input_is_xarray
                else self.input_paths,
                output_path=str(self.output_path),
                cmip6_vocab=self.vocab,
                variable_mapping=self.variable_mapping,
                compound_name=self.compound_name,
                drs_root=drs_root if drs_root else None,
                validate_frequency=self.validate_frequency,
                enable_resampling=self.enable_resampling,
                resampling_method=self.resampling_method,
                enable_chunking=self.enable_chunking,
            )
        elif table in ("Oyr", "Oday", "Omon", "SImon", "Ofx"):
            if self.source_id == "ACCESS-OM3" or self.model_id == "ACCESS-CM3":
                # ACCESS-OM3 uses MOM6 (C-grid) — requires dedicated CMORiser implementation
                # that handles C-grid supergrid logic, MOM6 metadata, and OM3-specific conventions
                self.cmoriser = CMIP6_Ocean_CMORiser_OM3(
                    input_data=self.input_dataset
                    if self.input_is_xarray
                    else self.input_paths,
                    output_path=str(self.output_path),
                    compound_name=self.compound_name,
                    cmip6_vocab=self.vocab,
                    variable_mapping=self.variable_mapping,
                    drs_root=drs_root if drs_root else None,
                )
            else:
                # ACCESS-OM2 uses MOM5 (B-grid) — handled by a separate CMORiser class
                # specialized for B-grid variable locations and OM2-specific metadata
                self.cmoriser = CMIP6_Ocean_CMORiser_OM2(
                    input_data=self.input_dataset
                    if self.input_is_xarray
                    else self.input_paths,
                    output_path=str(self.output_path),
                    compound_name=self.compound_name,
                    cmip6_vocab=self.vocab,
                    variable_mapping=self.variable_mapping,
                    drs_root=drs_root if drs_root else None,
                )

    def __getitem__(self, key):
        return self.cmoriser.ds[key]

    def __getattr__(self, attr):
        # This is only called if the attr is not found on CMORiser itself
        return getattr(self.cmoriser.ds, attr)

    def __setitem__(self, key, value):
        self.cmoriser.ds[key] = value

    def __repr__(self):
        return repr(self.cmoriser.ds)

    def to_dataset(self):
        """
        Returns the underlying xarray Dataset from the CMORiser.
        """
        return self.cmoriser.ds

    def to_iris(self):
        """
        Converts the underlying xarray Dataset to a single Iris Cube with proper
        auxiliary coordinates, masking, and bounds for curvilinear ocean grids.

        For ocean data with curvilinear grids (e.g. ACCESS-OM2, ACCESS-OM3):
        - latitude/longitude become auxiliary coordinates (not separate cubes)
        - CMIP6 fill values (1e20) are converted to masked arrays
        - Coordinate bounds (vertices_latitude/vertices_longitude) are preserved

        Requires ncdata and iris to be installed.

        Returns:
            iris.cube.Cube: A single Cube for the CMORised variable with proper
            auxiliary coordinates, bounds, and masking applied.
        """
        try:
            import numpy as np
            from ncdata.iris_xarray import cubes_from_xarray
        except ImportError:
            raise ImportError(
                "ncdata and iris are required for to_iris(). Please install ncdata and iris."
            )

        ds = self.cmoriser.ds.copy(deep=False)
        cmor_name = self.cmoriser.cmor_name

        # Promote 2D lat/lon and their bounds from data vars to coordinates
        aux_vars = [
            "latitude",
            "longitude",
            "vertices_latitude",
            "vertices_longitude",
        ]
        vars_to_promote = [v for v in aux_vars if v in ds.data_vars]
        if vars_to_promote:
            ds = ds.set_coords(vars_to_promote)

        # Convert CMIP6 fill values to NaN for proper iris masking
        if cmor_name in ds.data_vars:
            fill_value = ds[cmor_name].attrs.get("_FillValue")
            missing_value = ds[cmor_name].attrs.get("missing_value")
            fill_val = fill_value if fill_value is not None else missing_value

            if fill_val is not None:
                try:
                    fill_val = float(fill_val)
                    ds[cmor_name] = ds[cmor_name].where(ds[cmor_name] != fill_val)
                except (TypeError, ValueError):
                    pass

        # Drop time_bnds from conversion (iris handles time bounds separately)
        bnds_vars = [v for v in ds.data_vars if v.endswith("_bnds")]
        if bnds_vars:
            ds = ds.drop_vars(bnds_vars)

        cubes = cubes_from_xarray(ds)

        # Extract only the main variable cube
        main_cube = None
        for cube in cubes:
            if cube.var_name == cmor_name:
                main_cube = cube
                break

        if main_cube is None:
            raise ValueError(
                f"Could not find cube for variable '{cmor_name}' in converted CubeList. "
                f"Available cubes: {[c.var_name for c in cubes]}"
            )

        # Ensure NaN values are properly masked
        if np.any(np.isnan(main_cube.data)):
            main_cube.data = np.ma.masked_invalid(main_cube.data)

        return main_cube

    def run(self, write_output: bool = False):
        """
        Runs the CMORisation process, including variable selection, processing,
        attribute updates, and optional output writing."""

        self.cmoriser.run()
        if write_output:
            self.cmoriser.write()

    def write(self):
        """
        Writes the CMORised dataset to the specified output path.
        """
        self.cmoriser.write()
