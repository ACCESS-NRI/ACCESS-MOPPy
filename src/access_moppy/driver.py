import warnings
from pathlib import Path
from typing import Any, Dict, Optional, Union

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
        input_paths: Union[str, list],
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
        resampling_method: str = "auto",
    ):
        """
        Initializes the CMORiser with necessary parameters.
        :param input_paths: Path(s) to input NetCDF files.
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
        """

        self.input_paths = input_paths
        self.validate_frequency = validate_frequency
        self.enable_resampling = enable_resampling
        self.resampling_method = resampling_method
        self.output_path = Path(output_path)
        self.compound_name = compound_name
        self.experiment_id = experiment_id
        self.source_id = source_id
        self.variant_label = variant_label
        self.grid_label = grid_label
        self.activity_id = activity_id
        self.model_id = model_id
        self.variable_mapping = load_model_mappings(compound_name, model_id)
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
        if table in ("Amon", "Lmon", "Emon"):
            self.cmoriser = CMIP6_Atmosphere_CMORiser(
                input_paths=self.input_paths,
                output_path=str(self.output_path),
                cmip6_vocab=self.vocab,
                variable_mapping=self.variable_mapping,
                compound_name=self.compound_name,
                drs_root=drs_root if drs_root else None,
                validate_frequency=self.validate_frequency,
                enable_resampling=self.enable_resampling,
                resampling_method=self.resampling_method,
            )
        elif table in ("Oyr", "Oday", "Omon", "SImon"):
            if self.source_id == "ACCESS-OM3":
                # ACCESS-OM3 uses MOM6 (C-grid) — requires dedicated CMORiser implementation
                # that handles C-grid supergrid logic, MOM6 metadata, and OM3-specific conventions
                self.cmoriser = CMIP6_Ocean_CMORiser_OM3(
                    input_paths=self.input_paths,
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
                    input_paths=self.input_paths,
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
        Converts the underlying xarray Dataset to Iris CubeList format using ncdata for lossless conversion.
        Requires ncdata and iris to be installed.
        """
        try:
            from ncdata.iris_xarray import cubes_from_xarray

            return cubes_from_xarray(self.cmoriser.ds)
        except ImportError:
            raise ImportError(
                "ncdata and iris are required for to_iris(). Please install ncdata and iris."
            )

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
