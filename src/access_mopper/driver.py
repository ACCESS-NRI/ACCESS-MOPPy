import warnings
from pathlib import Path
from typing import Any, Dict, Optional, Union

from access_mopper.atmosphere import CMIP6_Atmosphere_CMORiser
from access_mopper.defaults import _default_parent_info
from access_mopper.ocean import CMIP6_Ocean_CMORiser
from access_mopper.utilities import load_cmip6_mappings
from access_mopper.vocabulary_processors import CMIP6Vocabulary


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
        """

        self.input_paths = input_paths
        self.output_path = Path(output_path)
        self.compound_name = compound_name
        self.experiment_id = experiment_id
        self.source_id = source_id
        self.variant_label = variant_label
        self.grid_label = grid_label
        self.activity_id = activity_id
        self.variable_mapping = load_cmip6_mappings(compound_name)
        self.drs_root = Path(drs_root) if isinstance(drs_root, str) else drs_root
        if not parent_info:
            warnings.warn(
                "No parent_info provided. Defaulting to piControl parent experiment metadata. "
                "You should verify this is appropriate. Incorrect parent settings may lead to invalid CMIP submission."
            )

        self.parent_info = {**_default_parent_info, **(parent_info or {})}

        # Create the CMIP6Vocabulary instance
        self.vocab = CMIP6Vocabulary(
            compound_name=compound_name,
            experiment_id=experiment_id,
            source_id=source_id,
            variant_label=variant_label,
            grid_label=grid_label,
            activity_id=activity_id,
            parent_info=self.parent_info,
        )

        # Initialize the CMORiser based on the compound name
        table, cmor_name = compound_name.split(".")
        if table in ("Amon", "Lmon", "Emon"):
            self.cmoriser = CMIP6_Atmosphere_CMORiser(
                input_paths=self.input_paths,
                output_path=str(self.output_path),
                cmor_name=cmor_name,
                cmip6_vocab=self.vocab,
                variable_mapping=self.variable_mapping,
                drs_root=drs_root if drs_root else None,
            )
        elif table in ("Oyr", "Oday", "Omon", "SImon"):
            self.cmoriser = CMIP6_Ocean_CMORiser(
                input_paths=self.input_paths,
                output_path=str(self.output_path),
                cmor_name=cmor_name,
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
