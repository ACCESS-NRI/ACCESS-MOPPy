from __future__ import annotations

import warnings
from contextlib import ExitStack
from importlib.resources import as_file
from pathlib import Path
from types import TracebackType
from typing import Any

import xarray as xr

from access_moppy.atmosphere import Atmosphere_CMORiser
from access_moppy.defaults import _default_parent_info
from access_moppy.ocean import Ocean_CMORiser_OM2, Ocean_CMORiser_OM3
from access_moppy.sea_ice import SeaIce_CMORiser
from access_moppy.utilities import (
    MappingNotFoundWarning,
    VariableMapping,
    load_cmip6_to_cmip7_mapping,
    _get_cmip7_to_cmip6_mapping,
    _model_mapping_file_exists,
    get_bundled_resource_path,
    load_model_mappings,
)
from access_moppy.vocabulary_processors import (
    CMIP6PlusMIPVocabulary,
    CMIP6PlusVocabulary,
    CMIP6Vocabulary,
    CMIP7Vocabulary,
)

_CONTRIBUTE_URL = "https://github.com/ACCESS-NRI/ACCESS-MOPPy"
_DEFAULT_MODEL_ID = "ACCESS-ESM1.6"


def _warn_if_mapping_missing(
    raw_mapping: dict[str, Any], compound_name: str, model_id: str | None
) -> None:
    """
    Emit a :class:`MappingNotFoundWarning` when no model mapping is found.

    Two distinct messages are produced:

    * If no mapping file exists for the model at all the user is told the model
      is not yet supported and is invited to contribute one.
    * If the mapping file exists but the requested variable is absent the user
      is told which variable is missing and is invited to contribute a mapping
      entry.

    Args:
        raw_mapping: The dict returned by :func:`load_model_mappings`.
        compound_name: CMIP6 compound name (e.g. ``'Amon.tas'``).
        model_id: Model identifier as supplied by the caller (may be ``None``).
    """
    if raw_mapping:
        return

    effective_model_id = model_id if model_id is not None else _DEFAULT_MODEL_ID

    # Extract the variable name; fall back to the full compound name if the
    # expected "table.variable" format is not present.
    parts = compound_name.split(".", 1)
    cmor_name = parts[1] if len(parts) == 2 else compound_name

    # stacklevel=4 targets the user's call site:
    #   user code → ACCESS_ESM_CMORiser.__init__ → _warn_if_mapping_missing
    #             → warnings.warn
    if not _model_mapping_file_exists(effective_model_id):
        warnings.warn(
            f"No mapping file found for model '{effective_model_id}'. "
            f"This model is not yet supported. "
            f"If you have access to the model output, consider contributing a "
            f"mapping file at: {_CONTRIBUTE_URL}",
            MappingNotFoundWarning,
            stacklevel=4,
        )
    else:
        warnings.warn(
            f"Variable '{cmor_name}' has no mapping for model '{effective_model_id}'. "
            f"The variable may not be supported for this model yet. "
            f"Consider contributing a mapping entry at: {_CONTRIBUTE_URL}",
            MappingNotFoundWarning,
            stacklevel=4,
        )


class ACCESS_ESM_CMORiser:
    """High-level public interface for ACCESS-ESM CMORisation.

    ``ACCESS_ESM_CMORiser`` selects the appropriate vocabulary and component
    CMORiser implementation for a requested CMIP variable, then delegates the
    scientific processing to that implementation.  It accepts either paths to
    raw NetCDF files or an already-open xarray object, manages DRS output
    metadata, and can be used as a context manager when bundled resource files
    are involved.
    """

    def __init__(
        self,
        input_data: str
        | Path
        | list[str | Path]
        | xr.Dataset
        | xr.DataArray
        | None = None,
        *,
        compound_name: str,
        experiment_id: str,
        source_id: str,
        variant_label: str,
        grid_label: str,
        cmip_version: str = "CMIP6",
        activity_id: str | None = None,
        output_path: str | Path | None = ".",
        drs_root: str | Path | None = None,
        parent_info: dict[str, dict[str, Any]] | None = None,
        model_id: str | None = None,
        validate_frequency: bool = True,
        enable_resampling: bool = False,
        enable_chunking: bool = False,
        resampling_method: str = "auto",
        # Backward compatibility
        input_paths: str | Path | list[str | Path] | None = None,
    ) -> None:
        """Initialise a CMORiser for one CMIP compound variable.

        Args:
            input_data: Path, paths, xarray dataset, or xarray data array to
                CMORise.  May be omitted for internally generated variables or
                variables backed by bundled resource files.
            compound_name: CMIP table and short name, e.g. ``"Amon.tas"``.
            experiment_id: CMIP experiment ID, e.g. ``"historical"``.
            source_id: CMIP source ID, e.g. ``"ACCESS-ESM1-5"``.
            variant_label: CMIP variant label, e.g. ``"r1i1p1f1"``.
            grid_label: CMIP grid label, e.g. ``"gn"``.
            cmip_version: Vocabulary family to use: ``"CMIP6"``,
                ``"CMIP6Plus"``, or ``"CMIP7"``.
            activity_id: Optional CMIP activity ID, e.g. ``"CMIP"``.
            output_path: Directory used by the component CMORiser when writing
                output.
            drs_root: Optional DRS root directory.  When supplied, output is
                written under this CMIP DRS tree.
            parent_info: Optional parent-experiment metadata keyed by CMIP
                attribute name.  Missing values fall back to ACCESS-MOPPy
                defaults for piControl parent metadata.
            model_id: Model mapping identifier, e.g. ``"ACCESS-ESM1.6"``.
            validate_frequency: Validate temporal frequency consistency across
                file inputs.  This is disabled automatically for xarray inputs.
            enable_resampling: Enable automatic temporal resampling when
                frequency mismatches are detected.
            enable_chunking: Enable dask chunking in supported component
                CMORisers.
            resampling_method: Temporal resampling method: ``"auto"``,
                ``"mean"``, ``"sum"``, ``"min"``, ``"max"``, ``"first"``, or
                ``"last"``.
            input_paths: Deprecated alias for ``input_data`` retained for
                backward compatibility.

        Raises:
            ValueError: If ``cmip_version`` is unsupported, both ``input_data``
                and ``input_paths`` are supplied, required input data is
                missing, or the CMIP table is not supported.

        Warns:
            MappingNotFoundWarning: Warned when a model mapping file or mapping
                entry is unavailable for the requested variable.

        Notes:
            Use :meth:`close` or a ``with`` statement to release temporary
            bundled-resource contexts when ``input_data`` is omitted and a
            resource-backed variable is used.
        """

        # Validate CMIP version
        if cmip_version not in ("CMIP6", "CMIP6Plus", "CMIP7"):
            raise ValueError(
                f"cmip_version must be 'CMIP6', 'CMIP6Plus', or 'CMIP7', got '{cmip_version}'"
            )

        self.cmip_version = cmip_version
        self._resource_stack = ExitStack()

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

        # For CMIP7, map the compound name to CMIP6 equivalent if needed
        self.compound_name = compound_name
        if cmip_version == "CMIP7":
            cmip6_equivalent = _get_cmip7_to_cmip6_mapping(compound_name)
            cmip7_compound_name = compound_name
            if cmip6_equivalent is None:
                # Allow callers to pass the CMIP6 equivalent directly while using
                # CMIP7 vocabularies/DRS, e.g. "Amon.rsdt" for a CMIP7 run.
                if compound_name.count(".") == 1:
                    cmip6_equivalent = compound_name
                    cmip7_compound_name = load_cmip6_to_cmip7_mapping().get(
                        cmip6_equivalent, compound_name
                    )
                elif compound_name.count(".") == 3:
                    raise ValueError(
                        "Could not map CMIP7 compound name "
                        f"'{compound_name}' to a CMIP6 equivalent. "
                        "This looks like a CMIP7 branded name missing its region "
                        "suffix. If you mean the global field, try "
                        f"'{compound_name}.GLB'."
                    )
                else:
                    raise ValueError(
                        "Could not map CMIP7 compound name "
                        f"'{compound_name}' to a CMIP6 equivalent. "
                        "Pass a valid CMIP7 compound name such as "
                        "'atmos.rsdt.tavg-u-hxy-u.mon.GLB' or provide the "
                        "CMIP6 equivalent in 'table.variable' form."
                    )
            # Load variable mapping to check if this is an internal calculation
            raw_mapping = load_model_mappings(cmip6_equivalent, model_id=model_id)
            _warn_if_mapping_missing(raw_mapping, cmip6_equivalent, model_id)
            self.variable_mapping = VariableMapping(
                raw_mapping, cmip6_equivalent, model_id=model_id
            )
            table, cmor_name = cmip6_equivalent.split(".")
            self.cmip6_compound_name = cmip6_equivalent
            self.cmip7_compound_name = cmip7_compound_name
        else:
            raw_mapping = load_model_mappings(compound_name, model_id=model_id)
            _warn_if_mapping_missing(raw_mapping, compound_name, model_id)
            self.variable_mapping = VariableMapping(
                raw_mapping, compound_name, model_id=model_id
            )
            table, cmor_name = compound_name.split(".")
            self.cmip6_compound_name = compound_name
            self.cmip7_compound_name = None

        # Check if this is an internal calculation that doesn't need input data
        is_internal_calc = False
        ressource_file = None
        model_vars = None
        if cmor_name in self.variable_mapping:
            calc = self.variable_mapping[cmor_name].get("calculation", {})
            is_internal_calc = calc.get("type") == "internal"
            ressource_file = self.variable_mapping[cmor_name].get("ressource_file")
            model_vars = self.variable_mapping[cmor_name].get("model_variables")

        # A self-contained calculation needs no primary input files: either it
        # is an "internal" type, or model_variables is an empty list (all data
        # is loaded inside the formula via load_ressource_data nested calls).
        is_self_contained = is_internal_calc or (
            isinstance(model_vars, list) and len(model_vars) == 0
        )

        if input_paths is None and input_data is None:
            if is_self_contained:
                pass  # no input data needed
            elif ressource_file is not None:
                resource_path = get_bundled_resource_path(ressource_file)
                resolved_path = self._resource_stack.enter_context(
                    as_file(resource_path)
                )
                input_data = str(resolved_path)
                print(
                    f"✓ No input data provided — using bundled ressource file for "
                    f"{cmor_name}: {ressource_file}"
                )
            else:
                raise ValueError(
                    "Must specify either 'input_data' or 'input_paths' for non-internal calculations."
                )

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

        # Create the appropriate Vocabulary instance based on CMIP version
        try:
            if self.cmip_version == "CMIP6":
                self.vocab = CMIP6Vocabulary(
                    compound_name=self.cmip6_compound_name,
                    experiment_id=experiment_id,
                    source_id=source_id,
                    variant_label=variant_label,
                    grid_label=grid_label,
                    activity_id=activity_id,
                    parent_info=self.parent_info,
                )
            elif self.cmip_version == "CMIP6Plus":
                # Auto-select MIP backend when the table name uses the new MIP
                # naming scheme (APmon, OPmon, LPmon, …) rather than the legacy
                # CMIP6 names (Amon, Omon, Lmon, …).
                table_id = self.cmip6_compound_name.split(".")[0]
                _mip_prefixes = (
                    "AP",
                    "AE",
                    "AC",
                    "OP",
                    "OB",
                    "LP",
                    "LI",
                    "SI",
                    "GIA",
                    "GIG",
                )
                vocab_cls = (
                    CMIP6PlusMIPVocabulary
                    if table_id.startswith(_mip_prefixes)
                    else CMIP6PlusVocabulary
                )
                self.vocab = vocab_cls(
                    compound_name=self.cmip6_compound_name,
                    experiment_id=experiment_id,
                    source_id=source_id,
                    variant_label=variant_label,
                    grid_label=grid_label,
                    activity_id=activity_id,
                    parent_info=self.parent_info,
                )
            else:  # CMIP7
                self.vocab = CMIP7Vocabulary(
                    compound_name=self.cmip7_compound_name,
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
        table, _ = self.cmip6_compound_name.split(
            "."
        )  # cmor_name now extracted internally
        _mip_atmos_prefixes = ("AP", "AE", "AC", "LP", "LI", "GIA", "GIG")
        _mip_ocean_prefixes = ("OP", "OB")
        _mip_seaice_prefixes = ("SI",)
        if table in (
            "Amon",
            "Lmon",
            "LImon",
            "Emon",
            "AERmon",
            "AERday",
            "day",
            "CFmon",
            "CFday",
            "3hr",
            "6hrPlev",
            "E1hr",
            "Eday",
            "fx",
            "Efx",
            "atmos",  # CMIP7 atmosphere table prefix
        ) or table.startswith(_mip_atmos_prefixes):
            self.cmoriser = Atmosphere_CMORiser(
                input_data=self.input_dataset
                if self.input_is_xarray
                else self.input_paths,
                output_path=str(self.output_path),
                vocab=self.vocab,
                variable_mapping=self.variable_mapping.to_dict(),
                compound_name=self.cmip6_compound_name,
                drs_root=drs_root if drs_root else None,
                validate_frequency=self.validate_frequency,
                enable_resampling=self.enable_resampling,
                resampling_method=self.resampling_method,
                enable_chunking=self.enable_chunking,
            )
        elif table in ("Oyr", "Oday", "Omon", "Ofx") or table.startswith(
            _mip_ocean_prefixes
        ):
            if self.source_id == "ACCESS-OM3" or self.model_id == "ACCESS-CM3":
                # ACCESS-OM3 uses MOM6 (C-grid) — requires dedicated CMORiser implementation
                # that handles C-grid supergrid logic, MOM6 metadata, and OM3-specific conventions
                self.cmoriser = Ocean_CMORiser_OM3(
                    input_data=self.input_dataset
                    if self.input_is_xarray
                    else self.input_paths,
                    output_path=str(self.output_path),
                    compound_name=self.cmip6_compound_name,
                    vocab=self.vocab,
                    variable_mapping=self.variable_mapping.to_dict(),
                    drs_root=drs_root if drs_root else None,
                )
            else:
                # ACCESS-OM2 uses MOM5 (B-grid) — handled by a separate CMORiser class
                # specialized for B-grid variable locations and OM2-specific metadata
                self.cmoriser = Ocean_CMORiser_OM2(
                    input_data=self.input_dataset
                    if self.input_is_xarray
                    else self.input_paths,
                    output_path=str(self.output_path),
                    compound_name=self.cmip6_compound_name,
                    vocab=self.vocab,
                    variable_mapping=self.variable_mapping.to_dict(),
                    drs_root=drs_root if drs_root else None,
                )
        elif table in ("SImon", "SIday") or table.startswith(_mip_seaice_prefixes):
            self.cmoriser = SeaIce_CMORiser(
                input_data=self.input_dataset
                if self.input_is_xarray
                else self.input_paths,
                output_path=str(self.output_path),
                compound_name=self.cmip6_compound_name,
                vocab=self.vocab,
                variable_mapping=self.variable_mapping,
                drs_root=drs_root if drs_root else None,
            )
        else:
            raise ValueError(
                f"Unsupported CMIP table '{table}' in compound_name '{compound_name}'. "
                f"Supported legacy CMIP6 tables — "
                f"atmosphere: ('Amon', 'Lmon', 'LImon', 'Emon', 'AERmon', 'AERday', 'day', 'CFmon', 'CFday', '3hr', '6hrPlev', 'E1hr', 'Eday', 'fx', 'Efx', 'atmos'), "
                f"ocean: ('Oyr', 'Oday', 'Omon', 'Ofx'), "
                f"sea-ice: ('SImon', 'SIday'). "
                f"MIP CMOR table prefixes are also supported: "
                f"atmosphere: AP*, AE*, AC*, LP*, LI*, GIA*, GIG*; "
                f"ocean: OP*, OB*; "
                f"sea-ice: SI*."
            )

    def __getitem__(self, key: str) -> xr.DataArray:
        """Return a variable from the wrapped CMORised dataset.

        Args:
            key: Dataset variable name to retrieve.

        Returns:
            The requested xarray data array.
        """
        return self.cmoriser.ds[key]

    def __getattr__(self, attr: str) -> Any:
        """Delegate unknown attributes to the wrapped xarray dataset.

        Args:
            attr: Attribute name requested by the caller.

        Returns:
            The corresponding attribute from the underlying
            :class:`xarray.Dataset`.

        Raises:
            AttributeError: If no component CMORiser has been initialised or
                the wrapped dataset does not expose ``attr``.
        """
        # Guard against infinite recursion when cmoriser itself is not yet set
        if attr == "cmoriser":
            raise AttributeError(
                "'ACCESS_ESM_CMORiser' has no 'cmoriser' — the table may not be supported"
            )
        # This is only called if the attr is not found on CMORiser itself
        return getattr(self.cmoriser.ds, attr)

    def __setitem__(self, key: str, value: Any) -> None:
        """Set a variable on the wrapped CMORised dataset.

        Args:
            key: Dataset variable name to update.
            value: Value accepted by :class:`xarray.Dataset` assignment.
        """
        self.cmoriser.ds[key] = value

    def __repr__(self) -> str:
        """Return the representation of the wrapped xarray dataset."""
        return repr(self.cmoriser.ds)

    def to_dataset(self) -> xr.Dataset:
        """Return the underlying CMORised xarray dataset.

        Returns:
            The dataset owned by the selected component CMORiser.

        Notes:
            The returned object is the live dataset, not a copy.  Mutating it
            mutates the data that :meth:`write` will serialise.
        """
        return self.cmoriser.ds

    def to_iris(self) -> Any:
        """Convert the underlying xarray dataset to a single Iris cube.

        Converts the underlying xarray Dataset to a single Iris Cube with proper
        auxiliary coordinates, masking, and bounds for curvilinear ocean grids.

        For ocean data with curvilinear grids (e.g. ACCESS-OM2, ACCESS-OM3):
        - latitude/longitude become auxiliary coordinates (not separate cubes)
        - CMIP fill values (1e20) are converted to masked arrays
        - Coordinate bounds (vertices_latitude/vertices_longitude) are preserved

        Requires ncdata and iris to be installed.

        Returns:
            A single ``iris.cube.Cube`` for the CMORised variable with proper
            auxiliary coordinates, bounds, and masking applied.

        Raises:
            ImportError: If ``ncdata`` or ``iris`` is unavailable.
            ValueError: If the converted cube list does not contain the
                CMORised variable.
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

        # Convert CMIP fill values to NaN for proper iris masking
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

    def run(self, write_output: bool = False) -> None:
        """Run CMORisation for the configured variable.

        Args:
            write_output: When ``True``, write the CMORised dataset after
                processing.
        """

        self.cmoriser.run()
        if write_output:
            self.cmoriser.write()

    def write(self) -> None:
        """Write the CMORised dataset to the configured output path."""
        self.cmoriser.write()

    def close(self) -> None:
        """Release any temporary contexts opened for bundled resource files."""
        self._resource_stack.close()

    def __enter__(self) -> ACCESS_ESM_CMORiser:
        """Return ``self`` for context-manager usage."""
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> bool:
        """Clean up resource contexts when leaving a ``with`` block."""
        self.close()
        return False
