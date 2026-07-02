"""Integration tests for CMIP7 baseline variable CMORisation.

These tests run the CMIP7 baseline selection from the example batch config
using CMIP7 compound names and validate each variable end-to-end.
"""

from __future__ import annotations

import os
from pathlib import Path
from tempfile import gettempdir

import pytest
import yaml

from access_moppy import ACCESS_ESM_CMORiser
from access_moppy.utilities import _get_cmip7_to_cmip6_mapping

from . import test_full_cmorisation as _full_cmor

BASELINE_CONFIG_PATH = (
    Path(__file__).resolve().parents[2]
    / "src/access_moppy/examples/batch_config_esm1-6_cmip7_baseline.yml"
)

CMIP7_REALM_TO_TABLE_FILE = {
    "atmos": "CMIP7_atmos.json",
    "ocean": "CMIP7_ocean.json",
    "seaIce": "CMIP7_seaIce.json",
    "aerosol": "CMIP7_aerosol.json",
    "land": "CMIP7_land.json",
    "landIce": "CMIP7_landIce.json",
}


def _load_cmip7_baseline_variables() -> list[str]:
    """Load CMIP7 variable list from the example baseline batch config."""
    with BASELINE_CONFIG_PATH.open("r", encoding="utf-8") as config_file:
        payload = yaml.safe_load(config_file)

    variables = payload.get("variables", [])
    if not isinstance(variables, list):
        raise ValueError(
            "Invalid CMIP7 baseline config: expected 'variables' to be a list"
        )

    return [str(variable).strip() for variable in variables if str(variable).strip()]


def _baseline_test_params() -> list[object]:
    """Generate pytest params for each CMIP7 baseline variable."""
    params = []

    for cmip7_compound_name in _load_cmip7_baseline_variables():
        realm = cmip7_compound_name.split(".", 1)[0]
        cmor_table_file = CMIP7_REALM_TO_TABLE_FILE.get(realm)

        if cmor_table_file is None:
            params.append(
                pytest.param(
                    cmip7_compound_name,
                    None,
                    None,
                    id=cmip7_compound_name,
                    marks=pytest.mark.skip(
                        reason=f"Unsupported CMIP7 realm '{realm}' for {cmip7_compound_name}"
                    ),
                )
            )
            continue

        cmip6_compound_name = _get_cmip7_to_cmip6_mapping(cmip7_compound_name)
        if cmip6_compound_name is None:
            params.append(
                pytest.param(
                    cmip7_compound_name,
                    None,
                    cmor_table_file,
                    id=cmip7_compound_name,
                    marks=pytest.mark.skip(
                        reason=(
                            "No CMIP7->CMIP6 mapping found for "
                            f"{cmip7_compound_name}"
                        )
                    ),
                )
            )
            continue

        params.append(
            pytest.param(
                cmip7_compound_name,
                cmip6_compound_name,
                cmor_table_file,
                id=cmip7_compound_name,
            )
        )

    return params


BASELINE_VARIABLE_PARAMS = _baseline_test_params()


class TestCMIP7BaselineCMORIntegration:
    """CMIP7 baseline integration tests using CMIP7 compound names."""

    @pytest.mark.slow
    @pytest.mark.integration
    @pytest.mark.parametrize(
        "cmip7_compound_name,cmip6_compound_name,cmor_table_file",
        BASELINE_VARIABLE_PARAMS,
    )
    def test_cmip7_baseline_variable(
        self,
        parent_experiment_config,
        compliance_validation_tool,
        cmip7_compound_name,
        cmip6_compound_name,
        cmor_table_file,
    ):
        """CMORise and validate one CMIP7 baseline variable."""
        full_helper = _full_cmor.TestFullCMORIntegration()

        if not cmip6_compound_name:
            pytest.skip(f"No CMIP6 mapping available for {cmip7_compound_name}")

        cmip6_table, cmor_name = cmip6_compound_name.split(".", 1)

        if cmip6_table == "Omon" and not full_helper._ocean_data_available():
            pytest.skip("Ocean data directory not available; set ACCESS_MOPPY_DATA_ROOT")

        input_files = full_helper._get_input_files_for_compound(
            cmip6_compound_name,
            model_id="ACCESS-ESM1-6",
        )

        if input_files is not None and (
            not input_files or not all(path.exists() for path in input_files)
        ):
            pytest.skip(
                f"Required input files not available for {cmip7_compound_name}; "
                "set ACCESS_MOPPY_DATA_ROOT"
            )

        parent_info = {
            **parent_experiment_config,
            "parent_source_id": "ACCESS-ESM1-6",
            "parent_mip_era": "CMIP7",
        }

        safe_name = cmip7_compound_name.replace(".", "_").replace("-", "_")
        output_dir = Path(gettempdir()) / f"cmor_output_cmip7_baseline_{safe_name}"
        output_dir.mkdir(parents=True, exist_ok=True)
        for netcdf_file in output_dir.rglob("*.nc"):
            netcdf_file.unlink(missing_ok=True)

        validate_with_wcrp = (
            compliance_validation_tool == "wcrp"
            and os.getenv("ACCESS_MOPPY_BASELINE_VALIDATE_WCRP") == "1"
        )
        drs_enabled = validate_with_wcrp

        with _full_cmor._get_cmor_table_path("CMIP7", cmor_table_file) as table_path:
            try:
                cmoriser = ACCESS_ESM_CMORiser(
                    input_paths=input_files,
                    compound_name=cmip7_compound_name,
                    experiment_id="historical",
                    source_id="ACCESS-ESM1-6",
                    variant_label="r1i1p1f1",
                    cmip_version="CMIP7",
                    activity_id="CMIP",
                    parent_info=parent_info,
                    output_path=output_dir,
                    drs_root=output_dir if drs_enabled else None,
                )

                cmoriser.run()
                cmoriser.write()

                output_files = sorted(output_dir.rglob("*.nc"))
                assert output_files, (
                    "No output files found for "
                    f"{cmip7_compound_name} in {output_dir}"
                )

                # Baseline integration focuses on end-to-end CMORisation for a
                # broad variable set. Strict WCRP compliance can be enabled
                # explicitly to avoid making this suite environment-dependent.
                if validate_with_wcrp:
                    full_helper._validate_output_compliance(
                        output_files[0],
                        cmor_name,
                        table_path,
                        "CMIP7",
                        compliance_validation_tool,
                    )

            except Exception as error:
                pytest.fail(
                    "Failed processing "
                    f"{cmip7_compound_name} (CMIP6 equivalent: {cmip6_compound_name}): "
                    f"{error}"
                )
