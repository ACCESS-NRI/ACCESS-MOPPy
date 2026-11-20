"""Public driver rejects unsupported local-CV and mapping combinations early."""

import pytest
import xarray as xr

from access_moppy.driver import ACCESS_ESM_CMORiser


def _arguments(tmp_path, project_cv):
    return {
        "input_data": xr.Dataset(),
        "compound_name": "Omon.tos",
        "experiment_id": "om3-real-run",
        "source_id": "ACCESS-OM3",
        "variant_label": "r1i1p1f1",
        "activity_id": "ACCESS-DEV",
        "model_id": "ACCESS-OM3",
        "project_cv": project_cv,
        "output_path": tmp_path,
        "enable_resampling": False,
    }


@pytest.mark.unit
@pytest.mark.parametrize("cmip_version", ["CMIP6Plus", "CMIP7"])
def test_driver_rejects_local_cv_for_unsupported_cmor_version(
    tmp_path, project_cv, cmip_version
):
    with pytest.raises(ValueError, match="project_cv currently supports CMIP6 only"):
        ACCESS_ESM_CMORiser(
            **_arguments(tmp_path, project_cv), cmip_version=cmip_version
        )


@pytest.mark.unit
def test_driver_rejects_invalid_grid_component(tmp_path, project_cv):
    with pytest.raises(ValueError, match="grid_component must be ocean or sea_ice"):
        ACCESS_ESM_CMORiser(
            **_arguments(tmp_path, project_cv), grid_component="atmosphere"
        )


@pytest.mark.unit
def test_driver_rejects_non_dictionary_mapping_overrides(tmp_path, project_cv):
    with pytest.raises(TypeError, match="mapping_overrides must be a dictionary"):
        ACCESS_ESM_CMORiser(
            **_arguments(tmp_path, project_cv), mapping_overrides=["invalid"]
        )


@pytest.mark.unit
def test_driver_rejects_calculation_parameters_for_direct_mapping(tmp_path, project_cv):
    with pytest.raises(ValueError, match="calculation_kwargs requires a formula"):
        ACCESS_ESM_CMORiser(
            **_arguments(tmp_path, project_cv),
            calculation_kwargs={"run_id": "om3-real-run"},
        )
