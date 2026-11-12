"""Driver mapping construction happens after caller and native-name adjustments."""

from unittest.mock import patch

import numpy as np
import pytest
import xarray as xr

from access_moppy.driver import ACCESS_ESM_CMORiser
from access_moppy.utilities import VariableMapping


def _atmosphere_input():
    return xr.Dataset({"tas": (("time", "lat", "lon"), np.full((1, 1, 1), 280.0))})


@pytest.mark.unit
def test_official_mapping_is_constructed_once(tmp_path):
    with (
        patch("access_moppy.driver.VariableMapping", wraps=VariableMapping) as build,
        patch("access_moppy.driver.Atmosphere_CMORiser"),
    ):
        driver = ACCESS_ESM_CMORiser(
            input_data=_atmosphere_input(),
            compound_name="Amon.tas",
            experiment_id="historical",
            source_id="ACCESS-ESM1-5",
            variant_label="r1i1p1f1",
            activity_id="CMIP",
            output_path=tmp_path,
            validate_frequency=False,
        )

    build.assert_called_once()
    assert build.call_args.args[1:] == ("Amon.tas",)
    assert driver.variable_mapping["tas"]["model_variables"]


@pytest.mark.unit
def test_mapping_overrides_and_formula_kwargs_precede_construction(tmp_path):
    override = {
        "model_variables": ["tas"],
        "calculation": {"type": "formula", "formula": "tas", "kwargs": {}},
    }
    with (
        patch("access_moppy.driver.VariableMapping", wraps=VariableMapping) as build,
        patch("access_moppy.driver.Atmosphere_CMORiser"),
    ):
        driver = ACCESS_ESM_CMORiser(
            input_data=_atmosphere_input(),
            compound_name="Amon.tas",
            experiment_id="historical",
            source_id="ACCESS-ESM1-5",
            variant_label="r1i1p1f1",
            activity_id="CMIP",
            output_path=tmp_path,
            validate_frequency=False,
            mapping_overrides=override,
            calculation_kwargs={"factor": 2},
        )

    build.assert_called_once()
    entry = build.call_args.args[0]["tas"]
    assert entry is driver.variable_mapping["tas"]
    assert entry["calculation"]["kwargs"] == {"factor": {"literal": 2}}
    assert override["calculation"]["kwargs"] == {}


@pytest.mark.unit
def test_mom6_depth_alias_precedes_mapping_construction(tmp_path, project_cv):
    native = xr.Dataset(
        {"thetao": (("time", "z_l", "yh", "xh"), np.ones((1, 1, 1, 1)))}
    )
    with (
        patch("access_moppy.driver.VariableMapping", wraps=VariableMapping) as build,
        patch("access_moppy.driver.Ocean_CMORiser_OM3"),
    ):
        driver = ACCESS_ESM_CMORiser(
            input_data=native,
            compound_name="Omon.thetao",
            experiment_id="om3-real-run",
            source_id="ACCESS-OM3",
            model_id="ACCESS-OM3",
            variant_label="r1i1p1f1",
            activity_id="ACCESS-DEV",
            project_cv=project_cv,
            output_path=tmp_path,
            validate_frequency=False,
        )

    build.assert_called_once()
    dimensions = build.call_args.args[0]["thetao"]["dimensions"]
    assert dimensions["z_l"] == "depth_coord"
    assert "zl" not in dimensions
    assert driver.variable_mapping["thetao"]["dimensions"] is dimensions
