from pathlib import Path
from unittest.mock import Mock, patch

import numpy as np
import pytest
import xarray as xr

from access_moppy.base import CMORiser
from access_moppy.qc import validate_cmip7_output
from access_moppy.qc.cmip7 import (
    _load_esm16_mapping_variables,
    _load_rules,
    main as qc_main,
)


def _write_cmip7_output(
    tmp_path: Path,
    *,
    values,
    experiment_id: str,
    variable_id: str = "tas",
    source_id: str = "ACCESS-ESM1-6",
    branded_variable: str | None = None,
    units: str = "K",
    filename: str = "cmip7_output.nc",
) -> Path:
    path = tmp_path / filename
    data_var_name = branded_variable or variable_id
    ds = xr.Dataset(
        {
            data_var_name: xr.DataArray(
                np.asarray(values, dtype=float),
                dims=["time"],
                attrs={"units": units},
            )
        },
        coords={"time": xr.DataArray(np.arange(len(values)), dims=["time"])},
        attrs={
            "mip_era": "CMIP7",
            "variable_id": variable_id,
            "branded_variable": data_var_name,
            "experiment_id": experiment_id,
            "source_id": source_id,
            "units": units,
        },
    )
    ds.to_netcdf(path)
    return path


@pytest.mark.unit
def test_validate_cmip7_output_tas_passes_for_historical_range(tmp_path):
    path = _write_cmip7_output(
        tmp_path, values=[285.0, 287.5, 289.0], experiment_id="historical"
    )

    validate_cmip7_output(path)


@pytest.mark.unit
def test_validate_cmip7_output_tas_fails_for_picontrol_range(tmp_path):
    path = _write_cmip7_output(
        tmp_path, values=[324.0, 326.5], experiment_id="piControl"
    )

    with pytest.raises(ValueError, match="tas.*piControl.*outside allowed range"):
        validate_cmip7_output(path)


@pytest.mark.unit
def test_cmoriser_write_runs_cmip7_qc_after_repack(tmp_path):
    vocab = Mock()
    vocab.mip_era = "CMIP7"
    vocab.compound_name = "atmos.tas"
    vocab.generate_filename = Mock(return_value="tas.nc")
    vocab.get_required_attribute_names = Mock(return_value=[])

    ds = xr.Dataset(
        {
            "tas": xr.DataArray(
                np.asarray([285.0, 286.0], dtype=float),
                dims=["time"],
                attrs={"units": "K"},
                coords={"time": xr.DataArray([0, 1], dims=["time"])},
            )
        },
        attrs={
            "mip_era": "CMIP7",
            "variable_id": "tas",
            "branded_variable": "tas",
            "experiment_id": "historical",
            "units": "K",
        },
    )

    cmoriser = CMORiser(
        input_data=ds,
        output_path=str(tmp_path),
        vocab=vocab,
        variable_mapping={"tas": {"dimensions": {"time": "time"}}},
        compound_name="Amon.tas",
    )
    cmoriser.ds = ds

    with patch.object(cmoriser, "_repack_cmip7_output") as repack_mock, patch(
        "access_moppy.base.validate_cmip7_output"
    ) as qc_mock:
        cmoriser.write()

    repack_mock.assert_called_once()
    qc_mock.assert_called_once()
    assert qc_mock.call_args.args[0] == tmp_path / "tas.nc"


@pytest.mark.unit
def test_qc_cli_main_returns_zero_when_all_files_pass(tmp_path, capsys):
    path = _write_cmip7_output(
        tmp_path,
        values=[285.0, 286.0, 287.0],
        experiment_id="historical",
        filename="passing_only.nc",
    )

    code = qc_main([str(path)])

    captured = capsys.readouterr()
    assert code == 0
    assert "PASS" in captured.out


@pytest.mark.unit
def test_qc_cli_main_returns_one_when_any_file_fails(tmp_path, capsys):
    passing = _write_cmip7_output(
        tmp_path,
        values=[285.0, 286.0, 287.0],
        experiment_id="historical",
        filename="passing.nc",
    )
    failing = _write_cmip7_output(
        tmp_path,
        values=[324.0, 326.5],
        experiment_id="piControl",
        filename="failing.nc",
    )

    code = qc_main([str(passing), str(failing)])

    captured = capsys.readouterr()
    assert code == 1
    assert "PASS" in captured.out
    assert "FAIL" in captured.out


@pytest.mark.unit
def test_esm16_mapping_inventory_is_loaded_for_all_variables():
    mapping = _load_esm16_mapping_variables()
    # ACCESS-ESM1-6 mapping currently defines 293 variables across realms.
    assert len(mapping) >= 293


@pytest.mark.unit
def test_all_esm16_mapped_variables_have_explicit_qc_rule_entries():
    mapped = set(_load_esm16_mapping_variables())
    configured = set(_load_rules())
    assert mapped.issubset(configured)


@pytest.mark.unit
def test_validate_cmip7_output_enforces_positive_up_from_esm16_mapping(tmp_path):
    path = _write_cmip7_output(
        tmp_path,
        values=[-0.1, 0.2],
        experiment_id="historical",
        variable_id="evspsblsoi",
        units="kg m-2 s-1",
        filename="evspsblsoi_negative.nc",
    )

    with pytest.raises(ValueError, match="positive: up"):
        validate_cmip7_output(path)


@pytest.mark.unit
def test_validate_cmip7_output_applies_variable_rules_for_other_sources(tmp_path):
    path = _write_cmip7_output(
        tmp_path,
        values=[-0.1, 0.2],
        experiment_id="historical",
        variable_id="evspsblsoi",
        source_id="ACCESS-CM3",
        units="kg m-2 s-1",
        filename="other_source.nc",
    )

    with pytest.raises(ValueError, match="outside allowed range"):
        validate_cmip7_output(path)


@pytest.mark.unit
def test_validate_cmip7_output_applies_unit_envelope_for_mapped_variable(tmp_path):
    path = _write_cmip7_output(
        tmp_path,
        values=[2.5e7, 2.6e7],
        experiment_id="historical",
        variable_id="psl",
        units="Pa",
        filename="psl_out_of_range.nc",
    )

    with pytest.raises(
        ValueError,
        match="psl.*outside allowed range",
    ):
        validate_cmip7_output(path)


@pytest.mark.unit
def test_validate_cmip7_output_validates_units_against_mapping(tmp_path):
    path = _write_cmip7_output(
        tmp_path,
        values=[100000.0, 100500.0],
        experiment_id="historical",
        variable_id="psl",
        units="hPa",
        filename="psl_bad_units.nc",
    )

    with pytest.raises(ValueError, match="expected units .*ACCESS-ESM1-6 mapping"):
        validate_cmip7_output(path)
