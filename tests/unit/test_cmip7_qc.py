from pathlib import Path
from unittest.mock import Mock, patch

import numpy as np
import pytest
import xarray as xr

from access_moppy.base import CMORiser
from access_moppy.qc import validate_cmip7_output
from access_moppy.qc.cmip7 import main as qc_main


def _write_cmip7_output(
    tmp_path: Path,
    *,
    values,
    experiment_id: str,
    filename: str = "cmip7_output.nc",
) -> Path:
    path = tmp_path / filename
    ds = xr.Dataset(
        {
            "tas": xr.DataArray(
                np.asarray(values, dtype=float),
                dims=["time"],
                attrs={"units": "K"},
            )
        },
        coords={"time": xr.DataArray(np.arange(len(values)), dims=["time"])},
        attrs={
            "mip_era": "CMIP7",
            "variable_id": "tas",
            "branded_variable": "tas",
            "experiment_id": experiment_id,
            "units": "K",
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
