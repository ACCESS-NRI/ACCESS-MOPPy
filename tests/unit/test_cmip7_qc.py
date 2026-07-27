from pathlib import Path
from unittest.mock import Mock, patch

import dask
import numpy as np
import pytest
import xarray as xr
from dask.array import Array

from access_moppy.base import CMORiser
from access_moppy.qc import validate_cmip7_output
from access_moppy.qc.cmip7 import (
    _compute_data_summary,
    _iter_missing_sentinels,
    _load_esm16_mapping_variables,
    _load_mapping_variable_ranges,
    _load_rules,
    _load_unit_envelopes,
    _mask_missing_sentinels_for_qc,
    _resolve_range_rule,
    _resolve_range_rule_from_mapping_definition,
    _select_experiment_rule,
    _select_output_variable,
    _validate_esm16_mapping_checks,
    validate_cmip7_output_detailed,
)
from access_moppy.qc.cmip7 import (
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
def test_iter_missing_sentinels_from_attrs():
    """Test that sentinels are collected from DataArray attrs."""
    da = xr.DataArray(
        np.array([1.0, 2.0, 3.0]),
        attrs={"missing_value": 1e20, "_FillValue": 9.96921e36},
    )

    sentinels = _iter_missing_sentinels(da)

    assert 1e20 in sentinels
    assert 9.96921e36 in sentinels
    assert len(sentinels) == 2


@pytest.mark.unit
def test_iter_missing_sentinels_from_encoding():
    """Test that sentinels are collected from DataArray encoding."""
    da = xr.DataArray(
        np.array([1.0, 2.0, 3.0]),
        attrs={},
    )
    da.encoding["_FillValue"] = 1e20

    sentinels = _iter_missing_sentinels(da)

    assert 1e20 in sentinels


@pytest.mark.unit
def test_iter_missing_sentinels_ignores_non_finite_values():
    """Test that non-finite sentinels (inf, nan) are ignored."""
    da = xr.DataArray(
        np.array([1.0, 2.0, 3.0]),
        attrs={"missing_value": np.inf, "_FillValue": np.nan},
    )

    sentinels = _iter_missing_sentinels(da)

    assert len(sentinels) == 0


@pytest.mark.unit
def test_iter_missing_sentinels_converts_non_numeric_to_float():
    """Test that non-numeric metadata values are safely ignored."""
    da = xr.DataArray(
        np.array([1.0, 2.0, 3.0]),
        attrs={"missing_value": "not_a_number"},
    )

    sentinels = _iter_missing_sentinels(da)

    assert len(sentinels) == 0


@pytest.mark.unit
def test_iter_missing_sentinels_with_array_values():
    """Test that array values in metadata are flattened and processed."""
    da = xr.DataArray(
        np.array([1.0, 2.0, 3.0]),
        attrs={"missing_value": np.array([1e20, 2e20])},
    )

    sentinels = _iter_missing_sentinels(da)

    assert 1e20 in sentinels
    assert 2e20 in sentinels


@pytest.mark.unit
def test_mask_missing_sentinels_for_qc_returns_unmodified_when_no_sentinels():
    """Early return when no sentinels found."""
    da = xr.DataArray(np.array([1.0, 2.0, 3.0]))

    result = _mask_missing_sentinels_for_qc(da)

    assert result is da  # Should return the same object


@pytest.mark.unit
def test_mask_missing_sentinels_for_qc_masks_with_tolerance():
    """Sentinels are masked using tolerance-aware matching."""
    da = xr.DataArray(
        np.array([280.0, 1.00000002e20, 285.0], dtype=np.float64),
        attrs={"_FillValue": 1e20},
    )

    result = _mask_missing_sentinels_for_qc(da)

    # Masked value should become NaN
    assert np.isnan(result.values[1])
    assert result.values[0] == 280.0
    assert result.values[2] == 285.0


@pytest.mark.unit
def test_compute_data_summary_batches_lazy_reductions():
    da = xr.DataArray(
        np.array([1.0, np.nan, 3.0]), dims=["time"]
    ).chunk({"time": 1})

    with patch("access_moppy.qc.cmip7.dask.compute", wraps=dask.compute) as compute:
        summary = _compute_data_summary(da)

    assert isinstance(da.data, Array)
    compute.assert_called_once()
    assert len(compute.call_args.args) == 3
    assert summary.non_missing == 2
    assert summary.minimum == 1.0
    assert summary.maximum == 3.0


@pytest.mark.unit
def test_validate_cmip7_output_opens_file_with_auto_chunks(tmp_path):
    path = _write_cmip7_output(
        tmp_path, values=[285.0, 287.5, 289.0], experiment_id="historical"
    )

    with patch("access_moppy.qc.cmip7.xr.open_dataset", wraps=xr.open_dataset) as open_dataset:
        validate_cmip7_output(path)

    assert open_dataset.call_args.kwargs["chunks"] == "auto"


@pytest.mark.unit
def test_validate_cmip7_output_tas_passes_for_historical_range(tmp_path):
    path = _write_cmip7_output(
        tmp_path, values=[285.0, 287.5, 289.0], experiment_id="historical"
    )

    validate_cmip7_output(path)


@pytest.mark.unit
def test_validate_cmip7_output_allows_tiny_negative_noise_at_zero_bound(tmp_path):
    path = _write_cmip7_output(
        tmp_path,
        values=[-6e-24, 0.5, 10.0],
        experiment_id="historical",
        variable_id="arag",
        units="mol m-3",
    )

    validate_cmip7_output(path)


@pytest.mark.unit
def test_validate_cmip7_output_allows_float32_boundary_noise_for_percent_range(
    tmp_path,
):
    path = _write_cmip7_output(
        tmp_path,
        values=[0.0, 100.00003],
        experiment_id="historical",
        variable_id="snc",
        units="%",
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

    with (
        patch.object(cmoriser, "_repack_cmip7_output") as repack_mock,
        patch("access_moppy.base.validate_cmip7_output") as qc_mock,
    ):
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
def test_validate_cmip7_output_allows_positive_up_mapping_signs(tmp_path):
    path = _write_cmip7_output(
        tmp_path,
        values=[-0.01, 0.02],
        experiment_id="historical",
        variable_id="evspsblsoi",
        units="kg m-2 s-1",
        filename="evspsblsoi_cross_zero.nc",
    )

    validate_cmip7_output(path)


@pytest.mark.unit
def test_validate_cmip7_output_allows_positive_down_mapping_signs(tmp_path):
    path = _write_cmip7_output(
        tmp_path,
        values=[1.0, 2.0],
        experiment_id="historical",
        variable_id="rldscs",
        units="W m-2",
        filename="rldscs_positive.nc",
    )

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


@pytest.mark.unit
def test_validate_esm16_mapping_checks_accepts_equivalent_numeric_units():
    da = xr.DataArray(
        np.array([34.0, 35.0]),
        dims=["time"],
        attrs={"units": "1E-03"},
    )

    _validate_esm16_mapping_checks(
        da,
        variable_id="sos",
        experiment_id="historical",
        mapping_entry={"units": "0.001"},
    )


@pytest.mark.unit
def test_select_output_variable_ignores_bounds_variables(tmp_path):
    """_bnds variables should not prevent identifying the main variable."""
    path = tmp_path / "tasmax_with_bnds.nc"
    ds = xr.Dataset(
        {
            "tasmax": xr.DataArray(
                np.array([310.0, 312.0]),
                dims=["time"],
                attrs={"units": "K"},
            ),
            "lat_bnds": xr.DataArray(np.zeros((2, 2)), dims=["lat", "bnds"]),
            "lon_bnds": xr.DataArray(np.zeros((2, 2)), dims=["lon", "bnds"]),
            "time_bnds": xr.DataArray(np.zeros((2, 2)), dims=["time", "bnds"]),
        },
        attrs={
            "mip_era": "CMIP7",
            "variable_id": "tasmax",
            "experiment_id": "historical",
            "source_id": "ACCESS-ESM1-6",
        },
    )
    ds.to_netcdf(path)
    # Should not raise — tasmax must be identified despite the *_bnds variables
    validate_cmip7_output(path)


@pytest.mark.unit
def test_select_output_variable_ignores_vertices_auxiliary_variables(tmp_path):
    """vertices_* variables should not hide the main CMIP variable."""
    path = tmp_path / "evs_with_vertices.nc"
    ds = xr.Dataset(
        {
            "evs": xr.DataArray(
                np.array([0.1, 0.2]),
                dims=["time"],
                attrs={"units": "kg m-2 s-1"},
            ),
            "time_bnds": xr.DataArray(np.zeros((2, 2)), dims=["time", "bnds"]),
            "vertices_latitude": xr.DataArray(
                np.zeros((2, 2, 4)),
                dims=["time", "j", "vertices"],
            ),
            "vertices_longitude": xr.DataArray(
                np.zeros((2, 2, 4)),
                dims=["time", "j", "vertices"],
            ),
        },
        attrs={
            "mip_era": "CMIP7",
            "variable_id": "evspsbl",
            "experiment_id": "historical",
            "source_id": "ACCESS-ESM1-6",
        },
    )
    ds.to_netcdf(path)

    output_variable = _select_output_variable(ds, dict(ds.attrs))
    assert output_variable == "evs"


@pytest.mark.unit
def test_validate_cmip7_output_requires_variable_id(tmp_path):
    """Validation fails if variable_id attribute is missing."""
    path = tmp_path / "no_var_id.nc"
    ds = xr.Dataset(
        {
            "tas": xr.DataArray(
                np.array([285.0, 286.0]),
                dims=["time"],
                attrs={"units": "K"},
            )
        },
        attrs={
            "mip_era": "CMIP7",
            "experiment_id": "historical",
            "source_id": "ACCESS-ESM1-6",
        },
    )
    ds.to_netcdf(path)

    with pytest.raises(ValueError, match="variable_id"):
        validate_cmip7_output(path)


@pytest.mark.unit
def test_validate_cmip7_output_requires_experiment_id(tmp_path):
    """Validation fails if experiment_id attribute is missing."""
    path = tmp_path / "no_exp_id.nc"
    ds = xr.Dataset(
        {
            "tas": xr.DataArray(
                np.array([285.0, 286.0]),
                dims=["time"],
                attrs={"units": "K"},
            )
        },
        attrs={
            "mip_era": "CMIP7",
            "variable_id": "tas",
            "source_id": "ACCESS-ESM1-6",
        },
    )
    ds.to_netcdf(path)

    with pytest.raises(ValueError, match="experiment_id"):
        validate_cmip7_output(path)


@pytest.mark.unit
def test_validate_cmip7_output_detects_all_missing_values(tmp_path):
    """Validation fails when all data is missing/NaN."""
    path = _write_cmip7_output(
        tmp_path,
        values=[np.nan, np.nan],
        experiment_id="historical",
        filename="all_nan.nc",
    )

    with pytest.raises(ValueError, match="missing"):
        validate_cmip7_output(path)


@pytest.mark.unit
def test_validate_cmip7_output_detects_infinity_values(tmp_path):
    """Validation fails when data contains infinity."""
    path = _write_cmip7_output(
        tmp_path,
        values=[285.0, np.inf],
        experiment_id="historical",
        filename="with_inf.nc",
    )

    with pytest.raises(ValueError, match="infinity"):
        validate_cmip7_output(path)


@pytest.mark.unit
def test_validate_cmip7_output_masks_rounded_fill_values_in_range_checks(tmp_path):
    """Rounded float32 fill values (e.g. 1.00000002e20) are ignored in QC."""
    path = tmp_path / "hur_with_rounded_fill.nc"
    rounded_fill = float(np.float32(1e20))

    ds = xr.Dataset(
        {
            "hur": xr.DataArray(
                np.array([50.0, rounded_fill], dtype=np.float64),
                dims=["time"],
                attrs={"units": "%", "_FillValue": 1e20, "missing_value": 1e20},
            )
        },
        attrs={
            "mip_era": "CMIP7",
            "variable_id": "hur",
            "branded_variable": "hur",
            "experiment_id": "historical",
            "source_id": "ACCESS-ESM1-6",
            "units": "%",
        },
    )
    ds.to_netcdf(path)

    validate_cmip7_output(path)

    result = validate_cmip7_output_detailed(path)
    assert result.passed is True
    assert result.observed_min == pytest.approx(50.0)
    assert result.observed_max == pytest.approx(50.0)


@pytest.mark.unit
def test_validate_cmip7_output_masks_multiple_sentinels_in_range_checks(tmp_path):
    """Multiple sentinels (missing_value and _FillValue) are both masked."""
    path = tmp_path / "tas_with_multiple_sentinels.nc"

    ds = xr.Dataset(
        {
            "tas": xr.DataArray(
                np.array([280.0, 1e20, 285.0], dtype=np.float64),
                dims=["time"],
                attrs={
                    "units": "K",
                    "missing_value": 1e20,
                },
            )
        },
        attrs={
            "mip_era": "CMIP7",
            "variable_id": "tas",
            "branded_variable": "tas",
            "experiment_id": "historical",
            "source_id": "ACCESS-ESM1-6",
            "units": "K",
        },
    )
    ds.to_netcdf(path)

    validate_cmip7_output(path)

    result = validate_cmip7_output_detailed(path)
    assert result.passed is True
    assert result.observed_min == pytest.approx(280.0)
    assert result.observed_max == pytest.approx(285.0)


@pytest.mark.unit
def test_validate_cmip7_output_masks_no_sentinels_early_return(tmp_path):
    """When no sentinels are present, masking is skipped."""
    path = tmp_path / "tas_no_sentinels.nc"

    ds = xr.Dataset(
        {
            "tas": xr.DataArray(
                np.array([280.0, 285.0, 290.0], dtype=np.float64),
                dims=["time"],
                attrs={"units": "K"},
            )
        },
        attrs={
            "mip_era": "CMIP7",
            "variable_id": "tas",
            "branded_variable": "tas",
            "experiment_id": "historical",
            "source_id": "ACCESS-ESM1-6",
            "units": "K",
        },
    )
    ds.to_netcdf(path)

    validate_cmip7_output(path)

    result = validate_cmip7_output_detailed(path)
    assert result.passed is True
    assert result.observed_min == pytest.approx(280.0)
    assert result.observed_max == pytest.approx(290.0)


@pytest.mark.unit
def test_validate_cmip7_output_masks_encoding_sentinels(tmp_path):
    """Sentinels from encoding (not just attrs) are detected and masked."""
    path = tmp_path / "tas_encoding_sentinels.nc"

    ds = xr.Dataset(
        {
            "tas": xr.DataArray(
                np.array([280.0, 1e20], dtype=np.float64),
                dims=["time"],
                attrs={"units": "K"},
            )
        },
        attrs={
            "mip_era": "CMIP7",
            "variable_id": "tas",
            "branded_variable": "tas",
            "experiment_id": "historical",
            "source_id": "ACCESS-ESM1-6",
            "units": "K",
        },
    )
    # Add sentinel to encoding instead of attrs
    ds["tas"].encoding["_FillValue"] = 1e20
    ds.to_netcdf(path)

    validate_cmip7_output(path)

    result = validate_cmip7_output_detailed(path)
    assert result.passed is True
    assert result.observed_min == pytest.approx(280.0)


@pytest.mark.unit
def test_validate_cmip7_output_masks_non_numeric_metadata(tmp_path):
    """Non-numeric metadata values in attrs/encoding are safely ignored."""
    path = tmp_path / "tas_non_numeric_metadata.nc"

    ds = xr.Dataset(
        {
            "tas": xr.DataArray(
                np.array([280.0, 285.0], dtype=np.float64),
                dims=["time"],
                attrs={
                    "units": "K",
                    "missing_value": "invalid_string",
                },
            )
        },
        attrs={
            "mip_era": "CMIP7",
            "variable_id": "tas",
            "branded_variable": "tas",
            "experiment_id": "historical",
            "source_id": "ACCESS-ESM1-6",
            "units": "K",
        },
    )
    ds.to_netcdf(path)

    validate_cmip7_output(path)

    result = validate_cmip7_output_detailed(path)
    assert result.passed is True
    assert result.observed_min == pytest.approx(280.0)
    assert result.observed_max == pytest.approx(285.0)


@pytest.mark.unit
def test_validate_cmip7_output_experiment_pattern_matching(tmp_path):
    """Experiment matching falls back to wildcard patterns - ssp370 uses ssp* rules."""
    # ssp370 should match ssp* pattern and pass with values in [180-335] range
    path = _write_cmip7_output(
        tmp_path,
        values=[330.0],
        experiment_id="ssp370",
        filename="ssp370.nc",
    )

    # ssp370 with 330K is within the ssp* range (180-335), so should pass
    validate_cmip7_output(path)

    # But 335.5K should fail
    path_fail = _write_cmip7_output(
        tmp_path,
        values=[335.5],
        experiment_id="ssp370",
        filename="ssp370_fail.nc",
    )
    with pytest.raises(ValueError, match="outside allowed range"):
        validate_cmip7_output(path_fail)


@pytest.mark.unit
def test_validate_cmip7_output_detailed_passes_without_rule_for_unconfigured_variable(
    tmp_path,
):
    path = _write_cmip7_output(
        tmp_path,
        values=[1.0, 2.0],
        experiment_id="historical",
        variable_id="customvar",
        source_id="ACCESS-CM3",
        units="1",
        filename="customvar.nc",
    )

    with patch("access_moppy.qc.cmip7._compute_data_summary") as compute_summary:
        result = validate_cmip7_output_detailed(path)

    assert result.passed is True
    compute_summary.assert_not_called()
    assert result.variable_id == "customvar"
    assert result.experiment_id == "historical"
    assert result.error is None


@pytest.mark.unit
def test_validate_cmip7_output_detailed_ignores_positive_sign_metadata(tmp_path):
    path = _write_cmip7_output(
        tmp_path,
        values=[-0.01, 0.02],
        experiment_id="historical",
        variable_id="evspsblsoi",
        units="kg m-2 s-1",
        filename="evspsblsoi_detailed.nc",
    )

    result = validate_cmip7_output_detailed(path)

    assert result.passed is True
    assert result.variable_id == "evspsblsoi"
    assert result.experiment_id == "historical"
    assert result.error is None
    assert result.observed_min == -0.01
    assert result.observed_max == 0.02


@pytest.mark.unit
def test_validate_cmip7_output_detailed_reports_units_mismatch(tmp_path):
    path = _write_cmip7_output(
        tmp_path,
        values=[285.0, 286.0],
        experiment_id="historical",
        variable_id="tas",
        source_id="ACCESS-CM3",
        units="degC",
        filename="tas_bad_units_detailed.nc",
    )

    result = validate_cmip7_output_detailed(path)

    assert result.passed is False
    assert result.variable_id == "tas"
    assert result.experiment_id == "historical"
    assert result.units == "degC"
    assert "Expected units" in result.error


@pytest.mark.unit
def test_validate_cmip7_output_detailed_returns_range_metadata_on_success(tmp_path):
    path = _write_cmip7_output(
        tmp_path,
        values=[285.0, 287.0],
        experiment_id="historical",
        filename="tas_detailed_success.nc",
    )

    result = validate_cmip7_output_detailed(path)

    assert result.passed is True
    assert result.units == "K"
    assert result.observed_min == 285.0
    assert result.observed_max == 287.0
    assert result.allowed_min is not None
    assert result.allowed_max is not None


@pytest.mark.unit
def test_validate_cmip7_output_detailed_reports_unexpected_selection_error(tmp_path):
    path = tmp_path / "ambiguous.nc"
    ds = xr.Dataset(
        {
            "tas": xr.DataArray(np.array([285.0]), dims=["time"], attrs={"units": "K"}),
            "pr": xr.DataArray(
                np.array([1.0]),
                dims=["time"],
                attrs={"units": "kg m-2 s-1"},
            ),
        },
        attrs={
            "mip_era": "CMIP7",
            "variable_id": "unknown_var",
            "experiment_id": "historical",
            "source_id": "ACCESS-CM3",
        },
    )
    ds.to_netcdf(path)

    result = validate_cmip7_output_detailed(path)

    assert result.passed is False
    assert result.error.startswith("Unexpected error: CMIP7 QC could not determine")


@pytest.mark.unit
def test_select_experiment_rule_returns_none_when_no_match():
    selected = _select_experiment_rule({"ssp*": {"min": 0, "max": 1}}, "historical")

    assert selected is None


@pytest.mark.unit
def test_resolve_range_rule_uses_default_when_no_experiment_match():
    with patch(
        "access_moppy.qc.cmip7._load_rules",
        return_value={"foo": {"default": {"units": "1", "min": 1, "max": 2}}},
    ):
        rule = _resolve_range_rule("foo", "unknown")

    assert rule is not None
    assert rule.rule_name == "default"
    assert rule.minimum == 1.0
    assert rule.maximum == 2.0


@pytest.mark.unit
def test_resolve_range_rule_returns_none_when_min_or_max_missing():
    with patch(
        "access_moppy.qc.cmip7._load_rules",
        return_value={"foo": {"default": {"units": "1", "min": 1}}},
    ):
        rule = _resolve_range_rule("foo", "historical")

    assert rule is None


@pytest.mark.unit
def test_load_unit_envelopes_and_mapping_ranges_defaults_to_empty_dict():
    with patch("access_moppy.qc.cmip7._load_qc_config", return_value={}):
        _load_unit_envelopes.cache_clear()
        _load_mapping_variable_ranges.cache_clear()
        envelopes = _load_unit_envelopes()
        ranges = _load_mapping_variable_ranges()

    assert envelopes == {}
    assert ranges == {}


@pytest.mark.unit
def test_resolve_range_rule_from_mapping_definition_returns_none_without_units():
    with (
        patch("access_moppy.qc.cmip7._load_mapping_variable_ranges", return_value={}),
        patch(
            "access_moppy.qc.cmip7._load_unit_envelopes",
            return_value={"K": {"min": 1, "max": 2}},
        ),
    ):
        rule = _resolve_range_rule_from_mapping_definition("tas", "historical", {})

    assert rule is None


@pytest.mark.unit
def test_resolve_range_rule_from_mapping_definition_uses_override_range_and_units():
    with (
        patch(
            "access_moppy.qc.cmip7._load_mapping_variable_ranges",
            return_value={"tas": {"units": "degC", "min": -80, "max": 60}},
        ),
        patch("access_moppy.qc.cmip7._load_unit_envelopes", return_value={}),
    ):
        rule = _resolve_range_rule_from_mapping_definition(
            "tas",
            "historical",
            {"units": "K"},
        )

    assert rule is not None
    assert rule.units == "degC"
    assert rule.minimum == -80.0
    assert rule.maximum == 60.0


@pytest.mark.unit
def test_resolve_range_rule_from_mapping_definition_returns_none_when_envelope_missing():
    with (
        patch("access_moppy.qc.cmip7._load_mapping_variable_ranges", return_value={}),
        patch("access_moppy.qc.cmip7._load_unit_envelopes", return_value={}),
    ):
        rule = _resolve_range_rule_from_mapping_definition(
            "tas",
            "historical",
            {"units": "K"},
        )

    assert rule is None


@pytest.mark.unit
def test_resolve_range_rule_from_mapping_definition_uses_unit_envelope_when_no_override_range():
    with (
        patch(
            "access_moppy.qc.cmip7._load_mapping_variable_ranges",
            return_value={"tas": {"units": "K"}},
        ),
        patch(
            "access_moppy.qc.cmip7._load_unit_envelopes",
            return_value={"K": {"min": 200.0, "max": 340.0}},
        ),
    ):
        rule = _resolve_range_rule_from_mapping_definition(
            "tas",
            "historical",
            {"units": "K"},
        )

    assert rule is not None
    assert rule.minimum == 200.0
    assert rule.maximum == 340.0


@pytest.mark.unit
def test_select_output_variable_returns_single_non_bounds_variable():
    ds = xr.Dataset(
        {
            "time_bnds": xr.DataArray(np.zeros((2, 2)), dims=["time", "bnds"]),
            "foo": xr.DataArray(np.array([1.0, 2.0]), dims=["time"]),
        }
    )

    assert _select_output_variable(ds, attrs={}) == "foo"


@pytest.mark.unit
def test_validate_cmip7_output_returns_early_when_rule_unavailable_for_mapping(
    tmp_path,
):
    path = _write_cmip7_output(
        tmp_path,
        values=[1.0, 2.0],
        experiment_id="historical",
        variable_id="unknown_mapped_var",
        units="1",
        filename="no_rule_mapping.nc",
    )

    with (
        patch("access_moppy.qc.cmip7._resolve_range_rule", return_value=None),
        patch(
            "access_moppy.qc.cmip7._load_esm16_mapping_variables",
            return_value={"unknown_mapped_var": {"units": "1"}},
        ),
        patch(
            "access_moppy.qc.cmip7._resolve_range_rule_from_mapping_definition",
            return_value=None,
        ),
    ):
        validate_cmip7_output(path)


@pytest.mark.unit
def test_validate_cmip7_output_raises_units_mismatch_for_explicit_rule(tmp_path):
    path = _write_cmip7_output(
        tmp_path,
        values=[280.0, 281.0],
        experiment_id="historical",
        variable_id="tas",
        source_id="ACCESS-CM3",
        units="degC",
        filename="units_mismatch_simple_rule.nc",
    )

    with pytest.raises(ValueError, match="expected units 'K', found 'degC'"):
        validate_cmip7_output(path)


@pytest.mark.unit
def test_validate_cmip7_output_returns_for_all_nan_after_masking(tmp_path):
    path = _write_cmip7_output(
        tmp_path,
        values=[np.nan, np.nan],
        experiment_id="historical",
        variable_id="tas",
        source_id="ACCESS-CM3",
        units="K",
        filename="all_nan_non_mapping.nc",
    )

    # Non-ACCESS-ESM1-6 source skips mapping checks, so NaN extrema should
    # trigger the early-return branch.
    validate_cmip7_output(path)


@pytest.mark.unit
def test_validate_cmip7_output_detailed_requires_variable_id(tmp_path):
    path = tmp_path / "no_var_id_detailed.nc"
    ds = xr.Dataset(
        {
            "tas": xr.DataArray(
                np.array([285.0, 286.0]),
                dims=["time"],
                attrs={"units": "K"},
            )
        },
        attrs={
            "mip_era": "CMIP7",
            "experiment_id": "historical",
            "source_id": "ACCESS-ESM1-6",
        },
    )
    ds.to_netcdf(path)

    result = validate_cmip7_output_detailed(path)

    assert result.passed is False
    assert "variable_id" in (result.error or "")


@pytest.mark.unit
def test_validate_cmip7_output_detailed_requires_experiment_id(tmp_path):
    path = tmp_path / "no_exp_id_detailed.nc"
    ds = xr.Dataset(
        {
            "tas": xr.DataArray(
                np.array([285.0, 286.0]),
                dims=["time"],
                attrs={"units": "K"},
            )
        },
        attrs={
            "mip_era": "CMIP7",
            "variable_id": "tas",
            "source_id": "ACCESS-ESM1-6",
        },
    )
    ds.to_netcdf(path)

    result = validate_cmip7_output_detailed(path)

    assert result.passed is False
    assert "experiment_id" in (result.error or "")


@pytest.mark.unit
def test_validate_cmip7_output_detailed_captures_mapping_check_value_error(tmp_path):
    path = _write_cmip7_output(
        tmp_path,
        values=[285.0, 286.0],
        experiment_id="historical",
        variable_id="tas",
        source_id="ACCESS-ESM1-6",
        units="K",
        filename="mapping_error_detailed.nc",
    )

    with patch(
        "access_moppy.qc.cmip7._validate_esm16_mapping_checks",
        side_effect=ValueError("mapping guard failed"),
    ):
        result = validate_cmip7_output_detailed(path)

    assert result.passed is False
    assert result.error == "mapping guard failed"


@pytest.mark.unit
def test_validate_cmip7_output_detailed_uses_mapping_fallback_rule(tmp_path):
    path = _write_cmip7_output(
        tmp_path,
        values=[281.0, 282.0],
        experiment_id="historical",
        variable_id="tas",
        source_id="ACCESS-ESM1-6",
        units="K",
        filename="mapping_fallback_rule_detailed.nc",
    )

    fallback_rule = Mock(units="K", minimum=270.0, maximum=300.0, rule_name="fallback")

    with (
        patch("access_moppy.qc.cmip7._resolve_range_rule", return_value=None),
        patch(
            "access_moppy.qc.cmip7._load_esm16_mapping_variables",
            return_value={"tas": {"units": "K"}},
        ),
        patch("access_moppy.qc.cmip7._validate_esm16_mapping_checks"),
        patch(
            "access_moppy.qc.cmip7._resolve_range_rule_from_mapping_definition",
            return_value=fallback_rule,
        ) as fallback_mock,
    ):
        result = validate_cmip7_output_detailed(path)

    assert result.passed is True
    assert result.allowed_min == 270.0
    assert result.allowed_max == 300.0
    fallback_mock.assert_called_once()


@pytest.mark.unit
def test_validate_cmip7_output_detailed_returns_pass_for_all_nan_non_mapping(tmp_path):
    path = _write_cmip7_output(
        tmp_path,
        values=[np.nan, np.nan],
        experiment_id="historical",
        variable_id="tas",
        source_id="ACCESS-CM3",
        units="K",
        filename="all_nan_non_mapping_detailed.nc",
    )

    result = validate_cmip7_output_detailed(path)

    assert result.passed is True
    assert result.units == "K"


@pytest.mark.unit
def test_validate_cmip7_output_detailed_reports_range_failure(tmp_path):
    path = _write_cmip7_output(
        tmp_path,
        values=[100.0, 101.0],
        experiment_id="historical",
        variable_id="tas",
        source_id="ACCESS-CM3",
        units="K",
        filename="range_fail_detailed.nc",
    )

    result = validate_cmip7_output_detailed(path)

    assert result.passed is False
    assert result.observed_min == 100.0
    assert result.observed_max == 101.0
    assert result.allowed_min is not None
    assert result.allowed_max is not None
