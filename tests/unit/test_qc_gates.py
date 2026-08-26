"""Unit tests for release gate recording.

A CMORised variable is a candidate for publication, not a finished product:
it still has to clear the value-range check, the WCRP compliance check, and
the repack. ACCESS-MOPPy computes all three during a run; these tests cover
keeping the results, so the batch report says what was checked instead of
leaving a reader to infer it from the task not having failed.
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import Mock, patch

import numpy as np
import pytest
import xarray as xr

from access_moppy.base import CMORiser
from access_moppy.qc import ValidationResult, compliance, validate_cmip7_output

CF = compliance.CF_SUITE
WCRP6 = compliance.WCRP_SUITES["CMIP6"]


def _cmoriser(tmp_path) -> CMORiser:
    """A minimal CMIP7 CMORiser with one variable ready to write."""
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
    return cmoriser


# ── The range gate ──────────────────────────────────────────────────────────


@pytest.mark.unit
def test_validate_cmip7_output_returns_its_result(tmp_path):
    """The enforcing entry point hands back what it computed.

    Without this the caller would have to re-read the file — and recompute
    the min/max over the whole array — just to record the outcome.
    """
    path = tmp_path / "tas.nc"
    xr.Dataset(
        {
            "tas": xr.DataArray(
                np.asarray([285.0, 286.0]), dims=["time"], attrs={"units": "K"}
            )
        },
        coords={"time": xr.DataArray([0, 1], dims=["time"])},
        attrs={
            "mip_era": "CMIP7",
            "variable_id": "tas",
            "branded_variable": "tas",
            "experiment_id": "historical",
            "source_id": "ACCESS-ESM1-6",
            "units": "K",
        },
    ).to_netcdf(path)

    result = validate_cmip7_output(path)

    assert isinstance(result, ValidationResult)
    assert result.passed is True
    assert result.warning is None
    assert result.variable_id == "tas"


@pytest.mark.unit
def test_range_gate_records_a_clean_pass(tmp_path):
    cmoriser = _cmoriser(tmp_path)

    cmoriser._record_range_gate(
        ValidationResult(
            file_path="tas.nc",
            passed=True,
            variable_id="tas",
            experiment_id="historical",
            units="K",
            observed_min=285.0,
            observed_max=286.0,
            allowed_min=150.0,
            allowed_max=350.0,
        )
    )

    assert cmoriser.qc_gates["range"] == {
        "result": "pass",
        "check_id": "cmip7_ranges",
        "observed": [285.0, 286.0],
        "allowed": [150.0, 350.0],
        "units": "K",
    }


@pytest.mark.unit
def test_range_gate_records_a_warning_with_its_evidence(tmp_path):
    """An out-of-range value warns rather than failing, so it has to be kept.

    This is the case the dashboard most needs: the task completed, but the
    values are implausible and someone should look before publication.
    """
    cmoriser = _cmoriser(tmp_path)

    cmoriser._record_range_gate(
        ValidationResult(
            file_path="tos.nc",
            passed=True,
            variable_id="tos",
            experiment_id="piControl",
            units="degC",
            observed_min=-2.1,
            observed_max=34.8,
            allowed_min=-2.0,
            allowed_max=34.0,
            warning="observed range is outside allowed range.",
        )
    )

    gate = cmoriser.qc_gates["range"]
    assert gate["result"] == "warn"
    assert gate["message"] == "observed range is outside allowed range."
    assert gate["observed"] == [-2.1, 34.8]
    assert gate["allowed"] == [-2.0, 34.0]


@pytest.mark.unit
def test_range_gate_ignores_a_patched_out_validator(tmp_path):
    """Tests that stub the validator must not have a Mock recorded as a result."""
    cmoriser = _cmoriser(tmp_path)

    cmoriser._record_range_gate(Mock())

    assert cmoriser.qc_gates == {}


@pytest.mark.unit
def test_write_records_the_range_gate_the_validator_returned(tmp_path):
    cmoriser = _cmoriser(tmp_path)
    result = ValidationResult(
        file_path="tas.nc", passed=True, variable_id="tas", experiment_id="historical"
    )

    with (
        patch.object(cmoriser, "_repack_cmip7_output"),
        patch("access_moppy.base.validate_cmip7_output", return_value=result),
    ):
        cmoriser.write()

    assert cmoriser.qc_gates["range"]["result"] == "pass"
    assert cmoriser.output_summary["gates"]["range"]["result"] == "pass"


# ── The repack gate ─────────────────────────────────────────────────────────


@pytest.mark.unit
def test_repack_gate_is_recorded_when_cmip7repack_succeeds(tmp_path):
    cmoriser = _cmoriser(tmp_path)

    with patch("access_moppy.base.subprocess.run") as run_mock:
        cmoriser._repack_cmip7_output(tmp_path / "tas.nc")

    run_mock.assert_called_once()
    assert cmoriser.qc_gates["repack"] == {"result": "pass", "tool": "cmip7repack"}


@pytest.mark.unit
def test_repack_gate_is_not_recorded_when_cmip7repack_fails(tmp_path):
    """A failed repack aborts the variable, so there is no outcome to record."""
    import subprocess

    cmoriser = _cmoriser(tmp_path)

    with patch(
        "access_moppy.base.subprocess.run",
        side_effect=subprocess.CalledProcessError(1, "cmip7repack"),
    ):
        with pytest.raises(subprocess.CalledProcessError):
            cmoriser._repack_cmip7_output(tmp_path / "tas.nc")

    assert "repack" not in cmoriser.qc_gates


@pytest.mark.unit
def test_repack_gate_is_skipped_for_non_cmip7_output(tmp_path):
    cmoriser = _cmoriser(tmp_path)
    cmoriser.vocab.mip_era = "CMIP6"

    with patch("access_moppy.base.subprocess.run") as run_mock:
        cmoriser._repack_cmip7_output(tmp_path / "tas.nc")

    run_mock.assert_not_called()
    assert cmoriser.qc_gates == {}


# ── Merging across files ────────────────────────────────────────────────────


@pytest.mark.unit
@pytest.mark.parametrize(
    ("results", "expected"),
    [
        (["pass", "pass"], "pass"),
        (["pass", "warn"], "warn"),
        (["warn", "pass"], "warn"),
        (["pass", "fail"], "fail"),
        (["fail", "warn"], "fail"),
    ],
)
def test_merge_gate_results_keeps_the_worst_outcome(results, expected):
    """One variable can write many files; a finding in any of them counts."""
    merged = CMORiser.merge_gate_results(
        {"range": {"result": result}} for result in results
    )

    assert merged["range"]["result"] == expected


@pytest.mark.unit
def test_merge_gate_results_keeps_the_evidence_of_the_worst_file():
    merged = CMORiser.merge_gate_results(
        [
            {"range": {"result": "pass", "observed": [285.0, 286.0]}},
            {"range": {"result": "warn", "observed": [-9999.0, 286.0]}},
        ]
    )

    assert merged["range"]["observed"] == [-9999.0, 286.0]


@pytest.mark.unit
def test_merge_gate_results_combines_different_gates():
    merged = CMORiser.merge_gate_results(
        [{"range": {"result": "warn"}}, {"repack": {"result": "pass"}}]
    )

    assert sorted(merged) == ["range", "repack"]


@pytest.mark.unit
def test_merge_gate_results_tolerates_empty_and_malformed_entries():
    merged = CMORiser.merge_gate_results(
        [{}, {"range": None}, {"range": {"result": "pass"}}]
    )

    assert merged == {"range": {"result": "pass"}}


@pytest.mark.unit
def test_recording_a_gate_twice_keeps_the_worse_result(tmp_path):
    cmoriser = _cmoriser(tmp_path)

    cmoriser._record_gate("range", "pass")
    cmoriser._record_gate("range", "warn", message="second file was odd")
    cmoriser._record_gate("range", "pass")

    assert cmoriser.qc_gates["range"] == {
        "result": "warn",
        "message": "second file was odd",
    }


@pytest.mark.unit
def test_output_summary_has_no_gates_key_before_anything_is_checked(tmp_path):
    cmoriser = _cmoriser(tmp_path)

    assert "gates" not in cmoriser.output_summary


# ── The WCRP compliance gate ────────────────────────────────────────────────


def _report(**sections: list[dict]) -> dict:
    return {suite: {"all_priorities": checks} for suite, checks in sections.items()}


def _check(
    name: str, weight: int, *, passed: bool, msgs: list[str] | None = None
) -> dict:
    return {
        "name": name,
        "weight": weight,
        "value": [3, 3] if passed else [1, 3],
        "msgs": msgs or [],
    }


@pytest.fixture
def fake_checker(monkeypatch):
    """Stub out the compliance-checker executable with a canned report."""

    def install(report: dict) -> None:
        monkeypatch.setattr(
            compliance.shutil, "which", lambda _: "/usr/bin/compliance-checker"
        )

        def fake_run(cmd, **kwargs):
            Path(cmd[cmd.index("--output") + 1]).write_text(json.dumps(report))

            class Result:
                returncode = 0
                stdout = ""
                stderr = ""

            return Result()

        monkeypatch.setattr(compliance.subprocess, "run", fake_run)

    return install


@pytest.fixture
def output_file(tmp_path) -> Path:
    path = tmp_path / "tos_Omon_ACCESS-ESM1-6_historical_r1i1p1f1_gn_185001-185012.nc"
    path.write_bytes(b"")
    return path


@pytest.mark.unit
def test_on_record_receives_a_passing_verdict(fake_checker, output_file, tmp_path):
    fake_checker(
        _report(**{CF: [_check("§1.2 Terminology", 3, passed=True)], WCRP6: []})
    )
    records: list[dict] = []

    compliance.enforce_compliance(output_file, tmp_path, on_record=records.append)

    assert len(records) == 1
    record = records[0]
    assert record["passed"] is True
    assert record["backfilled"] is False
    assert record["failed_checks"] is None
    assert record["error"] is None
    assert record["file"] == str(output_file)
    assert Path(record["report_path"]).exists()
    assert record["suites"] == [CF, WCRP6]


@pytest.mark.unit
def test_on_record_receives_a_failing_verdict_before_the_file_is_moved(
    fake_checker, output_file, tmp_path
):
    fake_checker(
        _report(
            **{
                CF: [_check("§2.2 Data Types", 3, passed=False, msgs=["lat is int64"])],
                WCRP6: [],
            }
        )
    )
    records: list[dict] = []

    with pytest.raises(RuntimeError):
        compliance.enforce_compliance(output_file, tmp_path, on_record=records.append)

    assert len(records) == 1
    record = records[0]
    assert record["passed"] is False
    assert "lat is int64" in record["failed_checks"]
    assert record["error"].startswith("Compliance check failed")
    assert record["renamed_to"].endswith(compliance.FAILED_SUFFIX)


@pytest.mark.unit
def test_on_record_flags_an_incomplete_checker_environment(
    fake_checker, output_file, tmp_path
):
    """A missing vocabulary database is not the data's fault, and must not
    read as a compliance pass earned on merit."""
    fake_checker(
        _report(
            **{
                CF: [],
                WCRP6: [
                    _check(
                        "[ATTR004] source_id",
                        3,
                        passed=False,
                        msgs=["Universe database is not installed or active."],
                    )
                ],
            }
        )
    )
    records: list[dict] = []

    compliance.enforce_compliance(output_file, tmp_path, on_record=records.append)

    assert records[0]["passed"] is True
    assert records[0]["environment_warning"] is True


@pytest.mark.unit
def test_enforce_compliance_still_works_without_a_callback(
    fake_checker, output_file, tmp_path
):
    fake_checker(
        _report(**{CF: [_check("§1.2 Terminology", 3, passed=True)], WCRP6: []})
    )

    report_path = compliance.enforce_compliance(output_file, tmp_path)

    assert report_path.exists()
    assert output_file.exists()
