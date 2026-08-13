"""Unit tests for access_moppy.qc.compliance."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from access_moppy.qc import compliance

CF = compliance.CF_SUITE
WCRP6 = compliance.WCRP_SUITES["CMIP6"]

UNIVERSE_MSG = "Universe database is not installed or active."


def _report(**sections: list[dict]) -> dict:
    """Build a compliance-checker JSON report from suite -> checks."""
    return {suite: {"all_priorities": checks} for suite, checks in sections.items()}


def _check(
    name: str, weight: int, *, passed: bool, msgs: list[str] | None = None
) -> dict:
    value = [3, 3] if passed else [1, 3]
    return {"name": name, "weight": weight, "value": value, "msgs": msgs or []}


@pytest.fixture
def fake_checker(monkeypatch):
    """Stub out the compliance-checker executable with a canned report."""

    def install(report: dict | None) -> list[list[str]]:
        commands: list[list[str]] = []
        monkeypatch.setattr(
            compliance.shutil, "which", lambda _: "/usr/bin/compliance-checker"
        )

        def fake_run(cmd, **kwargs):
            commands.append(cmd)
            if report is not None:
                Path(cmd[cmd.index("--output") + 1]).write_text(json.dumps(report))

            class Result:
                returncode = 0
                stdout = ""
                stderr = "checker exploded"

            return Result()

        monkeypatch.setattr(compliance.subprocess, "run", fake_run)
        return commands

    return install


@pytest.fixture
def output_file(tmp_path) -> Path:
    path = tmp_path / "tos_Omon_ACCESS-ESM1-5_historical_r1i1p1f1_gn_010101-011012.nc"
    path.write_bytes(b"not really netcdf")
    return path


@pytest.mark.unit
@pytest.mark.parametrize(
    ("cmip_version", "wcrp_suite"),
    [
        ("CMIP6", "wcrp_cmip6:1.0"),
        ("CMIP6Plus", "wcrp_cmip6plus:1.0"),
        ("CMIP7", "wcrp_cmip7:1.0"),
    ],
)
def test_resolve_suites_pairs_cf_with_the_matching_wcrp_suite(cmip_version, wcrp_suite):
    assert compliance.resolve_suites(cmip_version) == [CF, wcrp_suite]


@pytest.mark.unit
def test_resolve_suites_rejects_an_unknown_cmip_version():
    with pytest.raises(ValueError, match="cmip_version must be one of"):
        compliance.resolve_suites("CMIP5")


@pytest.mark.unit
def test_check_output_file_runs_both_suites_in_one_invocation(
    fake_checker, output_file, tmp_path
):
    commands = fake_checker(_report(**{CF: [], WCRP6: []}))

    compliance.check_output_file(output_file, tmp_path, cmip_version="CMIP6")

    assert len(commands) == 1
    assert commands[0].count("--test") == 2
    assert CF in commands[0]
    assert WCRP6 in commands[0]


@pytest.mark.unit
def test_check_output_file_aggregates_failures_from_both_suites(
    fake_checker, output_file, tmp_path
):
    fake_checker(
        _report(
            **{
                CF: [
                    _check("§2.2 Data Types", 3, passed=False),
                    _check("§7.1 Cell Boundaries", 2, passed=False),
                    _check("§1.2 Terminology", 3, passed=True),
                ],
                WCRP6: [_check("[ATTR004] experiment_id", 3, passed=False)],
            }
        )
    )

    failures, environment, report_path, _ = compliance.check_output_file(
        output_file, tmp_path, cmip_version="CMIP6", min_weight=3
    )

    assert [(check["suite"], check["name"]) for check in failures] == [
        (CF, "§2.2 Data Types"),
        (WCRP6, "[ATTR004] experiment_id"),
    ]
    assert environment == []
    assert report_path == tmp_path / f"compliance_{output_file.stem}.json"
    assert report_path.exists()


@pytest.mark.unit
def test_check_output_file_min_weight_one_catches_every_finding(
    fake_checker, output_file, tmp_path
):
    fake_checker(
        _report(
            **{
                CF: [
                    _check("§2.2 Data Types", 3, passed=False),
                    _check("§7.1 Cell Boundaries", 2, passed=False),
                ],
                WCRP6: [],
            }
        )
    )

    failures, _, _, _ = compliance.check_output_file(
        output_file, tmp_path, cmip_version="CMIP6", min_weight=1
    )

    assert len(failures) == 2


@pytest.mark.unit
def test_check_output_file_separates_environment_failures(
    fake_checker, output_file, tmp_path
):
    fake_checker(
        _report(
            **{
                CF: [],
                WCRP6: [
                    _check("[ATTR004] source_id", 3, passed=False, msgs=[UNIVERSE_MSG]),
                    _check("[FILE001] DRS Filename", 3, passed=False),
                ],
            }
        )
    )

    failures, environment, _, _ = compliance.check_output_file(
        output_file, tmp_path, cmip_version="CMIP6"
    )

    assert [check["name"] for check in failures] == ["[FILE001] DRS Filename"]
    assert [check["name"] for check in environment] == ["[ATTR004] source_id"]


@pytest.mark.unit
def test_check_output_file_records_provenance_in_the_report(
    fake_checker, output_file, tmp_path, monkeypatch
):
    fake_checker(_report(**{CF: [], WCRP6: []}))
    monkeypatch.setattr(compliance, "resolve_cv_version", lambda _: "1.2.16")

    _, _, report_path, cv_version = compliance.check_output_file(
        output_file, tmp_path, cmip_version="CMIP6", min_weight=3
    )

    assert cv_version == "1.2.16"
    assert json.loads(report_path.read_text())["access_moppy"] == {
        "suites": [CF, WCRP6],
        "cmip_version": "CMIP6",
        "cv_version": "1.2.16",
        "min_weight": 3,
    }


@pytest.mark.unit
def test_resolve_cv_version_returns_none_when_esgvoc_cannot_answer():
    assert compliance.resolve_cv_version("CMIP5") is None


@pytest.mark.unit
def test_enforce_compliance_prints_the_vocabulary_version(
    fake_checker, output_file, tmp_path, monkeypatch, capsys
):
    fake_checker(_report(**{CF: [], WCRP6: []}))
    monkeypatch.setattr(compliance, "resolve_cv_version", lambda _: "1.2.16")

    compliance.enforce_compliance(output_file, tmp_path, cmip_version="CMIP6")

    assert f"({CF} + {WCRP6}, CMIP6 CV 1.2.16)" in capsys.readouterr().out


@pytest.mark.unit
def test_check_output_file_rejects_non_integer_min_weight(output_file, tmp_path):
    with pytest.raises(ValueError, match="must be an integer"):
        compliance.check_output_file(output_file, tmp_path, min_weight="high")


@pytest.mark.unit
def test_check_output_file_raises_when_checker_writes_no_report(
    fake_checker, output_file, tmp_path
):
    fake_checker(None)

    with pytest.raises(RuntimeError, match="produced no report"):
        compliance.check_output_file(output_file, tmp_path)


@pytest.mark.unit
def test_check_output_file_raises_when_a_suite_section_is_missing(
    fake_checker, output_file, tmp_path
):
    fake_checker(_report(**{CF: []}))

    with pytest.raises(RuntimeError, match=f"has no '{WCRP6}' section"):
        compliance.check_output_file(output_file, tmp_path, cmip_version="CMIP6")


@pytest.mark.unit
def test_check_output_file_raises_when_checker_is_unavailable(
    monkeypatch, output_file, tmp_path
):
    monkeypatch.setattr(compliance.shutil, "which", lambda _: None)

    with pytest.raises(RuntimeError, match="not on PATH"):
        compliance.check_output_file(output_file, tmp_path)


@pytest.mark.unit
def test_enforce_compliance_keeps_a_passing_file_in_place(
    fake_checker, output_file, tmp_path
):
    fake_checker(
        _report(**{CF: [_check("§1.2 Terminology", 3, passed=True)], WCRP6: []})
    )

    report_path = compliance.enforce_compliance(output_file, tmp_path)

    assert output_file.exists()
    assert report_path.exists()


@pytest.mark.unit
def test_enforce_compliance_does_not_abort_on_environment_failures(
    fake_checker, output_file, tmp_path, capsys
):
    fake_checker(
        _report(
            **{
                CF: [],
                WCRP6: [
                    _check("[ATTR004] source_id", 3, passed=False, msgs=[UNIVERSE_MSG])
                ],
            }
        )
    )

    compliance.enforce_compliance(output_file, tmp_path)

    assert output_file.exists()
    assert "checker environment is incomplete" in capsys.readouterr().out


@pytest.mark.unit
def test_enforce_compliance_renames_and_raises_for_a_failing_file(
    fake_checker, output_file, tmp_path
):
    fake_checker(
        _report(
            **{
                CF: [
                    _check(
                        "§2.2 Data Types",
                        3,
                        passed=False,
                        msgs=["lat is int64, should be int32"],
                    )
                ],
                WCRP6: [],
            }
        )
    )

    with pytest.raises(RuntimeError) as excinfo:
        compliance.enforce_compliance(output_file, tmp_path)

    failed_path = output_file.with_name(output_file.name + compliance.FAILED_SUFFIX)
    assert not output_file.exists()
    assert failed_path.exists()

    message = str(excinfo.value)
    assert str(failed_path) in message
    assert f"[{CF} | weight 3] §2.2 Data Types" in message
    assert "lat is int64, should be int32" in message

    # The renamed file must not look like published DRS output, so a resumed
    # run cannot mistake it for a completed split.
    assert not failed_path.name.endswith(".nc")
