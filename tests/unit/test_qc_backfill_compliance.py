"""Unit tests for access_moppy.qc.backfill_compliance."""

from __future__ import annotations

import json
from pathlib import Path

import cftime
import netCDF4 as nc
import pytest

from access_moppy.qc import backfill_compliance as backfill
from access_moppy.qc import compliance

CF = compliance.CF_SUITE
WCRP6 = compliance.WCRP_SUITES["CMIP6"]


def _write_output(
    path: Path,
    *,
    variable_id: str = "tos",
    experiment_id: str = "historical",
    source_id: str = "ACCESS-ESM1-5",
    variant_label: str = "r1i1p1f1",
    grid_label: str = "gn",
    table_id: str | None = "Omon",
    start_year: int | None = 101,
    end_year: int | None = 110,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with nc.Dataset(path, "w") as dataset:
        attrs = {
            "experiment_id": experiment_id,
            "source_id": source_id,
            "variant_label": variant_label,
            "variable_id": variable_id,
            "grid_label": grid_label,
        }
        if table_id is not None:
            attrs["table_id"] = table_id
        dataset.setncatts(attrs)
        if start_year is not None:
            dataset.createDimension("time", 2)
            time_var = dataset.createVariable("time", "f8", ("time",))
            time_var.units = "days since 0001-01-01"
            time_var.calendar = "proleptic_gregorian"
            time_var[:] = nc.date2num(
                [
                    cftime.DatetimeProlepticGregorian(start_year, 1, 1),
                    cftime.DatetimeProlepticGregorian(end_year, 12, 31),
                ],
                units=time_var.units,
                calendar=time_var.calendar,
            )


@pytest.fixture
def fake_checker(monkeypatch):
    """Stub out the compliance-checker executable with a canned report."""

    def install(report: dict | None = None) -> list[list[str]]:
        commands: list[list[str]] = []
        monkeypatch.setattr(
            compliance.shutil, "which", lambda _: "/usr/bin/compliance-checker"
        )

        def fake_run(cmd, **kwargs):
            commands.append(cmd)
            payload = report if report is not None else {CF: {"all_priorities": []}}
            Path(cmd[cmd.index("--output") + 1]).write_text(json.dumps(payload))

            class Result:
                returncode = 0
                stdout = ""
                stderr = ""

            return Result()

        monkeypatch.setattr(compliance.subprocess, "run", fake_run)
        return commands

    return install


@pytest.mark.unit
class TestFindFirstFiles:
    def test_picks_the_earliest_chunk_per_variable(self, tmp_path):
        version = tmp_path / "v20260101"
        _write_output(version / "tos_010101-011012.nc", start_year=101, end_year=110)
        _write_output(version / "tos_011101-012012.nc", start_year=111, end_year=120)

        first_files = backfill.find_first_files(tmp_path)

        assert len(first_files) == 1
        entry = first_files[0]
        assert entry.path.name == "tos_010101-011012.nc"
        assert entry.chunk_count == 2

    def test_separates_by_directory_even_with_matching_identity(self, tmp_path):
        _write_output(tmp_path / "v1" / "tos_010101-011012.nc")
        _write_output(tmp_path / "v2" / "tos_010101-011012.nc")

        first_files = backfill.find_first_files(tmp_path)

        assert len(first_files) == 2

    def test_separates_by_variable_id(self, tmp_path):
        _write_output(tmp_path / "tos_010101-011012.nc", variable_id="tos")
        _write_output(tmp_path / "so_010101-011012.nc", variable_id="so")

        first_files = backfill.find_first_files(tmp_path)

        identities = {first_file.identity for first_file in first_files}
        assert len(first_files) == 2
        assert any("tos" in identity for identity in identities)
        assert any("so" in identity for identity in identities)

    def test_handles_a_fixed_field_variable_with_no_time_axis(self, tmp_path):
        _write_output(
            tmp_path / "areacello_gn.nc",
            variable_id="areacello",
            start_year=None,
            end_year=None,
        )

        first_files = backfill.find_first_files(tmp_path)

        assert len(first_files) == 1
        assert first_files[0].chunk_count == 1

    def test_skips_unreadable_or_non_cmor_files(self, tmp_path):
        (tmp_path / "not_netcdf.nc").write_bytes(b"garbage")
        _write_output(tmp_path / "tos_010101-011012.nc")

        first_files = backfill.find_first_files(tmp_path)

        assert len(first_files) == 1


@pytest.mark.unit
class TestRunComplianceBackfill:
    def test_checks_only_the_first_file_of_each_variable(self, tmp_path, fake_checker):
        commands = fake_checker()
        output_folder = tmp_path / "output"
        _write_output(
            output_folder / "tos_010101-011012.nc", start_year=101, end_year=110
        )
        _write_output(
            output_folder / "tos_011101-012012.nc", start_year=111, end_year=120
        )
        report_dir = tmp_path / "reports"

        entries = backfill.run_compliance_backfill(
            output_folder, report_dir, cmip_version="CMIP6", suites=[CF]
        )

        assert len(commands) == 1
        assert len(entries) == 1
        assert entries[0].passed
        assert entries[0].report_path.exists()

    def test_reuses_a_matching_report_instead_of_re_running_the_checker(
        self, tmp_path, fake_checker
    ):
        commands = fake_checker()
        output_folder = tmp_path / "output"
        _write_output(output_folder / "tos_010101-011012.nc")
        report_dir = tmp_path / "reports"

        backfill.run_compliance_backfill(
            output_folder, report_dir, cmip_version="CMIP6", suites=[CF]
        )
        backfill.run_compliance_backfill(
            output_folder, report_dir, cmip_version="CMIP6", suites=[CF]
        )

        assert len(commands) == 1

    def test_no_skip_existing_forces_a_re_check(self, tmp_path, fake_checker):
        commands = fake_checker()
        output_folder = tmp_path / "output"
        _write_output(output_folder / "tos_010101-011012.nc")
        report_dir = tmp_path / "reports"

        backfill.run_compliance_backfill(
            output_folder,
            report_dir,
            cmip_version="CMIP6",
            suites=[CF],
            skip_existing=False,
        )
        backfill.run_compliance_backfill(
            output_folder,
            report_dir,
            cmip_version="CMIP6",
            suites=[CF],
            skip_existing=False,
        )

        assert len(commands) == 2

    def test_records_a_failure_without_touching_the_file(self, tmp_path, fake_checker):
        fake_checker(
            {
                CF: {
                    "all_priorities": [
                        {
                            "name": "bad_units",
                            "weight": 3,
                            "value": [1, 3],
                            "msgs": ["units are wrong"],
                        }
                    ]
                }
            }
        )
        output_folder = tmp_path / "output"
        _write_output(output_folder / "tos_010101-011012.nc")
        report_dir = tmp_path / "reports"

        entries = backfill.run_compliance_backfill(
            output_folder, report_dir, cmip_version="CMIP6", suites=[CF]
        )

        assert len(entries) == 1
        assert not entries[0].passed
        assert entries[0].error is None
        # Unlike enforce_compliance, the published file is left in place.
        assert (output_folder / "tos_010101-011012.nc").exists()
        assert not (output_folder / "tos_010101-011012.nc.compliance_failed").exists()

    def test_records_an_error_when_the_checker_cannot_run(self, tmp_path, monkeypatch):
        monkeypatch.setattr(compliance.shutil, "which", lambda _: None)
        output_folder = tmp_path / "output"
        _write_output(output_folder / "tos_010101-011012.nc")
        report_dir = tmp_path / "reports"

        entries = backfill.run_compliance_backfill(
            output_folder, report_dir, cmip_version="CMIP6", suites=[CF]
        )

        assert len(entries) == 1
        assert entries[0].error is not None
        assert not entries[0].passed


@pytest.mark.unit
class TestSummarizeAndUpdateBatchReport:
    def test_summarize_counts_pass_fail_and_errors(self):
        entries = [
            backfill.ComplianceBackfillEntry(
                identity="a",
                file=Path("a.nc"),
                passed=True,
                failed_checks=[],
                environment_checks=[],
                report_path=Path("a.json"),
                cv_version=None,
            ),
            backfill.ComplianceBackfillEntry(
                identity="b",
                file=Path("b.nc"),
                passed=False,
                failed_checks=[{"name": "x", "weight": 3, "msgs": []}],
                environment_checks=[],
                report_path=Path("b.json"),
                cv_version=None,
            ),
            backfill.ComplianceBackfillEntry(
                identity="c",
                file=Path("c.nc"),
                passed=False,
                failed_checks=[],
                environment_checks=[],
                report_path=Path("c.json"),
                cv_version=None,
                error="checker unavailable",
            ),
        ]

        summary = backfill.summarize(entries, "CMIP6", 3, [CF])

        assert summary["total"] == 3
        assert summary["passed"] == 1
        assert summary["failed"] == 1
        assert len(summary["errors"]) == 1
        assert len(summary["failures"]) == 1
        assert summary["backfilled"] is True

    def test_update_batch_report_merges_the_compliance_key_in_place(self, tmp_path):
        report_path = tmp_path / "moppy_batch_report.json"
        report_path.write_text(
            json.dumps({"schema_version": "v2", "status": "completed"})
        )
        summary = {"total": 1, "passed": 1, "failed": 0}

        out_path = backfill.update_batch_report(report_path, summary)

        assert out_path == report_path
        data = json.loads(report_path.read_text())
        assert data["compliance"] == summary
        assert data["status"] == "completed"

    def test_update_batch_report_can_write_elsewhere(self, tmp_path):
        report_path = tmp_path / "moppy_batch_report.json"
        report_path.write_text(json.dumps({"status": "completed"}))
        out_path = tmp_path / "merged.json"

        result = backfill.update_batch_report(
            report_path, {"total": 0}, output_path=out_path
        )

        assert result == out_path
        assert json.loads(out_path.read_text())["compliance"] == {"total": 0}
        # Original report is untouched.
        assert "compliance" not in json.loads(report_path.read_text())


@pytest.mark.unit
class TestCli:
    def test_main_reports_failures_and_updates_the_batch_report(
        self, tmp_path, fake_checker
    ):
        fake_checker(
            {
                CF: {
                    "all_priorities": [
                        {
                            "name": "bad_units",
                            "weight": 3,
                            "value": [1, 3],
                            "msgs": ["units are wrong"],
                        }
                    ]
                }
            }
        )
        output_folder = tmp_path / "output"
        _write_output(output_folder / "tos_010101-011012.nc")
        batch_report = tmp_path / "moppy_batch_report.json"
        batch_report.write_text(json.dumps({"status": "completed"}))

        exit_code = backfill.main(
            [
                "--output-folder",
                str(output_folder),
                "--suite",
                CF,
                "--batch-report",
                str(batch_report),
            ]
        )

        assert exit_code == 1
        data = json.loads(batch_report.read_text())
        assert data["compliance"]["failed"] == 1

    def test_main_exits_zero_when_everything_passes(self, tmp_path, fake_checker):
        fake_checker()
        output_folder = tmp_path / "output"
        _write_output(output_folder / "tos_010101-011012.nc")

        exit_code = backfill.main(
            ["--output-folder", str(output_folder), "--suite", CF]
        )

        assert exit_code == 0


@pytest.mark.unit
class TestVariableTargeting:
    def test_find_first_files_filters_by_variable_id(self, tmp_path):
        _write_output(tmp_path / "tos_010101-011012.nc", variable_id="tos")
        _write_output(tmp_path / "so_010101-011012.nc", variable_id="so")

        first_files = backfill.find_first_files(tmp_path, variable_ids={"tos"})

        assert len(first_files) == 1
        assert first_files[0].attrs["variable_id"] == "tos"

    def test_find_first_files_filters_by_experiment_id(self, tmp_path):
        _write_output(
            tmp_path / "hist" / "tos_010101-011012.nc", experiment_id="historical"
        )
        _write_output(tmp_path / "ssp" / "tos_010101-011012.nc", experiment_id="ssp585")

        first_files = backfill.find_first_files(tmp_path, experiment_id="ssp585")

        assert len(first_files) == 1
        assert first_files[0].attrs["experiment_id"] == "ssp585"

    def test_run_compliance_backfill_only_checks_requested_variables(
        self, tmp_path, fake_checker
    ):
        commands = fake_checker()
        output_folder = tmp_path / "output"
        _write_output(output_folder / "tos_010101-011012.nc", variable_id="tos")
        _write_output(output_folder / "so_010101-011012.nc", variable_id="so")
        report_dir = tmp_path / "reports"

        entries = backfill.run_compliance_backfill(
            output_folder,
            report_dir,
            cmip_version="CMIP6",
            suites=[CF],
            variables=["Omon.tos"],
        )

        assert len(commands) == 1
        assert len(entries) == 1
        assert entries[0].file.name == "tos_010101-011012.nc"

    def test_cli_variable_flag_restricts_the_scan(self, tmp_path, fake_checker):
        commands = fake_checker()
        output_folder = tmp_path / "output"
        _write_output(output_folder / "tos_010101-011012.nc", variable_id="tos")
        _write_output(output_folder / "so_010101-011012.nc", variable_id="so")

        exit_code = backfill.main(
            [
                "--output-folder",
                str(output_folder),
                "--suite",
                CF,
                "--variable",
                "tos",
            ]
        )

        assert exit_code == 0
        assert len(commands) == 1


@pytest.mark.unit
class TestListCompletedVariables:
    def test_returns_only_completed_rows(self, tmp_path):
        from access_moppy.tracking import TaskTracker

        db_path = tmp_path / "cmor_tasks.db"
        with TaskTracker(db_path) as tracker:
            tracker.add_task("Omon.tos", "historical")
            tracker.mark_completed("Omon.tos", "historical")
            tracker.add_task("Amon.tas", "historical")
            tracker.mark_failed("Amon.tas", "historical", "boom")

        rows = backfill.list_completed_variables(db_path)

        assert rows == [("Omon.tos", "historical")]

    def test_filters_by_experiment_id(self, tmp_path):
        from access_moppy.tracking import TaskTracker

        db_path = tmp_path / "cmor_tasks.db"
        with TaskTracker(db_path) as tracker:
            tracker.add_task("Omon.tos", "historical")
            tracker.mark_completed("Omon.tos", "historical")
            tracker.add_task("Omon.tos", "ssp585")
            tracker.mark_completed("Omon.tos", "ssp585")

        rows = backfill.list_completed_variables(db_path, experiment_id="ssp585")

        assert rows == [("Omon.tos", "ssp585")]


@pytest.mark.unit
class TestDatabaseDrivenBackfill:
    def test_targets_completed_variables_and_writes_results_back(
        self, tmp_path, fake_checker
    ):
        from access_moppy.tracking import TaskTracker

        commands = fake_checker()
        output_folder = tmp_path / "output"
        _write_output(
            output_folder / "tos_010101-011012.nc",
            variable_id="tos",
            experiment_id="historical",
        )
        _write_output(
            output_folder / "so_010101-011012.nc",
            variable_id="so",
            experiment_id="historical",
        )
        db_path = tmp_path / "cmor_tasks.db"
        with TaskTracker(db_path) as tracker:
            tracker.add_task("Omon.tos", "historical")
            tracker.mark_completed("Omon.tos", "historical")
            tracker.add_task("Omon.so", "historical")
            tracker.mark_completed("Omon.so", "historical")

        task_rows = backfill.list_completed_variables(db_path)
        entries = backfill.run_compliance_backfill(
            output_folder,
            tmp_path / "reports",
            cmip_version="CMIP6",
            suites=[CF],
            task_rows=task_rows,
        )

        assert len(commands) == 2
        assert {entry.variable for entry in entries} == {"Omon.tos", "Omon.so"}

        updated = backfill.write_results_to_db(db_path, entries)
        assert updated == 2

        with TaskTracker(db_path) as tracker:
            stored = tracker.get_compliance("Omon.tos", "historical")
        assert stored["passed"] is True
        assert stored["backfilled"] is True

    def test_entries_from_a_plain_scan_are_not_written_back(
        self, tmp_path, fake_checker
    ):
        from access_moppy.tracking import TaskTracker

        fake_checker()
        output_folder = tmp_path / "output"
        _write_output(output_folder / "tos_010101-011012.nc")
        db_path = tmp_path / "cmor_tasks.db"
        with TaskTracker(db_path) as tracker:
            tracker.add_task("Omon.tos", "historical")
            tracker.mark_completed("Omon.tos", "historical")

        # No task_rows passed: entries are not matched to a database row.
        entries = backfill.run_compliance_backfill(
            output_folder, tmp_path / "reports", cmip_version="CMIP6", suites=[CF]
        )

        assert entries[0].variable is None
        updated = backfill.write_results_to_db(db_path, entries)
        assert updated == 0

    def test_cli_db_flag_targets_completed_variables_and_updates_the_database(
        self, tmp_path, fake_checker
    ):
        from access_moppy.tracking import TaskTracker

        fake_checker()
        output_folder = tmp_path / "output"
        _write_output(
            output_folder / "tos_010101-011012.nc",
            variable_id="tos",
            experiment_id="historical",
        )
        db_path = tmp_path / "cmor_tasks.db"
        with TaskTracker(db_path) as tracker:
            tracker.add_task("Omon.tos", "historical")
            tracker.mark_completed("Omon.tos", "historical")

        exit_code = backfill.main(
            [
                "--output-folder",
                str(output_folder),
                "--suite",
                CF,
                "--db",
                str(db_path),
            ]
        )

        assert exit_code == 0
        with TaskTracker(db_path) as tracker:
            stored = tracker.get_compliance("Omon.tos", "historical")
        assert stored["passed"] is True

    def test_cli_exits_early_when_nothing_matches(self, tmp_path):
        from access_moppy.tracking import TaskTracker

        output_folder = tmp_path / "output"
        output_folder.mkdir()
        db_path = tmp_path / "cmor_tasks.db"
        with TaskTracker(db_path):
            pass  # no completed tasks

        exit_code = backfill.main(
            ["--output-folder", str(output_folder), "--db", str(db_path)]
        )

        assert exit_code == 0


@pytest.mark.unit
class TestCmorNameParsing:
    """Regression coverage for review comments on PR #629."""

    @pytest.mark.parametrize(
        ("variable", "expected"),
        [
            ("Amon.tas", "tas"),
            ("tos", "tos"),
            # CMIP7 branded name: realm.physical_parameter.processing_info.frequency.region.
            # The bare variable is the *second* component, not the last (region).
            ("atmos.huss.tpt-h2m-hxy-u.3hr.glb", "huss"),
            ("atmos.pr.tavg-u-hxy-u.3hr.glb", "pr"),
        ],
    )
    def test_cmor_name_extracts_the_variable_not_the_last_component(
        self, variable, expected
    ):
        assert backfill._cmor_name(variable) == expected

    @pytest.mark.parametrize(
        ("variable", "expected"),
        [
            ("Amon.tas", "Amon"),
            ("tos", None),
            ("atmos.huss.tpt-h2m-hxy-u.3hr.glb", "atmos"),
        ],
    )
    def test_table_label_extracts_the_first_component(self, variable, expected):
        assert backfill._table_label(variable) == expected


@pytest.mark.unit
class TestSameVariableNameAcrossTables:
    """A bare variable name (e.g. 'tas') published under two different
    tables/frequencies must not collide onto the same tracker row."""

    def test_db_driven_backfill_keeps_amon_tas_and_day_tas_separate(
        self, tmp_path, fake_checker
    ):
        from access_moppy.tracking import TaskTracker

        commands = fake_checker()
        output_folder = tmp_path / "output"
        _write_output(
            output_folder / "Amon" / "tas_010101-011012.nc",
            variable_id="tas",
            table_id="Amon",
        )
        _write_output(
            output_folder / "day" / "tas_01010101-01101231.nc",
            variable_id="tas",
            table_id="day",
        )
        db_path = tmp_path / "cmor_tasks.db"
        with TaskTracker(db_path) as tracker:
            tracker.add_task("Amon.tas", "historical")
            tracker.mark_completed("Amon.tas", "historical")
            tracker.add_task("day.tas", "historical")
            tracker.mark_completed("day.tas", "historical")

        task_rows = backfill.list_completed_variables(db_path)
        entries = backfill.run_compliance_backfill(
            output_folder,
            tmp_path / "reports",
            cmip_version="CMIP6",
            suites=[CF],
            task_rows=task_rows,
        )

        # Both files get checked, and each entry is matched to its own row.
        assert len(commands) == 2
        assert {entry.variable for entry in entries} == {"Amon.tas", "day.tas"}
        by_variable = {entry.variable: entry.file.parent.name for entry in entries}
        assert by_variable == {"Amon.tas": "Amon", "day.tas": "day"}

        updated = backfill.write_results_to_db(db_path, entries)
        assert updated == 2

        # Each row keeps its own result -- neither was overwritten by the other.
        with TaskTracker(db_path) as tracker:
            amon_result = tracker.get_compliance("Amon.tas", "historical")
            day_result = tracker.get_compliance("day.tas", "historical")
        assert amon_result["file"].endswith("Amon/tas_010101-011012.nc")
        assert day_result["file"].endswith("day/tas_01010101-01101231.nc")


@pytest.mark.unit
class TestExperimentIdNarrowsTheScanWithDb:
    def test_experiment_id_combined_with_db_restricts_the_file_scan(
        self, tmp_path, fake_checker
    ):
        from access_moppy.tracking import TaskTracker

        commands = fake_checker()
        output_folder = tmp_path / "output"
        _write_output(
            output_folder / "hist" / "tos_010101-011012.nc",
            experiment_id="historical",
        )
        _write_output(
            output_folder / "ssp" / "tos_010101-011012.nc",
            experiment_id="ssp585",
        )
        db_path = tmp_path / "cmor_tasks.db"
        with TaskTracker(db_path) as tracker:
            tracker.add_task("Omon.tos", "historical")
            tracker.mark_completed("Omon.tos", "historical")

        exit_code = backfill.main(
            [
                "--output-folder",
                str(output_folder),
                "--suite",
                CF,
                "--db",
                str(db_path),
                "--experiment-id",
                "historical",
            ]
        )

        assert exit_code == 0
        # Only the historical file is checked; ssp585 is filtered out of the
        # scan even though it shares the same variable_id.
        assert len(commands) == 1
        assert commands[0][-1].endswith("hist/tos_010101-011012.nc")
