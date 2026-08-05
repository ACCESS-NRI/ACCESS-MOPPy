"""Unit tests for access_moppy.qc.plots."""

from __future__ import annotations

import warnings
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pytest
import xarray as xr

# Skip the whole module if matplotlib is not installed (it's an optional dep).
matplotlib = pytest.importorskip("matplotlib", reason="matplotlib not installed")


def _write_cmip_file(path: Path, *, has_time: bool = True, n_time: int = 4) -> Path:
    """Write a minimal CMORised-style NetCDF file for testing."""
    lat = np.linspace(-90, 90, 8)
    lon = np.linspace(0, 360, 12, endpoint=False)

    if has_time:
        time = xr.cftime_range("1850-01", periods=n_time, freq="ME")
        data = np.random.default_rng(0).random((n_time, len(lat), len(lon)))
        da = xr.DataArray(
            data,
            dims=["time", "lat", "lon"],
            coords={"time": time, "lat": lat, "lon": lon},
            attrs={"units": "K", "long_name": "Near-Surface Air Temperature"},
        )
    else:
        data = np.random.default_rng(0).random((len(lat), len(lon)))
        da = xr.DataArray(
            data,
            dims=["lat", "lon"],
            coords={"lat": lat, "lon": lon},
            attrs={"units": "1", "long_name": "Land Area Fraction"},
        )

    ds = xr.Dataset(
        {"tas": da},
        attrs={
            "variable_id": "tas",
            "experiment_id": "historical",
            "source_id": "ACCESS-ESM1-6",
        },
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    ds.to_netcdf(path)
    return path


@pytest.mark.unit
class TestFindPrimaryVariable:
    def test_finds_by_variable_id(self, temp_dir):
        nc_path = _write_cmip_file(temp_dir / "tas_Amon.nc")
        with xr.open_dataset(nc_path) as ds:
            from access_moppy.qc.plots import _find_primary_variable

            assert _find_primary_variable(ds) == "tas"

    def test_finds_sole_variable_when_no_attr(self, temp_dir):
        nc_path = _write_cmip_file(temp_dir / "tas_Amon.nc")
        with xr.open_dataset(nc_path) as ds:
            ds = ds.drop_attrs()
            from access_moppy.qc.plots import _find_primary_variable

            assert _find_primary_variable(ds) == "tas"

    def test_raises_when_ambiguous(self, temp_dir):
        nc_path = _write_cmip_file(temp_dir / "tas_Amon.nc")
        with xr.open_dataset(nc_path) as ds:
            ds2 = ds.assign({"other": ds["tas"]}).drop_attrs()
            from access_moppy.qc.plots import _find_primary_variable

            with pytest.raises(ValueError, match="primary variable"):
                _find_primary_variable(ds2)


@pytest.mark.unit
class TestGenerateQcPlots:
    def test_creates_snapshot_and_timeseries_pngs(self, temp_dir):
        nc_path = _write_cmip_file(temp_dir / "tas_Amon.nc", n_time=4)
        qc_dir = temp_dir / "qc"
        from access_moppy.qc.plots import generate_qc_plots

        result = generate_qc_plots(nc_path, qc_dir=qc_dir)

        assert result == qc_dir
        assert (qc_dir / "tas_Amon_snapshot.png").exists()
        assert (qc_dir / "tas_Amon_timeseries.png").exists()

    def test_snapshot_only_skips_timeseries_and_overlay(self, temp_dir):
        nc_path = _write_cmip_file(temp_dir / "tas_Amon.nc", n_time=4)
        qc_dir = temp_dir / "qc"
        from access_moppy.qc import plots as plots_module

        with patch.object(plots_module, "_load_overlay") as mock_overlay:
            result = plots_module.generate_qc_plots(
                nc_path,
                qc_dir=qc_dir,
                comparison_store=temp_dir / "comparison",
                make_snapshot=True,
                make_timeseries=False,
            )

        assert result == qc_dir
        assert (qc_dir / "tas_Amon_snapshot.png").exists()
        assert not (qc_dir / "tas_Amon_timeseries.png").exists()
        mock_overlay.assert_not_called()

    def test_timeseries_only_skips_snapshot(self, temp_dir):
        nc_path = _write_cmip_file(temp_dir / "tas_Amon.nc", n_time=4)
        qc_dir = temp_dir / "qc"
        from access_moppy.qc.plots import generate_qc_plots

        result = generate_qc_plots(
            nc_path,
            qc_dir=qc_dir,
            make_snapshot=False,
            make_timeseries=True,
        )

        assert result == qc_dir
        assert not (qc_dir / "tas_Amon_snapshot.png").exists()
        assert (qc_dir / "tas_Amon_timeseries.png").exists()

    def test_requested_plot_failure_warns_and_returns_none(self, temp_dir):
        nc_path = _write_cmip_file(temp_dir / "tas_Amon.nc", n_time=4)
        from access_moppy.qc import plots as plots_module

        with (
            patch.object(
                plots_module, "_make_snapshot_plot", side_effect=RuntimeError("boom")
            ),
            pytest.warns(UserWarning, match="QC plots failed"),
        ):
            result = plots_module.generate_qc_plots(
                nc_path,
                make_snapshot=True,
                make_timeseries=False,
            )

        assert result is None

    def test_creates_only_snapshot_for_fx_variable(self, temp_dir):
        nc_path = _write_cmip_file(temp_dir / "sftlf_fx.nc", has_time=False)
        qc_dir = temp_dir / "qc"
        from access_moppy.qc.plots import generate_qc_plots

        result = generate_qc_plots(nc_path, qc_dir=qc_dir)

        assert result == qc_dir
        assert (qc_dir / "sftlf_fx_snapshot.png").exists()
        assert not (qc_dir / "sftlf_fx_timeseries.png").exists()

    def test_defaults_qc_dir_to_sibling_folder(self, temp_dir):
        nc_path = _write_cmip_file(temp_dir / "sub" / "tas_Amon.nc")
        from access_moppy.qc.plots import generate_qc_plots

        result = generate_qc_plots(nc_path)

        assert result == temp_dir / "sub" / "qc_plots"
        assert (result / "tas_Amon_snapshot.png").exists()

    def test_returns_none_for_missing_file(self, temp_dir):
        from access_moppy.qc.plots import generate_qc_plots

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = generate_qc_plots(temp_dir / "does_not_exist.nc")

        assert result is None
        assert any("not found" in str(w.message) for w in caught)

    def test_returns_none_when_matplotlib_missing(self, temp_dir):
        nc_path = _write_cmip_file(temp_dir / "tas_Amon.nc")
        from access_moppy.qc import plots as plots_module

        with patch.dict("sys.modules", {"matplotlib": None, "matplotlib.pyplot": None}):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                result = plots_module.generate_qc_plots(nc_path)

        assert result is None
        assert any("matplotlib" in str(w.message).lower() for w in caught)

    def test_single_timestep_skips_timeseries(self, temp_dir):
        nc_path = _write_cmip_file(temp_dir / "tas_Amon.nc", n_time=1)
        qc_dir = temp_dir / "qc"
        from access_moppy.qc.plots import generate_qc_plots

        result = generate_qc_plots(nc_path, qc_dir=qc_dir)

        assert result == qc_dir
        assert (qc_dir / "tas_Amon_snapshot.png").exists()
        # timeseries plot requires ≥ 2 timesteps
        assert not (qc_dir / "tas_Amon_timeseries.png").exists()


def _write_split_cmip_files(
    base_dir: Path, base_stem: str, *, n_chunks: int = 2, months_per_chunk: int = 4
) -> list[Path]:
    """Write *n_chunks* split CMORised-style NetCDF files with time-range stems."""
    paths = []
    for i in range(n_chunks):
        time = xr.cftime_range(f"{1850 + i}-01", periods=months_per_chunk, freq="ME")
        lat = np.linspace(-90, 90, 8)
        lon = np.linspace(0, 360, 12, endpoint=False)
        data = np.random.default_rng(i).random((months_per_chunk, len(lat), len(lon)))
        da = xr.DataArray(
            data,
            dims=["time", "lat", "lon"],
            coords={"time": time, "lat": lat, "lon": lon},
            attrs={"units": "K"},
        )
        ds = xr.Dataset({"tas": da}, attrs={"variable_id": "tas"})
        start_yr = time[0].year
        end_yr = time[-1].year
        path = base_dir / f"{base_stem}_{start_yr}-{end_yr}.nc"
        path.parent.mkdir(parents=True, exist_ok=True)
        ds.to_netcdf(path)
        paths.append(path)
    return paths


@pytest.mark.unit
class TestSplitBaseKey:
    def test_strips_yyyymm_range(self):
        from access_moppy.qc.plots import _split_base_key

        base, has = _split_base_key(
            "tas_Amon_ACCESS-ESM1-6_historical_r1i1p1f1_gn_185001-200012"
        )
        assert base == "tas_Amon_ACCESS-ESM1-6_historical_r1i1p1f1_gn"
        assert has is True

    def test_strips_yyyy_range(self):
        from access_moppy.qc.plots import _split_base_key

        base, has = _split_base_key("tas_Amon_1850-2000")
        assert base == "tas_Amon"
        assert has is True

    def test_no_time_range_unchanged(self):
        from access_moppy.qc.plots import _split_base_key

        base, has = _split_base_key("tas_Amon")
        assert base == "tas_Amon"
        assert has is False

    def test_single_year_token(self):
        from access_moppy.qc.plots import _split_base_key

        base, has = _split_base_key("pr_day_1850")
        assert base == "pr_day"
        assert has is True


@pytest.mark.unit
class TestGenerateQcPlotsForSplitFiles:
    def test_empty_list_returns_none(self):
        from access_moppy.qc.plots import generate_qc_plots_for_split_files

        assert generate_qc_plots_for_split_files([]) is None

    def test_single_file_delegates_to_generate_qc_plots(self, temp_dir):
        nc_path = _write_cmip_file(temp_dir / "tas_Amon.nc", n_time=4)
        from access_moppy.qc import plots as plots_module

        called_with = []

        def _fake(path, *, qc_dir=None, comparison_store=None, preferred_member=None):
            called_with.append(path)
            return qc_dir or path.parent / "qc_plots"

        with patch.object(plots_module, "generate_qc_plots", side_effect=_fake):
            plots_module.generate_qc_plots_for_split_files(
                [nc_path], qc_dir=temp_dir / "qc"
            )

        assert called_with == [nc_path]

    def test_combined_timeseries_written(self, temp_dir):
        paths = _write_split_cmip_files(temp_dir, "tas_Amon", n_chunks=2)
        qc_dir = temp_dir / "qc"
        from access_moppy.qc.plots import generate_qc_plots_for_split_files

        result = generate_qc_plots_for_split_files(paths, qc_dir=qc_dir)

        assert result == qc_dir
        assert (qc_dir / "tas_Amon_timeseries.png").exists()

    def test_no_snapshot_for_combined(self, temp_dir):
        paths = _write_split_cmip_files(temp_dir, "tas_Amon", n_chunks=2)
        qc_dir = temp_dir / "qc"
        from access_moppy.qc.plots import generate_qc_plots_for_split_files

        generate_qc_plots_for_split_files(paths, qc_dir=qc_dir)

        assert not (qc_dir / "tas_Amon_snapshot.png").exists()

    def test_combined_stem_strips_time_range(self, temp_dir):
        paths = _write_split_cmip_files(temp_dir, "tas_Amon", n_chunks=3)
        qc_dir = temp_dir / "qc"
        from access_moppy.qc.plots import generate_qc_plots_for_split_files

        generate_qc_plots_for_split_files(paths, qc_dir=qc_dir)

        assert (qc_dir / "tas_Amon_timeseries.png").exists()

    def test_defaults_qc_dir_next_to_first_file(self, temp_dir):
        sub = temp_dir / "sub"
        paths = _write_split_cmip_files(sub, "tas_Amon", n_chunks=2)
        from access_moppy.qc.plots import generate_qc_plots_for_split_files

        result = generate_qc_plots_for_split_files(paths)

        assert result == sub / "qc_plots"

    def test_returns_none_on_failure(self, temp_dir):
        paths = _write_split_cmip_files(temp_dir, "tas_Amon", n_chunks=2)
        from access_moppy.qc import plots as plots_module

        with patch.object(
            plots_module.xr, "open_mfdataset", side_effect=RuntimeError("boom")
        ):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                result = plots_module.generate_qc_plots_for_split_files(
                    paths, qc_dir=temp_dir / "qc"
                )

        assert result is None
        assert any("combined timeseries" in str(w.message).lower() for w in caught)


@pytest.mark.unit
class TestCLI:
    """Tests for the moppy-qc-plots CLI (main / _build_cli_parser)."""

    def test_single_file_exits_zero(self, temp_dir):
        nc_path = _write_cmip_file(temp_dir / "tas_Amon.nc", n_time=4)
        from access_moppy.qc.plots import main

        rc = main([str(nc_path), "--qc-dir", str(temp_dir / "qc")])

        assert rc == 0
        assert (temp_dir / "qc" / "tas_Amon_snapshot.png").exists()

    def test_directory_scans_recursively(self, temp_dir):
        sub = temp_dir / "sub"
        sub.mkdir()
        _write_cmip_file(sub / "tas_Amon.nc", n_time=4)
        _write_cmip_file(sub / "pr_Amon.nc", n_time=4)
        qc_dir = temp_dir / "qc"
        from access_moppy.qc.plots import main

        rc = main([str(temp_dir), "--qc-dir", str(qc_dir)])

        assert rc == 0
        assert (qc_dir / "tas_Amon_snapshot.png").exists()
        assert (qc_dir / "pr_Amon_snapshot.png").exists()

    def test_empty_directory_exits_zero(self, temp_dir):
        empty = temp_dir / "empty"
        empty.mkdir()
        from access_moppy.qc.plots import main

        rc = main([str(empty)])

        assert rc == 0

    def test_non_nc_non_dir_path_raises_system_exit(self, temp_dir):
        bad = temp_dir / "file.txt"
        bad.write_text("not a netcdf")
        from access_moppy.qc.plots import main

        with pytest.raises(SystemExit):
            main([str(bad)])

    def test_qc_dir_flag_respected(self, temp_dir):
        nc_path = _write_cmip_file(temp_dir / "tas_Amon.nc", n_time=4)
        custom_qc = temp_dir / "my_plots"
        from access_moppy.qc.plots import main

        rc = main([str(nc_path), "--qc-dir", str(custom_qc)])

        assert rc == 0
        assert custom_qc.is_dir()
        assert (custom_qc / "tas_Amon_snapshot.png").exists()

    def test_failed_plot_exits_nonzero(self, temp_dir):
        nc_path = _write_cmip_file(temp_dir / "tas_Amon.nc", n_time=4)
        from access_moppy.qc import plots as plots_module

        with patch.object(plots_module, "generate_qc_plots", return_value=None):
            rc = plots_module.main([str(nc_path)])

        assert rc == 1

    def test_comparison_store_forwarded(self, temp_dir):
        nc_path = _write_cmip_file(temp_dir / "tas_Amon.nc", n_time=4)
        fake_store = temp_dir / "store"
        from access_moppy.qc import plots as plots_module

        calls = []

        def _fake_generate(
            path, *, qc_dir=None, comparison_store=None, preferred_member=None
        ):
            calls.append(
                {
                    "comparison_store": comparison_store,
                    "preferred_member": preferred_member,
                }
            )
            return qc_dir or path.parent / "qc_plots"

        with patch.object(
            plots_module, "generate_qc_plots", side_effect=_fake_generate
        ):
            plots_module.main(
                [
                    str(nc_path),
                    "--comparison-store",
                    str(fake_store),
                    "--preferred-member",
                    "r1i1p1f1",
                ]
            )

        assert len(calls) == 1
        assert calls[0]["comparison_store"] == str(fake_store)
        assert calls[0]["preferred_member"] == "r1i1p1f1"

    def test_workers_flag_uses_process_pool(self, temp_dir):
        nc_path = _write_cmip_file(temp_dir / "tas_Amon.nc", n_time=4)
        from concurrent.futures import ProcessPoolExecutor

        from access_moppy.qc import plots as plots_module

        with patch(
            "access_moppy.qc.plots.ProcessPoolExecutor",
            wraps=ProcessPoolExecutor,
        ) as mock_pool:
            rc = plots_module.main([str(nc_path), "--workers", "2"])

        assert rc == 0
        mock_pool.assert_called_once_with(max_workers=2)

    def test_split_files_produce_combined_timeseries(self, temp_dir):
        """When multiple files share a base key a combined timeseries is written."""
        paths = _write_split_cmip_files(temp_dir, "tas_Amon", n_chunks=2)
        qc_dir = temp_dir / "qc"
        from access_moppy.qc.plots import main

        rc = main([str(temp_dir), "--qc-dir", str(qc_dir)])

        assert rc == 0
        # Per-file plots
        for p in paths:
            assert (qc_dir / f"{p.stem}_snapshot.png").exists()
        # Combined timeseries
        assert (qc_dir / "tas_Amon_timeseries.png").exists()

    def test_non_split_files_no_combined_plot(self, temp_dir):
        """Files with different base keys do not trigger a combined plot."""
        _write_cmip_file(temp_dir / "tas_Amon.nc", n_time=4)
        _write_cmip_file(temp_dir / "pr_Amon.nc", n_time=4)
        qc_dir = temp_dir / "qc"
        from access_moppy.qc.plots import main

        rc = main([str(temp_dir), "--qc-dir", str(qc_dir)])

        assert rc == 0
        assert not (qc_dir / "tas_timeseries.png").exists()
        assert not (qc_dir / "pr_timeseries.png").exists()
