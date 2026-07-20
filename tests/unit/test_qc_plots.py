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

        def _fake_generate(path, *, qc_dir=None, comparison_store=None, preferred_member=None):
            calls.append({"comparison_store": comparison_store, "preferred_member": preferred_member})
            return qc_dir or path.parent / "qc_plots"

        with patch.object(plots_module, "generate_qc_plots", side_effect=_fake_generate):
            plots_module.main([
                str(nc_path),
                "--comparison-store", str(fake_store),
                "--preferred-member", "r1i1p1f1",
            ])

        assert len(calls) == 1
        assert calls[0]["comparison_store"] == str(fake_store)
        assert calls[0]["preferred_member"] == "r1i1p1f1"

    def test_workers_flag_uses_process_pool(self, temp_dir):
        nc_path = _write_cmip_file(temp_dir / "tas_Amon.nc", n_time=4)
        from access_moppy.qc import plots as plots_module
        from concurrent.futures import ProcessPoolExecutor

        with patch(
            "access_moppy.qc.plots.ProcessPoolExecutor",
            wraps=ProcessPoolExecutor,
        ) as mock_pool:
            rc = plots_module.main([str(nc_path), "--workers", "2"])

        assert rc == 0
        mock_pool.assert_called_once_with(max_workers=2)
