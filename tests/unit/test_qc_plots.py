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
