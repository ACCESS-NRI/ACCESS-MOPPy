"""Unit tests for access_moppy.qc.cmip6_overlay and the comparison overlay
integration in access_moppy.qc.plots."""

from __future__ import annotations

import warnings
from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
import xarray as xr

# ---------------------------------------------------------------------------
# Optional-dependency guards
# ---------------------------------------------------------------------------

try:
    import pyarrow  # noqa: F401

    _HAS_PYARROW = True
except ImportError:
    _HAS_PYARROW = False

_needs_pyarrow = pytest.mark.skipif(
    not _HAS_PYARROW, reason="pyarrow not installed (optional for qc-plots extra)"
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_STORE_COLUMNS = [
    "variable",
    "table_id",
    "model",
    "experiment",
    "member_id",
    "grid_label",
    "period",
    "summary_kind",
    "source_filename",
    "source_path",
    "parquet_path",
    "row_count",
    "columns",
    "time_start",
    "time_end",
]

_TS_COLUMNS = [
    "time",
    "global_min",
    "global_max",
    "global_mean",
    "variable",
    "table_id",
    "model",
    "experiment",
    "member_id",
    "grid_label",
    "period",
    "summary_kind",
]


def _make_fake_store(
    base: Path,
    variable: str = "tas",
    table_id: str = "Amon",
    experiment: str = "historical",
    member_id: str = "r1i1p1f1",
    grid_label: str = "gn",
    period: str = "185001-201412",
    n_timesteps: int = 12,
) -> Path:
    """Create a minimal fake Parquet store under *base* and return its path."""
    store = base / "fake_store"
    store.mkdir(parents=True, exist_ok=True)

    # Build the partitioned path
    pq_rel = (
        f"timeseries/variable={variable}/table_id={table_id}"
        f"/model=ACCESS-ESM1-5/experiment={experiment}"
        f"/member_id={member_id}/grid_label={grid_label}"
        f"/period={period}/summary_kind=min_max_mean_timeseries.parquet"
    )
    pq_abs = store / pq_rel
    pq_abs.parent.mkdir(parents=True, exist_ok=True)

    # Write the timeseries Parquet file
    rng = np.random.default_rng(42)
    times = pd.date_range("1850-01", periods=n_timesteps, freq="ME")
    df_ts = pd.DataFrame(
        {
            "time": times,
            "global_min": rng.random(n_timesteps) * 280.0,
            "global_max": rng.random(n_timesteps) * 310.0 + 280.0,
            "global_mean": rng.random(n_timesteps) * 30.0 + 285.0,
            "variable": variable,
            "table_id": table_id,
            "model": "ACCESS-ESM1-5",
            "experiment": experiment,
            "member_id": member_id,
            "grid_label": grid_label,
            "period": period,
            "summary_kind": "min_max_mean_timeseries",
        }
    )
    df_ts.to_parquet(pq_abs)

    # Write catalog CSV
    catalog_row = {
        "variable": variable,
        "table_id": table_id,
        "model": "ACCESS-ESM1-5",
        "experiment": experiment,
        "member_id": member_id,
        "grid_label": grid_label,
        "period": period,
        "summary_kind": "min_max_mean_timeseries",
        "source_filename": "",
        "source_path": "",
        "parquet_path": str(pq_abs),
        "row_count": n_timesteps,
        "columns": ",".join(_TS_COLUMNS),
        "time_start": str(times[0]),
        "time_end": str(times[-1]),
    }
    pd.DataFrame([catalog_row]).to_csv(store / "catalog.csv", index=False)

    return store


def _write_cmip_file(
    path: Path,
    *,
    variable: str = "tas",
    table_id: str = "Amon",
    experiment_id: str = "historical",
    grid_label: str = "gn",
    n_time: int = 12,
) -> Path:
    """Write a minimal CMORised-style NetCDF file for testing."""
    lat = np.linspace(-90, 90, 8)
    lon = np.linspace(0, 360, 12, endpoint=False)
    time = xr.cftime_range("1850-01", periods=n_time, freq="ME")
    data = np.random.default_rng(0).random((n_time, len(lat), len(lon))) * 30.0 + 285.0
    da = xr.DataArray(
        data,
        dims=["time", "lat", "lon"],
        coords={"time": time, "lat": lat, "lon": lon},
        attrs={"units": "K", "long_name": "Near-Surface Air Temperature"},
    )
    ds = xr.Dataset(
        {variable: da},
        attrs={
            "variable_id": variable,
            "table_id": table_id,
            "experiment_id": experiment_id,
            "grid_label": grid_label,
            "source_id": "ACCESS-ESM1-6",
        },
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    ds.to_netcdf(path)
    return path


# ---------------------------------------------------------------------------
# Tests for cmip6_overlay module
# ---------------------------------------------------------------------------


@pytest.mark.unit
@_needs_pyarrow
class TestLoadComparisonTimeseries:
    def test_returns_overlay_data_when_match_found(self, temp_dir):
        store = _make_fake_store(temp_dir)
        from access_moppy.qc.cmip6_overlay import load_comparison_timeseries

        result = load_comparison_timeseries(
            store,
            variable="tas",
            table_id="Amon",
            experiment="historical",
        )

        assert result is not None
        assert result.member_id == "r1i1p1f1"
        assert len(result.time) == 12
        assert len(result.global_mean) == 12
        assert result.global_min is not None
        assert result.global_max is not None

    def test_returns_none_when_store_missing(self, temp_dir):
        from access_moppy.qc.cmip6_overlay import load_comparison_timeseries

        result = load_comparison_timeseries(
            temp_dir / "nonexistent_store",
            variable="tas",
            table_id="Amon",
            experiment="historical",
        )
        assert result is None

    def test_returns_none_when_no_matching_variable(self, temp_dir):
        store = _make_fake_store(temp_dir)
        from access_moppy.qc.cmip6_overlay import load_comparison_timeseries

        result = load_comparison_timeseries(
            store,
            variable="pr",  # not in the store
            table_id="Amon",
            experiment="historical",
        )
        assert result is None

    def test_returns_none_when_no_matching_experiment(self, temp_dir):
        store = _make_fake_store(temp_dir, experiment="historical")
        from access_moppy.qc.cmip6_overlay import load_comparison_timeseries

        result = load_comparison_timeseries(
            store,
            variable="tas",
            table_id="Amon",
            experiment="piControl",  # not in store
        )
        assert result is None

    def test_preferred_member_respected(self, temp_dir):
        store = _make_fake_store(temp_dir, member_id="r1i1p1f1")
        # Add a second member to the catalog
        from access_moppy.qc.cmip6_overlay import load_comparison_timeseries

        # Add r2i1p1f1 row to catalog
        cat = pd.read_csv(store / "catalog.csv")
        extra = cat.copy()
        extra["member_id"] = "r2i1p1f1"
        extra["parquet_path"] = str(store / "timeseries/variable=tas/table_id=Amon/model=ACCESS-ESM1-5/experiment=historical/member_id=r1i1p1f1/grid_label=gn/period=185001-201412/summary_kind=min_max_mean_timeseries.parquet")
        # Point both to same parquet file for simplicity
        pd.concat([cat, extra], ignore_index=True).to_csv(
            store / "catalog.csv", index=False
        )

        result = load_comparison_timeseries(
            store,
            variable="tas",
            table_id="Amon",
            experiment="historical",
            preferred_member="r2i1p1f1",
        )
        assert result is not None
        assert result.member_id == "r2i1p1f1"

    def test_defaults_to_r1i1p1f1_when_no_preference(self, temp_dir):
        store = _make_fake_store(temp_dir, member_id="r1i1p1f1")
        from access_moppy.qc.cmip6_overlay import load_comparison_timeseries

        result = load_comparison_timeseries(
            store,
            variable="tas",
            table_id="Amon",
            experiment="historical",
            preferred_member=None,
        )
        assert result is not None
        assert result.member_id == "r1i1p1f1"

    def test_grid_label_filter(self, temp_dir):
        store = _make_fake_store(temp_dir, grid_label="gn")
        from access_moppy.qc.cmip6_overlay import load_comparison_timeseries

        # Correct grid_label matches
        result = load_comparison_timeseries(
            store,
            variable="tas",
            table_id="Amon",
            experiment="historical",
            grid_label="gn",
        )
        assert result is not None

        # Wrong grid_label → no match
        result2 = load_comparison_timeseries(
            store,
            variable="tas",
            table_id="Amon",
            experiment="historical",
            grid_label="gr",
        )
        assert result2 is None

    def test_concatenates_multiple_period_chunks(self, temp_dir):
        """Multiple period rows for the same member should be concatenated."""
        from access_moppy.qc.cmip6_overlay import load_comparison_timeseries

        # Create store with two period chunks
        store = temp_dir / "multi_period_store"
        store.mkdir(parents=True)

        rng = np.random.default_rng(0)
        rows = []
        for period, start in [("185001-186912", "1850-01"), ("187001-188912", "1870-01")]:
            pq_path = (
                store
                / f"timeseries/variable=tas/table_id=Amon/model=ACCESS-ESM1-5"
                f"/experiment=historical/member_id=r1i1p1f1/grid_label=gn"
                f"/period={period}/summary_kind=min_max_mean_timeseries.parquet"
            )
            pq_path.parent.mkdir(parents=True, exist_ok=True)
            times = pd.date_range(start, periods=6, freq="ME")
            df = pd.DataFrame(
                {
                    "time": times,
                    "global_min": rng.random(6) * 280.0,
                    "global_max": rng.random(6) * 310.0 + 280.0,
                    "global_mean": rng.random(6) * 30.0 + 285.0,
                    "variable": "tas",
                    "table_id": "Amon",
                    "model": "ACCESS-ESM1-5",
                    "experiment": "historical",
                    "member_id": "r1i1p1f1",
                    "grid_label": "gn",
                    "period": period,
                    "summary_kind": "min_max_mean_timeseries",
                }
            )
            df.to_parquet(pq_path)
            rows.append(
                {
                    "variable": "tas",
                    "table_id": "Amon",
                    "model": "ACCESS-ESM1-5",
                    "experiment": "historical",
                    "member_id": "r1i1p1f1",
                    "grid_label": "gn",
                    "period": period,
                    "summary_kind": "min_max_mean_timeseries",
                    "source_filename": "",
                    "source_path": "",
                    "parquet_path": str(pq_path),
                    "row_count": 6,
                    "columns": ",".join(_TS_COLUMNS),
                    "time_start": str(times[0]),
                    "time_end": str(times[-1]),
                }
            )
        pd.DataFrame(rows).to_csv(store / "catalog.csv", index=False)

        result = load_comparison_timeseries(
            store,
            variable="tas",
            table_id="Amon",
            experiment="historical",
        )
        assert result is not None
        assert len(result.time) == 12  # 6 + 6


# ---------------------------------------------------------------------------
# Tests for pick_member helper
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestPickMember:
    def test_prefers_r1i1p1f1_by_default(self):
        from access_moppy.qc.cmip6_overlay import _pick_member

        members = ["r10i1p1f1", "r2i1p1f1", "r1i1p1f1"]
        assert _pick_member(members, None) == "r1i1p1f1"

    def test_uses_preferred_when_present(self):
        from access_moppy.qc.cmip6_overlay import _pick_member

        members = ["r1i1p1f1", "r3i1p1f1", "r5i1p1f1"]
        assert _pick_member(members, "r3i1p1f1") == "r3i1p1f1"

    def test_falls_back_to_lex_first_when_r1_absent(self):
        from access_moppy.qc.cmip6_overlay import _pick_member

        members = ["r10i1p1f1", "r3i1p1f1", "r2i1p1f1"]
        # No r1i1p1f1 → lexicographic first → r10... comes before r2... and r3...
        result = _pick_member(members, None)
        assert result == sorted(members)[0]

    def test_falls_back_to_lex_when_preferred_absent(self):
        from access_moppy.qc.cmip6_overlay import _pick_member

        members = ["r1i1p1f1", "r2i1p1f1"]
        assert _pick_member(members, "r99i1p1f1") == "r1i1p1f1"


# ---------------------------------------------------------------------------
# Tests for generate_qc_plots with overlay
# ---------------------------------------------------------------------------


matplotlib = pytest.importorskip("matplotlib", reason="matplotlib not installed")


@pytest.mark.unit
class TestGenerateQcPlotsWithOverlay:
    def test_overlay_plotted_when_store_and_match_found(self, temp_dir):
        pytest.importorskip("pyarrow", reason="pyarrow not installed")
        store = _make_fake_store(temp_dir, n_timesteps=12)
        nc_path = _write_cmip_file(
            temp_dir / "tas_Amon.nc",
            variable="tas",
            table_id="Amon",
            experiment_id="historical",
            n_time=12,
        )
        qc_dir = temp_dir / "qc"
        from access_moppy.qc.plots import generate_qc_plots

        result = generate_qc_plots(
            nc_path, qc_dir=qc_dir, comparison_store=store
        )

        assert result == qc_dir
        assert (qc_dir / "tas_Amon_timeseries.png").exists()

    def test_no_overlay_when_store_missing(self, temp_dir):
        nc_path = _write_cmip_file(temp_dir / "tas_Amon.nc", n_time=4)
        qc_dir = temp_dir / "qc"
        from access_moppy.qc.plots import generate_qc_plots

        result = generate_qc_plots(
            nc_path,
            qc_dir=qc_dir,
            comparison_store=temp_dir / "no_such_store",
        )

        # Plot still created, just without overlay
        assert result == qc_dir
        assert (qc_dir / "tas_Amon_timeseries.png").exists()

    def test_no_overlay_when_variable_not_in_store(self, temp_dir):
        pytest.importorskip("pyarrow", reason="pyarrow not installed")
        store = _make_fake_store(temp_dir, variable="pr")  # store has 'pr', not 'tas'
        nc_path = _write_cmip_file(temp_dir / "tas_Amon.nc", variable="tas", n_time=4)
        qc_dir = temp_dir / "qc"
        from access_moppy.qc.plots import generate_qc_plots

        # Should complete without error even though no match exists
        result = generate_qc_plots(nc_path, qc_dir=qc_dir, comparison_store=store)

        assert result == qc_dir
        assert (qc_dir / "tas_Amon_timeseries.png").exists()

    def test_comparison_store_none_unchanged_behavior(self, temp_dir):
        """No comparison_store → identical behavior to original."""
        nc_path = _write_cmip_file(temp_dir / "tas_Amon.nc", n_time=4)
        qc_dir = temp_dir / "qc"
        from access_moppy.qc.plots import generate_qc_plots

        result = generate_qc_plots(nc_path, qc_dir=qc_dir, comparison_store=None)

        assert result == qc_dir
        assert (qc_dir / "tas_Amon_snapshot.png").exists()
        assert (qc_dir / "tas_Amon_timeseries.png").exists()

    def test_missing_table_id_attr_skips_overlay_cleanly(self, temp_dir):
        """When table_id is absent from dataset attrs, overlay is skipped cleanly."""
        pytest.importorskip("pyarrow", reason="pyarrow not installed")
        store = _make_fake_store(temp_dir)
        lat = np.linspace(-90, 90, 4)
        lon = np.linspace(0, 360, 6, endpoint=False)
        time = xr.cftime_range("1850-01", periods=4, freq="ME")
        data = np.ones((4, 4, 6)) * 290.0
        da = xr.DataArray(data, dims=["time", "lat", "lon"],
                          coords={"time": time, "lat": lat, "lon": lon})
        ds = xr.Dataset(
            {"tas": da},
            attrs={
                "variable_id": "tas",
                # deliberately omit table_id and experiment_id
                "source_id": "ACCESS-ESM1-6",
            },
        )
        nc_path = temp_dir / "no_table_id.nc"
        ds.to_netcdf(nc_path)
        qc_dir = temp_dir / "qc"
        from access_moppy.qc.plots import generate_qc_plots

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = generate_qc_plots(nc_path, qc_dir=qc_dir, comparison_store=store)

        assert result == qc_dir
        # No error-level warnings should be raised for the overlay
        overlay_warnings = [
            w for w in caught if "comparison" in str(w.message).lower()
        ]
        assert not overlay_warnings
