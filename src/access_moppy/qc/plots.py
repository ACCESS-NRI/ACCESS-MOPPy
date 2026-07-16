"""QC diagnostic plots for CMORised output files.

This module mirrors the visual quality-control plots originally produced by
APP4's ``quality_check.py --timeseries`` mode and adapts them for Moppy's
modern Python 3 / xarray stack.

Two PNG files are generated per CMORised NetCDF file:

* **snapshot** – a spatial map of the first available timestep (or the sole
  frame for time-independent ``fx`` variables).
* **timeseries** – per-timestep global statistics (mean, min, max, std dev)
  across all non-time spatial dimensions, rendered in a two-panel figure.
  Skipped for scalar and ``fx`` variables.

The entry point is :func:`generate_qc_plots`, which is intentionally lenient:
any failure emits a :class:`warnings.warn` and returns ``None`` rather than
raising, so plot failures never interrupt the CMORisation workflow.
"""

from __future__ import annotations

import warnings
from pathlib import Path
from typing import Any

import numpy as np
import xarray as xr

# Spatial dimension names used in CMIP/UGRID output.
_SPATIAL_DIM_NAMES = frozenset(
    {
        "lat",
        "lon",
        "latitude",
        "longitude",
        "i",
        "j",
        "x",
        "y",
        "ni",
        "nj",
        "nlat",
        "nlon",
        "ncells",
        "cell",
    }
)


def _find_primary_variable(ds: xr.Dataset) -> str:
    """Return the name of the primary data variable in a CMORised file.

    Mirrors the logic used by the range-validator so both QC subsystems
    agree on which variable to inspect.
    """
    for candidate in (ds.attrs.get("branded_variable"), ds.attrs.get("variable_id")):
        if isinstance(candidate, str) and candidate in ds.data_vars:
            return candidate

    primary = [
        v
        for v in ds.data_vars
        if not (str(v).endswith("_bnds") or str(v).startswith("vertices_"))
    ]
    if len(primary) == 1:
        return primary[0]

    available = ", ".join(sorted(str(v) for v in ds.data_vars))
    raise ValueError(
        "QC plots could not identify the primary variable in the CMORised file. "
        f"Available data variables: {available}"
    )


def _spatial_reduction_dims(da: xr.DataArray) -> list[str]:
    """Return all non-time dimension names (i.e. spatial + level dims)."""
    return [d for d in da.dims if d != "time"]


def _snapshot_array(da: xr.DataArray) -> np.ndarray:
    """Reduce *da* to a 2-D or 1-D array for a snapshot plot.

    Steps:
    1. Select the first timestep when a ``time`` dimension is present.
    2. Average over any remaining non-spatial dimensions (e.g. ``lev``, ``plev``).
    3. Return as a plain NumPy float array.
    """
    arr = da
    if "time" in arr.dims:
        arr = arr.isel(time=0, missing_dims="ignore")

    extra = [d for d in arr.dims if d.lower() not in _SPATIAL_DIM_NAMES]
    if extra:
        arr = arr.mean(dim=extra, skipna=True)

    return np.asarray(arr.values, dtype=float)


def _make_snapshot_plot(
    plt: Any,
    da: xr.DataArray,
    var_name: str,
    units: str,
    stem: str,
    qc_dir: Path,
) -> None:
    """Write a spatial snapshot PNG to *qc_dir*."""
    data = _snapshot_array(da)
    if data.ndim not in (1, 2):
        return

    fig, ax = plt.subplots(figsize=(8, 4))

    if data.ndim == 2:
        masked = np.ma.masked_invalid(data)
        im = ax.imshow(masked, origin="lower", aspect="auto")
        fig.colorbar(im, ax=ax, label=units or var_name)
        ax.set_xlabel("longitude index")
        ax.set_ylabel("latitude index")
    else:
        ax.plot(np.ma.masked_invalid(data))
        ax.set_ylabel(units or var_name)
        ax.set_xlabel("index")

    title = var_name
    if "time" in da.dims and da.sizes["time"] > 0:
        try:
            t0 = da.time.values[0]
            title = f"{var_name} – {t0}"
        except Exception:
            pass
    ax.set_title(title)

    fig.tight_layout()
    fig.savefig(qc_dir / f"{stem}_snapshot.png", dpi=100)
    plt.close(fig)


def _make_timeseries_plot(
    plt: Any,
    da: xr.DataArray,
    var_name: str,
    units: str,
    stem: str,
    qc_dir: Path,
) -> None:
    """Write a two-panel timeseries statistics PNG to *qc_dir*.

    Panel 1: per-timestep mean with min/max shading.
    Panel 2: per-timestep standard deviation.
    """
    if "time" not in da.dims or da.sizes["time"] < 2:
        return

    over_dims = _spatial_reduction_dims(da)

    if not over_dims:
        # Pure 1-D timeseries variable – just plot the raw values.
        values = np.asarray(da.values, dtype=float)
        time_idx = np.arange(len(values))
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.plot(time_idx, np.ma.masked_invalid(values))
        ax.set_xlabel("time step")
        ax.set_ylabel(units or var_name)
        ax.set_title(f"{var_name} – timeseries")
        fig.tight_layout()
        fig.savefig(qc_dir / f"{stem}_timeseries.png", dpi=100)
        plt.close(fig)
        return

    # Vectorised reductions – compute all in one Dask graph traversal.
    means_da = da.mean(dim=over_dims, skipna=True)
    mins_da = da.min(dim=over_dims, skipna=True)
    maxs_da = da.max(dim=over_dims, skipna=True)
    stds_da = da.std(dim=over_dims, skipna=True)
    means_da, mins_da, maxs_da, stds_da = xr.compute(
        means_da, mins_da, maxs_da, stds_da
    )

    means = np.asarray(means_da.values, dtype=float)
    mins = np.asarray(mins_da.values, dtype=float)
    maxs = np.asarray(maxs_da.values, dtype=float)
    stds = np.asarray(stds_da.values, dtype=float)
    time_idx = np.arange(len(means))

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 7), sharex=True)

    ax1.plot(time_idx, np.ma.masked_invalid(means), color="steelblue", label="mean")
    ax1.fill_between(
        time_idx,
        np.ma.masked_invalid(mins),
        np.ma.masked_invalid(maxs),
        alpha=0.25,
        color="steelblue",
        label="min / max",
    )
    ax1.set_ylabel(units or var_name)
    ax1.set_title(f"{var_name} – mean / min / max over time")
    ax1.legend(fontsize=8)

    ax2.plot(time_idx, np.ma.masked_invalid(stds), color="darkorange", label="std dev")
    ax2.set_ylabel(units or var_name)
    ax2.set_xlabel("time step")
    ax2.set_title(f"{var_name} – standard deviation over time")
    ax2.legend(fontsize=8)

    fig.tight_layout()
    fig.savefig(qc_dir / f"{stem}_timeseries.png", dpi=100)
    plt.close(fig)


def generate_qc_plots(
    output_path: str | Path,
    qc_dir: str | Path | None = None,
) -> Path | None:
    """Generate QC diagnostic plots for a single CMORised output file.

    Produces a spatial snapshot of the first timestep and (for time-varying
    variables) a two-panel timeseries of per-timestep global statistics.
    Both outputs are written as PNG files.

    Parameters
    ----------
    output_path:
        Path to a CMORised NetCDF file.
    qc_dir:
        Directory for output PNGs.  Defaults to a ``qc_plots`` sub-directory
        next to *output_path*.

    Returns
    -------
    Path | None
        Directory where plots were written, or ``None`` when matplotlib is
        unavailable or an unrecoverable error occurred (a warning is emitted
        in that case).
    """
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
    except ImportError:
        warnings.warn(
            "matplotlib is not installed; QC plots will not be generated. "
            "Install it with: pip install matplotlib",
            stacklevel=2,
        )
        return None

    path = Path(output_path)
    if not path.exists():
        warnings.warn(f"QC plots: file not found: {path}", stacklevel=2)
        return None

    if qc_dir is None:
        qc_dir = path.parent / "qc_plots"
    qc_dir = Path(qc_dir)
    qc_dir.mkdir(parents=True, exist_ok=True)

    stem = path.stem

    try:
        with xr.open_dataset(path, use_cftime=True) as ds:
            var_name = _find_primary_variable(ds)
            da = ds[var_name]
            units = str(da.attrs.get("units", ""))

            _make_snapshot_plot(plt, da, var_name, units, stem, qc_dir)
            _make_timeseries_plot(plt, da, var_name, units, stem, qc_dir)

    except Exception as exc:
        warnings.warn(
            f"QC plots failed for {path.name}: {exc}",
            stacklevel=2,
        )
        return None

    return qc_dir
