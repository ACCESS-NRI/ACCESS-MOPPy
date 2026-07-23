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

Optional comparison overlay
---------------------------
Pass *comparison_store* (a path to an external ACCESS-ESM1-5 CMIP6 Parquet
timeseries store) to overlay a reference global-mean timeseries on the
timeseries plot.  The overlay is silently skipped when the store is absent,
the catalog has no matching entry, or ``pyarrow`` is not installed.
"""

from __future__ import annotations

import argparse
import logging
import re
import warnings
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple

import numpy as np
import xarray as xr

if TYPE_CHECKING:
    from access_moppy.qc.cmip6_overlay import OverlayData

_log = logging.getLogger(__name__)

# Pattern matching the time-range token at the end of a CMORised file stem,
# e.g. "185001-200012" or "1850-2000".
_TIME_RANGE_RE = re.compile(r"^\d{4,8}(-\d{4,8})?$")


def _split_base_key(stem: str) -> tuple[str, bool]:
    """Return ``(base_key, has_time_range)`` for a CMORised file stem.

    Strips the trailing time-range token (e.g. ``"185001-200012"`` or
    ``"1850-2000"``) from *stem* when it matches :data:`_TIME_RANGE_RE`.
    """
    parts = stem.rsplit("_", 1)
    if len(parts) == 2 and _TIME_RANGE_RE.match(parts[1]):
        return parts[0], True
    return stem, False


def _group_by_base_key(nc_files: list[Path]) -> dict[str, list[Path]]:
    """Group *nc_files* by their base key (stem without the time-range token).

    Files whose stem does not carry a time-range token are placed in a
    singleton group under their full stem.
    """
    groups: dict[str, list[Path]] = defaultdict(list)
    for f in nc_files:
        base, _ = _split_base_key(f.stem)
        groups[base].append(f)
    return dict(groups)


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


def _to_datetime64(da: xr.DataArray) -> "np.ndarray | None":
    """Convert the ``time`` coordinate of *da* to ``numpy.datetime64`` values.

    Returns ``None`` when the conversion fails or the array has no time axis.
    """
    if "time" not in da.dims:
        return None
    try:
        import pandas as pd  # noqa: PLC0415

        raw = da.time.values
        return pd.DatetimeIndex([pd.Timestamp(str(t)) for t in raw]).to_numpy()
    except Exception as exc:
        _log.debug("Could not convert time axis to datetime64: %s", exc)
        return None


def _mask_fill_values(da: xr.DataArray) -> xr.DataArray:
    """Return *da* with fill-value sentinels replaced by NaN.

    Reads ``_FillValue`` then ``missing_value`` from *da* attributes
    (defaulting to the CMIP standard 1e20).  Uses a tolerance-aware
    comparison so float32-rounded representations of 1e20 are also caught.
    """
    fill_val = da.attrs.get("_FillValue", da.attrs.get("missing_value", 1e20))
    try:
        fill_val = float(fill_val)
    except (TypeError, ValueError):
        return da
    # atol scaled to the fill value magnitude avoids false positives near zero.
    atol = abs(fill_val) * 1e-4
    mask = np.isclose(da.values, fill_val, rtol=0.0, atol=atol, equal_nan=False)
    return da.where(~mask)


def _snapshot_array(da: xr.DataArray) -> np.ndarray:
    """Reduce *da* to a 2-D or 1-D array for a snapshot plot.

    Steps:
    1. Select the first timestep when a ``time`` dimension is present.
    2. Average over any remaining non-spatial dimensions (e.g. ``lev``, ``plev``).
    3. Return as a plain NumPy float array.
    """
    arr = da
    if "time" in arr.dims:
        if arr.sizes["time"] == 0:
            return np.empty((0,), dtype=float)
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

    _MISSING_COLOR = "lightgray"

    if data.ndim == 2:
        masked = np.ma.masked_invalid(data)
        import copy  # noqa: PLC0415

        cmap = copy.copy(plt.cm.viridis)
        cmap.set_bad(color=_MISSING_COLOR)
        im = ax.imshow(masked, origin="lower", aspect="auto", cmap=cmap)
        fig.colorbar(im, ax=ax, label=units or var_name)
        ax.set_xlabel("longitude index")
        ax.set_ylabel("latitude index")
    else:
        values = np.asarray(data, dtype=float)
        missing_idx = np.where(~np.isfinite(values))[0]
        ax.plot(np.ma.masked_invalid(values))
        if missing_idx.size:
            ax.plot(
                missing_idx,
                np.full(
                    missing_idx.shape,
                    np.nanmean(values) if np.any(np.isfinite(values)) else 0.0,
                ),
                marker="|",
                linestyle="none",
                color=_MISSING_COLOR,
                markersize=8,
                label="missing",
            )
            ax.legend(fontsize=8)
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
    overlay: "OverlayData | None" = None,
) -> None:
    """Write a two-panel timeseries statistics PNG to *qc_dir*.

    Panel 1: per-timestep mean with min/max shading.
    Panel 2: per-timestep standard deviation.

    When *overlay* is provided the reference global-mean timeseries is drawn
    on panel 1 and the X-axis uses actual dates for alignment.  If the time
    conversion fails the overlay is silently skipped.
    """
    if "time" not in da.dims or da.sizes["time"] < 2:
        return

    over_dims = _spatial_reduction_dims(da)

    if not over_dims:
        # Pure 1-D timeseries variable – just plot the raw values.
        values = np.asarray(da.values, dtype=float)
        time_x: Any = np.arange(len(values))
        fig, ax = plt.subplots(figsize=(10, 4))
        ax.plot(time_x, np.ma.masked_invalid(values), label=var_name)
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

    try:
        import dask  # noqa: PLC0415

        (means_da, mins_da, maxs_da, stds_da) = dask.compute(
            means_da, mins_da, maxs_da, stds_da
        )
    except ImportError:
        pass  # Not dask-backed; .values access below is sufficient

    means = np.asarray(means_da.values, dtype=float)
    mins = np.asarray(mins_da.values, dtype=float)
    maxs = np.asarray(maxs_da.values, dtype=float)
    stds = np.asarray(stds_da.values, dtype=float)

    # Use actual datetime axis when an overlay is requested so both series
    # share a common time reference.  Fall back to integer index when time
    # conversion fails or no overlay is needed.
    time_x = _to_datetime64(da) if overlay is not None else None
    if time_x is None:
        time_x = np.arange(len(means))
        xlabel = "time step"
        use_dates = False
    else:
        xlabel = "date"
        use_dates = True

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(10, 7), sharex=True)

    ax1.plot(time_x, np.ma.masked_invalid(means), color="steelblue", label="mean")
    ax1.fill_between(
        time_x,
        np.ma.masked_invalid(mins),
        np.ma.masked_invalid(maxs),
        alpha=0.25,
        color="steelblue",
        label="min / max",
    )

    # ------------------------------------------------------------------
    # Overlay ACCESS-ESM1-5 reference if available
    # ------------------------------------------------------------------
    if overlay is not None and use_dates:
        label = f"ACCESS-ESM1-5 {overlay.member_id} (ref)"
        ax1.plot(
            overlay.time,
            np.ma.masked_invalid(overlay.global_mean),
            color="darkorange",
            linewidth=1.0,
            alpha=0.8,
            label=label,
            zorder=3,
        )
        if overlay.global_min is not None and overlay.global_max is not None:
            ax1.fill_between(
                overlay.time,
                np.ma.masked_invalid(overlay.global_min),
                np.ma.masked_invalid(overlay.global_max),
                alpha=0.10,
                color="darkorange",
            )

    ax1.set_ylabel(units or var_name)
    ax1.set_title(f"{var_name} – mean / min / max over time")
    ax1.legend(fontsize=8)

    ax2.plot(time_x, np.ma.masked_invalid(stds), color="steelblue", label="std dev")
    ax2.set_ylabel(units or var_name)
    ax2.set_xlabel(xlabel)
    ax2.set_title(f"{var_name} – standard deviation over time")
    ax2.legend(fontsize=8)

    if use_dates:
        try:
            import matplotlib.dates as mdates  # noqa: PLC0415

            fig.autofmt_xdate()
            ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
        except Exception:
            pass

    fig.tight_layout()
    fig.savefig(qc_dir / f"{stem}_timeseries.png", dpi=100)
    plt.close(fig)


def generate_qc_plots(
    output_path: str | Path,
    qc_dir: str | Path | None = None,
    comparison_store: str | Path | None = None,
    preferred_member: str | None = None,
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
    comparison_store:
        Optional path to an external ACCESS-ESM1-5 CMIP6 Parquet timeseries
        store.  When supplied the matching reference global-mean series is
        overlaid on the timeseries plot.  Silently skipped when the store is
        absent or no matching entry is found.
    preferred_member:
        Optional preferred ensemble member label (e.g. ``"r1i1p1f1"``).  Only
        relevant when *comparison_store* is set.  Falls back to ``"r1i1p1f1"``
        and then to the lexicographically first available member.

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

            overlay = None
            if comparison_store is not None:
                overlay = _load_overlay(
                    ds=ds,
                    var_name=var_name,
                    store_path=Path(comparison_store),
                    preferred_member=preferred_member,
                )

            da = _mask_fill_values(da)
            _make_snapshot_plot(plt, da, var_name, units, stem, qc_dir)
            _make_timeseries_plot(
                plt, da, var_name, units, stem, qc_dir, overlay=overlay
            )

    except Exception as exc:
        warnings.warn(
            f"QC plots failed for {path.name}: {exc}",
            stacklevel=2,
        )
        return None

    return qc_dir


def generate_qc_plots_for_split_files(
    output_paths: list[str | Path],
    qc_dir: str | Path | None = None,
    comparison_store: str | Path | None = None,
    preferred_member: str | None = None,
) -> Path | None:
    """Generate a combined timeseries QC plot for a set of split output files.

    When CMORisation splits a variable across multiple time-chunk files this
    function concatenates them along the time axis and produces a single
    timeseries PNG spanning the full period.  A snapshot plot is *not*
    generated here – use :func:`generate_qc_plots` per file for that.

    Parameters
    ----------
    output_paths:
        Two or more paths to split CMORised NetCDF files for the same
        variable.  When a single path is supplied the call is delegated to
        :func:`generate_qc_plots`.
    qc_dir:
        Directory for the output PNG.  Defaults to a ``qc_plots``
        sub-directory next to the first file in *output_paths*.
    comparison_store:
        Optional path to an ACCESS-ESM1-5 CMIP6 Parquet timeseries store.
        When supplied the matching reference global-mean series is overlaid
        on the timeseries plot.
    preferred_member:
        Optional preferred ensemble member label for the comparison overlay.

    Returns
    -------
    Path | None
        Directory where the plot was written, or ``None`` on failure.
    """
    paths = [Path(p) for p in output_paths]
    if not paths:
        return None
    if len(paths) == 1:
        return generate_qc_plots(
            paths[0],
            qc_dir=qc_dir,
            comparison_store=comparison_store,
            preferred_member=preferred_member,
        )

    try:
        import matplotlib  # noqa: PLC0415

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt  # noqa: PLC0415
    except ImportError:
        warnings.warn(
            "matplotlib is not installed; QC plots will not be generated. "
            "Install it with: pip install matplotlib",
            stacklevel=2,
        )
        return None

    first = paths[0]
    if qc_dir is None:
        qc_dir = first.parent / "qc_plots"
    qc_dir = Path(qc_dir)
    qc_dir.mkdir(parents=True, exist_ok=True)

    base_stem, _ = _split_base_key(first.stem)

    try:
        with xr.open_mfdataset(
            sorted(str(p) for p in paths),
            combine="by_coords",
            use_cftime=True,
        ) as ds:
            var_name = _find_primary_variable(ds)
            da = ds[var_name]
            units = str(da.attrs.get("units", ""))

            overlay = None
            if comparison_store is not None:
                overlay = _load_overlay(
                    ds=ds,
                    var_name=var_name,
                    store_path=Path(comparison_store),
                    preferred_member=preferred_member,
                )

            da = _mask_fill_values(da)
            _make_timeseries_plot(
                plt, da, var_name, units, base_stem, qc_dir, overlay=overlay
            )

    except Exception as exc:
        warnings.warn(
            f"QC combined timeseries plot failed for {base_stem}: {exc}",
            stacklevel=2,
        )
        return None

    return qc_dir


# ---------------------------------------------------------------------------
# Internal overlay helper
# ---------------------------------------------------------------------------


def _load_overlay(
    ds: xr.Dataset,
    var_name: str,
    store_path: Path,
    preferred_member: str | None,
) -> "OverlayData | None":
    """Look up the CMIP6 reference for the variable described by *ds*.

    Extracts matching keys from *ds* global attributes, delegates to
    :func:`~access_moppy.qc.cmip6_overlay.load_comparison_timeseries`, and
    returns the overlay data or ``None`` on any failure.
    """
    try:
        from access_moppy.qc.cmip6_overlay import (  # noqa: PLC0415
            load_comparison_timeseries,
        )
    except ImportError:
        return None

    variable = str(ds.attrs.get("variable_id", var_name))
    table_id = str(ds.attrs.get("table_id", ""))
    experiment = str(ds.attrs.get("experiment_id", ""))
    grid_label = ds.attrs.get("grid_label")  # optional

    if not table_id or not experiment:
        _log.debug(
            "table_id or experiment_id missing from dataset attrs; "
            "comparison overlay skipped"
        )
        return None

    return load_comparison_timeseries(
        store_path=store_path,
        variable=variable,
        table_id=table_id,
        experiment=experiment,
        grid_label=str(grid_label) if grid_label else None,
        preferred_member=preferred_member,
    )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def _build_cli_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="moppy-qc-plots",
        description=(
            "Generate QC diagnostic plots (snapshot + timeseries) for one or more "
            "CMORised NetCDF files, or recursively for all .nc files under a directory."
        ),
    )
    parser.add_argument(
        "paths",
        nargs="+",
        metavar="PATH",
        help=(
            "One or more CMORised .nc files, or directories to scan recursively "
            "for .nc files."
        ),
    )
    parser.add_argument(
        "--qc-dir",
        metavar="DIR",
        default=None,
        help=(
            "Directory for output PNGs. Defaults to a qc_plots/ folder next to "
            "each input file when not set."
        ),
    )
    parser.add_argument(
        "--comparison-store",
        metavar="PATH",
        default=None,
        help=(
            "Path to an ACCESS-ESM1-5 CMIP6 Parquet timeseries store. "
            "When supplied the matching reference global-mean is overlaid on "
            "the timeseries plot for each variable."
        ),
    )
    parser.add_argument(
        "--preferred-member",
        metavar="MEMBER",
        default=None,
        help=(
            "Preferred ensemble member label for the CMIP6 comparison overlay "
            "(e.g. r1i1p1f1). Falls back to r1i1p1f1 then lex-first."
        ),
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        metavar="N",
        help="Number of parallel worker processes (default: 1).",
    )
    return parser


# _PlotArgs is module-level so _cli_plot_one is picklable by ProcessPoolExecutor.
class _PlotArgs(NamedTuple):
    nc_file: Path
    qc_dir: Path | None
    comparison_store: str | None
    preferred_member: str | None


def _cli_plot_one(args: _PlotArgs) -> tuple[Path, bool]:
    result = generate_qc_plots(
        args.nc_file,
        qc_dir=args.qc_dir,
        comparison_store=args.comparison_store,
        preferred_member=args.preferred_member,
    )
    return args.nc_file, result is not None


# _SplitPlotArgs is module-level so _cli_combined_plot_one is picklable.
class _SplitPlotArgs(NamedTuple):
    nc_files: tuple[Path, ...]
    base_stem: str
    qc_dir: Path | None
    comparison_store: str | None
    preferred_member: str | None


def _cli_combined_plot_one(args: _SplitPlotArgs) -> tuple[str, bool]:
    result = generate_qc_plots_for_split_files(
        list(args.nc_files),
        qc_dir=args.qc_dir,
        comparison_store=args.comparison_store,
        preferred_member=args.preferred_member,
    )
    return args.base_stem, result is not None


def main(argv: list[str] | None = None) -> int:
    parser = _build_cli_parser()
    args = parser.parse_args(argv)

    # Collect all .nc files from the supplied paths.
    nc_files: list[Path] = []
    for raw in args.paths:
        p = Path(raw)
        if p.is_dir():
            nc_files.extend(sorted(p.rglob("*.nc")))
        elif p.suffix == ".nc":
            nc_files.append(p)
        else:
            parser.error(f"Not a .nc file or directory: {p}")

    if not nc_files:
        print("No .nc files found.")
        return 0

    qc_dir = Path(args.qc_dir) if args.qc_dir else None
    plot_args = [
        _PlotArgs(f, qc_dir, args.comparison_store, args.preferred_member)
        for f in nc_files
    ]

    # Detect split-file groups: files sharing the same base key (stem without
    # the time-range token) are concatenated into a single combined timeseries.
    groups = _group_by_base_key(nc_files)
    split_args = [
        _SplitPlotArgs(
            tuple(sorted(files)),
            base_key,
            qc_dir,
            args.comparison_store,
            args.preferred_member,
        )
        for base_key, files in groups.items()
        if len(files) > 1
    ]

    failed = 0
    if args.workers > 1:
        with ProcessPoolExecutor(max_workers=args.workers) as executor:
            for nc_file, ok in executor.map(_cli_plot_one, plot_args):
                _report(nc_file, ok)
                if not ok:
                    failed += 1
            for base_stem, ok in executor.map(_cli_combined_plot_one, split_args):
                _report_combined(base_stem, ok)
                if not ok:
                    failed += 1
    else:
        for pa in plot_args:
            _, ok = _cli_plot_one(pa)
            _report(pa.nc_file, ok)
            if not ok:
                failed += 1
        for sa in split_args:
            _, ok = _cli_combined_plot_one(sa)
            _report_combined(sa.base_stem, ok)
            if not ok:
                failed += 1

    return 1 if failed else 0


def _report(nc_file: Path, ok: bool) -> None:
    status = "OK  " if ok else "SKIP"
    print(f"{status} {nc_file}")


def _report_combined(base_stem: str, ok: bool) -> None:
    status = "OK  " if ok else "SKIP"
    print(f"{status} {base_stem} (combined timeseries)")


if __name__ == "__main__":
    raise SystemExit(main())
