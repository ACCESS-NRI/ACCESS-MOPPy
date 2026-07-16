"""Load ACCESS-ESM1-5 CMIP6 reference timeseries for QC plot comparison.

This module reads a Parquet-backed catalog of ACCESS-ESM1-5 CMIP6 global
timeseries summaries and returns overlay data for use in QC diagnostic plots.

The external store is referenced by path and is expected to contain:

* ``catalog.parquet`` or ``catalog.csv`` — a flat catalog of all available
  timeseries with columns including ``variable``, ``table_id``, ``model``,
  ``experiment``, ``member_id``, ``grid_label``, ``period``, and
  ``parquet_path``.
* ``timeseries/`` — a Hive-partitioned directory tree of Parquet files with
  columns: ``time``, ``global_min``, ``global_max``, ``global_mean``,
  ``nh_min``, ``nh_max``, ``nh_mean``, ``sh_min``, ``sh_max``, ``sh_mean``,
  plus the partition key columns.

All public functions are **lenient**: any I/O failure, missing import, or
absent catalog entry returns ``None`` and emits at most a debug log message.
No exception is ever propagated to the caller.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd

_log = logging.getLogger(__name__)


@dataclass(frozen=True)
class OverlayData:
    """Pre-loaded comparison timeseries for a single ensemble member.

    Attributes
    ----------
    member_id:
        CMIP ensemble member label (e.g. ``"r1i1p1f1"``).
    time:
        1-D numpy array of ``numpy.datetime64`` timestamps.
    global_mean:
        1-D float array of per-timestep global-mean values.
    global_min:
        1-D float array of per-timestep global-minimum values, or ``None``.
    global_max:
        1-D float array of per-timestep global-maximum values, or ``None``.
    """

    member_id: str
    time: Any  # np.ndarray[datetime64]
    global_mean: Any  # np.ndarray[float]
    global_min: Any | None  # np.ndarray[float] | None
    global_max: Any | None  # np.ndarray[float] | None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _load_catalog(store_path: Path) -> "pd.DataFrame | None":
    """Load ``catalog.parquet`` or ``catalog.csv`` from *store_path*."""
    try:
        import pandas as pd  # noqa: PLC0415
    except ImportError:
        _log.debug("pandas not available; CMIP6 comparison overlay disabled")
        return None

    parquet_cat = store_path / "catalog.parquet"
    csv_cat = store_path / "catalog.csv"

    if parquet_cat.exists():
        try:
            df = pd.read_parquet(parquet_cat)
            _log.debug("Loaded CMIP6 catalog from %s (%d rows)", parquet_cat, len(df))
            return df
        except Exception as exc:
            _log.debug("Could not read catalog.parquet (%s); trying catalog.csv", exc)

    if csv_cat.exists():
        try:
            df = pd.read_csv(csv_cat, low_memory=False)
            _log.debug("Loaded CMIP6 catalog from %s (%d rows)", csv_cat, len(df))
            return df
        except Exception as exc:
            _log.debug("Could not read catalog.csv: %s", exc)

    _log.debug("No catalog (catalog.parquet / catalog.csv) found in %s", store_path)
    return None


def _resolve_parquet_path(store_path: Path, row: Any) -> Path:
    """Reconstruct the actual Parquet file path for a catalog row.

    The ``parquet_path`` column in the catalog may contain an absolute path
    from a different installation.  We extract the relative portion starting
    at ``timeseries/`` and re-root it under *store_path*.  If the column is
    absent or malformed we fall back to constructing the path from individual
    partition keys.
    """
    raw = str(row.get("parquet_path", ""))
    if "/timeseries/" in raw:
        rel = "timeseries/" + raw.split("/timeseries/", 1)[1]
        return store_path / rel

    # Fallback: rebuild from partition fields
    return (
        store_path
        / "timeseries"
        / f"variable={row['variable']}"
        / f"table_id={row['table_id']}"
        / f"model={row['model']}"
        / f"experiment={row['experiment']}"
        / f"member_id={row['member_id']}"
        / f"grid_label={row['grid_label']}"
        / f"period={row['period']}"
        / "summary_kind=min_max_mean_timeseries.parquet"
    )


def _pick_member(members: list[str], preferred: str | None) -> str:
    """Select a deterministic ensemble member from *members*.

    Priority:
    1. *preferred* if it is present in *members*.
    2. ``"r1i1p1f1"`` if present (stable, widely-used default).
    3. Lexicographically first member.
    """
    if preferred and preferred in members:
        return preferred
    if "r1i1p1f1" in members:
        return "r1i1p1f1"
    return sorted(members)[0]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_comparison_timeseries(
    store_path: str | Path,
    variable: str,
    table_id: str,
    experiment: str,
    grid_label: str | None = None,
    preferred_member: str | None = None,
) -> OverlayData | None:
    """Return pre-loaded overlay data for the best-matching ensemble member.

    Searches the external Parquet store at *store_path* for timeseries that
    match *variable*, *table_id*, *experiment* (and optionally *grid_label*),
    then loads and concatenates all period-chunks for a single member.

    Parameters
    ----------
    store_path:
        Root directory of the external Parquet store.
    variable:
        CMIP variable short name (e.g. ``"tas"``).
    table_id:
        CMIP MIP table (e.g. ``"Amon"``).
    experiment:
        CMIP experiment identifier (e.g. ``"historical"``).
    grid_label:
        Optional CMIP grid label (e.g. ``"gn"``).  When supplied the catalog
        filter includes an exact match on ``grid_label``.
    preferred_member:
        Optional preferred ensemble member label.  Falls back to
        ``"r1i1p1f1"`` and then to the lexicographically first member.

    Returns
    -------
    OverlayData | None
        The loaded overlay data, or ``None`` when no match is found or any
        error occurs.
    """
    store_path = Path(store_path)
    if not store_path.exists():
        _log.debug("CMIP6 comparison store not found: %s", store_path)
        return None

    catalog = _load_catalog(store_path)
    if catalog is None:
        return None

    # ------------------------------------------------------------------
    # Filter catalog
    # ------------------------------------------------------------------
    try:
        import pandas as pd  # noqa: PLC0415
    except ImportError:
        return None

    mask = (
        (catalog["variable"] == variable)
        & (catalog["table_id"] == table_id)
        & (catalog["experiment"] == experiment)
    )
    if grid_label and "grid_label" in catalog.columns:
        mask &= catalog["grid_label"] == grid_label

    subset = catalog[mask]
    if subset.empty:
        _log.debug(
            "No CMIP6 reference for %s/%s/%s (grid=%s) in %s",
            variable,
            table_id,
            experiment,
            grid_label,
            store_path,
        )
        return None

    # ------------------------------------------------------------------
    # Pick member and load period chunks
    # ------------------------------------------------------------------
    all_members = subset["member_id"].unique().tolist()
    member = _pick_member(all_members, preferred_member)
    member_rows = subset[subset["member_id"] == member]

    frames: list[Any] = []
    for _, row in member_rows.iterrows():
        pq_path = _resolve_parquet_path(store_path, row)
        if not pq_path.exists():
            _log.debug("Parquet file not found: %s", pq_path)
            continue
        try:
            df = pd.read_parquet(pq_path)
            frames.append(df)
        except Exception as exc:
            _log.debug("Could not read %s: %s", pq_path, exc)

    if not frames:
        _log.debug(
            "No parquet data loaded for %s/%s/%s member=%s",
            variable,
            table_id,
            experiment,
            member,
        )
        return None

    # ------------------------------------------------------------------
    # Combine, sort by time, return
    # ------------------------------------------------------------------
    combined = pd.concat(frames, ignore_index=True)
    combined = combined.sort_values("time").reset_index(drop=True)

    time_vals = pd.to_datetime(combined["time"]).to_numpy()
    global_mean = combined["global_mean"].to_numpy(dtype=float)
    global_min = (
        combined["global_min"].to_numpy(dtype=float)
        if "global_min" in combined.columns
        else None
    )
    global_max = (
        combined["global_max"].to_numpy(dtype=float)
        if "global_max" in combined.columns
        else None
    )

    _log.debug(
        "Loaded CMIP6 reference for %s/%s/%s member=%s (%d timesteps)",
        variable,
        table_id,
        experiment,
        member,
        len(time_vals),
    )
    return OverlayData(
        member_id=member,
        time=time_vals,
        global_mean=global_mean,
        global_min=global_min,
        global_max=global_max,
    )
