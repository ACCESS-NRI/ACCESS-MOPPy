"""
Automatic file discovery for ACCESS model raw output.

Given a CMIP compound name (e.g. ``"Omon.tos"``), this module finds the
corresponding raw NetCDF files under a payu archive root without the user
having to specify glob patterns manually.

Discovery resolution order
--------------------------
1. Per-variable ``file_pattern`` in the mapping entry — explicit override,
   useful for edge-cases or legacy folder layouts.
2. Component-level ``frequency_patterns`` from the ``model_info.file_discovery``
   block in the model mapping JSON, with ``{model_var}`` substituted by every
   entry in ``model_variables`` (one file per model variable, as used by the
   ocean component).  Atmosphere and sea-ice components pack all variables into
   a single file per frequency, so no substitution is needed there.
3. :class:`FileDiscoveryError` is raised when neither source provides a pattern.

Year-based filtering
--------------------
Pass ``start_year`` and/or ``end_year`` to restrict the returned file list to
a particular time range.  Filtering is performed by parsing the year directly
from the filename (no file I/O), so it is cheap even for large archives.
"""

from __future__ import annotations

import glob as _glob
import json
import re
from importlib.resources import files
from pathlib import Path
from typing import Optional

__all__ = ["discover_files", "FileDiscoveryError"]


# ---------------------------------------------------------------------------
# CMIP table → frequency key used inside file_discovery patterns
# ---------------------------------------------------------------------------

_TABLE_TO_FREQ: dict[str, str] = {
    # Monthly
    "Amon": "mon",
    "Lmon": "mon",
    "Omon": "mon",
    "SImon": "mon",
    "CFmon": "mon",
    "AERmon": "mon",
    "Emon": "mon",
    "LImon": "mon",
    "OImon": "mon",
    # Daily
    "day": "day",
    "Oday": "day",
    "SIday": "day",
    "Aday": "day",
    # 3-hourly
    "3hr": "3hr",
    "E3hr": "3hr",
    "CF3hr": "3hr",
    # 6-hourly
    "6hrLev": "6hr",
    "6hrPlev": "6hr",
    "6hrPlevPt": "6hr",
    "E6hrZ": "6hr",
    # Sub-hourly
    "1hr": "subhr",
    "Esubhr": "subhr",
    # Yearly
    "Oyr": "yr",
    "yr": "yr",
    "Eyr": "yr",
    # Fixed / time-invariant
    "fx": "fx",
    "Ofx": "fx",
}

# Search order for components inside a mapping JSON
_COMPONENT_SEARCH_ORDER = (
    "aerosol",
    "atmosphere",
    "land",
    "landIce",
    "ocean",
    "oceanBgchem",
    "sea_ice",
)


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class FileDiscoveryError(Exception):
    """Raised when no file pattern can be determined for a variable."""


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _load_full_mappings(model_id: str) -> dict:
    """Load and return the complete mapping JSON for *model_id*."""
    mapping_dir = files("access_moppy.mappings")
    model_file = f"{model_id}_mappings.json"
    entry = mapping_dir / model_file
    if not entry.is_file():
        raise FileDiscoveryError(
            f"No mapping file found for model '{model_id}'. "
            "Check that the model_id is correct and a mapping JSON is bundled."
        )
    return json.loads(entry.read_text(encoding="utf-8"))


def _find_variable_entry(
    all_mappings: dict, cmor_name: str
) -> tuple[str, dict] | None:
    """Return ``(component, variable_entry)`` for *cmor_name*, or ``None``."""
    for component in _COMPONENT_SEARCH_ORDER:
        comp_data = all_mappings.get(component, {})
        if cmor_name in comp_data:
            return component, comp_data[cmor_name]
    return None


def _extract_year_from_path(path: Path) -> int | None:
    """Parse the start year from a model output filename.

    Handles all known ACCESS filename conventions:

    Legacy patterns
    ~~~~~~~~~~~~~~~
    * ``ocean-2d-tos-1mon-mean-y_1850.nc``   → ``1850``  (ocean, annual file)
    * ``iceh-1monthly-mean_1850-01.nc``       → ``1850``  (ice, YYYY-MM suffix)
    * ``aiihca.pa-185001_mon.nc``             → ``1850``  (atmosphere, embedded YYYYMM)

    Proposed unified pattern (``YYYYMM-YYYYMM`` range)
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    * ``tos_mean_ocean_1mon_185001-185012.nc`` → ``1850``  (start of range)
    * ``wt_mean_ocean_1yr_234501-234512.nc``   → ``2345``
    """
    name = path.stem  # strip .nc extension
    # Unified pattern: _YYYYMM-YYYYMM at end of stem (start of time range)
    m = re.search(r"_(\d{6})-\d{6}$", name)
    if m:
        return int(m.group(1)[:4])
    # Legacy: _YYYY or _YYYY-MM at end of stem (ocean annual, ice monthly)
    m = re.search(r"_(\d{4})(?:-\d{2})?$", name)
    if m:
        return int(m.group(1))
    # Legacy: embedded YYYYMM inside stem (atmosphere: pa-185001_mon)
    m = re.search(r"-(\d{4})\d{2}(?:_|$)", name)
    if m:
        return int(m.group(1))
    # Fallback: last 4-digit sequence that looks like a plausible year
    candidates = [int(y) for y in re.findall(r"\d{4}", name) if 1000 <= int(y) <= 2999]
    return candidates[-1] if candidates else None


def _build_patterns(
    var_entry: dict,
    component: str,
    freq: str,
    file_discovery_cfg: dict,
) -> list[str]:
    """Return a list of relative glob patterns (relative to ``input_root``).

    Patterns are built from the per-variable ``file_pattern`` override if
    present, otherwise from the component-level ``frequency_patterns`` in
    ``file_discovery_cfg``.

    Each pattern is relative to the run root so that the caller can prepend
    ``input_root`` before globbing.
    """
    # --- Per-variable explicit override ---
    explicit = var_entry.get("file_pattern")
    if explicit:
        # Explicit patterns are already relative to input_root
        return [explicit] if isinstance(explicit, str) else list(explicit)

    # --- Component-level config ---
    comp_cfg = file_discovery_cfg.get("components", {}).get(component)
    if comp_cfg is None:
        raise FileDiscoveryError(
            f"No file_discovery config for component '{component}'. "
            "Add a 'file_discovery.components.{component}' block to model_info "
            "or a per-variable 'file_pattern' to the mapping entry."
        )

    freq_patterns = comp_cfg.get("frequency_patterns", {})
    file_glob = freq_patterns.get(freq)
    if file_glob is None:
        raise FileDiscoveryError(
            f"No pattern for frequency '{freq}' under component '{component}'. "
            f"Available frequencies: {list(freq_patterns)}."
        )

    subdir = comp_cfg.get("subdir", "")
    output_dir_pattern = file_discovery_cfg.get(
        "output_dir_pattern", "output[0-9][0-9][0-9]"
    )

    if "{model_var}" in file_glob:
        # Per-variable files (e.g. ocean): one pattern per model variable
        model_variables = var_entry.get("model_variables") or []
        if not model_variables:
            raise FileDiscoveryError(
                f"Pattern '{file_glob}' requires {{model_var}} substitution but "
                "the mapping entry has no 'model_variables'."
            )
        return [
            f"{output_dir_pattern}/{subdir}/{file_glob.replace('{model_var}', mv)}"
            for mv in model_variables
        ]
    else:
        # Single file per frequency (atmosphere, sea-ice): all vars packed in
        return [f"{output_dir_pattern}/{subdir}/{file_glob}"]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def discover_files(
    input_root: str | Path,
    compound_name: str,
    model_id: str = "ACCESS-ESM1.6",
    start_year: Optional[int] = None,
    end_year: Optional[int] = None,
) -> list[Path]:
    """Discover raw model output files for a CMIP variable.

    Parameters
    ----------
    input_root:
        Root directory of the payu archive (contains ``output000/``,
        ``output001/``, …).
    compound_name:
        CMIP compound name such as ``"Omon.tos"`` or ``"Amon.tas"``.
    model_id:
        ACCESS model identifier that selects the mapping JSON.
        Defaults to ``"ACCESS-ESM1.6"``.
    start_year:
        When given, files whose year (parsed from the filename) is strictly
        before *start_year* are excluded.  No file I/O is performed.
    end_year:
        When given, files whose year is strictly after *end_year* are excluded.

    Returns
    -------
    list[Path]
        Sorted, deduplicated list of matching :class:`~pathlib.Path` objects.

    Raises
    ------
    FileDiscoveryError
        If the variable has no mapping, the component has no
        ``file_discovery`` config, or the frequency is not recognised.
    ValueError
        If *compound_name* is not in ``"table.variable"`` format.

    Examples
    --------
    Discover all monthly ocean surface-temperature files::

        files = discover_files("/g/data/.../archive", "Omon.tos")

    Restrict to a decade::

        files = discover_files(
            "/g/data/.../archive", "Amon.tas",
            start_year=1990, end_year=1999,
        )
    """
    if "." not in compound_name:
        raise ValueError(
            f"Invalid compound_name '{compound_name}'. "
            "Expected 'table.variable' format, e.g. 'Omon.tos'."
        )

    input_root = Path(input_root)
    table, cmor_name = compound_name.split(".", 1)

    all_mappings = _load_full_mappings(model_id)
    model_info = all_mappings.get("model_info", {})
    file_discovery_cfg = model_info.get("file_discovery", {})

    # Locate the variable in the mapping
    found = _find_variable_entry(all_mappings, cmor_name)
    if found is None:
        raise FileDiscoveryError(
            f"Variable '{cmor_name}' not found in mappings for model '{model_id}'. "
            "Add a mapping entry or specify an explicit 'file_pattern' in the mapping."
        )
    component, var_entry = found

    # Determine output frequency from the CMIP table name
    freq = _TABLE_TO_FREQ.get(table)
    if freq is None:
        raise FileDiscoveryError(
            f"Unknown CMIP table '{table}' — cannot determine output frequency. "
            "Add it to file_discovery._TABLE_TO_FREQ or use a per-variable "
            "'file_pattern' in the mapping entry."
        )

    # Build glob patterns (relative to input_root)
    patterns = _build_patterns(var_entry, component, freq, file_discovery_cfg)

    # Glob
    found_paths: set[Path] = set()
    for pattern in patterns:
        full_pattern = str(input_root / pattern)
        for match in _glob.glob(full_pattern):
            found_paths.add(Path(match))

    # Year-based filtering — filename-only, no file I/O
    if start_year is not None or end_year is not None:
        filtered: set[Path] = set()
        for p in found_paths:
            year = _extract_year_from_path(p)
            if year is None:
                # Cannot parse year — keep the file to be safe
                filtered.add(p)
                continue
            if start_year is not None and year < start_year:
                continue
            if end_year is not None and year > end_year:
                continue
            filtered.add(p)
        found_paths = filtered

    return sorted(found_paths)
