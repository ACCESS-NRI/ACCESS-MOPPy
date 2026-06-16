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
import logging
import re
from functools import cache
from importlib.resources import files
from pathlib import Path
from typing import Optional, Union

logger = logging.getLogger(__name__)

__all__ = ["discover_files", "discover_year_range", "FileDiscoveryError"]


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


@cache
def _load_full_mappings(model_id: str) -> dict:
    """Load and return the complete mapping JSON for *model_id*."""
    mapping_dir = files("access_moppy").joinpath("mappings")
    model_file = f"{model_id}_mappings.json"
    entry = mapping_dir / model_file
    if not entry.is_file():
        raise FileDiscoveryError(
            f"No mapping file found for model '{model_id}'. "
            "Check that the model_id is correct and a mapping JSON is bundled."
        )
    return json.loads(entry.read_text(encoding="utf-8"))


def _find_variable_entry(all_mappings: dict, cmor_name: str) -> tuple[str, dict] | None:
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

    Spinup / alternative legacy pattern
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    * ``ocean-2d-surface_temp-1monthly-mean-ym_0001_01.nc`` → ``1``  (spinup, ``_YYYY_MM``)
    """
    name = path.stem  # strip .nc extension
    # Unified pattern: _YYYYMM-YYYYMM at end of stem (start of time range)
    m = re.search(r"_(\d{6})-\d{6}$", name)
    if m:
        return int(m.group(1)[:4])
    # Newer ACCESS convention: trailing _YYYY_MM (e.g. "...-mean-ym_0001_01")
    m = re.search(r"_(\d{4})_\d{2}$", name)
    if m:
        return int(m.group(1))
    # Legacy: _YYYY or _YYYY-MM at end of stem (ocean annual, ice monthly)
    m = re.search(r"_(\d{4})(?:-\d{2})?$", name)
    if m:
        return int(m.group(1))
    # Legacy: embedded YYYYMM inside stem (atmosphere: pa-185001_mon)
    m = re.search(r"-(\d{4})\d{2}(?:_|$)", name)
    if m:
        return int(m.group(1))
    # Fallback: last 4-digit sequence that looks like a plausible year.
    # Upper bound guards against matching unrelated integers; no lower bound
    # so that spinup years below 1000 are not silently dropped.
    candidates = [int(y) for y in re.findall(r"\d{4}", name) if int(y) <= 2999]
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
            f"Add a 'file_discovery.components.{component}' block to model_info "
            "or a per-variable 'file_pattern' to the mapping entry."
        )

    freq_patterns = comp_cfg.get("frequency_patterns", {})
    file_glob = freq_patterns.get(freq)
    if file_glob is None:
        raise FileDiscoveryError(
            f"No pattern for frequency '{freq}' under component '{component}'. "
            f"Available frequencies: {list(freq_patterns)}."
        )

    # A frequency may map to a single glob (string) or several alternative
    # globs (list).  The list form lets one mapping cover multiple raw-output
    # naming conventions that appear in different experiments — e.g. the legacy
    # "1mon-mean-y_*" and the newer "1monthly-mean-ym_*" ocean layouts.
    globs = [file_glob] if isinstance(file_glob, str) else list(file_glob)

    subdir = comp_cfg.get("subdir", "")
    output_dir_pattern = file_discovery_cfg.get(
        "output_dir_pattern", "output[0-9][0-9][0-9]"
    )

    patterns: list[str] = []
    for file_glob in globs:
        if "{model_var}" in file_glob:
            # Per-variable files (e.g. ocean): one pattern per model variable
            model_variables = var_entry.get("model_variables") or []
            if not model_variables:
                raise FileDiscoveryError(
                    f"Pattern '{file_glob}' requires {{model_var}} substitution but "
                    "the mapping entry has no 'model_variables'."
                )
            patterns.extend(
                f"{output_dir_pattern}/{subdir}/{file_glob.replace('{model_var}', mv)}"
                for mv in model_variables
            )
        else:
            # Single file per frequency (atmosphere, sea-ice): all vars packed in
            patterns.append(f"{output_dir_pattern}/{subdir}/{file_glob}")
    return patterns


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _diagnose_no_files(
    input_root: Path,
    compound_name: str,
    model_id: str,
    start_year: Optional[int],
    end_year: Optional[int],
) -> str:
    """Return a human-readable explanation of why ``discover_files`` found nothing.

    Called only when the main discovery returns an empty list.  Performs a
    second, cheaper pass to distinguish three failure modes:

    1. Year-filter excluded all matches — reports the years actually present.
    2. Glob matched nothing — reports the output directories searched and the
       patterns used.
    3. No output directories exist at all — suggests checking the archive root.
    """
    table, cmor_name = compound_name.split(".", 1)
    all_mappings = _load_full_mappings(model_id)
    model_info = all_mappings.get("model_info", {})
    file_discovery_cfg = model_info.get("file_discovery", {})

    found = _find_variable_entry(all_mappings, cmor_name)
    if found is None:
        return ""  # already handled by FileDiscoveryError before we reach this

    component, var_entry = found
    freq = _TABLE_TO_FREQ.get(table)
    if freq is None:
        return ""

    try:
        patterns = _build_patterns(var_entry, component, freq, file_discovery_cfg)
    except FileDiscoveryError:
        return ""

    # Glob without any year filter
    pre_filter: list[Path] = []
    for pattern in patterns:
        pre_filter.extend(Path(m) for m in _glob.glob(str(input_root / pattern)))

    if pre_filter:
        # Files exist — year filter was the culprit
        years = sorted(
            y for p in pre_filter if (y := _extract_year_from_path(p)) is not None
        )
        year_range = f"{years[0]}–{years[-1]}" if years else "unknown"
        if start_year is not None and end_year is not None:
            requested = f"{start_year}\u2013{end_year}"
        elif start_year is not None:
            requested = f"{start_year} onwards"
        elif end_year is not None:
            requested = f"up to {end_year}"
        else:
            requested = "unspecified"
        return (
            f"{len(pre_filter)} file(s) matched the '{freq}' pattern for '{cmor_name}' "
            f"(available years: {year_range}), but none fell within the requested "
            f"range ({requested})."
        )

    # No glob matches — check whether output directories exist at all
    output_dir_pattern = file_discovery_cfg.get(
        "output_dir_pattern", "output[0-9][0-9][0-9]"
    )
    output_dirs = sorted(input_root.glob(output_dir_pattern))
    if not output_dirs:
        return (
            f"No directories matching '{output_dir_pattern}' found under "
            f"'{input_root}'. Check that 'input_folder' points to the payu "
            "archive root (the directory that contains output000/, output001/, …)."
        )

    dir_summary = (
        f"{output_dirs[0].name}…{output_dirs[-1].name}"
        if len(output_dirs) > 1
        else output_dirs[0].name
    )
    return (
        f"No '{freq}' files for '{cmor_name}' found in {len(output_dirs)} output "
        f"director(y/ies) ({dir_summary}). "
        f"Patterns searched: {patterns}."
    )


def _parse_year_arg(value: Union[int, str, None], param: str) -> Optional[int]:
    """Normalise a *start_year* / *end_year* argument to a plain ``int``.

    Accepts integers directly and zero-padded strings such as ``"0001"``
    (common in pi-control experiments whose calendar begins at year 1).
    """
    if value is None or isinstance(value, int):
        return value
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError(
            f"'{param}' must be an integer or a zero-padded year string "
            f"(e.g. 1 or '0001'), got {value!r}."
        ) from None


def discover_files(
    input_root: str | Path,
    compound_name: str,
    model_id: str = "ACCESS-ESM1.6",
    start_year: Union[int, str, None] = None,
    end_year: Union[int, str, None] = None,
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
        Accepts an ``int`` or a zero-padded string (e.g. ``"0001"``), which
        is useful for pi-control experiments whose calendar starts at year 1.
    end_year:
        When given, files whose year is strictly after *end_year* are excluded.
        Accepts the same formats as *start_year*.

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
    start_year = _parse_year_arg(start_year, "start_year")
    end_year = _parse_year_arg(end_year, "end_year")

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

    result = sorted(found_paths)

    if result:
        years = [y for p in result if (y := _extract_year_from_path(p)) is not None]
        if years:
            logger.info(
                "discover_files: %d file(s) found for '%s' — year range %d–%d",
                len(result),
                compound_name,
                min(years),
                max(years),
            )
        else:
            logger.info(
                "discover_files: %d file(s) found for '%s' (year range unknown)",
                len(result),
                compound_name,
            )
    else:
        logger.info("discover_files: no files found for '%s'", compound_name)

    return result


def discover_year_range(
    input_root: str | Path,
    compound_name: str,
    model_id: str = "ACCESS-ESM1.6",
) -> tuple[int, int] | None:
    """Return the ``(first_year, last_year)`` span of available raw output files.

    The range is derived purely from filenames — no file I/O is performed —
    so it is cheap even for large archives.

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

    Returns
    -------
    tuple[int, int] | None
        ``(first_year, last_year)`` if any files are found, ``None`` otherwise.

    Raises
    ------
    FileDiscoveryError
        Same conditions as :func:`discover_files`.
    """
    # discover_files with no year filter — still filename-only, no file I/O
    paths = discover_files(input_root, compound_name, model_id=model_id)
    if not paths:
        return None
    years = [y for p in paths if (y := _extract_year_from_path(p)) is not None]
    if not years:
        return None
    return min(years), max(years)
