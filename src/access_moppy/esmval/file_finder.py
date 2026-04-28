"""
access_moppy.esmval.file_finder
=================================

Locate raw ACCESS-ESM1.6 model output files on disk for a given set of
model variables and a time range.

The raw ACCESS-ESM1.6 archive follows a conventional directory layout::

    <input_root>/
        output<NNN>/
            atmosphere/netCDF/
                <run_id>.<stream>-<YYYYMM>_<freq>.nc   # e.g. aiihca.pa-096110_mon.nc
            ocean/
                ocean-2d-<field>-1monthly-mean-ym_<YYYY>_<MM>.nc
            ice/
                iceh-1monthly-mean_<YYYYMM>-<DD>.nc

This module provides :class:`RawFileFinder` which:

1. Accepts a configurable root directory and optional glob-pattern overrides
   per compound name.
2. Resolves the required model variables from an :class:`~access_moppy.esmval.variable_mapper.VariableIndex`.
3. Returns a list of file paths covering the requested time range.
"""

from __future__ import annotations

import glob
import logging
import re
from pathlib import Path
from typing import Sequence

from access_moppy.esmval.variable_mapper import VariableIndex

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Default glob patterns (relative to input_root)
# ---------------------------------------------------------------------------
# These mirror the patterns used in the example batch_config.yml.  They are
# deliberately broad — the caller is responsible for further time filtering.

_DEFAULT_PATTERNS: dict[str, list[str]] = {
    # component → list of glob patterns (relative paths)
    "atmosphere": [
        "output[0-9]*/atmosphere/netCDF/*mon.nc",
        "output[0-9]*/atmosphere/netCDF/*dai.nc",
        "output[0-9]*/atmosphere/netCDF/*3hr.nc",
        "output[0-9]*/atmosphere/netCDF/*6hr.nc",
    ],
    "aerosol": [
        "output[0-9]*/atmosphere/netCDF/*mon.nc",
    ],
    "land": [
        "output[0-9]*/atmosphere/netCDF/*mon.nc",
    ],
    "landIce": [
        "output[0-9]*/atmosphere/netCDF/*mon.nc",
    ],
    "ocean": [
        "output[0-9]*/ocean/ocean-1d-*.nc",
        "output[0-9]*/ocean/ocean-2d-*.nc",
        "output[0-9]*/ocean/ocean-3d-*.nc",
        "output[0-9]*/ocean/ocean-scalar-*.nc",
    ],
    "oceanBgchem": [
        "output[0-9]*/ocean/ocean-2d-*.nc",
        "output[0-9]*/ocean/ocean-3d-*.nc",
    ],
    "sea_ice": [
        "output[0-9]*/ice/iceh-1monthly-mean*.nc",
        "output[0-9]*/ice/iceh-1daily-mean*.nc",
    ],
    "time_invariant": [
        "output[0-9]*/atmosphere/netCDF/*mon.nc",
        "output[0-9]*/ocean/ocean-*.nc",
        "output[0-9]*/ice/iceh-*.nc",
    ],
}


class RawFileFinder:
    """Locate raw ACCESS-ESM1.6 model output files.

    Parameters
    ----------
    input_root:
        Root directory of the raw ACCESS-ESM1.6 archive, e.g.
        ``"/g/data/p73/archive/CMIP7/ACCESS-ESM1-6/spinup/MyRun"``.
    variable_index:
        :class:`~access_moppy.esmval.variable_mapper.VariableIndex` used to
        resolve compound names to raw model variables.
    pattern_overrides:
        Optional mapping of ``"<mip>.<short_name>"`` → glob pattern (relative
        to *input_root*) that overrides the default component-level pattern for
        a specific variable.  Mirrors the ``file_patterns`` key in the batch
        config YAML.

    Examples
    --------
    >>> finder = RawFileFinder(input_root="/path/to/archive")
    >>> files = finder.find("Amon.tas")
    >>> files = finder.find("Omon.tos", timerange="1850/1860")
    """

    def __init__(
        self,
        input_root: str | Path,
        variable_index: VariableIndex | None = None,
        pattern_overrides: dict[str, str] | None = None,
    ) -> None:
        self._root = Path(input_root)
        self._index = variable_index or VariableIndex()
        self._overrides: dict[str, str] = pattern_overrides or {}

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def input_root(self) -> Path:
        return self._root

    def find(
        self,
        compound_name: str,
        timerange: str = "",
    ) -> list[Path]:
        """Return the raw input files needed to CMORise *compound_name*.

        Parameters
        ----------
        compound_name:
            ACCESS-MOPPy compound name, e.g. ``"Amon.tas"``.
        timerange:
            Optional ISO 8601 time range string (``"start/end"``) used to
            narrow down which output cycle directories to include.  When
            empty, all matching files are returned.

        Returns
        -------
        list[Path]
            Sorted list of matching absolute file paths.  An empty list is
            returned when no files are found.
        """
        mip, short_name = _split_compound(compound_name)
        entry = self._index.get(mip, short_name)

        if entry is None:
            logger.warning(
                "No mapping found for '%s' — cannot locate raw files.",
                compound_name,
            )
            return []

        # Variables backed by a bundled resource file need no raw input
        if entry.resource_file is not None:
            return []

        # Internal calculations generate data from scratch
        if entry.calculation_type == "internal":
            return []

        patterns = self._resolve_patterns(compound_name, entry.component)
        start_year, end_year = _parse_timerange(timerange)

        found: set[Path] = set()
        for pattern in patterns:
            full_pattern = str(self._root / pattern)
            for path_str in glob.glob(full_pattern, recursive=False):
                p = Path(path_str)
                if _path_in_timerange(p, start_year, end_year):
                    found.add(p)

        if not found:
            logger.warning(
                "No raw files found for '%s' under '%s'. "
                "Check input_root and consider adding a pattern_override.",
                compound_name,
                self._root,
            )

        return sorted(found)

    def find_many(
        self,
        compound_names: Sequence[str],
        timerange: str = "",
    ) -> dict[str, list[Path]]:
        """Call :meth:`find` for each compound name in *compound_names*.

        Returns
        -------
        dict[str, list[Path]]
            Mapping of compound_name → sorted file list.
        """
        return {cn: self.find(cn, timerange=timerange) for cn in compound_names}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_patterns(self, compound_name: str, component: str) -> list[str]:
        """Return the glob pattern(s) to use for *compound_name*."""
        if compound_name in self._overrides:
            raw = self._overrides[compound_name]
            # Strip a leading slash so it is relative to input_root
            return [raw.lstrip("/")]
        return _DEFAULT_PATTERNS.get(component, ["output[0-9]*/**/*.nc"])


# ---------------------------------------------------------------------------
# Utility functions
# ---------------------------------------------------------------------------


def _split_compound(compound_name: str) -> tuple[str, str]:
    """Split ``"Amon.tas"`` into ``("Amon", "tas")``."""
    parts = compound_name.split(".", 1)
    if len(parts) != 2:
        raise ValueError(
            f"Invalid compound_name '{compound_name}'. Expected '<mip>.<short_name>'."
        )
    return parts[0], parts[1]


def _parse_timerange(timerange: str) -> tuple[int | None, int | None]:
    """Parse an ISO 8601-ish time range string into (start_year, end_year).

    Handles the common ESMValTool ``"1850/2014"`` format as well as
    ``"1850-01-01/2014-12-31"`` extended representations.  Returns
    ``(None, None)`` when *timerange* is empty or cannot be parsed.
    """
    if not timerange:
        return None, None
    parts = timerange.split("/")
    if len(parts) != 2:
        return None, None
    try:
        start_year = _year_from_token(parts[0])
        end_year = _year_from_token(parts[1])
        return start_year, end_year
    except (ValueError, IndexError):
        logger.debug("Could not parse timerange '%s'.", timerange)
        return None, None


def _year_from_token(token: str) -> int | None:
    """Extract the four-digit year from a time token like ``"1850"`` or
    ``"1850-01-01"``."""
    # Match the first four consecutive digits as the year
    m = re.match(r"(\d{4})", token.strip())
    if m:
        return int(m.group(1))
    return None


# Patterns that appear in typical ACCESS-ESM1.6 output file names to extract
# a year.  Order matters: try specific ones first.
_YEAR_PATTERNS: list[re.Pattern] = [
    # ocean files: ocean-2d-...-ym_YYYY_MM.nc
    re.compile(r"ym_(\d{4})_\d{2}"),
    # atmosphere files: aiihca.pa-YYYYMM_mon.nc
    re.compile(r"-(\d{4})\d{2}_"),
    # ice files: iceh-1monthly-mean_YYYYMM-DD.nc
    re.compile(r"_(\d{4})\d{2}[-.]"),
    # generic: any 4-digit year preceded by non-digit
    re.compile(r"(?<!\d)(\d{4})(?!\d)"),
]


def _extract_year_from_path(path: Path) -> int | None:
    """Attempt to extract a representative year from a file name."""
    name = path.stem
    for pattern in _YEAR_PATTERNS:
        m = pattern.search(name)
        if m:
            year = int(m.group(1))
            # Sanity check: climate model years are typically 0001–9999
            if 1 <= year <= 9999:
                return year
    return None


def _path_in_timerange(
    path: Path,
    start_year: int | None,
    end_year: int | None,
) -> bool:
    """Return ``True`` when *path* falls within the requested time range.

    If no time range constraint is active (both bounds are ``None``), every
    path is accepted.  When only one bound is set, only that bound is
    enforced.  When the year cannot be determined from the file name the
    file is conservatively included.
    """
    if start_year is None and end_year is None:
        return True

    year = _extract_year_from_path(path)
    if year is None:
        # Cannot determine year → include to be safe
        return True

    if start_year is not None and year < start_year:
        return False
    if end_year is not None and year > end_year:
        return False
    return True
