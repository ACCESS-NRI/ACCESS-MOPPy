"""
access_moppy.esmval.variable_mapper
=====================================

Builds a queryable index from ACCESS-MOPPy's model-mapping JSON files so
that ``(mip_table, short_name)`` pairs can be resolved to compound names
and raw model variable names with a single look-up.

The index is constructed lazily on first use and cached as a class
attribute so repeated instantiation is cheap.
"""

from __future__ import annotations

import json
import logging
from importlib.resources import files
from pathlib import Path
from typing import NamedTuple

logger = logging.getLogger(__name__)

_COMPONENTS = (
    "aerosol",
    "atmosphere",
    "land",
    "landIce",
    "ocean",
    "oceanBgchem",
    "sea_ice",
    "time_invariant",
)


class MappingEntry(NamedTuple):
    """Resolved mapping information for one CMIP variable.

    Attributes
    ----------
    compound_name:
        ACCESS-MOPPy compound name, e.g. ``"Amon.tas"``.
    model_variables:
        Raw model variable names required to compute this variable
        (e.g. UM STASH codes, MOM5/MOM6 names, CICE names).
    component:
        Which model component owns this variable (``"atmosphere"``,
        ``"ocean"``, ``"sea_ice"``, …).
    calculation_type:
        The calculation type string from the mapping: ``"direct"``,
        ``"formula"``, ``"dataset_function"``, or ``"internal"``.
    resource_file:
        Bundled resource file name when no external input is required
        (``None`` for most variables).
    """

    compound_name: str
    model_variables: list[str]
    component: str
    calculation_type: str
    resource_file: str | None


class VariableIndex:
    """Index of all mappable variables for one ACCESS model.

    Parameters
    ----------
    model_id:
        Model identifier whose mapping JSON should be loaded, e.g.
        ``"ACCESS-ESM1.6"``.  Defaults to ``"ACCESS-ESM1.6"``.

    Examples
    --------
    >>> idx = VariableIndex()
    >>> entry = idx.get("Amon", "tas")
    >>> entry.compound_name
    'Amon.tas'
    >>> entry.model_variables
    ['fld_s03i236']
    >>> idx.is_supported("Amon", "tas")
    True
    >>> idx.is_supported("Amon", "notavariable")
    False
    """

    _cache: dict[str, dict[tuple[str, str], MappingEntry]] = {}

    def __init__(self, model_id: str = "ACCESS-ESM1.6") -> None:
        self._model_id = model_id
        if model_id not in VariableIndex._cache:
            VariableIndex._cache[model_id] = self._build_index(model_id)
        self._index: dict[tuple[str, str], MappingEntry] = VariableIndex._cache[
            model_id
        ]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    @property
    def model_id(self) -> str:
        return self._model_id

    def get(self, mip: str, short_name: str) -> MappingEntry | None:
        """Return the :class:`MappingEntry` for ``(mip, short_name)``, or
        ``None`` when the combination is not in the index."""
        return self._index.get((mip, short_name))

    def is_supported(self, mip: str, short_name: str) -> bool:
        """Return ``True`` when ACCESS-MOPPy can produce this variable."""
        return (mip, short_name) in self._index

    def unsupported(self, pairs: list[tuple[str, str]]) -> list[tuple[str, str]]:
        """Filter *pairs* to those that are **not** in the index."""
        return [(mip, sn) for mip, sn in pairs if not self.is_supported(mip, sn)]

    def all_compound_names(self) -> list[str]:
        """Return all compound names present in the index."""
        return sorted({e.compound_name for e in self._index.values()})

    # ------------------------------------------------------------------
    # Index construction
    # ------------------------------------------------------------------

    @staticmethod
    def _build_index(model_id: str) -> dict[tuple[str, str], MappingEntry]:
        """Read the model mapping JSON and return a flat (mip, short_name)
        → :class:`MappingEntry` dictionary."""
        mapping_dir = files("access_moppy.mappings")
        mapping_file = mapping_dir / f"{model_id}_mappings.json"

        if not mapping_file.is_file():
            logger.warning(
                "No mapping file found for model '%s'. "
                "Variable index will be empty.",
                model_id,
            )
            return {}

        raw: dict = json.loads(mapping_file.read_text(encoding="utf-8"))
        index: dict[tuple[str, str], MappingEntry] = {}

        for component in _COMPONENTS:
            comp_dict = raw.get(component, {})
            if not isinstance(comp_dict, dict):
                continue
            for cmor_name, var_def in comp_dict.items():
                if not isinstance(var_def, dict):
                    continue

                # Infer the MIP table from the dimensions or the table
                # stored inside the variable definition (if present).
                # Prefer the "table" key; fall back to scanning known tables.
                mip = var_def.get("table") or _infer_mip(cmor_name, component)

                calc: dict = var_def.get("calculation", {})
                calc_type: str = calc.get("type", "direct")
                model_vars = var_def.get("model_variables") or []
                resource_file: str | None = var_def.get("ressource_file")

                compound_name = f"{mip}.{cmor_name}"
                entry = MappingEntry(
                    compound_name=compound_name,
                    model_variables=list(model_vars),
                    component=component,
                    calculation_type=calc_type,
                    resource_file=resource_file,
                )

                key = (mip, cmor_name)
                if key in index:
                    logger.debug(
                        "Duplicate mapping for (%s, %s) — keeping first occurrence.",
                        mip,
                        cmor_name,
                    )
                else:
                    index[key] = entry

        logger.debug(
            "VariableIndex built for '%s': %d entries across %d components.",
            model_id,
            len(index),
            len(_COMPONENTS),
        )
        return index

    # ------------------------------------------------------------------
    # Alternate constructor: load from an arbitrary JSON path (testing)
    # ------------------------------------------------------------------

    @classmethod
    def from_json(
        cls, json_path: str | Path, model_id: str = "custom"
    ) -> "VariableIndex":
        """Build a :class:`VariableIndex` from an explicit JSON file path.

        Useful for unit tests that supply their own mapping fixture.
        """
        obj = cls.__new__(cls)
        obj._model_id = model_id
        raw = json.loads(Path(json_path).read_text(encoding="utf-8"))
        obj._index = {}
        for component in _COMPONENTS:
            comp_dict = raw.get(component, {})
            if not isinstance(comp_dict, dict):
                continue
            for cmor_name, var_def in comp_dict.items():
                if not isinstance(var_def, dict):
                    continue
                mip = var_def.get("table") or _infer_mip(cmor_name, component)
                calc = var_def.get("calculation", {})
                entry = MappingEntry(
                    compound_name=f"{mip}.{cmor_name}",
                    model_variables=list(var_def.get("model_variables") or []),
                    component=component,
                    calculation_type=calc.get("type", "direct"),
                    resource_file=var_def.get("ressource_file"),
                )
                obj._index[(mip, cmor_name)] = entry
        return obj


# ---------------------------------------------------------------------------
# Heuristic MIP table inference
# ---------------------------------------------------------------------------

# Component → most likely default table when the mapping does not include one
_COMPONENT_DEFAULT_TABLE: dict[str, str] = {
    "atmosphere": "Amon",
    "aerosol": "AERmon",
    "land": "Lmon",
    "landIce": "LImon",
    "ocean": "Omon",
    "oceanBgchem": "Omon",
    "sea_ice": "SImon",
    "time_invariant": "fx",
}

# Known variable → table overrides that are commonly mis-classified by
# component alone (e.g. daily atmosphere, ocean fixed fields, etc.)
_KNOWN_TABLE_OVERRIDES: dict[str, str] = {
    # ---- atmosphere daily ----
    "tasmax": "day",
    "tasmin": "day",
    # ---- atmosphere fx ----
    "areacella": "fx",
    "orog": "fx",
    "sftlf": "fx",
    "sftgif": "fx",
    # ---- ocean fx ----
    "areacello": "Ofx",
    "deptho": "Ofx",
    "sftof": "Ofx",
    "thkcello": "Ofx",
    "volcello": "Ofx",
    # ---- sea-ice daily ----
    "siconcd": "SIday",
    "sithickd": "SIday",
}


def _infer_mip(cmor_name: str, component: str) -> str:
    """Heuristically guess the MIP table for *cmor_name* in *component*."""
    if cmor_name in _KNOWN_TABLE_OVERRIDES:
        return _KNOWN_TABLE_OVERRIDES[cmor_name]
    return _COMPONENT_DEFAULT_TABLE.get(component, "Amon")
