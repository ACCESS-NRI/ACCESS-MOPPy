"""
access_moppy.esmval.config_gen
================================

Generate or update ESMValCore configuration so it can find CMORised
ACCESS-MOPPy output without any manual editing of the user's config.

The strategy is to write a **companion config file** that adds the
MOPPy cache directory to the CMIP6 rootpath list.  The user (or the
:func:`~access_moppy.esmval.cli_commands.main_run` wrapper) passes this
file to ``esmvaltool run`` via the ``--config`` flag so the user's main
configuration is left untouched.

Config file format written (ESMValCore ≥2.14)
---------------------------------------------
::

    rootpath:
      CMIP6:
        - /path/to/cache       # prepended

    drs:
      CMIP6: CMIP6

This is merged with whatever the user already has in their main config
when ``esmvaltool run`` is invoked.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

#: Default name for the generated overlay config file.
DEFAULT_CONFIG_FILENAME = "moppy-esmval-config.yml"


def write_esmval_config(
    cache_dir: str | Path,
    output_path: str | Path | None = None,
    extra_rootpaths: list[str | Path] | None = None,
) -> Path:
    """Write an ESMValCore config overlay that points to the MOPPy cache.

    Parameters
    ----------
    cache_dir:
        The directory where CMORised files live in CMIP DRS tree structure.
        This is the ``drs_root`` / ``cache_dir`` used by
        :class:`~access_moppy.esmval.orchestrator.CMORiseOrchestrator`.
    output_path:
        Where to write the generated YAML.  Defaults to
        ``./moppy-esmval-config.yml`` in the current working directory.
    extra_rootpaths:
        Additional paths to include in the CMIP6 rootpath list (e.g. an
        existing CMIP6 data store).

    Returns
    -------
    Path
        Path to the written config file.

    Examples
    --------
    >>> cfg_path = write_esmval_config("~/.cache/moppy-esmval")
    >>> # Then use it:
    >>> # esmvaltool run my_recipe.yml --config moppy-esmval-config.yml
    """
    cache = Path(cache_dir).expanduser().resolve()
    dest = Path(output_path) if output_path else Path.cwd() / DEFAULT_CONFIG_FILENAME

    rootpaths: list[str] = [str(cache)]
    for p in extra_rootpaths or []:
        rootpaths.append(str(Path(p).expanduser().resolve()))

    config: dict[str, Any] = {
        "rootpath": {
            "CMIP6": rootpaths,
        },
        "drs": {
            "CMIP6": "CMIP6",
        },
    }

    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "w", encoding="utf-8") as fh:
        yaml.dump(config, fh, default_flow_style=False, sort_keys=False)

    logger.info("Wrote ESMValCore config overlay to '%s'.", dest)
    return dest


def load_existing_config(config_path: str | Path) -> dict[str, Any]:
    """Read an existing ESMValCore config file and return it as a dict.

    Returns an empty dict when the file does not exist.
    """
    p = Path(config_path)
    if not p.exists():
        return {}
    with open(p, encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    return data if isinstance(data, dict) else {}


def merge_into_existing_config(
    cache_dir: str | Path,
    base_config_path: str | Path,
    output_path: str | Path | None = None,
) -> Path:
    """Merge the MOPPy cache path into an existing ESMValCore config file.

    This reads *base_config_path*, prepends *cache_dir* to the CMIP6
    rootpath list (creating it if absent), and writes the result to
    *output_path* (defaulting to the same file location as
    *base_config_path* but named ``moppy-esmval-config.yml``).

    The original *base_config_path* is **not** modified.

    Parameters
    ----------
    cache_dir:
        MOPPy cache directory.
    base_config_path:
        Path to the user's existing ``config-user.yml``.
    output_path:
        Where to write the merged config.

    Returns
    -------
    Path
        Path to the written merged config file.
    """
    cache = Path(cache_dir).expanduser().resolve()
    existing = load_existing_config(base_config_path)

    # Deep-copy the existing config so we don't mutate it
    import copy

    merged: dict[str, Any] = copy.deepcopy(existing)

    # Ensure rootpath section exists
    merged.setdefault("rootpath", {})
    cmip6_paths = merged["rootpath"].get("CMIP6", [])
    if isinstance(cmip6_paths, str):
        cmip6_paths = [cmip6_paths]
    cache_str = str(cache)
    if cache_str not in cmip6_paths:
        cmip6_paths = [cache_str] + list(cmip6_paths)
    merged["rootpath"]["CMIP6"] = cmip6_paths

    # Ensure drs section is set correctly
    merged.setdefault("drs", {})
    merged["drs"]["CMIP6"] = "CMIP6"

    # Determine output path
    dest = (
        Path(output_path)
        if output_path
        else Path(base_config_path).parent / DEFAULT_CONFIG_FILENAME
    )

    dest.parent.mkdir(parents=True, exist_ok=True)
    with open(dest, "w", encoding="utf-8") as fh:
        yaml.dump(merged, fh, default_flow_style=False, sort_keys=False)

    logger.info(
        "Wrote merged ESMValCore config to '%s' (based on '%s').",
        dest,
        base_config_path,
    )
    return dest
