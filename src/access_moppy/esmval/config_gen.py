"""
access_moppy.esmval.config_gen
================================

Generate ESMValCore configuration so it can find CMORised ACCESS-MOPPy
output without any manual editing of the user's config.

The strategy is to write a **named config file** into the ESMValCore user
config directory (``~/.config/esmvaltool/`` by default, or the directory
specified by the ``ESMVALTOOL_CONFIG_DIR`` environment variable).  ESMValCore
2.14+ automatically merges all YAML files in that directory, so the generated
file is picked up without any ``--config`` flag or environment variable.

Config file format written (ESMValCore ≥2.14)
---------------------------------------------
::

    projects:
      CMIP6:
        data:
          moppy-cache:
            type: esmvalcore.io.local.LocalDataSource
            rootpath: /path/to/cache
            dirname_template: >-
              {project}/{activity}/{institute}/{dataset}/{exp}/{ensemble}/
              {mip}/{short_name}/{grid}/{version}
            filename_template: "{short_name}_{mip}_{dataset}_{exp}_{ensemble}_{grid}*.nc"

This uses the ``LocalDataSource`` mechanism introduced in ESMValCore 2.14 to
register the CMIP DRS tree written by ACCESS-MOPPy as a named CMIP6 data
source.  Multiple sources may coexist in the same project block so this
does not interfere with other configured data stores.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

#: Default name for the generated config file placed in the ESMValCore config dir.
DEFAULT_CONFIG_FILENAME = "moppy-esmval-data.yml"

#: CMIP6 DRS directory template (matches ACCESS-MOPPy cache layout).
CMIP6_DIRNAME_TEMPLATE = (
    "{project}/{activity}/{institute}/{dataset}/{exp}/{ensemble}"
    "/{mip}/{short_name}/{grid}/{version}"
)

#: CMIP6 DRS filename template.
CMIP6_FILENAME_TEMPLATE = "{short_name}_{mip}_{dataset}_{exp}_{ensemble}_{grid}*.nc"


def _default_user_config_dir() -> Path:
    """Return the ESMValCore user config directory.

    Reads ``USER_CONFIG_DIR`` from ``esmvalcore`` when available, which
    already handles the ``ESMVALTOOL_CONFIG_DIR`` environment variable.
    Falls back to ``~/.config/esmvaltool`` if ESMValCore is not installed.
    """
    try:
        from esmvalcore.config._config_object import (
            USER_CONFIG_DIR,  # type: ignore[import]
        )

        return Path(USER_CONFIG_DIR)
    except ImportError:
        return Path("~/.config/esmvaltool").expanduser()


def write_esmval_config(
    cache_dir: str | Path,
    output_path: str | Path | None = None,
    extra_rootpaths: list[str | Path] | None = None,
) -> Path:
    """Write an ESMValCore 2.14+ config file that points to the MOPPy cache.

    The file is placed in the ESMValCore user config directory by default so
    that ``esmvaltool run <recipe>`` picks it up automatically without any
    ``--config`` flag or ``ESMVALTOOL_CONFIG_DIR`` environment variable.

    Parameters
    ----------
    cache_dir:
        The directory where CMORised files live in CMIP DRS tree structure.
        This is the ``drs_root`` / ``cache_dir`` used by
        :class:`~access_moppy.esmval.orchestrator.CMORiseOrchestrator`.
    output_path:
        Where to write the generated YAML.  Defaults to
        ``~/.config/esmvaltool/moppy-esmval-data.yml`` (the ESMValCore user
        config directory).  Specify this to write elsewhere, e.g. for testing.
    extra_rootpaths:
        Additional CMIP6 root paths to register as named data sources
        (``extra-0``, ``extra-1``, …).

    Returns
    -------
    Path
        Path to the written config file.

    Examples
    --------
    >>> cfg = write_esmval_config("~/.cache/moppy-esmval")
    >>> # esmvaltool run my_recipe.yml   # no --config needed
    """
    cache = Path(cache_dir).expanduser().resolve()

    if output_path is None:
        dest = _default_user_config_dir() / DEFAULT_CONFIG_FILENAME
    else:
        dest = Path(output_path)

    dest.parent.mkdir(parents=True, exist_ok=True)

    # Primary data source
    data_sources: dict[str, Any] = {
        "moppy-cache": {
            "type": "esmvalcore.io.local.LocalDataSource",
            "rootpath": str(cache),
            "dirname_template": CMIP6_DIRNAME_TEMPLATE,
            "filename_template": CMIP6_FILENAME_TEMPLATE,
        }
    }

    # Optional extra sources
    for i, p in enumerate(extra_rootpaths or []):
        resolved = str(Path(p).expanduser().resolve())
        data_sources[f"extra-{i}"] = {
            "type": "esmvalcore.io.local.LocalDataSource",
            "rootpath": resolved,
            "dirname_template": CMIP6_DIRNAME_TEMPLATE,
            "filename_template": CMIP6_FILENAME_TEMPLATE,
        }

    config: dict[str, Any] = {
        "projects": {
            "CMIP6": {
                "data": data_sources,
            }
        }
    }

    with open(dest, "w", encoding="utf-8") as fh:
        yaml.dump(config, fh, default_flow_style=False, sort_keys=False)

    logger.info("Wrote ESMValCore config to '%s'.", dest)
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
    """Write an ESMValCore 2.14+ data-source config file alongside an existing one.

    In ESMValCore 2.14+, configuration is no longer a single monolithic
    ``config-user.yml`` but a **directory** of YAML files that are merged
    automatically.  This function writes the MOPPy data-source config file
    (``moppy-esmval-data.yml``) into the same directory as
    *base_config_path*, so ESMValCore sees both files when it loads the
    config directory.

    *base_config_path* is **not** read or modified.

    Parameters
    ----------
    cache_dir:
        MOPPy cache directory (the CMIP DRS root).
    base_config_path:
        Path to any existing file in the target ESMValCore config directory.
        The config file is written next to it.
    output_path:
        Override the output file path.  Defaults to
        ``<parent of base_config_path>/moppy-esmval-data.yml``.

    Returns
    -------
    Path
        Path to the written config file.
    """
    dest = (
        Path(output_path)
        if output_path
        else Path(base_config_path).parent / DEFAULT_CONFIG_FILENAME
    )
    return write_esmval_config(cache_dir=cache_dir, output_path=dest)
