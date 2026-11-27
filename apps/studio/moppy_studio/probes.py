"""Validate only the read-only subset of a draft needed for input assessment."""

import copy
from pathlib import Path

from .configuration import IDENTIFIER, PATH


def assessment_config(config):
    result = {
        "cmip_version": config.get("cmip_version", "CMIP6"),
        "model_id": config.get("model_id", "ACCESS-ESM1-6"),
    }
    if result["cmip_version"] not in ("CMIP6", "CMIP6Plus", "CMIP7"):
        raise ValueError("Input assessment supports CMIP6, CMIP6Plus and CMIP7")
    if not isinstance(result["model_id"], str) or not IDENTIFIER.fullmatch(
        result["model_id"]
    ):
        raise ValueError("Choose a model mapping")
    names = config.get("variables")
    if not isinstance(names, list) or not 1 <= len(names) <= 2000:
        raise ValueError("Assess between 1 and 2000 variables")
    if any(
        not isinstance(n, str)
        or len(n) > 250
        or not IDENTIFIER.fullmatch(n)
        or "." not in n
        for n in names
    ):
        raise ValueError("Use literal compound variable names for assessment")
    if result["cmip_version"] != "CMIP7" and any(n.count(".") != 1 for n in names):
        raise ValueError("CMIP6 and CMIP6Plus assessment needs table.variable names")
    result["variables"] = list(dict.fromkeys(names))
    root = config.get("input_folder", "")
    if (
        not isinstance(root, str)
        or not PATH.fullmatch(root)
        or ".." in Path(root).parts
    ):
        raise ValueError("Choose an absolute input folder without parent traversal")
    result["input_folder"] = root
    for key in ("start_year", "end_year"):
        value = config.get(key)
        if value in (None, ""):
            continue
        if (
            isinstance(value, bool)
            or not str(value).isdigit()
            or not 1 <= int(value) <= 9999
        ):
            raise ValueError(f"{key} must be a year between 1 and 9999")
        result[key] = int(value)
    if result.get("start_year", 1) > result.get("end_year", 9999):
        raise ValueError("First year must not exceed last year")
    overrides = config.get("file_patterns", {})
    if not isinstance(overrides, dict):
        raise ValueError("file_patterns must map variables to relative glob patterns")
    result["file_patterns"] = {}
    for name in result["variables"]:
        patterns = overrides.get(name)
        if not patterns:
            continue
        values = patterns if isinstance(patterns, list) else [patterns]
        if not 1 <= len(values) <= 30 or any(
            not isinstance(p, str) or not p or len(p) > 4096 or ".." in Path(p).parts
            for p in values
        ):
            raise ValueError(
                "Use up to 30 relative file patterns per variable, without parent traversal"
            )
        result["file_patterns"][name] = copy.deepcopy(patterns)
    return result
