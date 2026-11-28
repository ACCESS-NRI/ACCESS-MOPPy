"""Read-only request/mapping probes, imported only by the analysis3 bridge.

No scientific arrays are loaded. A header sample is evidence of native inputs,
not a validation of time coverage, units, coordinates or CMOR output.
"""

import copy
import glob
import importlib.metadata
import json
import os
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

PRIORITIES = ("Core", "High", "Medium", "Low")
SOURCE = "https://github.com/CMIP-Data-Request/CMIP7_DReq_Content"
LIMITS = {
    "seconds": 50,
    "matches": 20000,
    "matches_per_pattern": 2000,
    "headers": 256,
    "samples_per_pattern": 3,
}


def request_rows(request, experiment, cutoff):
    """Priority buckets are disjoint: include every bucket up to the cutoff."""
    experiments = request.get("experiment", {})
    if experiment not in experiments:
        raise ValueError(
            f"Experiment '{experiment}' is not in this CMIP7 request release"
        )
    rows = {}
    for priority in PRIORITIES[: PRIORITIES.index(cutoff) + 1]:
        for name, subsets in experiments[experiment].get(priority, {}).items():
            row = rows.setdefault(
                name, {"name": name, "priority": priority, "time_subsets": []}
            )
            row["time_subsets"] = sorted(set(row["time_subsets"]) | set(subsets))
    return list(rows.values())


def query_request(payload):
    # The upstream API writes configuration on import/update. Give this probe a
    # private config before importing it; never change the user's API defaults.
    cache = Path(payload["cache_dir"])
    cache.mkdir(mode=0o700, parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="request-", dir=cache) as temporary:
        config = Path(temporary) / "config.json"
        config.write_text(
            json.dumps(
                {
                    "offline": payload.get("offline", False),
                    "export": "release",
                    "consolidate": True,
                    "log_level": "warning",
                    "log_file": str(Path(temporary) / "api.log"),
                    "cache_dir": str(cache),
                    "check_api_version": False,
                    "variable_name": "CMIP7 Compound Name",
                }
            )
        )
        os.environ["CMIP7_DR_API_CONFIGFILE"] = str(config)
        from data_request_api.content import dreq_content as dc
        from data_request_api.query import dreq_query as dq

        version = payload["request_version"]
        content = dc.load(version, offline=payload.get("offline", False))
        if not content:
            raise ValueError(
                f"CMIP7 request {version} is unavailable. Check network access or cache this release before using offline mode."
            )
        tables = dq.create_dreq_tables_for_request(
            content=content, dreq_version=version
        )
        request = dq.get_requested_variables(
            content=tables,
            dreq_version=version,
            use_opps="all",
            priority_cutoff=payload["priority"],
            time_subsets=True,
            verbose=False,
        )
        experiment = payload["experiment"]
        rows = (
            request_rows(request, experiment, payload["priority"]) if experiment else []
        )
        if rows:
            mapping = assess(
                {
                    "config": {
                        "variables": [r["name"] for r in rows],
                        "model_id": payload["model_id"],
                        "cmip_version": "CMIP7",
                    },
                    "scan": False,
                }
            )
            for row, capability in zip(rows, mapping["variables"]):
                row.update(capability)
        return {
            "request_version": version,
            "source": SOURCE,
            "experiment": experiment,
            "priority": payload["priority"],
            "opportunities": "all",
            "opportunity_names": request.get("Header", {}).get("Opportunities", []),
            "api_version": importlib.metadata.version("CMIP7_data_request_api"),
            "time_subsets_applied": False,
            "experiments": sorted(request["experiment"]),
            "variables": rows,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }


class HeaderScan:
    """Bound work across the entire request; reuse stream globs and headers."""

    def __init__(self, root, limits=None, reader=None):
        self.root = Path(root)
        if not self.root.is_dir():
            raise ValueError(
                "Input folder is not an accessible directory in this ARE session"
            )
        self.limits = limits or LIMITS
        self.deadline = time.monotonic() + self.limits["seconds"]
        self.matches = 0
        self.patterns = {}
        self.headers = {}
        self.reader = reader or self.read_header

    @staticmethod
    def read_header(path):
        from netCDF4 import Dataset

        with Dataset(path, "r") as dataset:
            return set(dataset.variables)

    def sample(self, pattern, start=None, end=None, recursive=False):
        from access_moppy.file_discovery import _extract_year_from_path

        key = (pattern, start, end, recursive)
        if key in self.patterns:
            return self.patterns[key]
        result = {"files": [], "matched": 0, "limited": False, "unknown_year": False}
        if self.matches >= self.limits["matches"] or time.monotonic() > self.deadline:
            result["limited"] = True
            self.patterns[key] = result
            return result
        seen = 0
        # Match the native batch override convention: patterns are relative to
        # input_folder even with a leading slash. Never follow a parent escape.
        if ".." in Path(pattern).parts:
            raise ValueError("Input patterns containing '..' cannot be assessed")
        for path in glob.iglob(
            str(self.root / pattern.lstrip("/")), recursive=recursive
        ):
            if (
                seen >= self.limits["matches_per_pattern"]
                or self.matches >= self.limits["matches"]
                or time.monotonic() > self.deadline
            ):
                result["limited"] = True
                break
            seen += 1
            self.matches += 1
            candidate = Path(path)
            if not candidate.is_file():
                continue
            year = _extract_year_from_path(candidate)
            if year is not None and (
                (start is not None and year < start) or (end is not None and year > end)
            ):
                continue
            if year is None and (start is not None or end is not None):
                result["unknown_year"] = True
            result["matched"] += 1
            if len(result["files"]) < self.limits["samples_per_pattern"]:
                result["files"].append(path)
        self.patterns[key] = result
        return result

    def inspect(self, patterns, required, start=None, end=None, recursive=False):
        samples = [self.sample(p, start, end, recursive) for p in patterns]
        paths = list(dict.fromkeys(p for s in samples for p in s["files"]))
        found, errors, checked = set(), [], []
        limited = any(s["limited"] for s in samples)
        for path in paths:
            if path not in self.headers:
                if (
                    len(self.headers) >= self.limits["headers"]
                    or time.monotonic() > self.deadline
                ):
                    limited = True
                    continue
                try:
                    self.headers[path] = (self.reader(path), None)
                except (OSError, ValueError, RuntimeError) as exc:
                    self.headers[path] = (set(), str(exc)[:300])
            variables, error = self.headers[path]
            if error:
                errors.append(
                    {"file": str(Path(path).relative_to(self.root)), "error": error}
                )
            else:
                found.update(variables)
                checked.append(str(Path(path).relative_to(self.root)))
        missing = sorted(set(required) - found)
        matched = sum(s["matched"] for s in samples)
        status = "found" if not missing else "missing_inputs"
        if not matched:
            status = "missing_files"
        if (limited or errors) and missing:
            status = "partial"
        if not required:
            status = "unavailable"
        return {
            "input_status": status,
            "missing_inputs": missing,
            "sample_files": checked,
            "matched_files": matched,
            "sampled": matched > len(paths),
            "limited": limited,
            "unknown_year": any(s["unknown_year"] for s in samples),
            "errors": errors,
        }


def assess(payload):
    from access_moppy import file_discovery as fd
    from access_moppy.derivations import optional_model_variables
    from access_moppy.utilities import (
        load_cmip7_to_cmip6_mapping,
        mapping_entry_is_self_contained,
    )

    config = payload["config"]
    model = config["model_id"]
    target = config["cmip_version"]
    mappings = fd._load_full_mappings(model)
    translations = (
        {k.lower(): v for k, v in load_cmip7_to_cmip6_mapping().items()}
        if target == "CMIP7"
        else {}
    )
    scan = HeaderScan(config["input_folder"]) if payload.get("scan") else None
    rows = []
    for name in config["variables"]:
        row = {
            "name": name,
            "mapping": "unsupported",
            "input_status": "not_scanned",
            "required_inputs": [],
            "optional_inputs": [],
        }
        rows.append(row)
        alias = translations.get(name.lower()) if name.count(".") > 1 else name
        row["alias"] = alias
        if not alias:
            row["reason"] = "No CMIP7 name translation in this MOPPy runtime"
            continue
        try:
            resolved = fd._find_variable_entry(mappings, alias)
            if not resolved:
                row["reason"] = "No supported table-specific mapping for this model"
                continue
            component, original = resolved
            entry = copy.deepcopy(original)
            supported = entry.get("supported_cmip_versions")
            if supported is not None and target not in supported:
                row["reason"] = f"Mapping supports {', '.join(supported)}, not {target}"
                continue
            entry["calculation"] = entry.get("calculation_overrides", {}).get(
                target, entry.get("calculation", {})
            )
            calculation = entry["calculation"] or {}
            optional = (
                optional_model_variables(calculation)
                if calculation.get("type") == "formula"
                else set()
            )
            native = entry.get("model_variables") or []
            row.update(
                mapping="mapped",
                reason="Mapping available; scientific validation still required",
                required_inputs=[n for n in native if n not in optional],
                optional_inputs=[n for n in native if n in optional],
            )
            if mapping_entry_is_self_contained(entry):
                row.update(
                    input_status="not_required",
                    reason="Generated or bundled inputs; runtime must validate the resource and grid",
                )
                continue
            if not scan:
                continue
            override = config.get("file_patterns", {}).get(name)
            frequency = fd._TABLE_TO_FREQ.get(alias.split(".")[0])
            if override:
                patterns = override if isinstance(override, list) else [override]
                # Native batch overrides do not apply auto-discovery year filters.
                start = end = None
            else:
                if frequency is None:
                    raise ValueError("No automatic discovery frequency for this table")
                patterns = fd._build_patterns(
                    entry,
                    component,
                    frequency,
                    mappings.get("model_info", {}).get("file_discovery", {}),
                )
                start = config.get("start_year") if frequency != "fx" else None
                end = config.get("end_year") if frequency != "fx" else None
            row["patterns"] = patterns
            row["explicit_patterns"] = bool(override)
            row.update(
                scan.inspect(
                    patterns,
                    row["required_inputs"],
                    start,
                    end,
                    recursive=bool(override),
                )
            )
        except (ValueError, fd.FileDiscoveryError) as exc:
            row.update(input_status="unavailable", reason=str(exc))
    return {
        "variables": rows,
        "model_id": model,
        "cmip_version": target,
        "input_folder": config.get("input_folder") if scan else None,
        "checked_at": datetime.now(timezone.utc).isoformat(),
        "scan": {
            "limits": LIMITS,
            "headers_read": len(scan.headers),
            "path_matches": scan.matches,
        }
        if scan
        else None,
    }
