"""Opt-in probe: run from apps/studio in analysis3, never submits a job.

    python tests/native_discovery_probe.py

Queries an official release and checks real NetCDF headers in a temporary archive.
"""

import json
import subprocess
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from moppy_studio.discovery import assess, query_request  # noqa: E402


def main():
    user_config = Path.home() / ".CMIP7_data_request_api_config"
    before = user_config.read_bytes() if user_config.exists() else None
    request = query_request(
        {
            "request_version": "v1.2.2.5",
            "priority": "Core",
            "experiment": "piControl",
            "model_id": "ACCESS-ESM1-6",
            "cache_dir": str(Path.home() / ".cache/moppy-studio/cmip7"),
        }
    )
    assert len(request["variables"]) == 131
    tas = "atmos.tas.tavg-h2m-hxy-u.mon.glb"
    assert (
        next(r for r in request["variables"] if r["name"] == tas)["mapping"] == "mapped"
    )

    from netCDF4 import Dataset

    with tempfile.TemporaryDirectory(prefix="studio-header-probe-") as temporary:
        root = Path(temporary)
        config = {
            "cmip_version": "CMIP7",
            "model_id": "ACCESS-ESM1-6",
            "input_folder": str(root),
            "variables": [
                tas,
                "ocean.tos.tavg-u-hxy-sea.mon.glb",
                "atmos.unknown.tavg-u-hxy-u.mon.glb",
            ],
        }
        rows = assess({"config": config, "scan": False})["variables"]
        native = rows[0]["required_inputs"]
        assert native
        path = root / "output000/atmosphere/netCDF/probe.pa-185001_mon.nc"
        path.parent.mkdir(parents=True)
        with Dataset(path, "w") as dataset:
            dataset.createDimension("time", 1)
            for name in native:
                dataset.createVariable(name, "f4", ("time",))
        result = assess({"config": config, "scan": True})
        rows = result["variables"]
        assert rows[0]["input_status"] == "found", rows[0]
        assert rows[1]["input_status"] == "missing_files", rows[1]
        assert rows[2]["mapping"] == "unsupported", rows[2]
        assert result["scan"]["headers_read"] == 1
        config.update(start_year=1851, end_year=1851)
        assert (
            assess({"config": config, "scan": True})["variables"][0]["input_status"]
            == "missing_files"
        )
        config["file_patterns"] = {tas: "output000/atmosphere/netCDF/*.nc"}
        assert (
            assess({"config": config, "scan": True})["variables"][0]["input_status"]
            == "found"
        )
        # Fixed products and native table restrictions must not collapse into a
        # short-name-only match. Optional frazil diagnostics are not required.
        extra = {
            **config,
            "variables": [
                "fx.areacella",
                "Amon.od550dust",
                "AERmon.od550dust",
                "Omon.hfds",
            ],
            "file_patterns": {},
        }
        evidence = {
            r["name"]: r for r in assess({"config": extra, "scan": False})["variables"]
        }
        assert evidence["fx.areacella"]["input_status"] == "not_required"
        assert evidence["Amon.od550dust"]["mapping"] == "unsupported"
        assert evidence["AERmon.od550dust"]["mapping"] == "mapped"
        assert "frazil_3d_int_z" in evidence["Omon.hfds"]["optional_inputs"]
        for target in ("CMIP6", "CMIP6Plus"):
            c = {
                **config,
                "cmip_version": target,
                "variables": ["Amon.tas"],
                "file_patterns": {"Amon.tas": "output000/atmosphere/netCDF/*.nc"},
            }
            assert (
                assess({"config": c, "scan": True})["variables"][0]["input_status"]
                == "found"
            )
    cached = subprocess.run(
        [sys.executable, "moppy_studio/bridge.py", "request"],
        input=json.dumps(
            {
                "request_version": "v1.2.2.5",
                "priority": "High",
                "experiment": "historical",
                "model_id": "ACCESS-ESM1-6",
                "cache_dir": str(Path.home() / ".cache/moppy-studio/cmip7"),
                "offline": True,
            }
        ),
        capture_output=True,
        text=True,
        check=True,
        timeout=90,
    )
    payload = json.loads(
        next(
            line.split("=", 1)[1]
            for line in cached.stdout.splitlines()
            if line.startswith("MOPPY_STUDIO_JSON=")
        )
    )
    assert len(payload["variables"]) == 1257
    assert payload["module"].startswith("conda/analysis3-")
    assert (user_config.read_bytes() if user_config.exists() else None) == before
    print(
        json.dumps(
            {
                "request_variables": len(request["variables"]),
                "mapped": sum(r["mapping"] == "mapped" for r in request["variables"]),
                "experiments": len(request["experiments"]),
                "netcdf_probe": "passed",
            }
        )
    )


if __name__ == "__main__":
    main()
