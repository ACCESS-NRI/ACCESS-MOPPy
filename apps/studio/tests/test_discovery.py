"""Request selection and conservative archive evidence, without science imports."""

import sys
from types import ModuleType, SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from moppy_studio.app import create_app
from moppy_studio.discovery import HeaderScan, request_rows
from moppy_studio.probes import assessment_config
from moppy_studio.service import Service


def test_request_cutoff_includes_core_and_deduplicates_time_subsets():
    request = {
        "experiment": {
            "historical": {
                "Core": {"atmos.tas.brand.mon.glb": ["all"]},
                "High": {
                    "atmos.tas.brand.mon.glb": ["hist72"],
                    "ocean.tos.brand.mon.glb": ["all"],
                },
                "Medium": {"atmos.pr.brand.day.glb": ["hist43"]},
            }
        }
    }
    rows = request_rows(request, "historical", "High")
    assert [r["name"] for r in rows] == [
        "atmos.tas.brand.mon.glb",
        "ocean.tos.brand.mon.glb",
    ]
    assert rows[0]["priority"] == "Core"
    assert rows[0]["time_subsets"] == ["all", "hist72"]
    with pytest.raises(ValueError, match="not in this CMIP7 request"):
        request_rows(request, "not-an-experiment", "Core")


@pytest.fixture
def archive(tmp_path, monkeypatch):
    fd = ModuleType("access_moppy.file_discovery")
    fd._extract_year_from_path = (
        lambda p: int(p.stem[-4:]) if p.stem[-4:].isdigit() else None
    )
    monkeypatch.setitem(sys.modules, "access_moppy", ModuleType("access_moppy"))
    monkeypatch.setitem(sys.modules, "access_moppy.file_discovery", fd)
    for path in (
        "daily/native_1850.nc",
        "monthly/native_1850.nc",
        "monthly/native_1851.nc",
    ):
        p = tmp_path / path
        p.parent.mkdir(exist_ok=True)
        p.touch()
    return tmp_path


def test_scan_uses_requested_stream_and_years_and_reuses_headers(archive):
    opened = []

    def reader(path):
        opened.append(path)
        return {"daily_only"} if "/daily/" in path else {"tas_native"}

    scan = HeaderScan(archive, reader=reader)
    assert (
        scan.inspect(["monthly/*.nc"], ["daily_only"], 1850, 1850)["input_status"]
        == "missing_inputs"
    )
    result = scan.inspect(["monthly/*.nc"], ["tas_native"], 1850, 1850)
    assert result["input_status"] == "found"
    assert result["sample_files"] == ["monthly/native_1850.nc"]
    assert len(opened) == 1
    assert (
        scan.inspect(["monthly/*.nc"], ["tas_native"], 1852, 1853)["input_status"]
        == "missing_files"
    )


def test_sampling_limits_and_read_failures_are_not_missing_evidence(archive):
    limits = {
        "seconds": 30,
        "matches": 20,
        "matches_per_pattern": 20,
        "headers": 0,
        "samples_per_pattern": 1,
    }
    scan = HeaderScan(archive, limits=limits, reader=lambda p: {"tas"})
    result = scan.inspect(["monthly/*.nc"], ["tas"])
    assert result["input_status"] == "partial"
    assert result["limited"] and result["sampled"]

    def unreadable(path):
        raise OSError("not a readable NetCDF")

    scan = HeaderScan(archive, reader=unreadable)
    result = scan.inspect(["daily/*.nc"], ["tas"])
    assert result["input_status"] == "partial"
    assert result["errors"][0]["error"] == "not a readable NetCDF"
    scan.deadline = 0
    assert scan.inspect(["absent/*.nc"], ["tas"])["input_status"] == "partial"


def test_assessment_does_not_need_submit_fields_or_forward_executable_setup():
    draft = {
        "variables": ["Amon.tas"],
        "input_folder": "/tmp/archive",
        "start_year": "0001",
        "worker_init": "untrusted",
        "output_folder": "/tmp/existing",
    }
    normalized = assessment_config(draft)
    assert normalized["start_year"] == 1
    assert "worker_init" not in normalized and "output_folder" not in normalized
    for change in (
        {"variables": ["Amon.*"]},
        {"input_folder": "/tmp/../etc"},
        {"file_patterns": {"Amon.tas": "../outside/*.nc"}},
        {"start_year": True},
        {"start_year": 2001, "end_year": 2000},
    ):
        with pytest.raises(ValueError):
            assessment_config({**draft, **change})


def test_probe_api_authentication_and_strict_request_validation(tmp_path):
    calls = []

    def call(action, payload, module):
        calls.append((action, payload, module))
        return {"variables": []}

    service = Service(tmp_path / "state", runtime=SimpleNamespace(call=call))
    client = TestClient(create_app(service, "x" * 40))
    assert client.post("/api/request", json={}).status_code == 401
    client.headers["X-MOPPY-Token"] = "x" * 40
    for body in (
        {"request_version": "latest_stable"},
        {"request_version": "../../path"},
        {"priority": "unknown"},
        {"model_id": "../../elsewhere"},
    ):
        assert client.post("/api/request", json=body).status_code == 422
    assert not calls
    assert (
        client.post("/api/request", json={"experiment": "piControl"}).status_code == 200
    )
    assert calls[0][0] == "request"
    assert calls[0][1]["cache_dir"] == str(service.state / "cmip7-cache")
    assert (
        client.post(
            "/api/assessment",
            json={
                "config": {"variables": ["Amon.tas"], "input_folder": "/tmp/archive"}
            },
        ).status_code
        == 200
    )
    assert calls[1][0] == "assess" and calls[1][1]["scan"]
    assert not list(service.state.glob("*.json"))
