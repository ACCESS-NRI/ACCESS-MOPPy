import sqlite3
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from moppy_studio.app import create_app
from moppy_studio.configuration import dump, parse_yaml, template, validate
from moppy_studio.monitor import read_tasks
from moppy_studio.scheduler import SubmissionUncertain
from moppy_studio.service import Service


class Runtime:
    def call(self, action, payload, module):
        if action == "initialize":
            database(
                Path(payload["config"]["output_folder"]), payload["config"]["variables"]
            )
            return {"version": "1.8.5"}
        return {
            "version": "1.8.5",
            "module": "conda/analysis3-26.10",
            "monitor_script": "#!/bin/bash\n#PBS -N " + payload["job_name"] + "\n",
            "monitor_walltime": "03:00:00",
        }


class PBS:
    def __init__(self):
        self.calls = 0
        self.error = None
        self.state = {"job_state": "Q"}

    def submit(self, script):
        self.calls += 1
        if self.error:
            raise self.error
        return "123.gadi-pbs"

    def query(self, jobs):
        return {j: self.state for j in jobs}

    def find(self, name, output):
        return ["123.gadi-pbs"]


def database(root, variables=("Amon.tas",), status="pending"):
    root.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(root / "cmor_tasks.db") as db:
        db.execute(
            "CREATE TABLE cmor_tasks (variable TEXT, experiment_id TEXT, status TEXT, pbs_job_id TEXT, error_message TEXT)"
        )
        db.executemany(
            "INSERT INTO cmor_tasks VALUES (?, ?, ?, ?, ?)",
            [(v, "piControl", status, None, None) for v in variables],
        )


@pytest.fixture
def setup(tmp_path):
    c = template("test")
    c["input_folder"] = str(tmp_path / "input")
    Path(c["input_folder"]).mkdir()
    c["output_folder"] = str(tmp_path / "output")
    c["scheduler_options"] = "#PBS -P iq82"
    pbs = PBS()
    svc = Service(tmp_path / "state", Runtime(), pbs)
    return svc, c, pbs


def test_roundtrip_retains_advanced_configuration():
    text = 'variables: [Amon.tas]\nparent_info: {branch_time_in_parent: 73048.0}\nfile_patterns: {Amon.tas: "output*/atmosphere/*.nc"}\nvariable_resources:\n  Amon.tas: &r {mem: 32GB}\n  Amon.pr: *r\n'
    data = parse_yaml(text)
    assert parse_yaml(dump(data)) == data
    assert data["variable_resources"]["Amon.pr"]["mem"] == "32GB"


@pytest.mark.parametrize(
    "text",
    [
        "x: 1\nx: 2",
        "[one, two]",
        "x: !!python/object/apply:os.system [echo bad]",
        "x: &x [*x]",
    ],
)
def test_invalid_yaml(text):
    with pytest.raises(ValueError):
        parse_yaml(text)


@pytest.mark.parametrize(
    "change",
    [
        {"input_folder": "/tmp/$(touch-pwn)"},
        {"grid_label": 'gn"; echo x'},
        {"scheduler_options": "#PBS -P iq82\n#PBS -N surprise"},
        {"cpus_per_node": True},
        {"variable_resources": {"Amon.tas": {"worker_init": "echo surprise"}}},
        {"variables": ["../../bad"]},
    ],
)
def test_reject_template_injection_and_invalid_resources(setup, change):
    _, c, _ = setup
    c.update(change)
    with pytest.raises(ValueError):
        validate(c)


def test_review_submit_is_once_and_immutable(setup):
    svc, c, pbs = setup
    r = svc.plan(c, "Test")
    assert not Path(c["output_folder"]).exists()
    assert "conda/analysis3-26.10" in r["config"]["worker_init"]
    with pytest.raises(ValueError):
        svc.submit(r["id"], "wrong hash")
    submitted = svc.submit(r["id"], r["sha256"])
    assert submitted["monitor_job"] == "123.gadi-pbs"
    assert svc.refresh(r["id"])["state"] == "queued"
    with pytest.raises(ValueError):
        svc.submit(r["id"], r["sha256"])
    assert pbs.calls == 1


def test_timeout_is_uncertain_and_reconciliation_never_resubmits(setup):
    svc, c, pbs = setup
    r = svc.plan(c, "Test")
    pbs.error = SubmissionUncertain("timeout")
    assert svc.submit(r["id"], r["sha256"])["state"] == "submission uncertain"
    with pytest.raises(ValueError):
        svc.submit(r["id"], r["sha256"])
    assert svc.reconcile(r["id"])["monitor_job"] == "123.gadi-pbs"
    assert pbs.calls == 1


def test_lost_job_history_is_not_success(setup):
    svc, c, pbs = setup
    r = svc.plan(c, "Test")
    svc.submit(r["id"], r["sha256"])
    pbs.state = {"job_state": "unknown"}
    assert svc.refresh(r["id"])["state"] == "unknown"
    pbs.state = {"job_state": "F", "Exit_status": 0}
    assert svc.refresh(r["id"])["state"] == "incomplete"


def test_attach_and_refresh_never_write_database(setup, tmp_path):
    svc, _, pbs = setup
    root = tmp_path / "existing"
    database(root, status="completed")
    before = (root / "cmor_tasks.db").read_bytes()
    r = svc.attach(str(root))
    assert svc.refresh(r["id"])["state"] == "completed"
    assert (root / "cmor_tasks.db").read_bytes() == before
    with pytest.raises(ValueError):
        svc.submit(r["id"], "anything")
    assert pbs.calls == 0


def test_monitor_missing_and_corrupt_database_are_explicit(tmp_path):
    assert read_tasks(tmp_path)[1]
    (tmp_path / "cmor_tasks.db").write_text("invalid")
    assert "unreadable" in read_tasks(tmp_path)[1]


def test_reading_status_does_not_overwrite_submission_record(setup):
    svc, c, _ = setup
    r = svc.plan(c, "Test")
    svc.submit(r["id"], r["sha256"])
    before = svc.path(r["id"]).read_bytes()
    svc.refresh(r["id"])
    assert svc.path(r["id"]).read_bytes() == before


def test_run_list_reports_live_status_without_changing_saved_records(
    setup, monkeypatch
):
    svc, c, pbs = setup
    r = svc.plan(c, "Test")
    svc.submit(r["id"], r["sha256"])
    before = svc.path(r["id"]).read_bytes()
    queries = []

    def query(jobs):
        queries.append(jobs)
        return {job: pbs.state for job in jobs}

    monkeypatch.setattr(pbs, "query", query)
    client = TestClient(create_app(svc, "x" * 40))
    headers = {"X-MOPPY-Token": "x" * 40}
    assert client.get("/api/runs", headers=headers).json()[0]["state"] == "queued"
    assert queries == [["123.gadi-pbs"]]
    pbs.state = {"job_state": "R"}
    assert client.get("/api/runs", headers=headers).json()[0]["state"] == "running"
    with sqlite3.connect(Path(c["output_folder"]) / "cmor_tasks.db") as db:
        db.execute("UPDATE cmor_tasks SET status='completed'")
    pbs.state = {"job_state": "F", "Exit_status": 0}
    assert client.get("/api/runs", headers=headers).json()[0]["state"] == "completed"
    assert svc.path(r["id"]).read_bytes() == before
    assert pbs.calls == 1


def test_existing_output_is_never_reused(setup):
    svc, c, _ = setup
    Path(c["output_folder"]).mkdir()
    with pytest.raises(ValueError, match="new output"):
        svc.plan(c, "Test")


def test_log_traversal_and_symlink_escape(setup, tmp_path):
    svc, c, _ = setup
    r = svc.plan(c, "Test")
    svc.submit(r["id"], r["sha256"])
    secret = tmp_path / "secret"
    secret.write_text("private")
    (Path(c["output_folder"]) / "moppy_monitor.out").symlink_to(secret)
    with pytest.raises(ValueError):
        svc.logs(r["id"])
    with pytest.raises(ValueError):
        svc.logs(r["id"], "worker_err", "../../secret")


def test_api_auth_xsrf_and_base_url(setup, monkeypatch):
    svc, c, _ = setup
    token = "x" * 40
    client = TestClient(create_app(svc, token), root_path="/node/test/123/studio")
    assert client.get("/api/health").status_code == 401
    headers = {"X-MOPPY-Token": token}
    assert client.get("/api/health", headers=headers).status_code == 200
    monkeypatch.setenv("MOPPY_STUDIO_REQUIRE_XSRF", "1")
    assert (
        client.post("/api/plans", headers=headers, json={"config": c}).status_code
        == 403
    )
    client.cookies.set("_xsrf", "session-value")
    headers["X-XSRFToken"] = "session-value"
    r = client.post("/api/plans", headers=headers, json={"config": c})
    assert r.status_code == 200, r.text
    assert client.get("/moppy-logo.png").status_code == 200


@pytest.mark.parametrize(
    "asset,content_type",
    [
        ("", "text/html"),
        ("style.css", "text/css"),
        ("app.js", "javascript"),
        ("moppy-logo.png", "image/png"),
        ("access-logo.png", "image/png"),
        ("favicon.png", "image/png"),
    ],
)
def test_installed_web_assets(setup, asset, content_type):
    svc, _, _ = setup
    client = TestClient(create_app(svc, "x" * 40))
    response = client.get("/" + asset)
    assert response.status_code == 200
    assert content_type in response.headers["content-type"]
    assert response.content
    assert client.get("/service.py").status_code == 404
