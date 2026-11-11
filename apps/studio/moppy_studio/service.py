"""Durable review → submit → monitor lifecycle, shared by the UI and CLI tests."""

import hashlib
import json
import os
import re
import subprocess
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .configuration import dump, module_setup, parse_yaml, validate
from .monitor import read_tasks, summarize, tail
from .runtime import Runtime
from .scheduler import JOB_ID, PBS, SubmissionUncertain


def now():
    return datetime.now(timezone.utc).isoformat()


def write_json(path, value):
    temporary = path.with_name("." + path.name + "." + uuid.uuid4().hex)
    with temporary.open("x") as stream:
        os.chmod(temporary, 0o600)
        json.dump(value, stream, indent=2)
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


class Service:
    def __init__(self, state, runtime=None, scheduler=None):
        self.state = Path(state).expanduser().resolve()
        self.state.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.runtime = runtime or Runtime()
        self.pbs = scheduler or PBS()

    def path(self, key):
        if not re.fullmatch(r"[a-f0-9]{32}", key):
            raise ValueError("Invalid run ID")
        return self.state / (key + ".json")

    def read(self, key):
        return json.loads(self.path(key).read_text())

    def save(self, record):
        write_json(self.path(record["id"]), record)
        return record

    def list(self, live=False):
        records = []
        for path in sorted(
            self.state.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True
        )[:300]:
            try:
                records.append(json.loads(path.read_text()))
            except (OSError, ValueError):
                continue
        if live:
            jobs = [r["monitor_job"] for r in records if r.get("monitor_job")]
            try:
                states = self.pbs.query(jobs) if jobs else {}
            except (OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
                states = {
                    job: {"job_state": "unknown", "error": str(exc)} for job in jobs
                }
            records = [
                self._refresh(r, states.get(r.get("monitor_job"), {})) for r in records
            ]
        return [
            {
                k: r.get(k)
                for k in (
                    "id",
                    "name",
                    "created",
                    "state",
                    "directory",
                    "runtime",
                    "monitor_job",
                    "read_only",
                )
            }
            for r in records
        ]

    def plan(self, config, name, module="conda/analysis3"):
        c = validate(config)
        key = uuid.uuid4().hex
        name = str(name).strip()[:160] or c["experiment_id"]
        job_name = "moppy_" + key[:12]
        runtime = self.runtime.call(
            "inspect", {"config": c, "job_name": job_name}, module
        )
        # Only our generated setup is rewritten. Imported executable setup is retained and reviewed.
        managed_setup = c["worker_init"].strip() in {
            module_setup(module).strip(),
            module_setup("conda/analysis3").strip(),
        }
        if managed_setup:
            c["worker_init"] = module_setup(runtime["module"])
            runtime = self.runtime.call(
                "inspect", {"config": c, "job_name": job_name}, runtime["module"]
            )
        warnings = [
            "Preflight checks configuration, paths and runtime compatibility. MOPPy validates variable definitions and scientific inputs when workers run."
        ]
        if not managed_setup:
            warnings.append(
                "Imported worker_init will execute as your account in monitor and worker jobs. Verify it uses the intended MOPPy environment; the displayed runtime version describes the Studio probe."
            )
        if c.get("experiment_id") not in ("piControl", "amip") and not c.get(
            "parent_info"
        ):
            warnings.append(
                "No parent_info supplied. Check experiment lineage before using MOPPy defaults."
            )
        record = {
            "id": key,
            "name": name,
            "created": now(),
            "state": "review",
            "config": c,
            "yaml": dump(c),
            "directory": c["output_folder"],
            "job_name": job_name,
            "runtime": {k: v for k, v in runtime.items() if k != "monitor_script"},
            "script": runtime["monitor_script"],
            "warnings": warnings,
            "read_only": False,
        }
        record["sha256"] = hashlib.sha256(
            (record["yaml"] + record["script"]).encode()
        ).hexdigest()
        return self.save(record)

    def submit(self, key, digest):
        r = self.read(key)
        if r.get("read_only") or r["state"] != "review" or digest != r.get("sha256"):
            raise ValueError("Only the unchanged reviewed plan can be submitted once")
        validate(r["config"])
        # mkdir is atomic across Lustre nodes; flock is only node-local on Gadi.
        # A claim is deliberately never recycled after an uncertain submission.
        claim = self.state / (key + ".submitted")
        try:
            claim.mkdir(mode=0o700)
        except FileExistsError as exc:
            raise ValueError(
                "Submission already attempted. Refresh or reconcile; do not resubmit."
            ) from exc
        r.update(state="submitting", submission_started=now())
        self.save(r)
        root = Path(r["directory"])
        try:
            root.parent.mkdir(parents=True, exist_ok=True)
            root.mkdir(mode=0o700)  # Never reuse another batch's output directory.
            (root / "batch_config.yml").write_text(r["yaml"])
            script = root / "moppy_monitor.sh"
            script.write_text(r["script"])
            script.chmod(0o700)
            initialized = self.runtime.call(
                "initialize", {"config": r["config"]}, r["runtime"]["module"]
            )
            if initialized["version"] != r["runtime"]["version"]:
                raise ValueError(
                    "MOPPy version changed since review; create and review a new batch"
                )
        except Exception as exc:
            r.update(state="preparation failed", error=str(exc))
            return self.save(r)
        # Persist intent before invoking qsub. A crash here is never a reason to repeat qsub.
        r["submission_intent"] = {
            "name": r["job_name"],
            "output": str(root / "moppy_monitor.out"),
            "time": now(),
        }
        self.save(r)
        try:
            job = self.pbs.submit(script)
        except (SubmissionUncertain, subprocess.TimeoutExpired, OSError) as exc:
            r.update(state="submission uncertain", error=str(exc))
            return self.save(r)
        except RuntimeError as exc:
            r.update(state="submission failed", error=str(exc))
            return self.save(r)
        r.update(state="submitted", monitor_job=job, submitted=now())
        # Save the accepted job before any optional filesystem operations.
        self.save(r)
        try:
            (root / ".moppy_main.jobid").write_text(job + "\n" + now() + "\n")
            write_json(
                root / "studio-run.json",
                {
                    "id": key,
                    "monitor_job": job,
                    "sha256": r["sha256"],
                    "runtime": r["runtime"],
                },
            )
        except OSError as exc:
            r["warning"] = (
                f"Job accepted, but auxiliary metadata could not be saved: {exc}"
            )
            self.save(r)
        return r

    def reconcile(self, key):
        r = self.read(key)
        if r["state"] not in ("submitting", "submission uncertain") or not r.get(
            "submission_intent"
        ):
            raise ValueError("There is no uncertain qsub attempt to reconcile")
        found = self.pbs.find(r["job_name"], Path(r["directory"]) / "moppy_monitor.out")
        if len(found) != 1:
            raise ValueError(
                f"Found {len(found)} matching owned jobs. Keep this run uncertain and inspect PBS history; no resubmission was made."
            )
        r.update(state="submitted", monitor_job=found[0], reconciled=now())
        r.pop("error", None)
        return self.save(r)

    def attach(self, path, name=""):
        root = Path(path).expanduser().resolve()
        if root.name == "cmor_tasks.db":
            root = root.parent
        tasks, error = read_tasks(root)
        if error:
            raise ValueError(error)
        for old in self.list():
            if old["directory"] == str(root):
                return self.read(old["id"])
        r = dict(
            id=uuid.uuid4().hex,
            name=str(name).strip()[:160] or root.name,
            directory=str(root),
            state="attached",
            read_only=True,
            created=now(),
        )
        sidecar = root / ".moppy_main.jobid"
        if sidecar.is_file():
            job = sidecar.read_text()[:500].splitlines()[0]
            if JOB_ID.fullmatch(job):
                r["monitor_job"] = job
        return self.save(r)

    def refresh(self, key):
        return self._refresh(self.read(key))

    def _refresh(self, r, scheduler=None):
        tasks, error = read_tasks(r["directory"], r.get("config"))
        expected = r.get("config", {}).get("variables")
        summary = summarize(tasks, expected)
        r.update(tasks=tasks, summary=summary, tracking_error=error, refreshed=now())
        job = r.get("monitor_job")
        if scheduler is None and job:
            try:
                scheduler = self.pbs.query([job]).get(job, {})
            except (OSError, RuntimeError, subprocess.TimeoutExpired) as exc:
                scheduler = {"job_state": "unknown", "error": str(exc)}
        scheduler = scheduler or {}
        r["scheduler"] = scheduler
        if r["state"] in (
            "review",
            "preparation failed",
            "submission failed",
            "submission uncertain",
            "submitting",
        ):
            return r
        s = scheduler.get("job_state")
        if error:
            r["state"] = "tracking unavailable"
        elif summary["total"] and summary["completed"] == summary["total"]:
            r["state"] = "completed"
        elif s in ("Q", "H", "W"):
            r["state"] = "queued"
        elif s in ("R", "E", "B"):
            r["state"] = "running"
        elif s in ("F", "X"):
            r["state"] = "failed" if summary["failed"] else "incomplete"
        elif (
            summary["failed"]
            and summary["failed"] + summary["completed"] == summary["total"]
        ):
            r["state"] = "completed with errors"
        else:
            r["state"] = "unknown"
        # Derived status is returned, not persisted: a concurrent refresh cannot overwrite submission.
        return r

    def logs(self, key, kind="monitor_out", variable=None):
        r = self.read(key)
        root = Path(r["directory"]).resolve()
        files = {"monitor_out": "moppy_monitor.out", "monitor_err": "moppy_monitor.err"}
        if kind in files:
            target = root / files[kind]
        elif kind in ("worker_out", "worker_err"):
            tasks, error = read_tasks(root, r.get("config"))
            if variable not in {t["variable"] for t in tasks} or not re.fullmatch(
                r"[A-Za-z0-9_.-]+", variable
            ):
                raise ValueError("Choose a tracked variable")
            safe = variable.replace(".", "_")
            target = (
                root
                / "logs"
                / safe
                / ("cmor_" + safe + (".out" if kind.endswith("out") else ".err"))
            )
        else:
            raise ValueError("Unknown log type")
        if not target.resolve().is_relative_to(root):
            raise ValueError("Log must remain inside the batch directory")
        return {"path": str(target), "text": tail(target)}

    def load(self, path):
        target = Path(path).expanduser().resolve()
        if target.suffix not in (".yml", ".yaml") or target.stat().st_size > 512000:
            raise ValueError("Choose a YAML batch config smaller than 512 KB")
        text = target.read_text()
        return {"yaml": text, "config": parse_yaml(text), "path": str(target)}
