"""PBS transport. Job IDs always originate from submissions or owned run records."""

import getpass
import json
import re
import subprocess

JOB_ID = re.compile(r"^\d+(?:\[\d*\])?(?:\.[A-Za-z0-9_.-]+)?$")


class SubmissionUncertain(RuntimeError):
    pass


class PBS:
    def submit(self, script, dependencies=()):
        args = ["qsub"]
        for job in dependencies:
            if not JOB_ID.fullmatch(job):
                raise ValueError("Invalid dependency job ID")
        if dependencies:
            args += ["-W", "depend=afterany:" + ":".join(dependencies)]
        args.append(str(script))
        try:
            result = subprocess.run(
                args, text=True, capture_output=True, timeout=45, cwd=script.parent
            )
        except subprocess.TimeoutExpired as e:
            raise SubmissionUncertain(
                "qsub timed out; reconcile PBS before another submission"
            ) from e
        if result.returncode:
            raise RuntimeError(result.stderr.strip() or result.stdout.strip())
        job = result.stdout.strip()
        if not JOB_ID.fullmatch(job):
            raise SubmissionUncertain(
                "qsub did not return an unambiguous job ID; reconcile before retrying"
            )
        return job

    def query(self, jobs):
        jobs = sorted({j for j in jobs if JOB_ID.fullmatch(j)})
        if not jobs:
            return {}
        p = subprocess.run(
            ["qstat", "-xf", "-F", "json", *jobs],
            text=True,
            capture_output=True,
            timeout=20,
        )
        try:
            records = json.loads(p.stdout).get("Jobs", {})
        except ValueError:
            return {
                j: {
                    "job_state": "unknown",
                    "error": p.stderr.strip() or "PBS did not return JSON",
                }
                for j in jobs
            }
        return {
            j: next(
                (v for k, v in records.items() if k.split(".")[0] == j.split(".")[0]),
                {"job_state": "unknown", "error": "Not present in PBS history"},
            )
            for j in jobs
        }

    def find(self, name, output_path):
        result = subprocess.run(
            ["qselect", "-x", "-u", getpass.getuser(), "-N", name],
            text=True,
            capture_output=True,
            timeout=30,
        )
        if result.returncode:
            raise RuntimeError(result.stderr.strip() or "PBS lookup failed")
        jobs = [j for j in result.stdout.split() if JOB_ID.fullmatch(j)]
        states = self.query(jobs)
        return [
            j
            for j, s in states.items()
            if s.get("Job_Owner", "").split("@")[0] == getpass.getuser()
            and s.get("Output_Path", "").split(":", 1)[-1] == str(output_path)
        ]

    def cancel(self, jobs):
        states = self.query(jobs)
        outcomes = {}
        for job, info in states.items():
            if info.get("job_state") in {"F", "X"}:
                continue
            owner = info.get("Job_Owner", "").split("@")[0]
            if owner != getpass.getuser():
                outcomes[job] = "Ownership or scheduler state could not be verified"
                continue
            p = subprocess.run(
                ["qdel", job], text=True, capture_output=True, timeout=20
            )
            if p.returncode:
                outcomes[job] = p.stderr.strip()
        return outcomes
