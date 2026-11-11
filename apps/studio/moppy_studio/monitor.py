"""Read existing MOPPy databases without initializing or migrating them."""

import json
import sqlite3
from collections import Counter
from pathlib import Path


def read_tasks(directory, config=None):
    database = Path(directory) / "cmor_tasks.db"
    if not database.is_file():
        return [], "Waiting for MOPPy tracking database"
    try:
        with sqlite3.connect(
            database.resolve().as_uri() + "?mode=ro", uri=True, timeout=2
        ) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA query_only=ON")
            conn.execute("PRAGMA mmap_size=0")
            if config:
                rows = conn.execute(
                    "SELECT * FROM cmor_tasks WHERE experiment_id=? ORDER BY variable LIMIT 10000",
                    (config["experiment_id"],),
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM cmor_tasks ORDER BY variable LIMIT 10000"
                ).fetchall()
        tasks = [dict(row) for row in rows]
        if config:
            tasks = [t for t in tasks if t["variable"] in config["variables"]]
        for task in tasks:
            for key in (
                "pbs_info_json",
                "output_summary_json",
                "compliance_json",
                "worker_memory_json",
            ):
                if task.get(key):
                    try:
                        task[key.removesuffix("_json")] = json.loads(task[key])
                    except (TypeError, ValueError):
                        pass
        return tasks, None
    except (sqlite3.Error, OSError) as exc:
        return [], f"Tracking database temporarily unreadable: {exc}"


def summarize(tasks, expected=None):
    counts = Counter(t.get("status", "unknown") for t in tasks)
    total = len(expected) if expected is not None else len(tasks)
    counts["pending"] += max(0, total - len(tasks))
    return {
        "total": total,
        "completed": counts["completed"],
        "running": counts["running"],
        "failed": counts["failed"],
        "pending": counts["pending"],
        "percent": round(100 * counts["completed"] / total, 1) if total else 0,
    }


def tail(path, limit=60000):
    try:
        with Path(path).open("rb") as stream:
            stream.seek(0, 2)
            size = stream.tell()
            stream.seek(max(0, size - limit))
            return (
                "[Earlier log text omitted]\n" if size > limit else ""
            ) + stream.read(limit).decode("utf-8", errors="replace")
    except FileNotFoundError:
        return "This log has not been created yet."
