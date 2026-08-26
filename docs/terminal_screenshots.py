"""Generate the terminal screenshots used in the documentation.

The screenshots are SVG "terminal windows" produced by ``rich``: the
``moppy-tui`` ones are rendered by calling the dashboard's own
:func:`render` on a throwaway tracker database, so a change to the TUI
layout shows up in the documentation on the next build instead of leaving a
hand-drawn mock-up behind.  The shell transcripts are scripted here, because
``moppy-cmorise`` needs a PBS scheduler and a real model archive to run; the
lines are copied verbatim from the ``print()`` calls in
``access_moppy/batch_cmoriser.py``.

Called from ``docs/source/conf.py`` at build time; the SVGs it writes are
generated artefacts and are not committed (see ``.gitignore``).  It can also
be run directly while editing the fixtures::

    python docs/terminal_screenshots.py [output_dir]

The example NCI project codes, user names, job IDs and paths below are
fabricated for illustration.
"""

from __future__ import annotations

import importlib.util
import os
import sqlite3
import sys
import tempfile
import time
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path

DOCS_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = DOCS_DIR.parent
PACKAGE_DIR = PROJECT_ROOT / "src" / "access_moppy"

DEFAULT_OUT_DIR = DOCS_DIR / "source" / "_generated" / "terminal"

# Wide enough for a branded variable name plus the experiment, status, start
# time and duration columns without the table folding onto a second line.
CONSOLE_WIDTH = 118

# Screenshots are taken from an NCI Gadi login node, so render the timestamps
# the way a user there would see them rather than in the builder's timezone.
SCREENSHOT_TZ = "Australia/Sydney"

PROMPT = "[rb1234@gadi-login-04 ~]$ "

QUICKSTART_DB = "/scratch/xp65/rb1234/moppy_output/first_run/cmor_tasks.db"
BASELINE_DB = "/scratch/xp65/rb1234/moppy_output/piControl/cmor_tasks.db"


# --------------------------------------------------------------------------
# Loading the dashboard code without importing the package
# --------------------------------------------------------------------------
# ``import access_moppy`` runs ``_config.load_moppy_config()``, which prompts
# on stdin for name/email/ORCID when ``~/.moppy/user.yml`` is missing.  That
# would hang (or, with stdin closed, fail) a Read the Docs build, so the two
# modules needed here are loaded straight from their files instead.


def _load_module(name: str, path: Path, package_path: Path | None = None):
    """Load a single module from ``path`` without importing its package.

    ``package_path`` sets ``__path__`` on a synthetic parent package so that
    modules using relative imports (``cli_dashboard`` imports ``._time``)
    still resolve.
    """
    if package_path is not None:
        parent = name.rpartition(".")[0]
        if parent and parent not in sys.modules:
            pkg = types.ModuleType(parent)
            pkg.__path__ = [str(package_path)]
            sys.modules[parent] = pkg
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:  # pragma: no cover - defensive
        raise ImportError(f"cannot load {name} from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


def _dashboard():
    dashboard_dir = PACKAGE_DIR / "dashboard"
    _load_module(
        "_moppy_docs_dashboard._time", dashboard_dir / "_time.py", dashboard_dir
    )
    return _load_module(
        "_moppy_docs_dashboard.cli_dashboard",
        dashboard_dir / "cli_dashboard.py",
        dashboard_dir,
    )


def _tracker_class():
    """Return the real ``TaskTracker`` so the fixture DB uses the real schema."""
    return _load_module("_moppy_docs_tracking", PACKAGE_DIR / "tracking.py").TaskTracker


# --------------------------------------------------------------------------
# Fixtures
# --------------------------------------------------------------------------
# Task times are expressed as minutes before "now" rather than as absolute
# dates: the TUI computes the duration of a running task against the current
# time, so anchoring the fixtures to the build time keeps every duration and
# ETA in the screenshots self-consistent (and stops the dates going stale).


def _task(variable, status, started_min_ago=None, ran_for_min=None, error=None):
    return {
        "variable": variable,
        "status": status,
        "started_min_ago": started_min_ago,
        "ran_for_min": ran_for_min,
        "error": error,
    }


# The three-variable batch from the CMIP7 FastTrack quick start.
QUICKSTART_VARIABLES = [
    "atmos.tas.tavg-h2m-hxy-u.mon.glb",
    "atmos.pr.tavg-u-hxy-u.mon.glb",
    "ocean.tos.tavg-u-hxy-sea.mon.glb",
]

QUICKSTART_RUNNING = [
    _task(QUICKSTART_VARIABLES[0], "completed", 74.2, 22.7),
    _task(QUICKSTART_VARIABLES[1], "running", 21.4),
    _task(QUICKSTART_VARIABLES[2], "pending"),
]

QUICKSTART_DONE = [
    _task(QUICKSTART_VARIABLES[0], "completed", 74.2, 22.7),
    _task(QUICKSTART_VARIABLES[1], "completed", 51.1, 26.3),
    _task(QUICKSTART_VARIABLES[2], "completed", 24.6, 19.9),
]

# A larger baseline batch, part-way through, with two failures.
_PICONTROL_ARCHIVE = "/g/data/p73/archive/CMIP7/ACCESS-ESM1-6/piControl/HI-C-05-r1"

# A worker failure carries the exception text it died on (see
# templates/cmor_python_script.j2); a failure the monitor reconciles from PBS
# is the pipe-separated summary built by format_pbs_error().
BASELINE_TASKS = [
    _task("ocean.so.tavg-ol-hxy-sea.mon.glb", "running", 96.3),
    _task("ocean.sos.tavg-u-hxy-sea.mon.glb", "running", 96.1),
    _task("ocean.thetao.tavg-ol-hxy-sea.mon.glb", "running", 95.4),
    _task("land.mrso.tavg-u-hxy-lnd.mon.glb", "pending"),
    _task("ocean.mlotst.tavg-u-hxy-sea.mon.glb", "pending"),
    _task("seaIce.siconc.tavg-u-hxy-u.mon.glb", "pending"),
    _task("seaIce.simass.tavg-u-hxy-si.mon.glb", "pending"),
    _task(
        "land.mrro.tavg-u-hxy-lnd.mon.glb",
        "failed",
        88.5,
        1.1,
        f"No files found for 'Lmon.mrro' under '{_PICONTROL_ARCHIVE}'. "
        "Pass explicit file paths via 'input_data' to bypass discovery.",
    ),
    _task(
        "seaIce.sithick.tavg-u-hxy-si.mon.glb",
        "failed",
        84.2,
        2.2,
        "job 147021391.gadi-pbs | exit_status=137 | pbs_comment='job killed: "
        "mem 68.2gb exceeded limit 64gb' | mem_used=68.2gb | "
        "walltime_used=00:02:11",
    ),
    _task("atmos.clt.tavg-u-hxy-u.mon.glb", "completed", 172.4, 8.6),
    _task("atmos.hfls.tavg-u-hxy-u.mon.glb", "completed", 170.2, 9.1),
    _task("atmos.pr.tavg-u-hxy-u.mon.glb", "completed", 165.8, 7.4),
    _task("atmos.psl.tavg-u-hxy-u.mon.glb", "completed", 158.3, 6.8),
    _task("atmos.rlut.tavg-u-hxy-u.mon.glb", "completed", 150.9, 11.2),
    _task("atmos.tas.tavg-h2m-hxy-u.mon.glb", "completed", 143.1, 7.7),
]

# Exactly what moppy-cmorise prints on a successful submission; see
# access_moppy/batch_cmoriser.py (main()).
_FIRST_RUN_DIR = "/scratch/xp65/rb1234/moppy_output/first_run"
_MONITOR_JOB = "147021374.gadi-pbs"

CMORISE_TRANSCRIPT = [
    (
        "moppy-cmorise my_first_run.yml",
        [
            f"Database initialized with 3 tasks at: {_FIRST_RUN_DIR}/cmor_tasks.db",
            f"Created monitor script: {_FIRST_RUN_DIR}/moppy_monitor.sh",
            "",
            f"Submitted monitor job {_MONITOR_JOB}",
            "  Watches 3 variable(s)",
            f"  Sub-jobs are qsub'd from the monitor (see {_FIRST_RUN_DIR}/moppy_monitor.out)",
            f"  Sidecar file: {_FIRST_RUN_DIR}/.moppy_main.jobid",
            f"  Track progress: qstat -x {_MONITOR_JOB}",
            "Dashboard available at: http://localhost:8501",
        ],
    ),
]


# --------------------------------------------------------------------------
# Rendering
# --------------------------------------------------------------------------


def _console(record_width: int = CONSOLE_WIDTH):
    from rich.console import Console

    # file=os.devnull: the screenshot is taken from the recording, nothing
    # should reach the docs build log.
    return Console(
        record=True,
        width=record_width,
        file=open(os.devnull, "w"),
        no_color=False,
    )


def _svg_format():
    """rich's SVG template with the Fira Code ``@font-face`` rules removed.

    Sphinx embeds these SVGs with ``<img>``, which never fetches external
    resources, so the webfont links would only leave dead CDN URLs in the
    published assets.  Every text run carries an explicit ``textLength``, so
    the fallback monospace font still lines up.
    """
    from rich._export_format import CONSOLE_SVG_FORMAT

    start = CONSOLE_SVG_FORMAT.find("@font-face")
    end = CONSOLE_SVG_FORMAT.find(".{unique_id}-matrix")
    if start == -1 or end == -1 or end < start:
        # rich reorganised its template; the webfont is a cosmetic detail, so
        # fall back to the stock one rather than failing the docs build.
        return CONSOLE_SVG_FORMAT
    return CONSOLE_SVG_FORMAT[:start] + CONSOLE_SVG_FORMAT[end:]


def _save(console, out_dir: Path, name: str, title: str) -> Path:
    path = out_dir / f"{name}.svg"
    console.save_svg(str(path), title=title, code_format=_svg_format())
    return path


def _build_fixture_db(db_path: Path, tasks, experiment_id: str) -> None:
    """Populate a tracker DB with the real schema and fixed task times."""
    tracker_cls = _tracker_class()
    with tracker_cls(db_path) as tracker:
        for task in tasks:
            tracker.add_task(task["variable"], experiment_id)

    now = datetime.now(timezone.utc).replace(tzinfo=None, microsecond=0)
    conn = sqlite3.connect(db_path)
    with conn:
        for task in tasks:
            start = end = None
            if task["started_min_ago"] is not None:
                start_dt = now - timedelta(minutes=task["started_min_ago"])
                start = start_dt.isoformat(sep=" ")
                if task["ran_for_min"] is not None:
                    end = (start_dt + timedelta(minutes=task["ran_for_min"])).isoformat(
                        sep=" "
                    )
            conn.execute(
                "UPDATE cmor_tasks SET status = ?, start_time = ?, end_time = ?, "
                "error_message = ? WHERE variable = ? AND experiment_id = ?",
                (
                    task["status"],
                    start,
                    end,
                    task["error"],
                    task["variable"],
                    experiment_id,
                ),
            )
    conn.close()


def _tui_svg(
    out_dir: Path,
    name: str,
    *,
    tasks,
    experiment_id: str,
    display_db: str,
    command: str,
    statuses=None,
    experiment_filter=None,
    page_size: int = 20,
    show_footer: bool = True,
) -> Path:
    dashboard = _dashboard()

    with tempfile.TemporaryDirectory() as tmp:
        db_path = Path(tmp) / "cmor_tasks.db"
        _build_fixture_db(db_path, tasks, experiment_id)
        snapshot = dashboard.load_snapshot(
            db_path, statuses=statuses, experiment=experiment_filter
        )

    # The real path is a throwaway temporary directory; show the path a user
    # would actually have typed.
    snapshot.db_path = Path(display_db)

    console = _console()
    console.print(
        dashboard.render(snapshot, page_size=page_size, show_footer=show_footer)
    )
    return _save(console, out_dir, name, command)


def _shell_svg(out_dir: Path, name: str, blocks, title: str) -> Path:
    from rich.text import Text

    console = _console()
    body = Text()
    for index, (command, output) in enumerate(blocks):
        if index:
            body.append("\n")
        body.append(PROMPT, style="bold green")
        body.append(command + "\n", style="bold")
        for line in output:
            body.append(line + "\n")
    body.append(PROMPT, style="bold green")
    console.print(body, highlight=False)
    return _save(console, out_dir, name, title)


# --------------------------------------------------------------------------
# Entry point
# --------------------------------------------------------------------------


def generate(out_dir: Path | str = DEFAULT_OUT_DIR) -> list[Path]:
    """Write every documentation terminal screenshot into ``out_dir``."""
    try:
        import rich  # noqa: F401
    except ModuleNotFoundError as exc:  # pragma: no cover - build environment
        raise ModuleNotFoundError(
            "The documentation terminal screenshots need 'rich'. Install the "
            "docs requirements (pip install -e '.[docs]') and rebuild."
        ) from exc

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    previous_tz = os.environ.get("TZ")
    os.environ["TZ"] = SCREENSHOT_TZ
    if hasattr(time, "tzset"):
        time.tzset()
    try:
        written = [
            _shell_svg(
                out_dir,
                "moppy-cmorise-submit",
                CMORISE_TRANSCRIPT,
                "rb1234@gadi-login-04",
            ),
            _tui_svg(
                out_dir,
                "moppy-tui-running",
                tasks=QUICKSTART_RUNNING,
                experiment_id="historical",
                display_db=QUICKSTART_DB,
                command=f"moppy-tui --db {QUICKSTART_DB}",
            ),
            _tui_svg(
                out_dir,
                "moppy-tui-complete",
                tasks=QUICKSTART_DONE,
                experiment_id="historical",
                display_db=QUICKSTART_DB,
                command=f"moppy-tui --db {QUICKSTART_DB}",
            ),
            _tui_svg(
                out_dir,
                "moppy-tui-failures",
                tasks=BASELINE_TASKS,
                experiment_id="piControl",
                display_db=BASELINE_DB,
                command=f"moppy-tui --db {BASELINE_DB}",
                page_size=10,
            ),
            _tui_svg(
                out_dir,
                "moppy-tui-filtered",
                tasks=BASELINE_TASKS,
                experiment_id="piControl",
                display_db=BASELINE_DB,
                command="moppy-tui --status failed --experiment piControl",
                statuses=["failed"],
                experiment_filter="piControl",
            ),
        ]
    finally:
        if previous_tz is None:
            os.environ.pop("TZ", None)
        else:
            os.environ["TZ"] = previous_tz
        if hasattr(time, "tzset"):
            time.tzset()

    return written


if __name__ == "__main__":
    target = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_OUT_DIR
    for svg in generate(target):
        print(f"wrote {svg}")
