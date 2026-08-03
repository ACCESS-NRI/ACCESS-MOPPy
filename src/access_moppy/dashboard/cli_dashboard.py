"""Terminal dashboard for the CMORisation tracker.

A `rich`-based alternative to the Streamlit dashboard. Reads the same
`cmor_tasks` SQLite database, so it can run side-by-side with the web UI.

Live mode supports interactive paging:

    Tab       switch focus between the tasks and failures panels
    j / ↓     scroll down one row
    k / ↑     scroll up one row
    n / Space page down
    p / b     page up
    g         jump to top
    G         jump to bottom
    r         force DB re-read
    q / Ctrl-C / Ctrl-D    quit
"""

from __future__ import annotations

import argparse
import json
import os
import select
import sqlite3
import sys
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from ._time import format_local_time

STATUS_ORDER = ("running", "pending", "failed", "completed")
STATUS_STYLE = {
    "running": "cyan",
    "pending": "dim",
    "failed": "red",
    "completed": "green",
}

# Hard cap on rows pulled from the DB into a Snapshot. Larger than any
# realistic batch size, so pagination operates fully in-memory.
DEFAULT_LOAD_LIMIT = 10_000


def resolve_db_path(cli_value: Optional[str]) -> Path:
    if cli_value:
        return Path(cli_value).expanduser()
    env_value = os.environ.get("CMOR_TRACKER_DB")
    if env_value:
        return Path(env_value).expanduser()
    return Path.home() / ".moppy" / "db" / "cmor_tasks.db"


@dataclass
class Snapshot:
    db_path: Path
    refreshed_at: datetime
    summary: dict[str, int] = field(default_factory=dict)
    tasks: list[dict] = field(default_factory=list)
    failures: list[dict] = field(default_factory=list)
    total: int = 0
    completed: int = 0
    avg_completed_seconds: Optional[float] = None
    error: Optional[str] = None

    @property
    def progress_fraction(self) -> float:
        return self.completed / self.total if self.total else 0.0

    @property
    def eta_seconds(self) -> Optional[float]:
        if self.avg_completed_seconds is None:
            return None
        remaining = self.total - self.completed
        if remaining <= 0:
            return 0.0
        return self.avg_completed_seconds * remaining


def _open_readonly(db_path: Path) -> sqlite3.Connection:
    uri = f"file:{db_path}?mode=ro"
    return sqlite3.connect(uri, uri=True, timeout=5)


def load_snapshot(
    db_path: Path,
    statuses: Optional[list[str]] = None,
    experiment: Optional[str] = None,
    max_rows: int = DEFAULT_LOAD_LIMIT,
    max_failures: int = DEFAULT_LOAD_LIMIT,
) -> Snapshot:
    snap = Snapshot(db_path=db_path, refreshed_at=datetime.now())

    if not db_path.exists():
        snap.error = f"Database not found: {db_path}"
        return snap

    try:
        conn = _open_readonly(db_path)
    except sqlite3.OperationalError as exc:
        snap.error = f"Cannot open database (read-only): {exc}"
        return snap

    try:
        conn.row_factory = sqlite3.Row
        try:
            summary_rows = conn.execute(
                "SELECT status, COUNT(*) AS n FROM cmor_tasks GROUP BY status"
            ).fetchall()
        except sqlite3.OperationalError as exc:
            snap.error = f"Table 'cmor_tasks' not ready yet: {exc}"
            return snap

        snap.summary = {row["status"]: row["n"] for row in summary_rows}
        snap.total = sum(snap.summary.values())
        snap.completed = snap.summary.get("completed", 0)

        avg_row = conn.execute(
            """
            SELECT AVG((julianday(end_time) - julianday(start_time)) * 86400.0) AS avg_s
            FROM cmor_tasks
            WHERE status = 'completed'
              AND start_time IS NOT NULL
              AND end_time IS NOT NULL
            """
        ).fetchone()
        if avg_row and avg_row["avg_s"] is not None:
            snap.avg_completed_seconds = float(avg_row["avg_s"])

        clauses, params = [], []
        if statuses:
            placeholders = ",".join("?" for _ in statuses)
            clauses.append(f"status IN ({placeholders})")
            params.extend(statuses)
        if experiment:
            clauses.append("experiment_id = ?")
            params.append(experiment)
        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = conn.execute(
            f"""
            SELECT variable, experiment_id, status, start_time, end_time
            FROM cmor_tasks
            {where}
            ORDER BY
                CASE status
                    WHEN 'running'   THEN 0
                    WHEN 'pending'   THEN 1
                    WHEN 'failed'    THEN 2
                    ELSE 3
                END,
                start_time IS NULL,
                start_time DESC,
                variable ASC
            LIMIT ?
            """,
            (*params, max_rows),
        ).fetchall()
        snap.tasks = [dict(r) for r in rows]

        fail_rows = conn.execute(
            """
            SELECT variable, experiment_id, error_message, end_time
            FROM cmor_tasks
            WHERE status = 'failed'
            ORDER BY end_time DESC
            LIMIT ?
            """,
            (max_failures,),
        ).fetchall()
        snap.failures = [dict(r) for r in fail_rows]
    finally:
        conn.close()
    return snap


def _format_duration(seconds: Optional[float]) -> str:
    if seconds is None or seconds < 0:
        return "—"
    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def _row_duration(row: dict) -> Optional[float]:
    start = row.get("start_time")
    end = row.get("end_time")
    if not start:
        return None
    try:
        start_dt = datetime.fromisoformat(start)
    except ValueError:
        return None
    if end:
        try:
            end_dt = datetime.fromisoformat(end)
        except ValueError:
            return None
    else:
        # start_time/end_time are written via SQLite's datetime('now'), which is
        # UTC. Comparing against local now() here would skew in-flight durations
        # by the local UTC offset.
        end_dt = datetime.now(timezone.utc).replace(tzinfo=None)
    return (end_dt - start_dt).total_seconds()


def clamp_offset(offset: int, page_size: int, n_visible: int) -> int:
    """Snap an offset into the valid range after DB changes or paging."""
    if n_visible <= 0 or page_size <= 0:
        return 0
    max_offset = max(0, n_visible - page_size)
    return max(0, min(offset, max_offset))


# Lines consumed by the header/progress/summary/footer panels and by each
# table panel's own frame (border + column header + separator + border),
# outside of the data rows themselves.
_FIXED_CHROME_LINES = 13  # header(3) + progress(4) + summary(3) + footer(3)
_TABLE_FRAME_LINES = 4
_COLLAPSED_ROWS = 3


def compute_table_row_counts(
    console_height: int,
    requested_page_size: int,
    focus: str,
    has_failures: bool,
    show_footer: bool,
) -> tuple[int, int]:
    """Pick (tasks_rows, failures_rows) so the whole layout fits on screen.

    With both the tasks and failures tables always rendered at full
    `requested_page_size`, the combined layout easily exceeds a normal
    terminal height, pushing the unfocused panel off-screen — its scroll
    keys then appear to do nothing, since the changes happen where nothing
    is visible. Instead, the focused panel gets as many rows as fit (up to
    `requested_page_size`); the other one collapses to a small preview.
    Non-interactive callers (show_footer=False, e.g. --once/--json) or
    snapshots without failures are unaffected.
    """
    if not has_failures or not show_footer:
        return requested_page_size, requested_page_size

    collapsed = min(_COLLAPSED_ROWS, requested_page_size)
    available = (
        console_height - _FIXED_CHROME_LINES - 2 * _TABLE_FRAME_LINES - collapsed
    )
    expanded = max(1, min(requested_page_size, available))

    if focus == "tasks":
        return expanded, collapsed
    return collapsed, expanded


def apply_key(key: str, offset: int, page_size: int, n_visible: int) -> int:
    """Pure key→offset mapping. Unit-tested without termios."""
    max_offset = max(0, n_visible - page_size)
    if key in ("j", "\x1b[B"):
        return min(offset + 1, max_offset)
    if key in ("k", "\x1b[A"):
        return max(0, offset - 1)
    if key in ("n", " ", "\x1b[6~"):
        return min(offset + page_size, max_offset)
    if key in ("p", "b", "\x1b[5~"):
        return max(0, offset - page_size)
    if key == "g":
        return 0
    if key == "G":
        return max_offset
    return offset


def is_quit_key(key: str) -> bool:
    return key in ("q", "Q", "\x03", "\x04")


def render(
    snap: Snapshot,
    offset: int = 0,
    page_size: int = 20,
    show_footer: bool = False,
    failures_offset: int = 0,
    focus: str = "tasks",
    failures_page_size: Optional[int] = None,
):
    """Build the rich renderable for a snapshot.

    `offset` and `page_size` slice `snap.tasks` for paging; `failures_offset`
    and `failures_page_size` (defaults to `page_size`) slice `snap.failures`
    the same way — kept independent so the focused panel can be shown larger
    than the unfocused one (see `compute_table_row_counts`). `focus` ("tasks"
    or "failures") highlights which panel the paging keys currently apply to.
    `show_footer` adds the keyboard-hint line (only useful in live mode).
    """
    if failures_page_size is None:
        failures_page_size = page_size
    from rich.console import Group
    from rich.panel import Panel
    from rich.progress_bar import ProgressBar
    from rich.table import Table
    from rich.text import Text

    if snap.error:
        return Panel(
            Text(snap.error, style="red"),
            title="ACCESS-MOPPy CMORisation Monitor",
            border_style="red",
        )

    header = Text.assemble(
        ("DB: ", "bold"),
        (str(snap.db_path), "magenta"),
        "    ",
        ("refreshed: ", "bold"),
        (snap.refreshed_at.strftime("%Y-%m-%d %H:%M:%S"), "magenta"),
    )

    bar = ProgressBar(total=max(snap.total, 1), completed=snap.completed, width=40)
    pct = f"{snap.progress_fraction * 100:5.1f}%"
    eta_text = _format_duration(snap.eta_seconds)
    progress_line = Text.assemble(
        (f"  {pct}", "bold"),
        (f"   completed {snap.completed} / {snap.total}", ""),
        (f"   ETA {eta_text}", "dim"),
    )
    progress_group = Group(bar, progress_line)

    summary_text = Text("  ")
    for status in STATUS_ORDER:
        n = snap.summary.get(status, 0)
        summary_text.append(f"{status} ", style=STATUS_STYLE.get(status, ""))
        summary_text.append(f"{n}   ", style=f"bold {STATUS_STYLE.get(status, '')}")

    n_visible = len(snap.tasks)
    offset = clamp_offset(offset, page_size, n_visible)
    page = snap.tasks[offset : offset + page_size]
    first = offset + 1 if n_visible else 0
    last = offset + len(page)

    task_table = Table(
        expand=True, show_lines=False, header_style="bold", pad_edge=False
    )
    task_table.add_column("#", justify="right", style="dim", no_wrap=True)
    task_table.add_column("Variable", overflow="fold")
    task_table.add_column("Experiment", overflow="fold")
    task_table.add_column("Status")
    task_table.add_column("Started (local)", overflow="fold")
    task_table.add_column("Duration", justify="right")
    for i, row in enumerate(page, start=offset + 1):
        status = row["status"]
        task_table.add_row(
            str(i),
            row["variable"] or "",
            row["experiment_id"] or "",
            Text(status, style=STATUS_STYLE.get(status, "")),
            format_local_time(row.get("start_time")),
            _format_duration(_row_duration(row)),
        )

    if snap.total == n_visible:
        title = f"Tasks {first}-{last} of {n_visible}"
    else:
        title = (
            f"Tasks {first}-{last} of {n_visible} filtered " f"(DB total {snap.total})"
        )
    if show_footer and focus == "tasks":
        title += "  (focused)"

    panels = [
        Panel(
            header,
            border_style="blue",
            title="ACCESS-MOPPy CMORisation Monitor",
        ),
        Panel(progress_group, border_style="blue", title="Progress"),
        Panel(summary_text, border_style="blue", title="Summary"),
        Panel(
            task_table,
            border_style="bright_blue" if focus == "tasks" else "blue",
            title=title,
        ),
    ]

    if show_footer:
        hint = Text.assemble(
            ("  Tab", "bold"),
            (" switch panel  ", "dim"),
            ("j/", "dim"),
            ("↓", "bold"),
            (" down  ", "dim"),
            ("k/", "dim"),
            ("↑", "bold"),
            (" up  ", "dim"),
            ("n/", "dim"),
            ("Space", "bold"),
            (" pgDn  ", "dim"),
            ("p/", "dim"),
            ("b", "bold"),
            (" pgUp  ", "dim"),
            ("g", "bold"),
            (" top  ", "dim"),
            ("G", "bold"),
            (" bottom  ", "dim"),
            ("r", "bold"),
            (" refresh  ", "dim"),
            ("q", "bold"),
            (" quit", "dim"),
        )
        panels.append(Panel(hint, border_style="dim"))

    if snap.failures:
        n_fail_visible = len(snap.failures)
        failures_offset = clamp_offset(
            failures_offset, failures_page_size, n_fail_visible
        )
        fail_page = snap.failures[
            failures_offset : failures_offset + failures_page_size
        ]
        fail_first = failures_offset + 1 if n_fail_visible else 0
        fail_last = failures_offset + len(fail_page)

        fail_table = Table(expand=True, header_style="bold red", show_lines=False)
        fail_table.add_column("Variable")
        fail_table.add_column("Experiment")
        fail_table.add_column("Error", overflow="fold")
        for f in fail_page:
            err = (f.get("error_message") or "").strip().replace("\n", " ")
            if len(err) > 160:
                err = err[:157] + "..."
            fail_table.add_row(
                f["variable"] or "",
                f["experiment_id"] or "",
                err,
            )
        fail_title = f"Recent failures {fail_first}-{fail_last} of {n_fail_visible}"
        if show_footer and focus == "failures":
            fail_title += "  (focused)"
        panels.append(
            Panel(
                fail_table,
                border_style="bright_red" if focus == "failures" else "red",
                title=fail_title,
            )
        )

    return Group(*panels)


def snapshot_to_jsonable(snap: Snapshot) -> dict:
    return {
        "db_path": str(snap.db_path),
        "refreshed_at": snap.refreshed_at.isoformat(timespec="seconds"),
        "summary": snap.summary,
        "total": snap.total,
        "completed": snap.completed,
        "progress_fraction": snap.progress_fraction,
        "avg_completed_seconds": snap.avg_completed_seconds,
        "eta_seconds": snap.eta_seconds,
        "tasks": snap.tasks,
        "failures": snap.failures,
        "error": snap.error,
    }


# ---------------------------------------------------------------------------
# Keyboard handling (Live mode only)
# ---------------------------------------------------------------------------


@contextmanager
def _cbreak_mode():
    """Put stdin in cbreak mode if it's a TTY. Yields whether it succeeded."""
    if not sys.stdin.isatty():
        yield False
        return
    try:
        import termios
        import tty
    except ImportError:
        yield False
        return
    fd = sys.stdin.fileno()
    old = termios.tcgetattr(fd)
    try:
        tty.setcbreak(fd)
        yield True
    finally:
        termios.tcsetattr(fd, termios.TCSADRAIN, old)


_KEY_BUFFER: list[str] = []


def _read_key(timeout: float) -> Optional[str]:
    """Non-blocking single-keystroke read. Returns a logical key string.

    Uses ``os.read`` directly to bypass Python's TextIO buffering — without
    this, terminal escape sequences like ``ESC [ A`` (arrow keys) arrive
    as a single 3-byte burst from the kernel but Python's BufferedReader
    pulls the trailing ``[A`` into its own buffer, where a subsequent
    ``select(fd)`` cannot see them and they get dropped.
    """
    if _KEY_BUFFER:
        return _KEY_BUFFER.pop(0)
    fd = sys.stdin.fileno()
    rlist, _, _ = select.select([fd], [], [], timeout)
    if not rlist:
        return None
    try:
        data = os.read(fd, 32)
    except OSError:
        return None
    if not data:
        return None
    s = data.decode("utf-8", errors="replace")
    if s.startswith("\x1b"):
        return s
    if len(s) > 1:
        _KEY_BUFFER.extend(list(s[1:]))
    return s[0]


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="moppy-tui",
        description="Terminal dashboard for ACCESS-MOPPy CMORisation tasks.",
    )
    parser.add_argument(
        "--db",
        help=(
            "Path to cmor_tasks.db "
            "(default: $CMOR_TRACKER_DB or ~/.moppy/db/cmor_tasks.db)."
        ),
    )
    parser.add_argument(
        "--refresh",
        type=float,
        default=5.0,
        help="DB poll interval in seconds (default: 5.0). Ignored with --once/--json.",
    )
    parser.add_argument(
        "--status",
        help="Comma-separated statuses to include "
        "(pending,running,completed,failed).",
    )
    parser.add_argument("--experiment", help="Filter by experiment_id.")
    parser.add_argument(
        "--max-rows",
        "--page-size",
        dest="page_size",
        type=int,
        default=20,
        help="Page size: rows shown in the tasks table at a time (default: 20).",
    )
    parser.add_argument(
        "--page",
        type=int,
        default=1,
        help="1-based starting page (default 1). Only meaningful in --once/--json.",
    )
    parser.add_argument(
        "--max-failures",
        type=int,
        default=DEFAULT_LOAD_LIMIT,
        help="Rows loaded into the failures panel (paged with j/k, same as "
        "tasks; Tab switches focus to it).",
    )
    parser.add_argument(
        "--once",
        action="store_true",
        help="Render one snapshot and exit (useful for cron / logs).",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit a machine-readable JSON snapshot and exit.",
    )
    parser.add_argument("--no-color", action="store_true", help="Disable ANSI colors.")
    return parser


def _parse_statuses(value: Optional[str]) -> Optional[list[str]]:
    if not value:
        return None
    parsed = [s.strip() for s in value.split(",") if s.strip()]
    invalid = [s for s in parsed if s not in STATUS_STYLE]
    if invalid:
        raise SystemExit(
            f"Invalid status value(s): {invalid}. " f"Allowed: {sorted(STATUS_STYLE)}."
        )
    return parsed


def _run_live(args, db_path, statuses) -> int:
    from rich.console import Console
    from rich.live import Live

    console = Console(no_color=args.no_color)
    page_size = max(args.page_size, 1)

    def take_snapshot() -> Snapshot:
        return load_snapshot(
            db_path,
            statuses=statuses,
            experiment=args.experiment,
            max_rows=DEFAULT_LOAD_LIMIT,
            max_failures=args.max_failures,
        )

    snap = take_snapshot()
    offset = clamp_offset((args.page - 1) * page_size, page_size, len(snap.tasks))
    failures_offset = 0
    focus = "tasks"
    last_db_refresh = time.monotonic()
    dirty = True

    def row_counts() -> tuple[int, int]:
        # Recomputed each cycle: cheap, and picks up terminal resizes and
        # focus changes so the focused panel always gets the room to scroll.
        return compute_table_row_counts(
            console.size.height, page_size, focus, bool(snap.failures), interactive
        )

    with _cbreak_mode() as interactive:
        try:
            tasks_rows, failures_rows = row_counts()
            with Live(
                render(
                    snap,
                    offset=offset,
                    page_size=tasks_rows,
                    show_footer=interactive,
                    failures_offset=failures_offset,
                    focus=focus,
                    failures_page_size=failures_rows,
                ),
                console=console,
                refresh_per_second=10,
                screen=False,
            ) as live:
                while True:
                    tasks_rows, failures_rows = row_counts()
                    now = time.monotonic()
                    if now - last_db_refresh >= args.refresh:
                        snap = take_snapshot()
                        tasks_rows, failures_rows = row_counts()
                        offset = clamp_offset(offset, tasks_rows, len(snap.tasks))
                        failures_offset = clamp_offset(
                            failures_offset, failures_rows, len(snap.failures)
                        )
                        last_db_refresh = now
                        dirty = True

                    if interactive:
                        key = _read_key(timeout=0.05)
                        if key is not None:
                            if is_quit_key(key):
                                return 0
                            if key == "r":
                                snap = take_snapshot()
                                last_db_refresh = now
                                dirty = True
                            elif key == "\t":
                                focus = "failures" if focus == "tasks" else "tasks"
                                dirty = True
                            elif focus == "tasks":
                                new_offset = apply_key(
                                    key, offset, tasks_rows, len(snap.tasks)
                                )
                                if new_offset != offset:
                                    offset = new_offset
                                    dirty = True
                            else:
                                new_offset = apply_key(
                                    key,
                                    failures_offset,
                                    failures_rows,
                                    len(snap.failures),
                                )
                                if new_offset != failures_offset:
                                    failures_offset = new_offset
                                    dirty = True
                    else:
                        time.sleep(min(0.5, args.refresh))

                    if dirty:
                        tasks_rows, failures_rows = row_counts()
                        live.update(
                            render(
                                snap,
                                offset=offset,
                                page_size=tasks_rows,
                                show_footer=interactive,
                                failures_offset=failures_offset,
                                focus=focus,
                                failures_page_size=failures_rows,
                            )
                        )
                        dirty = False
        except KeyboardInterrupt:
            return 0
    return 0


def main(argv: Optional[list[str]] = None) -> int:
    args = _build_parser().parse_args(argv)
    db_path = resolve_db_path(args.db)
    statuses = _parse_statuses(args.status)
    page_size = max(args.page_size, 1)

    def take_snapshot() -> Snapshot:
        return load_snapshot(
            db_path,
            statuses=statuses,
            experiment=args.experiment,
            max_rows=DEFAULT_LOAD_LIMIT,
            max_failures=args.max_failures,
        )

    if args.json:
        json.dump(snapshot_to_jsonable(take_snapshot()), sys.stdout, indent=2)
        sys.stdout.write("\n")
        return 0

    from rich.console import Console

    if args.once:
        console = Console(no_color=args.no_color)
        snap = take_snapshot()
        offset = clamp_offset((args.page - 1) * page_size, page_size, len(snap.tasks))
        console.print(
            render(snap, offset=offset, page_size=page_size, show_footer=False)
        )
        return 0

    return _run_live(args, db_path, statuses)


if __name__ == "__main__":
    raise SystemExit(main())
