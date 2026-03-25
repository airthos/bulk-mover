"""
BatchTUI — rich.Live terminal UI for parallel batch migration.

Non-TTY fallback: plain timestamped lines to stdout.
"""

import threading
import time
from dataclasses import dataclass, field
from typing import Optional

_SPINNERS = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"

_PHASE_STYLE = {
    "waiting":   ("dim",           "waiting"),
    "scanning":  ("cyan",          "scanning"),
    "resuming":  ("magenta",       "resuming SP job"),
    "copying":   ("bright_blue",   "copying"),
    "copied":    ("dim",           "copied"),
    "verifying": ("yellow",        "verifying"),
    "done":      ("green",         "done"),
    "error":     ("red",           "error"),
}


def _fmt(seconds: float) -> str:
    m, s = divmod(int(seconds), 60)
    h, m = divmod(m, 60)
    if h:
        return f"{h}h{m:02d}m"
    return f"{m}m{s:02d}s"


@dataclass
class _WorkerState:
    batch_name: str
    file_count: int = 0
    phase: str = "waiting"
    pct: float = 0.0
    ok_count: int = 0
    issue_count: int = 0
    start_time: float = field(default_factory=time.monotonic)
    end_time: float = 0.0       # set when complete() is called
    done: bool = False


class BatchTUI:
    """
    Context manager that renders a live migration dashboard.
    Call register() when a batch starts, update() for phase/progress changes,
    and complete() when a batch finishes.
    """

    def __init__(
        self,
        source: str,
        dest: str,
        total_batches: int,
        n_workers: int,
        initial_completed: int = 0,
    ):
        from rich.console import Console
        self._console = Console()
        self._is_tty = self._console.is_terminal
        self._source = source
        self._dest = dest
        self._total = total_batches
        self._n_workers = n_workers

        self._lock = threading.Lock()
        self._workers: dict[str, _WorkerState] = {}
        self._active_order: list[str] = []
        self._history: list[_WorkerState] = []
        self._completed = initial_completed
        self._total_files_done: int = 0
        self._session_start = time.monotonic()

        self._live = None
        self._refresh_thread: Optional[threading.Thread] = None
        self._running = False

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    def __enter__(self):
        self._running = True
        if self._is_tty:
            from rich.live import Live
            self._live = Live(
                self._render(),
                console=self._console,
                auto_refresh=False,
                screen=False,
            )
            self._live.__enter__()
            self._refresh_thread = threading.Thread(
                target=self._refresh_loop, daemon=True
            )
            self._refresh_thread.start()
        return self

    def __exit__(self, *args):
        self._running = False
        if self._refresh_thread:
            self._refresh_thread.join(timeout=1.0)
        if self._live:
            with self._lock:
                renderable = self._render()
            self._live.update(renderable, refresh=True)
            self._live.__exit__(*args)

    def _refresh_loop(self):
        while self._running:
            with self._lock:
                renderable = self._render()
            self._live.update(renderable, refresh=True)   # refresh=True pushes to terminal
            time.sleep(0.25)

    # ------------------------------------------------------------------
    # Worker API — safe to call from any thread
    # ------------------------------------------------------------------

    def register(self, batch_name: str, file_count: int = 0) -> None:
        with self._lock:
            self._workers[batch_name] = _WorkerState(
                batch_name=batch_name, file_count=file_count
            )
            self._active_order.append(batch_name)
        if not self._is_tty:
            self._plain(f"[{batch_name}] started")

    def update(
        self,
        batch_name: str,
        phase: str,
        pct: float = 0.0,
        file_count: int = 0,
    ) -> None:
        with self._lock:
            w = self._workers.get(batch_name)
            if w is None:
                return
            w.phase = phase
            w.pct = pct
            if file_count:
                w.file_count = file_count
        if not self._is_tty:
            if phase == "copying" and pct > 0 and int(pct) % 25 == 0:
                self._plain(f"[{batch_name}] copying {pct:.0f}%")
            elif phase not in ("copying",):
                self._plain(f"[{batch_name}] {phase}")

    def complete(
        self,
        batch_name: str,
        ok_count: int = 0,
        issue_count: int = 0,
    ) -> None:
        with self._lock:
            w = self._workers.get(batch_name)
            if w is None:
                return
            w.done = True
            w.phase = "done"
            w.ok_count = ok_count
            w.issue_count = issue_count
            w.end_time = time.monotonic()
            self._history.append(w)
            if len(self._history) > 8:
                self._history = self._history[-8:]
            if batch_name in self._active_order:
                self._active_order.remove(batch_name)
            self._completed += 1
            self._total_files_done += ok_count + issue_count
        if not self._is_tty:
            self._plain(
                f"[{batch_name}] done — {ok_count} OK, {issue_count} issues"
            )

    def _plain(self, msg: str) -> None:
        ts = time.strftime("%H:%M:%S")
        self._console.print(f"[{ts}] {msg}")

    # ------------------------------------------------------------------
    # Rendering (called from refresh thread with lock held)
    # ------------------------------------------------------------------

    def _render(self):
        from rich.console import Group
        from rich.panel import Panel
        from rich.table import Table
        from rich.text import Text

        now = time.monotonic()
        elapsed = now - self._session_start
        tick = int(elapsed * 4) % len(_SPINNERS)

        # files/min — only from this run's elapsed time
        if elapsed >= 5.0 and self._total_files_done > 0:
            fpm = self._total_files_done / (elapsed / 60.0)
            rate_str = f"   [dim]{fpm:.0f} files/min[/dim]"
        else:
            rate_str = ""

        # Header
        hdr = (
            f"[bold]{self._source}[/bold] → [bold]{self._dest}[/bold]"
            f"   [cyan]{self._completed}/{self._total}[/cyan] batches"
            f"   [dim]{_fmt(elapsed)} elapsed[/dim]"
            f"{rate_str}"
        )
        # ETA: only meaningful once we've completed at least one batch this run
        batches_this_run = len(self._history)
        if batches_this_run > 0 and elapsed > 0:
            avg = elapsed / batches_this_run
            remaining_batches = self._total - self._completed
            if remaining_batches > 0:
                eta = avg * remaining_batches / max(self._n_workers, 1)
                hdr += f"   [dim]~{_fmt(eta)} remaining[/dim]"
        header = Panel(
            Text.from_markup(hdr),
            title="[bold bright_blue]OneDrive → SharePoint Migration[/bold bright_blue]",
            border_style="bright_blue",
        )

        parts = [header]

        # Workers table — always n_workers rows, one per slot
        # "copied" and "done" phases are treated as idle (user doesn't need to see them)
        _SHOW_PHASES = {"scanning", "resuming", "copying", "verifying"}
        visible = [
            self._workers[n]
            for n in self._active_order
            if n in self._workers and self._workers[n].phase in _SHOW_PHASES
        ]

        tbl = Table(show_header=False, box=None, padding=(0, 1))
        tbl.add_column("", width=2, no_wrap=True)
        tbl.add_column("Batch", min_width=24, no_wrap=True)
        tbl.add_column("Status", min_width=30, no_wrap=True)
        tbl.add_column("Time", justify="right", width=8)

        for i in range(self._n_workers):
            if i < len(visible):
                w = visible[i]
                style, label = _PHASE_STYLE.get(w.phase, ("white", w.phase))
                elapsed_w = _fmt(now - w.start_time)
                spinner = _SPINNERS[tick]

                if w.phase == "resuming":
                    fc = f"  [dim]{w.file_count} files[/dim]" if w.file_count else ""
                    status = f"[{style}]resuming SP job[/{style}]{fc}"
                elif w.phase == "copying":
                    if w.pct > 0:
                        filled = int(w.pct / 100 * 12)
                        bar = f"{'█' * filled}{'░' * (12 - filled)}"
                        status = f"[{style}]copying[/{style}]  [{style}]{bar}[/{style}]  [{style}]{w.pct:.0f}%[/{style}]"
                        if w.file_count:
                            status += f"  [dim]{w.file_count} files[/dim]"
                    else:
                        status = f"[{style}]copying[/{style}]  [dim]{w.file_count or '?'} files[/dim]"
                elif w.phase == "verifying":
                    fc = f" {w.file_count} files" if w.file_count else ""
                    status = f"[{style}]verifying[/{style}][dim]{fc}[/dim]"
                elif w.phase == "scanning":
                    fc = f"  [dim]{w.file_count} found[/dim]" if w.file_count else ""
                    status = f"[{style}]scanning[/{style}]{fc}"
                else:
                    status = f"[{style}]{label}[/{style}]"

                tbl.add_row(
                    f"[{style}]{spinner}[/{style}]",
                    f"[{style}]{w.batch_name}[/{style}]",
                    Text.from_markup(status),
                    f"[dim]{elapsed_w}[/dim]",
                )
            else:
                tbl.add_row("[dim]·[/dim]", "[dim]—[/dim]", "[dim]idle[/dim]", "")

        parts.append(Panel(tbl, title="[bold]Workers[/bold]", border_style="blue", padding=(0, 1)))

        # Footer
        parts.append(
            Text.from_markup(
                f"  [dim]{self._n_workers} worker{'s' if self._n_workers != 1 else ''}   {self._completed}/{self._total} batches done[/dim]"
            )
        )

        return Group(*parts)
