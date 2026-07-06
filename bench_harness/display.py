"""Sweep console output: a rich live progress bar on real terminals, plain
line-oriented logging everywhere else (pipes, CI, background logs).

Both implementations expose the same small protocol, so the sweep loop is
display-agnostic. Client subprocess output never reaches this console — the
runner redirects it to a per-cell client.log — so the display owns the tty.
"""

from __future__ import annotations

import sys
from typing import Any


def _efficiency_markup(summary: dict[str, Any] | None, paced: bool) -> tuple[str, str]:
    """(plain_text, rich_markup) for a run outcome."""
    if summary is None:
        return "FAILED (no summary)", "[bold red]FAILED (no summary)[/]"
    text = (
        f"efficiency={summary['efficiency']:.3f} "
        f"failed={summary['failed_requests']} "
        f"incomplete={summary['incomplete_requests']}"
    )
    if summary["failed_requests"] or summary["incomplete_requests"]:
        color = "red"
    elif not paced:
        color = "cyan"
    elif summary["efficiency"] >= 0.97:
        color = "green"
    elif summary["efficiency"] >= 0.90:
        color = "yellow"
    else:
        color = "red"
    return text, f"[{color}]{text}[/]"


class PlainDisplay:
    """The original line-oriented output. Safe for pipes and log files."""

    def plan(self, description: str, run_dir: str, total: int) -> None:
        print(f"sweep plan: {description} = {total} client-runs -> {run_dir}", flush=True)

    def rung_start(self, header: str) -> None:
        print(f"\n=== {header}", flush=True)

    def run_start(self, label: str) -> None:
        print(label, flush=True)

    def run_done(
        self, label: str, seconds: float, summary: dict[str, Any] | None, paced: bool
    ) -> None:
        text, _ = _efficiency_markup(summary, paced)
        print(f"{label} done in {seconds:.1f}s · {text}", flush=True)

    def command(self, argv: str) -> None:
        print("  +", argv, flush=True)

    def warn(self, message: str) -> None:
        print(f"warning: {message}", file=sys.stderr, flush=True)

    def stop(self, message: str, new_total: int) -> None:
        print(f"stop {message}", flush=True)

    def rung_progress(self, status: str) -> None:
        print(f"progress: {status}", flush=True)

    def complete(self, message: str) -> None:
        print(f"\n{message}", flush=True)

    def close(self) -> None:
        pass


class RichDisplay:
    """Live progress bar + colored event lines via rich. Event lines print
    above the bar; the bar tracks completed/total with elapsed and ETA, and
    its total shrinks when stop rules prune a client's remaining rungs."""

    def __init__(self, force_terminal: bool = False) -> None:
        from rich.console import Console
        from rich.progress import (
            BarColumn,
            Progress,
            SpinnerColumn,
            TextColumn,
            TimeElapsedColumn,
            TimeRemainingColumn,
        )

        self._console = Console(force_terminal=force_terminal or None)
        self._progress = Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(bar_width=None),
            TextColumn("{task.completed}/{task.total}"),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TimeElapsedColumn(),
            TextColumn("eta"),
            TimeRemainingColumn(),
            console=self._console,
        )
        self._task_id = None

    def plan(self, description: str, run_dir: str, total: int) -> None:
        self._console.print(
            f"[bold]sweep plan:[/] {description} = [bold]{total}[/] client-runs -> {run_dir}"
        )
        self._task_id = self._progress.add_task("starting", total=total)
        self._progress.start()

    def rung_start(self, header: str) -> None:
        self._progress.console.print(f"\n[bold]{header}[/]")

    def run_start(self, label: str) -> None:
        self._progress.update(self._task_id, description=label)

    def run_done(
        self, label: str, seconds: float, summary: dict[str, Any] | None, paced: bool
    ) -> None:
        _, markup = _efficiency_markup(summary, paced)
        mark = "[green]✓[/]" if summary is not None else "[red]✗[/]"
        self._progress.console.print(f"{mark} {label} [dim]{seconds:5.1f}s[/] {markup}")
        self._progress.advance(self._task_id)

    def command(self, argv: str) -> None:
        pass  # full argv lives in the plain logs and cell dirs; keep the tty clean

    def warn(self, message: str) -> None:
        self._progress.console.print(f"[yellow]warning:[/] {message}")

    def stop(self, message: str, new_total: int) -> None:
        self._progress.console.print(f"[bold red]stop[/] {message}")
        self._progress.update(self._task_id, total=new_total)

    def rung_progress(self, status: str) -> None:
        pass  # the bar itself carries completed/total/elapsed/eta

    def complete(self, message: str) -> None:
        self._progress.stop()
        self._console.print(f"\n[bold]{message}[/]")

    def close(self) -> None:
        if self._task_id is not None:
            self._progress.stop()


def create_display(mode: str = "auto"):
    """mode: auto (rich on a tty, plain otherwise) | rich | plain."""
    if mode == "plain":
        return PlainDisplay()
    try:
        from rich.console import Console
    except ImportError:
        if mode == "rich":
            print("warning: rich not installed; falling back to plain output", file=sys.stderr)
        return PlainDisplay()
    if mode == "rich":
        return RichDisplay(force_terminal=True)
    if Console().is_terminal:
        return RichDisplay()
    return PlainDisplay()
