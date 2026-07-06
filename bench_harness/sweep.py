from __future__ import annotations

import argparse
import dataclasses
import json
import resource
import shutil
import subprocess
import sys
import threading
import time
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from bench_harness import clients as client_registry
from bench_harness.display import PlainDisplay, create_display

ROOT = Path(__file__).resolve().parents[1]

# Client names, python modules, and rust --client mappings all live in the
# registry (bench_harness/clients.py); KNOWN_CLIENTS is derived from it so
# SweepConfig.validate() stays in sync with the registry automatically.
PYTHON_VARIANTS = {
    name: (spec.module, spec.required_modules)
    for name, spec in client_registry.CLIENTS.items()
    if spec.kind == "python"
}

KNOWN_CLIENTS = set(client_registry.CLIENTS)


@dataclass(frozen=True)
class SweepTier:
    name: str
    events_per_second: int
    ttfc_ms: int


@dataclass(frozen=True)
class SweepConfig:
    base_url: str
    tiers: tuple[SweepTier, ...]
    concurrencies: tuple[int, ...]
    clients: tuple[str, ...]
    duration_seconds: float
    warmup_seconds: float
    repeats: int
    cooldown_seconds: float
    chunks_per_response: int
    chunk_bytes: int
    stop_efficiency_below: float
    stop_ttfc_excess_p95_ms: float
    stop_failure_fraction: float
    output_dir: str
    # CPU allocation (all optional). server_worker_threads caps the server's
    # tokio runtime; server_cpus/client_cpus are `taskset -c` core lists
    # (e.g. "0-7") applied on Linux so server and client don't fight for cores.
    server_worker_threads: int | None = None
    server_cpus: str | None = None
    client_cpus: str | None = None

    @classmethod
    def from_path(cls, path: str | Path) -> "SweepConfig":
        raw = json.loads(Path(path).read_text())
        provided = set(raw.keys())
        data = dict(raw)
        tiers_raw = data.pop("tiers", None)
        concurrencies_raw = data.pop("concurrencies", None)
        clients_raw = data.pop("clients", None)
        try:
            kwargs: dict[str, Any] = data
            if tiers_raw is not None:
                kwargs["tiers"] = tuple(SweepTier(**tier) for tier in tiers_raw)
            if concurrencies_raw is not None:
                kwargs["concurrencies"] = tuple(concurrencies_raw)
            if clients_raw is not None:
                kwargs["clients"] = tuple(clients_raw)
            config = cls(**kwargs)
        except TypeError as error:
            field_names = {field.name for field in dataclasses.fields(cls)}
            unknown = sorted(provided - field_names)
            required = {
                field.name
                for field in dataclasses.fields(cls)
                if field.default is dataclasses.MISSING
                and field.default_factory is dataclasses.MISSING  # type: ignore[comparison-overlap]
            }
            missing = sorted(required - provided)
            details = []
            if unknown:
                details.append(f"unknown key(s): {', '.join(unknown)}")
            if missing:
                details.append(f"missing key(s): {', '.join(missing)}")
            if not details:
                details.append(str(error))
            raise ValueError(f"invalid sweep config {path}: {'; '.join(details)}") from error
        config.validate()
        return config

    def validate(self) -> None:
        """Fail fast with a readable, all-in-one-shot error rather than a
        confusing crash deep inside the sweep loop or a subprocess."""
        problems: list[str] = []

        if not self.concurrencies:
            problems.append("concurrencies must be non-empty")
        else:
            if any(c <= 0 for c in self.concurrencies):
                problems.append(f"concurrencies must all be positive, got {list(self.concurrencies)}")
            if list(self.concurrencies) != sorted(self.concurrencies):
                problems.append(f"concurrencies must be ascending, got {list(self.concurrencies)}")

        if self.repeats < 1:
            problems.append(f"repeats must be >= 1, got {self.repeats}")
        if self.duration_seconds <= 0:
            problems.append(f"duration_seconds must be > 0, got {self.duration_seconds}")
        if self.warmup_seconds < 0:
            problems.append(f"warmup_seconds must be >= 0, got {self.warmup_seconds}")
        if self.cooldown_seconds < 0:
            problems.append(f"cooldown_seconds must be >= 0, got {self.cooldown_seconds}")
        if self.chunks_per_response <= 0:
            problems.append(f"chunks_per_response must be > 0, got {self.chunks_per_response}")
        if self.chunk_bytes <= 0:
            problems.append(f"chunk_bytes must be > 0, got {self.chunk_bytes}")
        if not (0.0 <= self.stop_failure_fraction <= 1.0):
            problems.append(
                f"stop_failure_fraction must be in [0, 1], got {self.stop_failure_fraction}"
            )
        for tier in self.tiers:
            if tier.events_per_second < 0:
                problems.append(
                    f"tier {tier.name!r}: events_per_second must be >= 0, got {tier.events_per_second}"
                )
            if tier.ttfc_ms < 0:
                problems.append(f"tier {tier.name!r}: ttfc_ms must be >= 0, got {tier.ttfc_ms}")

        if self.server_worker_threads is not None and self.server_worker_threads <= 0:
            problems.append(
                f"server_worker_threads must be None or > 0, got {self.server_worker_threads!r}"
            )

        unknown_clients = sorted(set(self.clients) - KNOWN_CLIENTS)
        if unknown_clients:
            problems.append(
                f"unknown client(s) {unknown_clients}; known clients are {sorted(KNOWN_CLIENTS)}"
            )

        if problems:
            raise ValueError("; ".join(problems))

    def as_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["tiers"] = [asdict(tier) for tier in self.tiers]
        data["concurrencies"] = list(self.concurrencies)
        data["clients"] = list(self.clients)
        return data


def build_workload(sweep: SweepConfig, tier: SweepTier, concurrency: int) -> dict[str, Any]:
    return {
        "base_url": sweep.base_url,
        "duration_seconds": sweep.duration_seconds,
        "warmup_seconds": sweep.warmup_seconds,
        "concurrency": concurrency,
        "chunks_per_response": sweep.chunks_per_response,
        "chunk_bytes": sweep.chunk_bytes,
        "ttfc_ms": tier.ttfc_ms,
        "events_per_second": tier.events_per_second,
        "output_dir": sweep.output_dir,
    }


def rotated(items: tuple[str, ...], repeat: int) -> list[str]:
    if not items:
        return []
    shift = repeat % len(items)
    return list(items[shift:] + items[:shift])


def stop_reason(sweep: SweepConfig, tier: SweepTier, summaries: list[dict[str, Any]]) -> str | None:
    if not summaries:
        return "client produced no results"
    total = sum(
        s["successful_requests"] + s["incomplete_requests"] + s["failed_requests"]
        for s in summaries
    )
    if total == 0:
        return "no requests completed"
    bad = sum(s["incomplete_requests"] + s["failed_requests"] for s in summaries)
    failure_fraction = bad / total
    if failure_fraction > sweep.stop_failure_fraction:
        return f"failure fraction {failure_fraction:.4f} above {sweep.stop_failure_fraction}"
    if tier.events_per_second > 0:
        mean_efficiency = sum(s["efficiency"] for s in summaries) / len(summaries)
        if mean_efficiency < sweep.stop_efficiency_below:
            return f"efficiency {mean_efficiency:.3f} below {sweep.stop_efficiency_below}"
        mean_excess = (
            sum(s["p95_time_to_first_chunk_ms"] for s in summaries) / len(summaries)
            - tier.ttfc_ms
        )
        if mean_excess > sweep.stop_ttfc_excess_p95_ms:
            return f"p95 TTFC excess {mean_excess:.1f}ms above {sweep.stop_ttfc_excess_p95_ms}ms"
    return None


def resolve_stop_reason(
    sweep: SweepConfig,
    tier: SweepTier,
    summaries: list[dict[str, Any]],
    failed_runs: int,
) -> str | None:
    """Combine crashed/timed-out repeats with the summary-based stop rules.

    A repeat that produced no summary (crash, timeout, missing summary.json)
    must not be silently dropped from consideration: without this, a client
    that fails 2 of 3 repeats but "succeeds" on the third can look healthy to
    `stop_reason`, which only ever sees the summaries that did land.
    """
    if failed_runs:
        return f"{failed_runs} run(s) produced no summary"
    return stop_reason(sweep, tier, summaries)


def format_duration(seconds: float) -> str:
    total = int(seconds)
    if total < 60:
        return f"{total}s"
    if total < 3600:
        return f"{total // 60}m{total % 60:02d}s"
    return f"{total // 3600}h{(total % 3600) // 60:02d}m"


class SweepProgress:
    """Tracks completed vs planned client-runs so the console shows how far
    through the sweep we are. Stop rules prune the plan as clients drop out,
    so the denominator shrinks instead of the percentage stalling."""

    def __init__(self, sweep: SweepConfig) -> None:
        self._rungs = len(sweep.concurrencies)
        self._repeats = sweep.repeats
        self.total = len(sweep.tiers) * self._rungs * self._repeats * len(sweep.clients)
        self.completed = 0
        self._durations: list[float] = []
        self._started = time.monotonic()

    def finish_run(self, duration_seconds: float) -> None:
        self.completed += 1
        self._durations.append(duration_seconds)

    def drop_client(self, rung_index: int) -> int:
        """A client stopped at this rung: its runs on later rungs won't happen."""
        removed = (self._rungs - rung_index - 1) * self._repeats
        self.total -= removed
        return removed

    def percent(self) -> float:
        return 100.0 * self.completed / self.total if self.total else 100.0

    def eta_seconds(self) -> float | None:
        if not self._durations:
            return None
        mean_duration = sum(self._durations) / len(self._durations)
        return mean_duration * (self.total - self.completed)

    def elapsed_seconds(self) -> float:
        return time.monotonic() - self._started

    def status(self) -> str:
        eta = self.eta_seconds()
        eta_text = f", ~{format_duration(eta)} left" if eta is not None else ""
        return (
            f"{self.completed}/{self.total} runs ({self.percent():.0f}%), "
            f"elapsed {format_duration(self.elapsed_seconds())}{eta_text}"
        )


class CpuSampler:
    """Samples %CPU for named pids via `ps` on a background thread."""

    def __init__(self, pids: dict[str, int], interval_seconds: float = 0.5) -> None:
        self._pids = pids
        self._interval = interval_seconds
        self._samples: dict[str, list[float]] = {name: [] for name in pids}
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, daemon=True)

    def start(self) -> "CpuSampler":
        self._thread.start()
        return self

    def _run(self) -> None:
        while not self._stop.is_set():
            for name, pid in self._pids.items():
                result = subprocess.run(
                    ["ps", "-p", str(pid), "-o", "%cpu="],
                    capture_output=True,
                    text=True,
                )
                value = result.stdout.strip()
                if result.returncode == 0 and value:
                    try:
                        self._samples[name].append(float(value))
                    except ValueError:
                        pass
            self._stop.wait(self._interval)

    def stop(self) -> dict[str, dict[str, float]]:
        self._stop.set()
        self._thread.join(timeout=5)
        report: dict[str, dict[str, float]] = {}
        for name, values in self._samples.items():
            if values:
                report[name] = {
                    "mean_percent": sum(values) / len(values),
                    "max_percent": max(values),
                    "samples": len(values),
                }
            else:
                report[name] = {"mean_percent": 0.0, "max_percent": 0.0, "samples": 0}
        return report


def raise_file_limit(target: int = 65536) -> None:
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    desired = target if hard == resource.RLIM_INFINITY else min(target, hard)
    if soft < desired:
        resource.setrlimit(resource.RLIMIT_NOFILE, (desired, hard))


def http_get_json(url: str) -> dict[str, Any]:
    with urllib.request.urlopen(url, timeout=5) as response:
        return json.loads(response.read())


def http_post(url: str) -> None:
    request = urllib.request.Request(url, method="POST")
    with urllib.request.urlopen(request, timeout=5) as response:
        response.read()


def wait_for_health(url: str, timeout_seconds: float = 30.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=0.5) as response:
                if response.status == 200:
                    return
        except OSError as error:
            last_error = error
        time.sleep(0.1)
    raise RuntimeError(f"server did not become healthy at {url}: {last_error}")


def build_binaries() -> dict[str, Path]:
    subprocess.run(
        ["cargo", "build", "--release", "--manifest-path", str(ROOT / "server-rust" / "Cargo.toml")],
        check=True,
    )
    subprocess.run(
        ["cargo", "build", "--release", "--manifest-path", str(ROOT / "rust-client" / "Cargo.toml")],
        check=True,
    )
    go_binary = ROOT / "go-client" / "bin" / "bench-go-client"
    go_binary.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run(["go", "build", "-o", str(go_binary), "."], cwd=ROOT / "go-client", check=True)
    return {
        "server": ROOT / "server-rust" / "target" / "release" / "synthetic-openai-server",
        "rust": ROOT / "rust-client" / "target" / "release" / "rust-benchmark-client",
        "go": go_binary,
    }


def taskset_prefix(cpus: str | None) -> list[str]:
    """`taskset -c <cpus>` prefix when core pinning is requested and available
    (Linux). On macOS there is no taskset; warn once and run unpinned."""
    if not cpus:
        return []
    if shutil.which("taskset") is None:
        if cpus not in _warned_no_taskset:
            _warned_no_taskset.add(cpus)
            print(
                f"warning: cpu pinning to {cpus!r} requested but taskset is not "
                "available on this platform; running unpinned",
                file=sys.stderr,
            )
        return []
    return ["taskset", "-c", cpus]


_warned_no_taskset: set[str] = set()


def server_command(sweep: SweepConfig, binaries: dict[str, Path], bind: str) -> list[str]:
    command = taskset_prefix(sweep.server_cpus) + [str(binaries["server"]), "--bind", bind]
    if sweep.server_worker_threads is not None:
        command += ["--worker-threads", str(sweep.server_worker_threads)]
    return command


def client_command(name: str, binaries: dict[str, Path], config_path: Path, out_dir: Path) -> list[str]:
    return client_registry.command(name, binaries, config_path, out_dir, sys.executable)


def run_cell_client(
    sweep: SweepConfig,
    binaries: dict[str, Path],
    server_pid: int,
    client_name: str,
    config_path: Path,
    out_dir: Path,
    display: Any = None,
) -> dict[str, Any] | None:
    display = display or PlainDisplay()
    out_dir.mkdir(parents=True, exist_ok=True)
    try:
        http_post(f"{sweep.base_url}/stats/reset")
    except OSError as error:
        display.warn(f"failed to reset server stats before {client_name}: {error}")

    command = taskset_prefix(sweep.client_cpus) + client_command(
        client_name, binaries, config_path, out_dir
    )
    display.command(" ".join(command))
    # Client stdout/stderr goes to a per-cell log so the console belongs to
    # the sweep display, and a failed cell can be diagnosed after the run.
    with open(out_dir / "client.log", "w") as client_log:
        process = subprocess.Popen(command, cwd=ROOT, stdout=client_log, stderr=client_log)
        sampler = CpuSampler({"server": server_pid, "client": process.pid}).start()

        timeout = sweep.warmup_seconds + sweep.duration_seconds + 120.0
        timed_out = False
        try:
            returncode = process.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            process.kill()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                display.warn(f"{client_name} did not reap after kill, continuing")
            display.warn(f"{client_name} timed out after {timeout:.0f}s, killed")
            timed_out = True
            returncode = None
        except Exception:
            process.kill()
            raise

    cpu = sampler.stop()
    try:
        server_stats = http_get_json(f"{sweep.base_url}/stats")
    except OSError as error:
        display.warn(f"failed to fetch server stats after {client_name}: {error}")
        server_stats = {}

    (out_dir / "server_stats.json").write_text(json.dumps(server_stats, indent=2) + "\n")
    (out_dir / "cpu.json").write_text(json.dumps(cpu, indent=2) + "\n")

    if timed_out:
        return None
    if returncode != 0:
        display.warn(f"{client_name} exited {returncode} (see {out_dir / 'client.log'})")
        return None
    summary_path = out_dir / "summary.json"
    if not summary_path.exists():
        display.warn(f"{client_name} wrote no summary.json (see {out_dir / 'client.log'})")
        return None
    return json.loads(summary_path.read_text())


def write_sweep_record(run_dir: Path, record: dict[str, Any]) -> None:
    (run_dir / "sweep.json").write_text(json.dumps(record, indent=2) + "\n")


def python_client_ready(module: str = "httpx") -> bool:
    """Check whether the running interpreter (used to launch python client
    subprocesses via sys.executable) can import the given module."""
    result = subprocess.run(
        [sys.executable, "-c", f"import {module}"],
        capture_output=True,
    )
    return result.returncode == 0


def missing_python_modules(clients: tuple[str, ...]) -> list[str]:
    required = client_registry.required_python_modules(clients)
    return sorted(module for module in required if not python_client_ready(module))


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a tier x concurrency benchmark sweep.")
    parser.add_argument(
        "--config",
        default="config/sweep.default.json",
        help="Path to sweep JSON, resolved against the repo root (default: %(default)s).",
    )
    parser.add_argument(
        "--bind",
        default="127.0.0.1:8080",
        help="Server bind address (default: %(default)s).",
    )
    parser.add_argument(
        "--results-dir",
        default="results",
        help="Root directory for run output (default: %(default)s).",
    )
    parser.add_argument(
        "--display",
        choices=("auto", "rich", "plain"),
        default="auto",
        help="Console output style: rich live progress on a terminal, plain "
             "lines for pipes/CI (default: %(default)s).",
    )
    args = parser.parse_args()

    sweep = SweepConfig.from_path(ROOT / args.config)

    base_host_port = urlparse(sweep.base_url).netloc
    if args.bind != base_host_port:
        print(
            f"error: --bind {args.bind} disagrees with config base_url {sweep.base_url}; "
            "clients and health checks use base_url — pass a config whose base_url "
            "matches or omit --bind",
            file=sys.stderr,
        )
        return 2

    missing = missing_python_modules(sweep.clients)
    if missing:
        print(
            f"error: python clients require {', '.join(missing)} in the running "
            f"interpreter ({sys.executable}); run via .venv/bin/python or uv run "
            f"(after `uv sync`), or remove the python clients from the config",
            file=sys.stderr,
        )
        return 2

    raise_file_limit()
    binaries = build_binaries()

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = ROOT / args.results_dir / timestamp
    (run_dir / "configs").mkdir(parents=True, exist_ok=True)

    server = subprocess.Popen(server_command(sweep, binaries, args.bind))
    record: dict[str, Any] = {
        "config": sweep.as_dict(),
        "started_at": datetime.now(timezone.utc).isoformat(),
        "stops": {},
        "cells": [],
    }
    progress = SweepProgress(sweep)
    display = create_display(args.display)
    display.plan(
        f"{len(sweep.tiers)} tiers x {len(sweep.concurrencies)} concurrencies "
        f"x {sweep.repeats} repeats x {len(sweep.clients)} clients",
        str(run_dir),
        progress.total,
    )
    try:
        wait_for_health(f"{sweep.base_url}/health")

        for tier_index, tier in enumerate(sweep.tiers):
            active = list(sweep.clients)
            paced = tier.events_per_second > 0
            for rung_index, concurrency in enumerate(sweep.concurrencies):
                if not active:
                    break
                workload = build_workload(sweep, tier, concurrency)
                config_path = run_dir / "configs" / f"{tier.name}-c{concurrency}.json"
                config_path.write_text(json.dumps(workload, indent=2) + "\n")

                display.rung_start(
                    f"{tier.name} c={concurrency} "
                    f"(tier {tier_index + 1}/{len(sweep.tiers)}, "
                    f"rung {rung_index + 1}/{len(sweep.concurrencies)}) "
                    f"active: {', '.join(active)}"
                )

                cell_summaries: dict[str, list[dict[str, Any]]] = {name: [] for name in active}
                cell_failures: dict[str, int] = {name: 0 for name in active}
                for repeat in range(sweep.repeats):
                    for client_name in rotated(tuple(active), repeat):
                        run_label = (
                            f"[{progress.completed + 1}/{progress.total}] "
                            f"{tier.name} c={concurrency} r{repeat} {client_name}"
                        )
                        display.run_start(run_label)
                        run_started = time.monotonic()
                        out_dir = run_dir / tier.name / f"c{concurrency}" / f"r{repeat}" / client_name
                        result = run_cell_client(
                            sweep, binaries, server.pid, client_name, config_path, out_dir,
                            display=display,
                        )
                        run_seconds = time.monotonic() - run_started
                        progress.finish_run(run_seconds)
                        display.run_done(
                            run_label,
                            run_seconds,
                            result["summary"] if result is not None else None,
                            paced,
                        )
                        record["cells"].append({
                            "tier": tier.name,
                            "concurrency": concurrency,
                            "repeat": repeat,
                            "client": client_name,
                            "ok": result is not None,
                        })
                        if result is not None:
                            cell_summaries[client_name].append(result["summary"])
                        else:
                            cell_failures[client_name] += 1

                for client_name in list(active):
                    reason = resolve_stop_reason(
                        sweep, tier, cell_summaries[client_name], cell_failures.get(client_name, 0)
                    )
                    if reason:
                        active.remove(client_name)
                        record["stops"][f"{tier.name}:{client_name}"] = {
                            "concurrency": concurrency,
                            "reason": reason,
                        }
                        pruned = progress.drop_client(rung_index)
                        display.stop(
                            f"{tier.name}/{client_name} at c={concurrency}: {reason} "
                            f"({pruned} runs pruned)",
                            progress.total,
                        )

                write_sweep_record(run_dir, record)
                display.rung_progress(progress.status())

                if sweep.cooldown_seconds > 0:
                    time.sleep(sweep.cooldown_seconds)
    finally:
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait(timeout=5)
        record["finished_at"] = datetime.now(timezone.utc).isoformat()
        write_sweep_record(run_dir, record)
        display.close()

    display.complete(
        f"sweep complete: {progress.completed}/{progress.total} runs "
        f"in {format_duration(progress.elapsed_seconds())}, "
        f"{len(record['stops'])} stop(s)"
    )
    print(run_dir)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
