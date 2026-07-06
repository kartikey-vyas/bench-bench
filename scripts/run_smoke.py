from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

from bench_harness import clients as client_registry

ROOT = Path(__file__).resolve().parents[1]


def require_tool(name: str) -> str | None:
    return shutil.which(name)


def wait_for_health(url: str, timeout_seconds: float = 10.0) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with urlopen(url, timeout=0.5) as response:
                if response.status == 200:
                    return
        except URLError as error:
            last_error = error
        time.sleep(0.1)
    raise RuntimeError(f"server did not become healthy at {url}: {last_error}")


def run_command(command: list[str], cwd: Path = ROOT) -> int:
    print("+", " ".join(command))
    return subprocess.run(command, cwd=cwd).returncode


def python_client_has_dependencies(modules: tuple[str, ...] = ("httpx",)) -> bool:
    return (
        subprocess.run(
            [sys.executable, "-c", ";".join(f"import {module}" for module in modules)],
            cwd=ROOT,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).returncode
        == 0
    )


def start_server(bind: str) -> subprocess.Popen[str]:
    cargo = require_tool("cargo")
    if cargo is None:
        raise RuntimeError("cargo is required to start the Rust synthetic server")

    command = [
        cargo,
        "run",
        "--manifest-path",
        str(ROOT / "server-rust" / "Cargo.toml"),
        "--release",
        "--",
        "--bind",
        bind,
    ]
    print("+", " ".join(command))
    return subprocess.Popen(command, cwd=ROOT, text=True)


def run_clients(config: Path, run_dir: Path) -> int:
    failures = 0
    ordered_specs = [client_registry.CLIENTS[name] for name in client_registry.CLIENT_ORDER]

    for spec in ordered_specs:
        if spec.kind != "python":
            continue
        if python_client_has_dependencies(spec.required_modules):
            failures += run_command(
                [
                    sys.executable,
                    "-m",
                    spec.module,
                    *spec.extra_args,
                    "--config",
                    str(config),
                    "--output-dir",
                    str(run_dir / spec.name),
                ]
            )
        else:
            print(
                f"skip {spec.name} client: "
                f"{'/'.join(spec.required_modules)} not installed for this interpreter"
            )

    if require_tool("go"):
        for spec in ordered_specs:
            if spec.kind != "go":
                continue
            failures += run_command(
                [
                    "go",
                    "run",
                    ".",
                    "--config",
                    str(config),
                    "--output-dir",
                    str(run_dir / spec.name),
                ],
                cwd=ROOT / "go-client",
            )
    else:
        print("skip go client: go is not installed")

    if require_tool("cargo"):
        for spec in ordered_specs:
            if spec.kind != "rust":
                continue
            failures += run_command(
                [
                    "cargo",
                    "run",
                    "--manifest-path",
                    str(ROOT / "rust-client" / "Cargo.toml"),
                    "--release",
                    "--",
                    "--config",
                    str(config),
                    "--output-dir",
                    str(run_dir / spec.name),
                    "--client",
                    spec.rust_kind,
                ]
            )
    else:
        print("skip rust client: cargo is not installed")

    return failures


def summaries_share_schema(run_dir: Path) -> bool:
    """Assert every client's summary.json under run_dir has the identical
    sorted key set in `summary`. All clients must emit byte-key-identical
    summaries (the cross-language contract, see docs/CONTRACTS.md) — a schema
    drift here would otherwise slip through silently until a report or a
    downstream consumer chokes on a missing key."""
    schemas: dict[str, list[str]] = {}
    for path in sorted(run_dir.glob("**/summary.json")):
        data = json.loads(path.read_text())
        schemas[path.parent.name] = sorted(data.get("summary", {}).keys())

    if not schemas:
        return True

    reference_client, reference_keys = next(iter(schemas.items()))
    ok = True
    for client, keys in schemas.items():
        if keys != reference_keys:
            missing = sorted(set(reference_keys) - set(keys))
            extra = sorted(set(keys) - set(reference_keys))
            print(
                f"schema mismatch: {client} summary keys differ from {reference_client} "
                f"(missing: {missing or 'none'}, extra: {extra or 'none'})",
                file=sys.stderr,
            )
            ok = False
    return ok


def main() -> int:
    parser = argparse.ArgumentParser(description="Run a local smoke benchmark against the synthetic server.")
    parser.add_argument("--config", default="config/workload.smoke.json", help="Path to workload JSON.")
    parser.add_argument("--bind", default="127.0.0.1:8080", help="Server bind address.")
    parser.add_argument("--results-dir", default="results", help="Root directory for timestamped run output.")
    args = parser.parse_args()

    config = (ROOT / args.config).resolve()
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = ROOT / args.results_dir / timestamp
    run_dir.mkdir(parents=True, exist_ok=True)

    try:
        server = start_server(args.bind)
    except RuntimeError as error:
        print(f"smoke unavailable: {error}", file=sys.stderr)
        return 2

    try:
        wait_for_health(f"http://{args.bind}/health")
        failures = run_clients(config, run_dir)
        if not summaries_share_schema(run_dir):
            failures += 1
        compare_exit = run_command([sys.executable, str(ROOT / "scripts" / "compare_results.py"), str(run_dir)])
        return failures or compare_exit
    finally:
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait(timeout=5)


if __name__ == "__main__":
    raise SystemExit(main())
