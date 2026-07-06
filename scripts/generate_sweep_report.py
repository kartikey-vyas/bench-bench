"""Back-compat shim: the sweep report generator lives in bench_harness.sweep_report.

Prefer the console command (`uv run bench-sweep-report [run_dirs...]`).
This path keeps `python scripts/generate_sweep_report.py` and existing
imports working.
"""
from bench_harness.sweep_report import *  # noqa: F401,F403
from bench_harness.sweep_report import main

if __name__ == "__main__":
    raise SystemExit(main())
