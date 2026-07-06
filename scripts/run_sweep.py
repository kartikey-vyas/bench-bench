"""Back-compat shim: the sweep runner lives in bench_harness.sweep.

Prefer the console command (`uv run bench-sweep --config config/sweep.linux.json`)
or `make sweep CONFIG=...`. This path keeps `python scripts/run_sweep.py`
and existing imports working.
"""
from bench_harness.sweep import *  # noqa: F401,F403
from bench_harness.sweep import main

if __name__ == "__main__":
    raise SystemExit(main())
