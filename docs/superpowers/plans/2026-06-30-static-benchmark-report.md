# Static Benchmark Report Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a dependency-free static HTML report generator for benchmark result summaries.

**Architecture:** `scripts/generate_report.py` will load nested `summary.json` files from a selected run directory, compute derived comparison metrics, and render one self-contained HTML report with inline CSS and SVG charts. Python unit tests will create temporary result directories so report behavior is verified independently of local benchmark runs.

**Tech Stack:** Python standard library, `unittest`, inline HTML/CSS/SVG.

---

## File Structure

- Create `scripts/generate_report.py`: report data loading, derived metrics, HTML/SVG rendering, and CLI.
- Create `tests/test_generate_report.py`: unit tests for latest-run selection, summary loading, speedup calculations, and rendered report contents.
- Modify `.gitignore`: ignore `.superpowers/` and generated `reports/`.
- Modify `README.md`: document report generation and interpretation.

## Task 1: Report Data Loading And Derived Metrics

**Files:**
- Create: `tests/test_generate_report.py`
- Create: `scripts/generate_report.py`

- [ ] **Step 1: Write failing tests**

```python
import json
import tempfile
import unittest
from pathlib import Path

from scripts.generate_report import (
    compute_rows,
    find_latest_results_dir,
    load_summaries,
    render_report,
)


def write_summary(path: Path, language: str, implementation: str, req_s: float, chunks_s: float):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({
        "language": language,
        "implementation": implementation,
        "started_at": "2026-06-30T00:00:00Z",
        "config": {
            "total_requests": 10,
            "concurrency": 2,
            "chunks_per_response": 3,
            "chunk_bytes": 8,
            "delay_us": 0,
            "warmup_requests": 1
        },
        "summary": {
            "duration_ms": 10.0,
            "successful_requests": 10,
            "failed_requests": 0,
            "total_chunks": 30,
            "total_bytes": 240,
            "requests_per_second": req_s,
            "chunks_per_second": chunks_s,
            "mean_request_latency_ms": 1.0,
            "p50_request_latency_ms": 1.0,
            "p95_request_latency_ms": 2.0,
            "p99_request_latency_ms": 3.0,
            "mean_time_to_first_chunk_ms": 0.4,
            "p50_time_to_first_chunk_ms": 0.3,
            "p95_time_to_first_chunk_ms": 0.5,
            "p99_time_to_first_chunk_ms": 0.8,
            "per_chunk_overhead_ms": 0.1
        }
    }))
```

Tests must assert:

- `find_latest_results_dir()` chooses the newest timestamp-like child.
- `load_summaries()` finds nested `summary.json` files.
- `compute_rows()` sorts Python, Go, Rust reqwest, Rust Hyper and computes speedups.
- `render_report()` contains chart headings, scaling explanations, and implementation labels.

- [ ] **Step 2: Verify tests fail before implementation**

Run: `python3 -m unittest tests.test_generate_report -v`

Expected: FAIL because `scripts.generate_report` does not exist.

- [ ] **Step 3: Implement generator functions**

Implement:

- `find_latest_results_dir(root: Path) -> Path`
- `load_summaries(results_dir: Path) -> list[dict]`
- `implementation_key(item: dict) -> tuple[int, str]`
- `compute_rows(summaries: list[dict]) -> list[dict]`
- `render_report(results_dir: Path, summaries: list[dict]) -> str`
- `write_report(results_dir: Path, output: Path) -> Path`
- `main() -> int`

- [ ] **Step 4: Verify tests pass**

Run: `python3 -m unittest tests.test_generate_report -v`

Expected: PASS.

## Task 2: Report CLI, Documentation, And Generated Output

**Files:**
- Modify: `.gitignore`
- Modify: `README.md`
- Create: generated `reports/latest/index.html` during verification only; do not commit it.

- [ ] **Step 1: Ignore generated/scratch directories**

Add:

```gitignore
.superpowers/
reports/
```

- [ ] **Step 2: Document report generation**

README must include:

```bash
uv run python scripts/generate_report.py
uv run python scripts/generate_report.py results/20260629T141233Z --output reports/latest/index.html
open reports/latest/index.html
```

Also mention that the report explains scaling behavior and benchmark caveats.

- [ ] **Step 3: Generate report from latest results**

Run: `uv run python scripts/generate_report.py`

Expected: prints `reports/latest/index.html` and creates the file.

- [ ] **Step 4: Verify all Python tests pass**

Run: `python3 -m unittest discover -s tests -v`

Expected: PASS.

- [ ] **Step 5: Verify full project tests still pass**

Run:

```bash
make test-go
make test-rust
git diff --check
```

Expected: all commands exit 0.

- [ ] **Step 6: Commit and push**

Run:

```bash
git add .gitignore README.md scripts/generate_report.py tests/test_generate_report.py docs/superpowers/plans/2026-06-30-static-benchmark-report.md
git commit -m "Add static benchmark report generator"
git push
```
