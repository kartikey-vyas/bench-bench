# Static Benchmark Report Design

## Purpose

Add a lightweight visualization layer for the local LLM harness benchmark results. The report should make throughput, latency, per-chunk overhead, and scaling behavior easy to inspect without introducing a frontend build system or long-running dashboard service.

The report is meant to answer two questions:

- What happened in this benchmark run?
- Why do the Python, Go, Rust reqwest, and Rust Hyper clients scale differently?

## Scope

The first version will generate a static HTML report from existing `summary.json` result files. It will not rerun benchmarks, collect time-series traces, or provide an interactive dashboard.

Included:

- A report generator script.
- A self-contained HTML report with inline CSS and inline SVG charts.
- Explanatory scaling notes for each client implementation.
- README documentation for generating and opening reports.
- Tests for summary loading, derived metrics, and HTML generation.

Excluded:

- Browser-side JavaScript frameworks.
- PNG chart generation.
- Live benchmark execution controls.
- Multi-run statistical analysis beyond comparing the summaries present in a selected run directory.

## Inputs

The generator reads a result run directory that contains one or more nested `summary.json` files:

```text
results/20260629T141233Z/
  python/summary.json
  go/summary.json
  rust-reqwest/summary.json
  rust-hyper/summary.json
```

If no input directory is provided, the script should use the newest timestamp-like directory under `results/`.

The generator should tolerate missing clients. For example, a smoke run with only Go and Rust should still produce a valid report.

## Output

Default output:

```text
reports/latest/index.html
```

The report should be self-contained:

- Inline CSS.
- Inline SVG charts.
- No external images, scripts, fonts, or CDN dependencies.

Generated report files should be ignored by git by default. The repo can add a checked-in sample later if needed, but normal report output should not churn source control.

## Report Structure

The HTML report should contain:

1. Header
   - Report title.
   - Source result directory.
   - Generated timestamp.

2. Workload Overview
   - Total requests.
   - Concurrency.
   - Chunks per response.
   - Chunk bytes.
   - Delay.
   - Warmup requests.

3. Headline Metrics
   - Best requests/sec.
   - Best chunks/sec.
   - Lowest p95 request latency.
   - Lowest p95 time-to-first-chunk.
   - Total failures across clients.

4. Throughput Charts
   - Requests/sec by implementation.
   - Chunks/sec by implementation.

5. Latency Charts
   - Request latency grouped by p50, p95, and p99.
   - Time-to-first-chunk grouped by p50, p95, and p99.

6. Efficiency Table
   - Duration.
   - Successful requests.
   - Failed requests.
   - Total chunks.
   - Requests/sec.
   - Chunks/sec.
   - Per-chunk overhead.
   - Speedup vs Python.
   - Speedup vs Rust reqwest when available.

7. Scaling Explanations
   - Python asyncio + httpx: expected to hit interpreter scheduling, Python object allocation, JSON parsing, and async stream overhead first.
   - Go net/http + goroutines: expected to scale well on many concurrent local HTTP streams because the implementation uses a small worker pool, blocking reads, mature HTTP transport, and efficient goroutine scheduling.
   - Rust reqwest + Tokio: expected to have low latency and strong scaling, but the higher-level reqwest streaming path adds abstraction overhead in this tiny zero-delay workload.
   - Rust Hyper + Tokio: expected to reduce HTTP client overhead compared with reqwest by draining lower-level Hyper body frames directly, while still paying for the current allocating SSE parser and JSON parsing.

8. Caveats
   - This is a localhost synthetic benchmark.
   - No TLS, model inference, provider queueing, or WAN latency is included.
   - Very high concurrency can hit file descriptor limits such as `ulimit -n`.
   - The benchmark measures harness/client overhead, not absolute LLM provider performance.
   - Single runs are directional; repeated runs are needed for rigorous claims.

## Visual Style

The report should look like an engineering performance artifact rather than a marketing page:

- Dense but readable layout.
- White or near-white background.
- High-contrast text.
- Restrained color palette with distinct colors per implementation.
- Tables and charts optimized for scanning.
- No decorative hero, gradients, or unrelated imagery.

## Implementation

Create `scripts/generate_report.py`.

CLI:

```bash
python3 scripts/generate_report.py [results_dir] --output reports/latest/index.html
```

Behavior:

- Resolve `results_dir` from the CLI argument or newest child of `results/`.
- Load all nested `summary.json` files.
- Sort implementations consistently: Python, Go, Rust reqwest, Rust Hyper, then any unknown implementations.
- Compute derived fields:
  - `speedup_vs_python`
  - `speedup_vs_rust_reqwest`
  - best metric labels for headline cards.
- Render a full HTML document.
- Create parent output directories as needed.
- Print the generated report path.

The script should use only Python standard library modules.

## Testing

Add Python unit tests covering:

- Loading nested summary files.
- Selecting the newest results directory.
- Computing speedup ratios.
- Rendering HTML that contains chart sections, scaling explanations, and the expected implementation names.

Tests should not depend on existing local `results/` output. They should create temporary summary files.

## Documentation

Update the README with:

- How to generate a report from the latest run.
- How to generate a report from a specific run.
- How to open the generated file.
- What the scaling explanations mean and what caveats apply.

## Open Questions Resolved

- Use a static HTML report rather than Markdown/PNG output or an interactive dashboard.
- Keep the report generator dependency-free.
- Ignore generated reports by default.
- Include written scaling explanations in the report, not only charts.
