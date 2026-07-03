from __future__ import annotations

import argparse
import html
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RUN_DIR_RE = re.compile(r"^\d{8}T\d{6}Z$")

IMPLEMENTATION_ORDER = {
    ("python", "asyncio-httpx"): 0,
    ("go", "net-http-goroutines"): 1,
    ("rust", "reqwest-tokio"): 2,
    ("rust", "hyper-tokio"): 3,
}

IMPLEMENTATION_COLORS = {
    "Python asyncio-httpx": "#4b5563",
    "Go net-http-goroutines": "#0f766e",
    "Rust reqwest-tokio": "#b45309",
    "Rust hyper-tokio": "#2563eb",
}


def find_latest_results_dir(root: Path) -> Path:
    candidates = [path for path in root.iterdir() if path.is_dir() and RUN_DIR_RE.match(path.name)]
    if not candidates:
        raise FileNotFoundError(f"No timestamped result directories found under {root}")
    return sorted(candidates, key=lambda path: path.name)[-1]


def load_summaries(results_dir: Path) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for path in sorted(results_dir.glob("**/summary.json")):
        data = json.loads(path.read_text())
        data["_path"] = str(path)
        summaries.append(data)
    summaries.sort(key=implementation_key)
    return summaries


def implementation_key(item: dict[str, Any]) -> tuple[int, str]:
    language = item.get("language", "")
    implementation = item.get("implementation", "")
    return (IMPLEMENTATION_ORDER.get((language, implementation), 100), f"{language}:{implementation}")


def implementation_label(item: dict[str, Any]) -> str:
    language = str(item.get("language", "")).title()
    implementation = item.get("implementation", "")
    return f"{language} {implementation}".strip()


def compute_rows(summaries: list[dict[str, Any]]) -> list[dict[str, Any]]:
    ordered = sorted(summaries, key=implementation_key)
    python_rps = first_metric(ordered, "python", "requests_per_second")
    rust_reqwest_rps = first_metric_by_implementation(ordered, "reqwest-tokio", "requests_per_second")
    rows: list[dict[str, Any]] = []

    for item in ordered:
        summary = item["summary"]
        requests_per_second = float(summary.get("requests_per_second", 0.0))
        row = {
            "label": implementation_label(item),
            "language": item.get("language", ""),
            "implementation": item.get("implementation", ""),
            "summary": summary,
            "config": item.get("config", {}),
            "path": item.get("_path", ""),
            "speedup_vs_python": ratio(requests_per_second, python_rps),
            "speedup_vs_rust_reqwest": ratio(requests_per_second, rust_reqwest_rps),
        }
        rows.append(row)

    return rows


def render_report(results_dir: Path, summaries: list[dict[str, Any]]) -> str:
    if not summaries:
        raise ValueError("Cannot render report without summaries")

    rows = compute_rows(summaries)
    config = summaries[0].get("config", {})
    generated_at = datetime.now(timezone.utc).isoformat()
    headline_cards = render_headline_cards(rows)

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>LLM Harness Benchmark Report</title>
  <style>
{css()}
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <p class="eyebrow">Local synthetic OpenAI-style streaming benchmark</p>
        <h1>LLM Harness Benchmark Report</h1>
      </div>
      <dl class="meta">
        <div><dt>Results</dt><dd>{escape(str(results_dir))}</dd></div>
        <div><dt>Generated</dt><dd>{escape(generated_at)}</dd></div>
      </dl>
    </header>

    <section>
      <h2>Workload</h2>
      {render_workload(config)}
    </section>

    <section>
      <h2>Headline Metrics</h2>
      {headline_cards}
    </section>

    <section>
      <h2>Throughput</h2>
      <div class="chart-grid">
        {bar_chart(rows, "requests_per_second", "Requests/sec", higher_is_better=True)}
        {bar_chart(rows, "chunks_per_second", "Chunks/sec", higher_is_better=True)}
      </div>
    </section>

    <section>
      <h2>Request Latency</h2>
      <div class="chart-grid">
        {latency_distribution_chart(rows, "request", "Request latency distribution")}
        {latency_distribution_chart(rows, "ttfc", "Time To First Chunk distribution")}
      </div>
    </section>

    <section>
      <h2>Efficiency Table</h2>
      {render_table(rows)}
    </section>

    <section>
      <h2>Caveats</h2>
      {render_caveats()}
    </section>
  </main>
</body>
</html>
"""


def write_report(results_dir: Path, output: Path) -> Path:
    summaries = load_summaries(results_dir)
    if not summaries:
        raise FileNotFoundError(f"No summary.json files found under {results_dir}")
    html_report = render_report(results_dir, summaries)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(html_report)
    return output


def first_metric(rows: list[dict[str, Any]], language: str, metric: str) -> float | None:
    for item in rows:
        if item.get("language") == language:
            return float(item["summary"].get(metric, 0.0))
    return None


def first_metric_by_implementation(rows: list[dict[str, Any]], implementation: str, metric: str) -> float | None:
    for item in rows:
        if item.get("implementation") == implementation:
            return float(item["summary"].get(metric, 0.0))
    return None


def ratio(value: float, baseline: float | None) -> float | None:
    if baseline is None or baseline <= 0:
        return None
    return value / baseline


def render_headline_cards(rows: list[dict[str, Any]]) -> str:
    best_rps = max(rows, key=lambda row: row["summary"].get("requests_per_second", 0.0))
    best_chunks = max(rows, key=lambda row: row["summary"].get("chunks_per_second", 0.0))
    best_p95 = min(rows, key=lambda row: row["summary"].get("p95_request_latency_ms", float("inf")))
    best_ttfc = min(rows, key=lambda row: row["summary"].get("p95_time_to_first_chunk_ms", float("inf")))
    failures = sum(int(row["summary"].get("failed_requests", 0)) for row in rows)

    cards = [
        ("Best requests/sec", best_rps["label"], format_number(best_rps["summary"]["requests_per_second"])),
        ("Best chunks/sec", best_chunks["label"], format_number(best_chunks["summary"]["chunks_per_second"])),
        ("Lowest p95 request latency", best_p95["label"], f"{format_number(best_p95['summary']['p95_request_latency_ms'])} ms"),
        ("Lowest p95 TTFC", best_ttfc["label"], f"{format_number(best_ttfc['summary']['p95_time_to_first_chunk_ms'])} ms"),
        ("Total failures", "all clients", str(failures)),
    ]

    rendered = []
    for title, label, value in cards:
        rendered.append(
            f"""<article class="metric-card">
  <h3>{escape(title)}</h3>
  <strong>{escape(value)}</strong>
  <p>{escape(label)}</p>
</article>"""
        )
    return f"<div class=\"metric-grid\">{''.join(rendered)}</div>"


def render_workload(config: dict[str, Any]) -> str:
    items = [
        ("duration s", config.get("duration_seconds", "n/a")),
        ("warmup s", config.get("warmup_seconds", "n/a")),
        ("concurrency", config.get("concurrency", "n/a")),
        ("chunks/response", config.get("chunks_per_response", "n/a")),
        ("chunk bytes", config.get("chunk_bytes", "n/a")),
        ("ttfc ms", config.get("ttfc_ms", "n/a")),
        ("events/s/request", config.get("events_per_second", "n/a")),
    ]
    cells = [f"<div><dt>{escape(label)}</dt><dd>{escape(str(value))}</dd></div>" for label, value in items]
    return f"<dl class=\"workload-grid\">{''.join(cells)}</dl>"


def bar_chart(rows: list[dict[str, Any]], metric: str, title: str, higher_is_better: bool) -> str:
    values = [float(row["summary"].get(metric, 0.0)) for row in rows]
    max_value = max(values) if values else 0.0
    chart_rows = []
    for row, value in zip(rows, values):
        width = 0 if max_value == 0 else max(2, int((value / max_value) * 100))
        color = IMPLEMENTATION_COLORS.get(row["label"], "#475569")
        chart_rows.append(
            f"""<div class="bar-row">
  <span>{escape(row['label'])}</span>
  <div class="bar-track"><div class="bar" style="width:{width}%;background:{color}"></div></div>
  <strong>{escape(format_number(value))}</strong>
</div>"""
        )
    direction = "higher is better" if higher_is_better else "lower is better"
    return f"""<article class="chart-card">
  <h3>{escape(title)}</h3>
  <p>{direction}</p>
  <div class="bar-chart">{''.join(chart_rows)}</div>
</article>"""


def latency_distribution_chart(rows: list[dict[str, Any]], kind: str, title: str) -> str:
    if kind == "request":
        metrics = [
            ("p50", "p50_request_latency_ms"),
            ("p95", "p95_request_latency_ms"),
            ("p99", "p99_request_latency_ms"),
        ]
    else:
        metrics = [
            ("p50", "p50_time_to_first_chunk_ms"),
            ("p95", "p95_time_to_first_chunk_ms"),
            ("p99", "p99_time_to_first_chunk_ms"),
        ]

    width = 920
    row_height = 68
    label_width = 170
    chart_width = 430
    value_x = label_width + chart_width + 22
    plot_top = 84
    height = plot_top + (len(rows) * row_height) + 22
    all_values = [float(row["summary"].get(metric, 0.0)) for row in rows for _, metric in metrics]
    max_value = max(all_values) if all_values else 0.0
    positive_values = [value for value in all_values if value > 0]
    min_positive = min(positive_values) if positive_values else 0.0
    use_log_scale = min_positive > 0 and max_value / min_positive >= 10

    def x_for(value: float) -> float:
        if max_value <= 0:
            return float(label_width)
        if use_log_scale:
            lower = math.log10(min_positive)
            upper = math.log10(max_value)
            if upper == lower:
                return float(label_width + chart_width / 2)
            clamped = max(value, min_positive)
            scaled = (math.log10(clamped) - lower) / (upper - lower)
        else:
            scaled = value / max_value
        return label_width + (scaled * chart_width)

    def axis_ticks() -> list[float]:
        if max_value <= 0:
            return [0.0]
        if use_log_scale:
            mid = math.sqrt(min_positive * max_value)
            return [min_positive, mid, max_value]
        return [0.0, max_value / 2, max_value]

    scale_note = "log scale; lower is better" if use_log_scale else "linear scale; lower is better"
    svg_parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="{escape(title)}">',
        f'<text x="0" y="20" class="svg-title">{escape(title)}</text>',
        f'<text x="0" y="42" class="svg-note">{escape(scale_note)}</text>',
        '<g class="latency-legend">',
        f'<line x1="{label_width}" y1="42" x2="{label_width + 26}" y2="42" class="latency-range-sample"></line>',
        f'<text x="{label_width + 34}" y="46" class="svg-value">p50-p95 range</text>',
        f'<circle cx="{label_width + 152}" cy="42" r="5" class="latency-dot-sample"></circle>',
        f'<text x="{label_width + 164}" y="46" class="svg-value">p99 tail</text>',
        "</g>",
    ]

    for tick in axis_ticks():
        x = x_for(tick)
        svg_parts.append(f'<line x1="{x:.1f}" y1="58" x2="{x:.1f}" y2="{height - 16}" class="latency-grid"></line>')
        svg_parts.append(f'<text x="{x:.1f}" y="72" class="svg-tick">{escape(format_number(tick))} ms</text>')

    y = plot_top
    for row in rows:
        color = IMPLEMENTATION_COLORS.get(row["label"], "#475569")
        values = {percentile_label: float(row["summary"].get(metric, 0.0)) for percentile_label, metric in metrics}
        p50_x = x_for(values["p50"])
        p95_x = x_for(values["p95"])
        p99_x = x_for(values["p99"])
        range_x = min(p50_x, p95_x)
        range_width = max(3.0, abs(p95_x - p50_x))
        center_y = y + 28
        summary = (
            f"p50 {format_number(values['p50'])} | "
            f"p95 {format_number(values['p95'])} | "
            f"p99 {format_number(values['p99'])} ms"
        )
        svg_parts.append(f'<g class="latency-lane" data-client="{escape(row["label"])}">')
        svg_parts.append(f'<text x="0" y="{center_y + 4}" class="svg-label">{escape(row["label"])}</text>')
        svg_parts.append(
            f'<line x1="{label_width}" y1="{center_y}" x2="{label_width + chart_width}" y2="{center_y}" class="latency-track"></line>'
        )
        svg_parts.append(
            f'<rect x="{range_x:.1f}" y="{center_y - 8}" width="{range_width:.1f}" height="16" '
            f'fill="{color}" class="latency-range" data-percentile="p50-p95" rx="8"></rect>'
        )
        svg_parts.append(
            f'<line x1="{p50_x:.1f}" y1="{center_y - 13}" x2="{p50_x:.1f}" y2="{center_y + 13}" '
            f'stroke="{color}" class="latency-marker" data-percentile="p50"></line>'
        )
        svg_parts.append(
            f'<line x1="{p95_x:.1f}" y1="{center_y - 13}" x2="{p95_x:.1f}" y2="{center_y + 13}" '
            f'stroke="{color}" class="latency-marker" data-percentile="p95"></line>'
        )
        svg_parts.append(
            f'<circle cx="{p99_x:.1f}" cy="{center_y}" r="5.5" fill="{color}" '
            f'class="latency-tail" data-percentile="p99"></circle>'
        )
        svg_parts.append(f'<text x="{value_x}" y="{center_y + 4}" class="svg-value">{escape(summary)}</text>')
        svg_parts.append("</g>")
        y += row_height
    svg_parts.append("</svg>")
    return f"<article class=\"chart-card svg-card\">{''.join(svg_parts)}</article>"


def render_table(rows: list[dict[str, Any]]) -> str:
    headers = [
        "Implementation",
        "Req/s",
        "Chunks/s",
        "p95 req ms",
        "p95 TTFC ms",
        "Efficiency",
        "Failures",
        "Incomplete",
        "vs Python",
        "vs Rust reqwest",
    ]
    body = []
    for row in rows:
        summary = row["summary"]
        body.append(
            "<tr>"
            f"<th>{escape(row['label'])}</th>"
            f"<td>{escape(format_number(summary.get('requests_per_second', 0.0)))}</td>"
            f"<td>{escape(format_number(summary.get('chunks_per_second', 0.0)))}</td>"
            f"<td>{escape(format_number(summary.get('p95_request_latency_ms', 0.0)))}</td>"
            f"<td>{escape(format_number(summary.get('p95_time_to_first_chunk_ms', 0.0)))}</td>"
            f"<td>{escape(format_number(summary.get('efficiency', 0.0), digits=3))}</td>"
            f"<td>{escape(str(summary.get('failed_requests', 0)))}</td>"
            f"<td>{escape(str(summary.get('incomplete_requests', 0)))}</td>"
            f"<td>{escape(format_ratio(row['speedup_vs_python']))}</td>"
            f"<td>{escape(format_ratio(row['speedup_vs_rust_reqwest']))}</td>"
            "</tr>"
        )
    header_html = "".join(f"<th>{escape(header)}</th>" for header in headers)
    return f"<div class=\"table-wrap\"><table><thead><tr>{header_html}</tr></thead><tbody>{''.join(body)}</tbody></table></div>"


def render_caveats() -> str:
    caveats = [
        "This is a localhost synthetic benchmark, not a real LLM provider benchmark; clients are minimal hand-rolled loops, not official SDKs.",
        "HTTP/1.1 cleartext only — no TLS or HTTP/2, unlike production providers.",
        "No model inference, provider queueing, or WAN latency is included.",
        "Very high concurrency can hit file descriptor limits such as ulimit -n.",
        "The benchmark measures harness/client overhead rather than absolute provider performance.",
        "Single runs are directional; repeated runs are needed for rigorous claims.",
    ]
    return "<ul class=\"caveats\">" + "".join(f"<li>{escape(item)}</li>" for item in caveats) + "</ul>"


def format_number(value: Any, digits: int = 2) -> str:
    if isinstance(value, int):
        return f"{value:,}"
    try:
        number = float(value)
    except (TypeError, ValueError):
        return str(value)
    return f"{number:,.{digits}f}"


def format_ratio(value: float | None) -> str:
    if value is None:
        return "n/a"
    return f"{value:.2f}x"


def escape(value: str) -> str:
    return html.escape(value, quote=True)


def css() -> str:
    return """
:root {
  color-scheme: light;
  font-family: ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  background: #f8fafc;
  color: #111827;
}
* { box-sizing: border-box; }
body { margin: 0; background: #f8fafc; }
main { width: min(1180px, calc(100vw - 40px)); margin: 0 auto; padding: 32px 0 56px; }
header { display: flex; justify-content: space-between; gap: 24px; align-items: flex-start; margin-bottom: 28px; }
h1 { font-size: 32px; line-height: 1.15; margin: 6px 0 0; letter-spacing: 0; }
h2 { font-size: 20px; margin: 0 0 14px; letter-spacing: 0; }
h3 { font-size: 15px; margin: 0 0 8px; letter-spacing: 0; }
p { margin: 0; color: #475569; line-height: 1.55; }
section { margin-top: 22px; padding: 20px; border: 1px solid #e2e8f0; border-radius: 8px; background: #ffffff; }
.eyebrow { color: #0f766e; font-size: 13px; font-weight: 700; text-transform: uppercase; letter-spacing: .04em; }
.meta, .workload-grid { margin: 0; display: grid; gap: 10px; }
.meta { min-width: 360px; }
.meta div, .workload-grid div { padding: 10px 12px; border: 1px solid #e2e8f0; border-radius: 6px; background: #f8fafc; }
dt { color: #64748b; font-size: 12px; text-transform: uppercase; font-weight: 700; letter-spacing: .04em; }
dd { margin: 3px 0 0; font-weight: 700; color: #111827; overflow-wrap: anywhere; }
.workload-grid { grid-template-columns: repeat(7, minmax(0, 1fr)); }
.metric-grid { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 12px; }
.metric-card { border: 1px solid #e2e8f0; background: #f8fafc; border-radius: 6px; padding: 14px; min-height: 122px; }
.metric-card strong { display: block; font-size: 24px; margin: 8px 0 4px; }
.chart-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 16px; }
.chart-card { border: 1px solid #e2e8f0; border-radius: 6px; padding: 16px; background: #ffffff; min-width: 0; }
.bar-chart { display: grid; gap: 12px; margin-top: 14px; }
.bar-row { display: grid; grid-template-columns: minmax(150px, 210px) 1fr minmax(90px, auto); gap: 12px; align-items: center; font-size: 13px; }
.bar-track { height: 16px; background: #e5e7eb; border-radius: 999px; overflow: hidden; }
.bar { height: 100%; border-radius: 999px; }
.svg-card svg { width: 100%; height: auto; display: block; }
.svg-title { font: 700 17px system-ui, sans-serif; fill: #111827; }
.svg-label { font: 12px system-ui, sans-serif; fill: #334155; }
.svg-value { font: 12px system-ui, sans-serif; fill: #475569; }
.svg-note { font: 12px system-ui, sans-serif; fill: #64748b; }
.svg-tick { font: 11px system-ui, sans-serif; fill: #64748b; text-anchor: middle; }
.latency-grid { stroke: #e2e8f0; stroke-width: 1; }
.latency-track { stroke: #cbd5e1; stroke-width: 7; stroke-linecap: round; }
.latency-range { opacity: .72; }
.latency-marker { stroke-width: 2.25; stroke-linecap: round; }
.latency-tail { stroke: #ffffff; stroke-width: 2; }
.latency-range-sample { stroke: #64748b; stroke-width: 8; stroke-linecap: round; opacity: .72; }
.latency-dot-sample { fill: #64748b; stroke: #ffffff; stroke-width: 2; }
.table-wrap { overflow-x: auto; }
table { width: 100%; border-collapse: collapse; font-size: 13px; }
th, td { border-bottom: 1px solid #e2e8f0; padding: 10px 8px; text-align: right; white-space: nowrap; }
th:first-child, td:first-child { text-align: left; }
thead th { color: #475569; font-size: 12px; text-transform: uppercase; letter-spacing: .04em; }
.caveats { margin: 0; padding-left: 20px; color: #475569; line-height: 1.65; }
@media (max-width: 900px) {
  header, .chart-grid { grid-template-columns: 1fr; display: grid; }
  .meta { min-width: 0; }
  .workload-grid, .metric-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
}
@media (max-width: 640px) {
  main { width: min(100vw - 24px, 1180px); padding-top: 20px; }
  .workload-grid, .metric-grid { grid-template-columns: 1fr; }
  .bar-row { grid-template-columns: 1fr; gap: 6px; }
}
"""


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate a static HTML benchmark report.")
    parser.add_argument("results_dir", nargs="?", default=None, help="Result run directory. Defaults to newest under results/.")
    parser.add_argument("--output", default="reports/latest/index.html", help="Output HTML path.")
    args = parser.parse_args()

    results_dir = Path(args.results_dir) if args.results_dir else find_latest_results_dir(Path("results"))
    output = write_report(results_dir, Path(args.output))
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
