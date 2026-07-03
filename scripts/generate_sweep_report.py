from __future__ import annotations

import argparse
import html
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

RUN_DIR_RE = re.compile(r"^\d{8}T\d{6}Z$")

# Reference dataviz palette, fixed slot order (validated for CVD separation).
# Drain is the neutral reference, not a categorical slot.
CLIENT_STYLE = {
    "python": {"light": "#2a78d6", "dark": "#3987e5", "dash": ""},
    "go": {"light": "#1baf7a", "dark": "#199e70", "dash": ""},
    "rust-reqwest": {"light": "#eda100", "dark": "#c98500", "dash": ""},
    "rust-hyper": {"light": "#008300", "dark": "#008300", "dash": ""},
    "drain": {"light": "#898781", "dark": "#898781", "dash": "6 4"},
}
CLIENT_ORDER = ["drain", "python", "go", "rust-reqwest", "rust-hyper"]


def find_latest_results_dir(root: Path) -> Path:
    candidates = [p for p in root.iterdir() if p.is_dir() and RUN_DIR_RE.match(p.name)]
    if not candidates:
        raise FileNotFoundError(f"No timestamped result directories found under {root}")
    return sorted(candidates, key=lambda p: p.name)[-1]


def load_json_if_exists(path: Path) -> dict[str, Any]:
    if path.exists():
        return json.loads(path.read_text())
    return {}


def load_cells(run_dir: Path) -> list[dict[str, Any]]:
    cells: list[dict[str, Any]] = []
    for summary_path in sorted(run_dir.glob("*/c*/r*/*/summary.json")):
        client_dir = summary_path.parent
        repeat_dir = client_dir.parent
        concurrency_dir = repeat_dir.parent
        tier_dir = concurrency_dir.parent
        data = json.loads(summary_path.read_text())
        cells.append({
            "tier": tier_dir.name,
            "concurrency": int(concurrency_dir.name[1:]),
            "repeat": int(repeat_dir.name[1:]),
            "client": client_dir.name,
            "config": data.get("config", {}),
            "summary": data["summary"],
            "server_stats": load_json_if_exists(client_dir / "server_stats.json"),
            "cpu": load_json_if_exists(client_dir / "cpu.json"),
        })
    return cells


def mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def aggregate_cells(cells: list[dict[str, Any]]) -> dict[tuple[str, str, int], dict[str, Any]]:
    grouped: dict[tuple[str, str, int], list[dict[str, Any]]] = {}
    for cell in cells:
        grouped.setdefault((cell["tier"], cell["client"], cell["concurrency"]), []).append(cell)

    aggregates: dict[tuple[str, str, int], dict[str, Any]] = {}
    for key, group in grouped.items():
        efficiencies = [c["summary"]["efficiency"] for c in group]
        config = group[0]["config"]
        ttfc_ms = float(config.get("ttfc_ms", 0))
        aggregates[key] = {
            "repeats": len(group),
            "efficiency_mean": mean(efficiencies),
            "efficiency_min": min(efficiencies),
            "efficiency_max": max(efficiencies),
            "chunks_per_second_mean": mean([c["summary"]["chunks_per_second"] for c in group]),
            "ttfc_excess_p95_mean": mean(
                [c["summary"]["p95_time_to_first_chunk_ms"] - ttfc_ms for c in group]
            ),
            "stretch_p95_mean": mean([c["summary"]["p95_stream_stretch"] for c in group]),
            "max_gap_p99_mean": mean([c["summary"]["p99_max_gap_ms"] for c in group]),
            "failed": sum(c["summary"]["failed_requests"] for c in group),
            "incomplete": sum(c["summary"]["incomplete_requests"] for c in group),
            "server_slip_p99_max": max(
                (c["server_stats"].get("slip_p99_ms", 0.0) for c in group), default=0.0
            ),
            "client_cpu_mean": mean(
                [c["cpu"].get("client", {}).get("mean_percent", 0.0) for c in group]
            ),
            "server_cpu_mean": mean(
                [c["cpu"].get("server", {}).get("mean_percent", 0.0) for c in group]
            ),
            "events_per_second": int(config.get("events_per_second", 0)),
            "ttfc_ms": ttfc_ms,
        }
    return aggregates


def escape(value: str) -> str:
    return html.escape(str(value), quote=True)


def format_number(value: float, digits: int = 2) -> str:
    return f"{value:,.{digits}f}"


def clients_in(aggregates: dict, tier: str) -> list[str]:
    present = {client for (t, client, _) in aggregates if t == tier}
    ordered = [c for c in CLIENT_ORDER if c in present]
    return ordered + sorted(present - set(ordered))


def concurrencies_in(aggregates: dict, tier: str) -> list[int]:
    return sorted({c for (t, _, c) in aggregates if t == tier})


def line_chart(
    title: str,
    y_label: str,
    concurrencies: list[int],
    series: list[dict[str, Any]],
    y_max: float,
    reference_y: float | None = None,
    value_digits: int = 2,
) -> str:
    """One SVG line chart. series = [{name, points: {concurrency: (mean, lo, hi)}}]."""
    width, height = 860, 360
    left, right, top, bottom = 64, 150, 48, 40
    plot_w, plot_h = width - left - right, height - top - bottom

    def x_for(concurrency: int) -> float:
        index = concurrencies.index(concurrency)
        if len(concurrencies) == 1:
            return left + plot_w / 2
        return left + index * plot_w / (len(concurrencies) - 1)

    def y_for(value: float) -> float:
        clamped = min(max(value, 0.0), y_max)
        return top + plot_h - (clamped / y_max) * plot_h if y_max > 0 else top + plot_h

    LABEL_GAP = 14.0

    parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="{escape(title)}">',
        f'<text x="0" y="20" class="svg-title">{escape(title)}</text>',
        f'<text x="0" y="38" class="svg-note">{escape(y_label)} vs concurrency</text>',
    ]
    end_labels: list[tuple[str, float, float]] = []  # (name, anchor_x, desired_y)
    for tick_index in range(5):
        tick_value = y_max * tick_index / 4
        y = y_for(tick_value)
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left + plot_w}" y2="{y:.1f}" class="grid"></line>')
        parts.append(f'<text x="{left - 8}" y="{y + 4:.1f}" class="tick tick-y">{format_number(tick_value, value_digits)}</text>')
    for concurrency in concurrencies:
        x = x_for(concurrency)
        parts.append(f'<text x="{x:.1f}" y="{height - 16}" class="tick tick-x">{concurrency}</text>')
    if reference_y is not None and reference_y <= y_max:
        y = y_for(reference_y)
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left + plot_w}" y2="{y:.1f}" class="reference"></line>')
        parts.append(f'<text x="{left + plot_w + 6}" y="{y + 4:.1f}" class="svg-note">ideal</text>')

    for entry in series:
        name = entry["name"]
        style = CLIENT_STYLE.get(name, {"dash": ""})
        dash = f' stroke-dasharray="{style["dash"]}"' if style.get("dash") else ""
        points = [
            (concurrency, entry["points"][concurrency])
            for concurrency in concurrencies
            if concurrency in entry["points"]
        ]
        if not points:
            continue
        coords = " ".join(f"{x_for(c):.1f},{y_for(v[0]):.1f}" for c, v in points)
        parts.append(f'<polyline points="{coords}" class="line series-{escape(name)}"{dash}></polyline>')
        for concurrency, (value, low, high) in points:
            x = x_for(concurrency)
            if high > low:
                parts.append(
                    f'<line x1="{x:.1f}" y1="{y_for(low):.1f}" x2="{x:.1f}" y2="{y_for(high):.1f}" '
                    f'class="whisker series-{escape(name)}"></line>'
                )
            parts.append(
                f'<circle cx="{x:.1f}" cy="{y_for(value):.1f}" r="4" class="marker series-{escape(name)}">'
                f'<title>{escape(name)} · c={concurrency} · {format_number(value, value_digits)}</title></circle>'
            )
        last_concurrency, (last_value, _, _) = points[-1]
        end_labels.append((name, x_for(last_concurrency), y_for(last_value) + 4))

    if end_labels:
        end_labels.sort(key=lambda entry: entry[2])
        resolved: list[float] = []
        for _, _, desired_y in end_labels:
            prev_y = resolved[-1] if resolved else None
            y = desired_y if prev_y is None else max(desired_y, prev_y + LABEL_GAP)
            resolved.append(y)
        plot_bottom = top + plot_h + 10
        overflow = resolved[-1] - plot_bottom
        if overflow > 0:
            resolved = [y - overflow for y in resolved]
            plot_top = top + 10
            resolved = [max(y, plot_top) for y in resolved]
        for (name, anchor_x, _), y in zip(end_labels, resolved):
            parts.append(
                f'<text x="{anchor_x + 10:.1f}" y="{y:.1f}" class="direct-label">{escape(name)}</text>'
            )

    parts.append("</svg>")
    return f'<article class="chart-card">{"".join(parts)}</article>'


def tier_series(aggregates: dict, tier: str, metric: str) -> list[dict[str, Any]]:
    series = []
    for client in clients_in(aggregates, tier):
        points: dict[int, tuple[float, float, float]] = {}
        for concurrency in concurrencies_in(aggregates, tier):
            entry = aggregates.get((tier, client, concurrency))
            if entry is None:
                continue
            if metric == "efficiency":
                points[concurrency] = (
                    entry["efficiency_mean"], entry["efficiency_min"], entry["efficiency_max"],
                )
            elif metric == "chunks_per_second":
                value = entry["chunks_per_second_mean"]
                points[concurrency] = (value, value, value)
            else:  # ttfc_excess
                value = entry["ttfc_excess_p95_mean"]
                points[concurrency] = (value, value, value)
        series.append({"name": client, "points": points})
    return series


def render_tier_section(aggregates: dict, tier: str) -> str:
    concurrencies = concurrencies_in(aggregates, tier)
    sample = next(v for (t, _, _), v in aggregates.items() if t == tier)
    paced = sample["events_per_second"] > 0

    charts = []
    if paced:
        charts.append(line_chart(
            f"Delivery efficiency — {tier}", "observed / ideal events per second",
            concurrencies, tier_series(aggregates, tier, "efficiency"),
            y_max=1.1, reference_y=1.0,
        ))
        ttfc_values = [
            entry["ttfc_excess_p95_mean"]
            for (t, _, _), entry in aggregates.items() if t == tier
        ]
        ttfc_max = max(max(ttfc_values, default=0.0) * 1.15, 1.0)
        charts.append(line_chart(
            f"p95 TTFC excess — {tier}", "p95 time-to-first-chunk minus configured TTFC (ms)",
            concurrencies, tier_series(aggregates, tier, "ttfc_excess"),
            y_max=ttfc_max, value_digits=1,
        ))
    else:
        throughput = [
            entry["chunks_per_second_mean"]
            for (t, _, _), entry in aggregates.items() if t == tier
        ]
        charts.append(line_chart(
            f"Observed events/sec — {tier}", "mean parsed content events per second",
            concurrencies, tier_series(aggregates, tier, "chunks_per_second"),
            y_max=max(max(throughput, default=0.0) * 1.15, 1.0), value_digits=0,
        ))

    rows = []
    for client in clients_in(aggregates, tier):
        for concurrency in concurrencies:
            entry = aggregates.get((tier, client, concurrency))
            if entry is None:
                continue
            rows.append(
                "<tr>"
                f"<th>{escape(client)}</th>"
                f"<td>{concurrency}</td>"
                f"<td>{format_number(entry['efficiency_mean'], 3) if paced else 'n/a'}</td>"
                f"<td>{format_number(entry['chunks_per_second_mean'], 0)}</td>"
                f"<td>{format_number(entry['ttfc_excess_p95_mean'], 1) if paced else 'n/a'}</td>"
                f"<td>{format_number(entry['stretch_p95_mean'], 3) if paced else 'n/a'}</td>"
                f"<td>{format_number(entry['max_gap_p99_mean'], 1)}</td>"
                f"<td>{entry['failed']}</td>"
                f"<td>{entry['incomplete']}</td>"
                f"<td>{format_number(entry['server_slip_p99_max'], 2)}</td>"
                f"<td>{format_number(entry['client_cpu_mean'], 0)}%</td>"
                f"<td>{format_number(entry['server_cpu_mean'], 0)}%</td>"
                "</tr>"
            )
    headers = (
        "Client", "Concurrency", "Efficiency", "Events/s", "p95 TTFC excess ms",
        "p95 stretch", "p99 max gap ms", "Failed", "Incomplete",
        "Server slip p99 ms", "Client CPU", "Server CPU",
    )
    header_html = "".join(f"<th>{escape(h)}</th>" for h in headers)
    table = (
        f'<div class="table-wrap"><table><thead><tr>{header_html}</tr></thead>'
        f'<tbody>{"".join(rows)}</tbody></table></div>'
    )
    legend = "".join(
        f'<span class="legend-item"><span class="legend-swatch series-{escape(c)}"></span>{escape(c)}</span>'
        for c in clients_in(aggregates, tier)
    )
    return (
        f'<section><h2>Tier: {escape(tier)}</h2>'
        f'<div class="legend">{legend}</div>'
        f'<div class="chart-grid">{"".join(charts)}</div>{table}</section>'
    )


def render_stops(sweep_meta: dict[str, Any]) -> str:
    stops = sweep_meta.get("stops", {})
    if not stops:
        return "<p>No stop rules triggered.</p>"
    rows = []
    for key, info in sorted(stops.items()):
        tier, _, client = key.partition(":")
        rows.append(
            f"<tr><th>{escape(tier)}</th><td>{escape(client)}</td>"
            f"<td>{info.get('concurrency', '')}</td><td>{escape(info.get('reason', ''))}</td></tr>"
        )
    return (
        '<div class="table-wrap"><table>'
        "<thead><tr><th>Tier</th><th>Client</th><th>Knee concurrency</th><th>Reason</th></tr></thead>"
        f'<tbody>{"".join(rows)}</tbody></table></div>'
    )


def css() -> str:
    light = "".join(
        f"--series-{name}: {style['light']};" for name, style in CLIENT_STYLE.items()
    )
    dark = "".join(
        f"--series-{name}: {style['dark']};" for name, style in CLIENT_STYLE.items()
    )
    series_rules = "".join(
        f".series-{name} {{ stroke: var(--series-{name}); }}"
        f".legend-swatch.series-{name} {{ background: var(--series-{name}); }}"
        for name in CLIENT_STYLE
    )
    return f"""
:root {{
  color-scheme: light dark;
  --surface-1: #fcfcfb; --page: #f9f9f7; --ink: #0b0b0b; --ink-2: #52514e;
  --muted: #898781; --grid: #e1e0d9; --border: rgba(11,11,11,0.10);
  {light}
}}
@media (prefers-color-scheme: dark) {{
  :root {{
    --surface-1: #1a1a19; --page: #0d0d0d; --ink: #ffffff; --ink-2: #c3c2b7;
    --grid: #2c2c2a; --border: rgba(255,255,255,0.10);
    {dark}
  }}
}}
* {{ box-sizing: border-box; }}
body {{ margin: 0; background: var(--page); color: var(--ink);
  font-family: system-ui, -apple-system, "Segoe UI", sans-serif; }}
main {{ width: min(1180px, calc(100vw - 40px)); margin: 0 auto; padding: 32px 0 56px; }}
h1 {{ font-size: 30px; margin: 6px 0 4px; }}
h2 {{ font-size: 20px; margin: 0 0 12px; }}
p {{ color: var(--ink-2); line-height: 1.55; }}
section {{ margin-top: 22px; padding: 20px; border: 1px solid var(--border);
  border-radius: 8px; background: var(--surface-1); }}
.eyebrow {{ color: var(--muted); font-size: 13px; font-weight: 700;
  text-transform: uppercase; letter-spacing: .04em; }}
.chart-grid {{ display: grid; grid-template-columns: 1fr; gap: 16px; margin-bottom: 16px; }}
.chart-card svg {{ width: 100%; height: auto; display: block; }}
.svg-title {{ font: 700 16px system-ui, sans-serif; fill: var(--ink); }}
.svg-note {{ font: 12px system-ui, sans-serif; fill: var(--muted); }}
.tick {{ font: 11px system-ui, sans-serif; fill: var(--muted);
  font-variant-numeric: tabular-nums; }}
.tick-y {{ text-anchor: end; }}
.tick-x {{ text-anchor: middle; }}
.grid {{ stroke: var(--grid); stroke-width: 1; }}
.reference {{ stroke: var(--muted); stroke-width: 1; stroke-dasharray: 2 3; }}
.line {{ fill: none; stroke-width: 2; }}
.whisker {{ stroke-width: 1.5; opacity: .6; }}
.marker {{ fill: var(--surface-1); stroke-width: 2; }}
.direct-label {{ font: 12px system-ui, sans-serif; fill: var(--ink-2); }}
.legend {{ display: flex; gap: 16px; flex-wrap: wrap; margin-bottom: 10px; }}
.legend-item {{ display: inline-flex; align-items: center; gap: 6px;
  font-size: 13px; color: var(--ink-2); }}
.legend-swatch {{ width: 14px; height: 3px; border-radius: 2px; display: inline-block; }}
.table-wrap {{ overflow-x: auto; }}
table {{ width: 100%; border-collapse: collapse; font-size: 13px; }}
th, td {{ border-bottom: 1px solid var(--grid); padding: 8px;
  text-align: right; white-space: nowrap; font-variant-numeric: tabular-nums; }}
th:first-child, td:first-child {{ text-align: left; }}
thead th {{ color: var(--ink-2); font-size: 12px; text-transform: uppercase;
  letter-spacing: .04em; }}
{series_rules}
"""


def render_report(run_dir: Path, cells: list[dict[str, Any]], sweep_meta: dict[str, Any]) -> str:
    if not cells:
        raise ValueError("Cannot render sweep report without cells")
    aggregates = aggregate_cells(cells)
    tiers = sorted({tier for (tier, _, _) in aggregates})
    # Paced tiers ordered by rate, then the unpaced tier(s) last.
    def tier_key(tier: str) -> tuple[int, int, str]:
        sample = next(v for (t, _, _), v in aggregates.items() if t == tier)
        eps = sample["events_per_second"]
        return (1, 0, tier) if eps == 0 else (0, eps, tier)
    tiers.sort(key=tier_key)

    sections = "".join(render_tier_section(aggregates, tier) for tier in tiers)
    generated_at = datetime.now(timezone.utc).isoformat()
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Streaming Client Sweep Report</title>
<style>{css()}</style>
</head>
<body>
<main>
<header>
  <p class="eyebrow">Concurrency sweep — synthetic OpenAI-style streaming</p>
  <h1>Streaming Client Sweep Report</h1>
  <p>Run: {escape(str(run_dir))} · Generated: {escape(generated_at)}</p>
  <p>Efficiency = observed parsed events/sec ÷ the achievable closed-loop ideal
  (concurrency × chunks ÷ (TTFC + (chunks−1)/rate)).
  The drain client is a parse-free reference: any gap it shows is server/OS,
  any gap below it is client overhead.</p>
</header>
<section><h2>Stop rules triggered (knees)</h2>{render_stops(sweep_meta)}</section>
{sections}
</main>
</body>
</html>
"""


def write_report(run_dir: Path, output: Path) -> Path:
    cells = load_cells(run_dir)
    if not cells:
        raise FileNotFoundError(f"No cell summary.json files found under {run_dir}")
    sweep_meta = load_json_if_exists(run_dir / "sweep.json")
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_report(run_dir, cells, sweep_meta))
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate a static HTML sweep report.")
    parser.add_argument("results_dir", nargs="?", default=None,
                        help="Sweep run directory. Defaults to newest under results/.")
    parser.add_argument("--output", default="reports/sweep/index.html", help="Output HTML path.")
    args = parser.parse_args()

    results_dir = Path(args.results_dir) if args.results_dir else find_latest_results_dir(Path("results"))
    output = write_report(results_dir, Path(args.output))
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
