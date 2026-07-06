from __future__ import annotations

import argparse
import html
import json
import math
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from bench_harness import clients as client_registry

ROOT = Path(__file__).resolve().parents[1]

RUN_DIR_RE = re.compile(r"^\d{8}T\d{6}Z$")

# Client order and chart palette (slot order validated for CVD separation;
# drain is the neutral reference, not a categorical slot) live in the single
# client registry: bench_harness/clients.py. Unknown/legacy client names
# (e.g. an old `rust-drain` result dir) fall back to a neutral style via
# client_registry.style() rather than crashing.
CLIENT_STYLE = {name: client_registry.style(name) for name in client_registry.CLIENT_ORDER}
CLIENT_ORDER = list(client_registry.CLIENT_ORDER)


def process_count(client: str) -> int:
    """Worker processes a client fans out to (1 unless its registry
    implementation label carries an -mpN suffix, e.g. asyncio-openai-sdk-mp12)."""
    spec = client_registry.CLIENTS.get(client)
    if spec is None:
        return 1
    match = re.search(r"-mp(\d+)$", spec.implementation)
    return int(match.group(1)) if match else 1


def is_python_client(client: str) -> bool:
    spec = client_registry.CLIENTS.get(client)
    return spec is not None and spec.kind == "python"


def stops_by_tier(sweep_meta: dict[str, Any]) -> dict[str, dict[str, tuple[int, str]]]:
    """{tier: {client: (knee_concurrency, reason)}} from sweep.json stops."""
    result: dict[str, dict[str, tuple[int, str]]] = {}
    for key, info in (sweep_meta.get("stops") or {}).items():
        tier, _, client = key.partition(":")
        concurrency = info.get("concurrency")
        if concurrency is None:
            continue
        result.setdefault(tier, {})[client] = (int(concurrency), str(info.get("reason", "")))
    return result


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


# Config keys that every cell within a (tier, client, concurrency) group must
# agree on. Disagreement means the group is silently mixing runs from
# incompatible configs (e.g. a re-run with a different ttfc_ms merged into the
# same report) — we still aggregate rather than crash, but we must say so.
MERGE_CONSISTENCY_KEYS = ("ttfc_ms", "events_per_second", "chunks_per_response", "duration_seconds")


def warn_on_config_disagreement(key: tuple[str, str, int], group: list[dict[str, Any]]) -> None:
    tier, client, concurrency = key
    for config_key in MERGE_CONSISTENCY_KEYS:
        values = {cell["config"].get(config_key) for cell in group}
        if len(values) > 1:
            print(
                f"warning: merged cells for tier={tier!r} client={client!r} "
                f"concurrency={concurrency} disagree on {config_key!r}: "
                f"{sorted(values, key=str)} — aggregating anyway, results may be misleading",
                file=sys.stderr,
            )


def aggregate_cells(cells: list[dict[str, Any]]) -> dict[tuple[str, str, int], dict[str, Any]]:
    grouped: dict[tuple[str, str, int], list[dict[str, Any]]] = {}
    for cell in cells:
        grouped.setdefault((cell["tier"], cell["client"], cell["concurrency"]), []).append(cell)

    aggregates: dict[tuple[str, str, int], dict[str, Any]] = {}
    for key, group in grouped.items():
        warn_on_config_disagreement(key, group)
        efficiencies = [c["summary"]["efficiency"] for c in group]
        config = group[0]["config"]
        ttfc_ms = float(config.get("ttfc_ms", 0))
        aggregates[key] = {
            "repeats": len(group),
            "efficiency_mean": mean(efficiencies),
            "efficiency_min": min(efficiencies),
            "efficiency_max": max(efficiencies),
            "chunks_per_second_mean": mean([c["summary"]["chunks_per_second"] for c in group]),
            "ttfc_excess_p50_mean": mean(
                [c["summary"]["p50_time_to_first_chunk_ms"] - ttfc_ms for c in group]
            ),
            "ttfc_excess_p95_mean": mean(
                [c["summary"]["p95_time_to_first_chunk_ms"] - ttfc_ms for c in group]
            ),
            "ttfc_excess_p99_mean": mean(
                [c["summary"]["p99_time_to_first_chunk_ms"] - ttfc_ms for c in group]
            ),
            "ideal_eps_mean": mean(
                [c["summary"].get("ideal_events_per_second", 0.0) for c in group]
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
        entry = aggregates[key]
        # Straggler dilution: one late worker stretches the closed-loop window
        # while the rest idle, deflating aggregate efficiency even though every
        # stream ran on schedule. Low efficiency with on-schedule streams and a
        # clean TTFC tail is that artifact, not a client knee.
        entry["dilution_suspect"] = bool(
            entry["events_per_second"] > 0
            and entry["efficiency_mean"] < 0.95
            and entry["stretch_p95_mean"] < 1.05
            and entry["ttfc_excess_p95_mean"] < 50.0
            and entry["failed"] == 0
            and entry["incomplete"] == 0
        )
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


LABEL_GAP = 14.0


def stack_labels(
    end_labels: list[tuple[str, float, float]], plot_top: float, plot_bottom: float
) -> list[str]:
    """Greedy vertical stacking of direct labels so converging series stay legible.
    end_labels = [(name, anchor_x, desired_y)]; returns SVG text fragments."""
    if not end_labels:
        return []
    end_labels = sorted(end_labels, key=lambda entry: entry[2])
    resolved: list[float] = []
    for _, _, desired_y in end_labels:
        prev_y = resolved[-1] if resolved else None
        y = desired_y if prev_y is None else max(desired_y, prev_y + LABEL_GAP)
        resolved.append(y)
    overflow = resolved[-1] - plot_bottom
    if overflow > 0:
        resolved = [max(y - overflow, plot_top) for y in resolved]
    return [
        f'<text x="{anchor_x + 10:.1f}" y="{y:.1f}" class="direct-label">{escape(name)}</text>'
        for (name, anchor_x, _), y in zip(end_labels, resolved)
    ]


def decade_ticks(y_min: float, y_max: float) -> list[float]:
    """Powers of 10 spanning [y_min, y_max], endpoints included."""
    ticks = [y_min]
    power = math.ceil(math.log10(y_min)) if y_min > 0 else 0
    while 10.0 ** power < y_max:
        value = 10.0 ** power
        if value > y_min:
            ticks.append(value)
        power += 1
    ticks.append(y_max)
    return ticks


def line_chart(
    title: str,
    y_label: str,
    concurrencies: list[int],
    series: list[dict[str, Any]],
    y_max: float,
    reference_y: float | None = None,
    value_digits: int = 2,
    y_log: bool = False,
    y_min: float = 1.0,
    reference_points: dict[int, float] | None = None,
    stop_marks: dict[str, tuple[int, str]] | None = None,
) -> str:
    """One SVG line chart. series = [{name, points: {concurrency: (mean, lo, hi)}}].
    y_log renders a log10 y-axis over [y_min, y_max] with decade gridlines.
    reference_points draws a per-concurrency dashed ideal line (non-horizontal).
    stop_marks = {client: (knee_concurrency, reason)} draws an ✕ where a stop
    rule pruned the client, so a line ending reads as "failed here", not "no data"."""
    width, height = 860, 360
    left, right, top, bottom = 64, 150, 48, 40
    plot_w, plot_h = width - left - right, height - top - bottom

    def x_for(concurrency: int) -> float:
        index = concurrencies.index(concurrency)
        if len(concurrencies) == 1:
            return left + plot_w / 2
        return left + index * plot_w / (len(concurrencies) - 1)

    def y_for(value: float) -> float:
        if y_log:
            clamped = min(max(value, y_min), y_max)
            span = math.log10(y_max) - math.log10(y_min)
            frac = (math.log10(clamped) - math.log10(y_min)) / span if span > 0 else 0.0
            return top + plot_h - frac * plot_h
        clamped = min(max(value, 0.0), y_max)
        return top + plot_h - (clamped / y_max) * plot_h if y_max > 0 else top + plot_h

    parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="{escape(title)}">',
        f'<text x="0" y="20" class="svg-title">{escape(title)}</text>',
        f'<text x="0" y="38" class="svg-note">{escape(y_label)} vs concurrency</text>',
    ]
    end_labels: list[tuple[str, float, float]] = []  # (name, anchor_x, desired_y)
    tick_values = (
        decade_ticks(y_min, y_max) if y_log else [y_max * i / 4 for i in range(5)]
    )
    for tick_value in tick_values:
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
    if reference_points:
        ref = [(c, reference_points[c]) for c in concurrencies if c in reference_points]
        if ref:
            coords = " ".join(f"{x_for(c):.1f},{y_for(v):.1f}" for c, v in ref)
            parts.append(f'<polyline points="{coords}" class="reference" fill="none"></polyline>')
            last_c, last_v = ref[-1]
            parts.append(
                f'<text x="{x_for(last_c) + 6:.1f}" y="{y_for(last_v) + 4:.1f}" class="svg-note">ideal</text>'
            )

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
        mark = (stop_marks or {}).get(name)
        if mark is not None and mark[0] in dict(points):
            knee_c, reason = mark
            value = dict(points)[knee_c][0]
            x, y = x_for(knee_c), y_for(value)
            parts.append(
                f'<g class="stopx series-{escape(name)}">'
                f'<line x1="{x - 6:.1f}" y1="{y - 6:.1f}" x2="{x + 6:.1f}" y2="{y + 6:.1f}"></line>'
                f'<line x1="{x - 6:.1f}" y1="{y + 6:.1f}" x2="{x + 6:.1f}" y2="{y - 6:.1f}"></line>'
                f'<title>{escape(name)} stopped at c={knee_c}: {escape(reason)}</title></g>'
            )

    parts.extend(stack_labels(end_labels, top + 10, top + plot_h + 10))
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
            else:
                value = {
                    "chunks_per_second": entry["chunks_per_second_mean"],
                    "ttfc_excess": entry["ttfc_excess_p95_mean"],
                    "max_gap_p99": entry["max_gap_p99_mean"],
                    "server_slip": entry["server_slip_p99_max"],
                    "server_cpu": entry["server_cpu_mean"],
                }[metric]
                points[concurrency] = (value, value, value)
        series.append({"name": client, "points": points})
    return series


def cpu_per_event_series(aggregates: dict, tier: str) -> list[dict[str, Any]]:
    """CPU%-per-1000-events/s for single-process clients. Multiprocess clients
    are excluded: the sampler only sees the parent process, so their CPU
    numbers are meaningless for cost-per-event."""
    series = []
    for client in clients_in(aggregates, tier):
        if process_count(client) > 1:
            continue
        points: dict[int, tuple[float, float, float]] = {}
        for concurrency in concurrencies_in(aggregates, tier):
            entry = aggregates.get((tier, client, concurrency))
            if entry is None:
                continue
            cpu = entry["client_cpu_mean"]
            eps = entry["chunks_per_second_mean"]
            if cpu > 0 and eps > 0:
                value = cpu / (eps / 1000.0)
                points[concurrency] = (value, value, value)
        if points:
            series.append({"name": client, "points": points})
    return series


def knee_heatmap(tier: str, aggregates: dict, tier_stops: dict[str, tuple[int, str]]) -> str:
    """Clients × concurrency grid, cells colored by efficiency band, the
    stop-rule cell ringed with an ✕, pruned rungs hatched out. The whole
    'where does each setup bottleneck' answer in one glance."""
    clients = clients_in(aggregates, tier)
    concurrencies = concurrencies_in(aggregates, tier)
    if not clients or not concurrencies:
        return ""
    width, left, pad = 860, 150, 10
    cell_w = (width - left - pad) / len(concurrencies)
    cell_h, grid_top = 30.0, 66.0
    height = int(grid_top + len(clients) * cell_h + 12)

    parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="Client knee map — {escape(tier)}">',
        f'<text x="0" y="20" class="svg-title">Client knee map — {escape(tier)}</text>',
        '<text x="0" y="38" class="svg-note">delivery efficiency per cell · '
        "✕ = stop rule fired there · — = pruned by that stop</text>",
    ]
    for col, concurrency in enumerate(concurrencies):
        x = left + col * cell_w + cell_w / 2
        parts.append(f'<text x="{x:.1f}" y="{grid_top - 8:.1f}" class="tick tick-x">{concurrency}</text>')
    for row, client in enumerate(clients):
        y = grid_top + row * cell_h
        parts.append(
            f'<text x="{left - 8}" y="{y + cell_h / 2 + 4:.1f}" class="tick tick-y">{escape(client)}</text>'
        )
        knee = tier_stops.get(client)
        for col, concurrency in enumerate(concurrencies):
            x = left + col * cell_w
            entry = aggregates.get((tier, client, concurrency))
            cx, cy = x + cell_w / 2, y + cell_h / 2 + 4
            if entry is not None:
                efficiency = entry["efficiency_mean"]
                band = "good" if efficiency >= 0.97 else "warn" if efficiency >= 0.90 else "bad"
                tooltip = (
                    f"{client} · c={concurrency} · efficiency {efficiency:.3f} · "
                    f"p95 TTFC excess {entry['ttfc_excess_p95_mean']:.1f}ms · "
                    f"failed {entry['failed']}"
                )
                is_knee = knee is not None and knee[0] == concurrency
                if is_knee:
                    tooltip += f" · STOPPED: {knee[1]}"
                parts.append(
                    f'<rect x="{x:.1f}" y="{y:.1f}" width="{cell_w:.1f}" height="{cell_h:.1f}" '
                    f'class="heat-cell heat-{band}"><title>{escape(tooltip)}</title></rect>'
                )
                value_text = f"{efficiency:.2f}" + (" ✕" if is_knee else "")
                parts.append(f'<text x="{cx:.1f}" y="{cy:.1f}" class="heat-value">{escape(value_text)}</text>')
                if is_knee:
                    parts.append(
                        f'<rect x="{x + 1:.1f}" y="{y + 1:.1f}" width="{cell_w - 2:.1f}" '
                        f'height="{cell_h - 2:.1f}" class="heat-knee"></rect>'
                    )
            elif knee is not None and concurrency > knee[0]:
                parts.append(
                    f'<rect x="{x:.1f}" y="{y:.1f}" width="{cell_w:.1f}" height="{cell_h:.1f}" '
                    f'class="heat-cell heat-pruned"><title>{escape(client)} · c={concurrency} · '
                    f"pruned by stop at c={knee[0]}</title></rect>"
                )
                parts.append(f'<text x="{cx:.1f}" y="{cy:.1f}" class="heat-value heat-muted">—</text>')
    parts.append("</svg>")
    return f'<article class="chart-card">{"".join(parts)}</article>'


def xy_chart(
    title: str,
    note: str,
    series: list[dict[str, Any]],
    *,
    x_min: float,
    x_max: float,
    x_ticks: list[float],
    y_max: float,
    y_floor: float = 0.0,
    x_log: bool = True,
    guide_x: float | None = None,
    guide_y: float | None = None,
    draw_lines: bool = False,
    y_tick_digits: int = 2,
    annotations: tuple[tuple[str, str], ...] = (),
) -> str:
    """Generic scatter/line chart over arbitrary x values (optionally log-scale).
    series = [{name, points: [(x, y, tooltip, radius)]}]. annotations are
    ("tl"|"br", text) corner notes explaining what each region means."""
    width, height = 860, 360
    left, right, top, bottom = 64, 150, 48, 40
    plot_w, plot_h = width - left - right, height - top - bottom

    def x_for(x: float) -> float:
        clamped = min(max(x, x_min), x_max)
        if x_log:
            span = math.log10(x_max) - math.log10(x_min)
            frac = (math.log10(clamped) - math.log10(x_min)) / span if span > 0 else 0.5
        else:
            frac = (clamped - x_min) / (x_max - x_min) if x_max > x_min else 0.5
        return left + frac * plot_w

    def y_for(value: float) -> float:
        clamped = min(max(value, y_floor), y_max)
        span = y_max - y_floor
        frac = (clamped - y_floor) / span if span > 0 else 0.0
        return top + plot_h - frac * plot_h

    parts = [
        f'<svg viewBox="0 0 {width} {height}" role="img" aria-label="{escape(title)}">',
        f'<text x="0" y="20" class="svg-title">{escape(title)}</text>',
        f'<text x="0" y="38" class="svg-note">{escape(note)}</text>',
    ]
    for tick_index in range(5):
        tick_value = y_floor + (y_max - y_floor) * tick_index / 4
        y = y_for(tick_value)
        parts.append(f'<line x1="{left}" y1="{y:.1f}" x2="{left + plot_w}" y2="{y:.1f}" class="grid"></line>')
        parts.append(
            f'<text x="{left - 8}" y="{y + 4:.1f}" class="tick tick-y">{format_number(tick_value, y_tick_digits)}</text>'
        )
    for tick in x_ticks:
        x = x_for(tick)
        parts.append(
            f'<text x="{x:.1f}" y="{height - 16}" class="tick tick-x">{format_number(tick, 0)}</text>'
        )
    if guide_x is not None and x_min <= guide_x <= x_max:
        x = x_for(guide_x)
        parts.append(
            f'<line x1="{x:.1f}" y1="{top}" x2="{x:.1f}" y2="{top + plot_h}" class="reference"></line>'
        )
    if guide_y is not None and y_floor <= guide_y <= y_max:
        y = y_for(guide_y)
        parts.append(
            f'<line x1="{left}" y1="{y:.1f}" x2="{left + plot_w}" y2="{y:.1f}" class="reference"></line>'
        )
    for corner, text in annotations:
        if corner == "tl":
            parts.append(f'<text x="{left + 8}" y="{top + 16}" class="svg-note">{escape(text)}</text>')
        else:  # "br"
            parts.append(
                f'<text x="{left + plot_w - 8}" y="{top + plot_h - 8}" class="svg-note" '
                f'text-anchor="end">{escape(text)}</text>'
            )

    end_labels: list[tuple[str, float, float]] = []
    for entry in series:
        name = entry["name"]
        points = sorted(entry["points"], key=lambda p: p[0])
        if not points:
            continue
        style = CLIENT_STYLE.get(name, {"dash": ""})
        dash = f' stroke-dasharray="{style["dash"]}"' if style.get("dash") else ""
        if draw_lines:
            coords = " ".join(f"{x_for(px):.1f},{y_for(py):.1f}" for px, py, _, _ in points)
            parts.append(f'<polyline points="{coords}" class="line series-{escape(name)}"{dash}></polyline>')
        for px, py, tooltip, radius in points:
            parts.append(
                f'<circle cx="{x_for(px):.1f}" cy="{y_for(py):.1f}" r="{radius:.1f}" '
                f'class="marker series-{escape(name)}"><title>{escape(tooltip)}</title></circle>'
            )
        if draw_lines:
            last = points[-1]
            end_labels.append((name, x_for(last[0]), y_for(last[1]) + 4))
    parts.extend(stack_labels(end_labels, top + 10, top + plot_h + 10))
    parts.append("</svg>")
    return f'<article class="chart-card">{"".join(parts)}</article>'


def failure_mode_scatter(tier: str, aggregates: dict) -> str:
    """p95 TTFC excess (x, log) vs p95 stream stretch (y). Separates the two
    failure modes the line charts conflate: admission queueing (right, flat)
    vs mid-stream starvation (up)."""
    concurrencies = concurrencies_in(aggregates, tier)
    c_max = max(concurrencies) if concurrencies else 1
    series = []
    x_values: list[float] = []
    y_values: list[float] = []
    for client in clients_in(aggregates, tier):
        points = []
        for concurrency in concurrencies:
            entry = aggregates.get((tier, client, concurrency))
            if entry is None:
                continue
            x = max(entry["ttfc_excess_p95_mean"], 0.5)
            y = entry["stretch_p95_mean"]
            radius = 3.0 + 5.0 * math.sqrt(concurrency / c_max)
            tooltip = (
                f"{client} · c={concurrency} · TTFC excess {x:.1f}ms · stretch {y:.3f}"
            )
            points.append((x, y, tooltip, radius))
            x_values.append(x)
            y_values.append(y)
        if points:
            series.append({"name": client, "points": points})
    if not series:
        return ""
    x_max = max(max(x_values) * 1.5, 200.0)
    y_max = max(max(y_values) * 1.05, 1.1)
    return xy_chart(
        f"Failure modes — {tier}",
        "p95 TTFC excess (ms, log) vs p95 stream stretch · marker size = concurrency",
        series,
        x_min=0.5, x_max=x_max, x_ticks=decade_ticks(1.0, x_max),
        y_max=y_max, y_floor=1.0, x_log=True,
        guide_x=100.0, guide_y=1.05, y_tick_digits=2,
        annotations=(
            ("tl", "↑ mid-stream starvation (streams stretched)"),
            ("br", "admission queueing (late first chunk, streams on pace) →"),
        ),
    )


def collapse_curve(tier: str, aggregates: dict) -> str:
    """p95 TTFC excess vs streams-per-process for the python variants. If
    single-process and -mp points land on one curve per stack, per-process
    load is the invariant and worker count just rescales it — the sizing law."""
    series = []
    x_values: list[float] = []
    y_values: list[float] = []
    for client in clients_in(aggregates, tier):
        if not is_python_client(client):
            continue
        procs = process_count(client)
        points = []
        for concurrency in concurrencies_in(aggregates, tier):
            entry = aggregates.get((tier, client, concurrency))
            if entry is None:
                continue
            x = concurrency / procs
            y = entry["ttfc_excess_p95_mean"]
            tooltip = (
                f"{client} · c={concurrency} ({procs} proc) · "
                f"{x:.0f} streams/proc · {y:.1f}ms"
            )
            points.append((x, y, tooltip, 4.0))
            x_values.append(x)
            y_values.append(y)
        if points:
            series.append({"name": client, "points": points})
    if not series:
        return ""
    x_min = max(min(x_values) / 1.3, 1.0)
    x_max = max(x_values) * 1.3
    ticks: list[float] = []
    power = math.floor(math.log2(x_min)) if x_min > 0 else 0
    while 2.0 ** power <= x_max:
        if 2.0 ** power >= x_min:
            ticks.append(2.0 ** power)
        power += 1
    return xy_chart(
        f"TTFC excess vs per-process load — {tier}",
        "p95 TTFC excess (ms) vs streams per worker process (log2) · python variants",
        series,
        x_min=x_min, x_max=x_max, x_ticks=ticks,
        y_max=max(max(y_values) * 1.15, 1.0), y_floor=0.0,
        x_log=True, guide_y=None, draw_lines=True, y_tick_digits=1,
    )


def ttfc_band_grid(tier: str, aggregates: dict) -> str:
    """Small-multiples: one mini chart per client, p50–p99 TTFC-excess band
    with the p95 line. Tail widening (p50 fine, p99 exploding) is the early
    signature of queueing, invisible in a single-percentile line."""
    clients = clients_in(aggregates, tier)
    concurrencies = concurrencies_in(aggregates, tier)
    if not concurrencies:
        return ""
    y_max = 1.0
    for client in clients:
        for concurrency in concurrencies:
            entry = aggregates.get((tier, client, concurrency))
            if entry is not None:
                y_max = max(y_max, entry["ttfc_excess_p99_mean"])
    y_max *= 1.1

    width, height = 420, 200
    left, right, top, bottom = 52, 14, 34, 26
    plot_w, plot_h = width - left - right, height - top - bottom

    def x_for(concurrency: int) -> float:
        index = concurrencies.index(concurrency)
        if len(concurrencies) == 1:
            return left + plot_w / 2
        return left + index * plot_w / (len(concurrencies) - 1)

    def y_for(value: float) -> float:
        clamped = min(max(value, 0.0), y_max)
        return top + plot_h - (clamped / y_max) * plot_h

    cards = []
    for client in clients:
        rows = [
            (concurrency, aggregates[(tier, client, concurrency)])
            for concurrency in concurrencies
            if (tier, client, concurrency) in aggregates
        ]
        if not rows:
            continue
        parts = [
            f'<svg viewBox="0 0 {width} {height}" role="img" '
            f'aria-label="TTFC excess percentiles — {escape(client)}">',
            f'<text x="0" y="16" class="svg-title" style="font-size:13px">{escape(client)}</text>',
        ]
        for tick_index in range(3):
            tick_value = y_max * tick_index / 2
            y = y_for(tick_value)
            parts.append(
                f'<line x1="{left}" y1="{y:.1f}" x2="{left + plot_w}" y2="{y:.1f}" class="grid"></line>'
            )
            parts.append(
                f'<text x="{left - 6}" y="{y + 4:.1f}" class="tick tick-y">{format_number(tick_value, 0)}</text>'
            )
        for concurrency in concurrencies:
            parts.append(
                f'<text x="{x_for(concurrency):.1f}" y="{height - 8}" class="tick tick-x">{concurrency}</text>'
            )
        upper = " ".join(f"{x_for(c):.1f},{y_for(e['ttfc_excess_p99_mean']):.1f}" for c, e in rows)
        lower = " ".join(
            f"{x_for(c):.1f},{y_for(e['ttfc_excess_p50_mean']):.1f}" for c, e in reversed(rows)
        )
        parts.append(f'<polygon points="{upper} {lower}" class="band fill-{escape(client)}"></polygon>')
        p95 = " ".join(f"{x_for(c):.1f},{y_for(e['ttfc_excess_p95_mean']):.1f}" for c, e in rows)
        parts.append(f'<polyline points="{p95}" class="line series-{escape(client)}"></polyline>')
        for concurrency, entry in rows:
            parts.append(
                f'<circle cx="{x_for(concurrency):.1f}" cy="{y_for(entry["ttfc_excess_p95_mean"]):.1f}" r="3" '
                f'class="marker series-{escape(client)}"><title>{escape(client)} · c={concurrency} · '
                f"p50 {entry['ttfc_excess_p50_mean']:.1f} / p95 {entry['ttfc_excess_p95_mean']:.1f} / "
                f"p99 {entry['ttfc_excess_p99_mean']:.1f} ms</title></circle>"
            )
        parts.append("</svg>")
        cards.append(f'<article class="chart-card">{"".join(parts)}</article>')
    if not cards:
        return ""
    return (
        "<h3>TTFC excess percentiles per client (p50–p99 band, p95 line, shared y-axis)</h3>"
        f'<div class="chart-grid two-col">{"".join(cards)}</div>'
    )


def render_tier_section(
    aggregates: dict, tier: str, tier_stops: dict[str, tuple[int, str]] | None = None
) -> str:
    tier_stops = tier_stops or {}
    concurrencies = concurrencies_in(aggregates, tier)
    sample = next(v for (t, _, _), v in aggregates.items() if t == tier)
    paced = sample["events_per_second"] > 0
    tier_entries = [entry for (t, _, _), entry in aggregates.items() if t == tier]

    heatmap = ""
    diagnostics = ""
    bands = ""
    charts = []
    if paced:
        heatmap = knee_heatmap(tier, aggregates, tier_stops)
        charts.append(line_chart(
            f"Delivery efficiency — {tier}", "observed / ideal events per second",
            concurrencies, tier_series(aggregates, tier, "efficiency"),
            y_max=1.1, reference_y=1.0, stop_marks=tier_stops,
        ))
        throughput = [e["chunks_per_second_mean"] for e in tier_entries]
        ideal_points = {
            concurrency: max(
                (
                    aggregates[(tier, client, concurrency)]["ideal_eps_mean"]
                    for client in clients_in(aggregates, tier)
                    if (tier, client, concurrency) in aggregates
                ),
                default=0.0,
            )
            for concurrency in concurrencies
        }
        charts.append(line_chart(
            f"Absolute throughput — {tier}", "observed events per second (dashed = ideal)",
            concurrencies, tier_series(aggregates, tier, "chunks_per_second"),
            y_max=max(max(throughput, default=0.0), max(ideal_points.values(), default=0.0)) * 1.1 or 1.0,
            value_digits=0, reference_points=ideal_points, stop_marks=tier_stops,
        ))
        ttfc_values = [e["ttfc_excess_p95_mean"] for e in tier_entries]
        ttfc_max = max(max(ttfc_values, default=0.0) * 1.15, 1.0)
        charts.append(line_chart(
            f"p95 TTFC excess — {tier}", "p95 time-to-first-chunk minus configured TTFC (ms)",
            concurrencies, tier_series(aggregates, tier, "ttfc_excess"),
            y_max=ttfc_max, value_digits=1, stop_marks=tier_stops,
        ))
        gap_values = [e["max_gap_p99_mean"] for e in tier_entries if e["max_gap_p99_mean"] > 0]
        if gap_values:
            charts.append(line_chart(
                f"p99 max inter-chunk gap — {tier}",
                "worst per-request stall, ms (log scale) — the leading indicator of decode backlog",
                concurrencies, tier_series(aggregates, tier, "max_gap_p99"),
                y_max=max(max(gap_values) * 1.3, 10.0), value_digits=0,
                y_log=True, y_min=1.0, stop_marks=tier_stops,
            ))
        cpu_series = cpu_per_event_series(aggregates, tier)
        if cpu_series:
            cpu_max = max(v[0] for s in cpu_series for v in s["points"].values())
            charts.append(line_chart(
                f"CPU cost per delivered throughput — {tier}",
                "client CPU % per 1,000 events/s · single-process clients only "
                "(mp CPU sampling covers the parent process only)",
                concurrencies, cpu_series,
                y_max=max(cpu_max * 1.15, 1.0), value_digits=1,
            ))
        diagnostic_cards = failure_mode_scatter(tier, aggregates) + collapse_curve(tier, aggregates)
        if diagnostic_cards:
            diagnostics = f'<div class="chart-grid two-col">{diagnostic_cards}</div>'
        bands = ttfc_band_grid(tier, aggregates)
        health_cards = ""
        slip_values = [e["server_slip_p99_max"] for e in tier_entries]
        if any(v > 0 for v in slip_values):
            health_cards += line_chart(
                f"Server schedule slip p99 — {tier}",
                "server's own lateness vs its timetable (ms) — flat = server never the limit",
                concurrencies, tier_series(aggregates, tier, "server_slip"),
                y_max=max(max(slip_values) * 1.3, 1.0), value_digits=1,
            )
        server_cpu_values = [e["server_cpu_mean"] for e in tier_entries]
        if any(v > 0 for v in server_cpu_values):
            health_cards += line_chart(
                f"Server CPU — {tier}", "server process CPU %",
                concurrencies, tier_series(aggregates, tier, "server_cpu"),
                y_max=max(max(server_cpu_values) * 1.15, 1.0), value_digits=0,
            )
        if health_cards:
            diagnostics += f'<div class="chart-grid two-col">{health_cards}</div>'
    else:
        throughput = [e["chunks_per_second_mean"] for e in tier_entries]
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
                f'<td>{"<span class=\"flag\">window dilution</span>" if entry["dilution_suspect"] else ""}</td>'
                "</tr>"
            )
    headers = (
        "Client", "Concurrency", "Efficiency", "Events/s", "p95 TTFC excess ms",
        "p95 stretch", "p99 max gap ms", "Failed", "Incomplete",
        "Server slip p99 ms", "Client CPU", "Server CPU", "Artifact?",
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
        f"{heatmap}"
        f'<div class="chart-grid">{"".join(charts)}</div>'
        f"{diagnostics}{bands}{table}</section>"
    )


def render_stops(sweep_meta: dict[str, Any], aggregates: dict) -> str:
    stops = sweep_meta.get("stops", {})
    if not stops:
        return "<p>No stop rules triggered.</p>"
    rows = []
    for key, info in sorted(stops.items()):
        tier, _, client = key.partition(":")
        concurrency = info.get("concurrency", "")
        reason = escape(info.get("reason", ""))
        entry = aggregates.get((tier, client, concurrency))
        if entry and entry["dilution_suspect"]:
            reason += (
                ' <span class="flag">likely window-dilution artifact — streams ran '
                "on schedule (p95 stretch &lt; 1.05); treat this knee as suspect</span>"
            )
        rows.append(
            f"<tr><th>{escape(tier)}</th><td>{escape(client)}</td>"
            f"<td>{concurrency}</td><td>{reason}</td></tr>"
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
        f".fill-{name} {{ fill: var(--series-{name}); }}"
        for name in CLIENT_STYLE
    )
    return f"""
:root {{
  color-scheme: light dark;
  --surface-1: #fcfcfb; --page: #f9f9f7; --ink: #0b0b0b; --ink-2: #52514e;
  --muted: #898781; --grid: #e1e0d9; --border: rgba(11,11,11,0.10);
  --heat-good: #c9e5cb; --heat-warn: #f2e2ae; --heat-bad: #f2c7c2;
  --heat-pruned: #ecebe4;
  {light}
}}
@media (prefers-color-scheme: dark) {{
  :root {{
    --surface-1: #1a1a19; --page: #0d0d0d; --ink: #ffffff; --ink-2: #c3c2b7;
    --grid: #2c2c2a; --border: rgba(255,255,255,0.10);
    --heat-good: #1e4d2b; --heat-warn: #55431a; --heat-bad: #57221f;
    --heat-pruned: #232322;
    {dark}
  }}
}}
* {{ box-sizing: border-box; }}
body {{ margin: 0; background: var(--page); color: var(--ink);
  font-family: system-ui, -apple-system, "Segoe UI", sans-serif; }}
main {{ width: min(1180px, calc(100vw - 40px)); margin: 0 auto; padding: 32px 0 56px; }}
h1 {{ font-size: 30px; margin: 6px 0 4px; }}
h2 {{ font-size: 20px; margin: 0 0 12px; }}
h3 {{ font-size: 15px; margin: 16px 0 10px; color: var(--ink-2); }}
p {{ color: var(--ink-2); line-height: 1.55; }}
section {{ margin-top: 22px; padding: 20px; border: 1px solid var(--border);
  border-radius: 8px; background: var(--surface-1); }}
.eyebrow {{ color: var(--muted); font-size: 13px; font-weight: 700;
  text-transform: uppercase; letter-spacing: .04em; }}
.chart-grid {{ display: grid; grid-template-columns: 1fr; gap: 16px; margin-bottom: 16px; }}
.chart-grid.two-col {{ grid-template-columns: repeat(auto-fit, minmax(380px, 1fr)); }}
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
.stopx line {{ stroke-width: 2.5; }}
.band {{ opacity: .16; stroke: none; }}
.heat-cell {{ stroke: var(--surface-1); stroke-width: 2; }}
.heat-good {{ fill: var(--heat-good); }}
.heat-warn {{ fill: var(--heat-warn); }}
.heat-bad {{ fill: var(--heat-bad); }}
.heat-pruned {{ fill: var(--heat-pruned); }}
.heat-knee {{ fill: none; stroke: var(--ink); stroke-width: 1.5; stroke-dasharray: 3 2; }}
.heat-value {{ font: 11px system-ui, sans-serif; fill: var(--ink); text-anchor: middle;
  font-variant-numeric: tabular-nums; pointer-events: none; }}
.heat-muted {{ fill: var(--muted); }}
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
.scope {{ color: var(--ink); font-weight: 600; }}
.flag {{ color: #b45309; font-weight: 600; white-space: normal; }}
{series_rules}
"""


def normalize_run_dirs(run_dirs: Path | list[Path]) -> list[Path]:
    return [run_dirs] if isinstance(run_dirs, Path) else list(run_dirs)


def load_merged(run_dirs: list[Path]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Merge cells and sweep metadata from one or more run directories.
    Stops are unioned; the config is kept only when every run agrees, so the
    scope line never claims a duration that only some cells used."""
    cells: list[dict[str, Any]] = []
    stops: dict[str, Any] = {}
    configs: list[dict[str, Any]] = []
    for run_dir in run_dirs:
        cells.extend(load_cells(run_dir))
        meta = load_json_if_exists(run_dir / "sweep.json")
        stops.update(meta.get("stops", {}))
        if meta.get("config"):
            configs.append(meta["config"])
    merged: dict[str, Any] = {"stops": stops}
    if configs and all(c == configs[0] for c in configs):
        merged["config"] = configs[0]
    return cells, merged


def describe_scope(cells: list[dict[str, Any]], sweep_meta: dict[str, Any]) -> str:
    """One header line saying how big this run actually was, so a smoke run
    can never be mistaken for a full sweep."""
    config = sweep_meta.get("config") or {}
    tiers = sorted({cell["tier"] for cell in cells})
    concurrencies = sorted({cell["concurrency"] for cell in cells})
    # Max multiplicity across (tier, client, concurrency) groups, not
    # 1 + max(repeat): merged single-repeat runs (each cell's repeat index is
    # always 0) would otherwise report "1 repeat" even when several runs were
    # merged together, hiding the true per-cell sample count.
    group_counts: dict[tuple[str, str, int], int] = {}
    for cell in cells:
        key = (cell["tier"], cell["client"], cell["concurrency"])
        group_counts[key] = group_counts.get(key, 0) + 1
    repeats = max(group_counts.values(), default=0)
    clients = sorted({cell["client"] for cell in cells})
    duration = config.get("duration_seconds")
    duration_text = f" · {format_number(duration, 0)}s measured windows" if duration else ""
    scope = (
        f"Run scope: {len(tiers)} tier(s) [{', '.join(tiers)}] · "
        f"concurrency {', '.join(str(c) for c in concurrencies)} · "
        f"{repeats} repeat(s) · {len(clients)} clients · "
        f"{len(cells)} cell-runs{duration_text}."
    )
    planned = config.get("concurrencies")
    if planned and len(concurrencies) < len(planned):
        missing = [c for c in planned if c not in concurrencies]
        scope += (
            f" Planned rungs missing from this run: "
            f"{', '.join(str(c) for c in missing)}."
        )
    return f'<p class="scope">{escape(scope)}</p>'


def render_report(
    run_dirs: Path | list[Path], cells: list[dict[str, Any]], sweep_meta: dict[str, Any]
) -> str:
    if not cells:
        raise ValueError("Cannot render sweep report without cells")
    run_label = " + ".join(str(d) for d in normalize_run_dirs(run_dirs))
    aggregates = aggregate_cells(cells)
    tiers = sorted({tier for (tier, _, _) in aggregates})
    # Paced tiers ordered by rate, then the unpaced tier(s) last.
    def tier_key(tier: str) -> tuple[int, int, str]:
        sample = next(v for (t, _, _), v in aggregates.items() if t == tier)
        eps = sample["events_per_second"]
        return (1, 0, tier) if eps == 0 else (0, eps, tier)
    tiers.sort(key=tier_key)

    all_stops = stops_by_tier(sweep_meta)
    sections = "".join(
        render_tier_section(aggregates, tier, all_stops.get(tier, {})) for tier in tiers
    )
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
  <p>Run: {escape(run_label)} · Generated: {escape(generated_at)}</p>
  {describe_scope(cells, sweep_meta)}
  <p>Efficiency = observed parsed events/sec ÷ the achievable closed-loop ideal
  (concurrency × chunks ÷ (TTFC + (chunks−1)/rate)).
  The drain client is a parse-free reference: any gap it shows is server/OS,
  any gap below it is client overhead.</p>
</header>
<section><h2>Stop rules triggered (knees)</h2>{render_stops(sweep_meta, aggregates)}</section>
{sections}
</main>
</body>
</html>
"""


def write_report(run_dirs: Path | list[Path], output: Path) -> Path:
    dirs = normalize_run_dirs(run_dirs)
    cells, sweep_meta = load_merged(dirs)
    if not cells:
        raise FileNotFoundError(
            f"No cell summary.json files found under {', '.join(str(d) for d in dirs)}"
        )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(render_report(dirs, cells, sweep_meta))
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description="Generate a static HTML sweep report.")
    parser.add_argument("results_dirs", nargs="*", default=None,
                        help="One or more sweep run directories (cells are merged). "
                             "Defaults to newest under results/.")
    parser.add_argument("--output", default=str(ROOT / "reports/sweep/index.html"), help="Output HTML path.")
    args = parser.parse_args()

    if args.results_dirs:
        dirs = [Path(d) for d in args.results_dirs]
    else:
        dirs = [find_latest_results_dir(ROOT / "results")]
    output = write_report(dirs, Path(args.output))
    print(output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
