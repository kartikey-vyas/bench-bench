from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def load_summaries(root: Path) -> list[dict[str, Any]]:
    summaries: list[dict[str, Any]] = []
    for path in sorted(root.glob("**/summary.json")):
        data = json.loads(path.read_text())
        data["_path"] = str(path)
        summaries.append(data)
    return summaries


def format_number(value: Any) -> str:
    if isinstance(value, int):
        return str(value)
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def print_table(summaries: list[dict[str, Any]]) -> None:
    rows = []
    for item in summaries:
        summary = item["summary"]
        rows.append(
            [
                item["language"],
                item.get("implementation", ""),
                format_number(summary["requests_per_second"]),
                format_number(summary["chunks_per_second"]),
                format_number(summary["p95_request_latency_ms"]),
                format_number(summary["failed_requests"]),
                item["_path"],
            ]
        )

    headers = ["language", "implementation", "req/s", "chunks/s", "p95 req ms", "failures", "file"]
    widths = [len(header) for header in headers]
    for row in rows:
        widths = [max(width, len(cell)) for width, cell in zip(widths, row)]

    print("  ".join(header.ljust(width) for header, width in zip(headers, widths)))
    print("  ".join("-" * width for width in widths))
    for row in rows:
        print("  ".join(cell.ljust(width) for cell, width in zip(row, widths)))


def main() -> int:
    parser = argparse.ArgumentParser(description="Print a comparison table from benchmark summaries.")
    parser.add_argument("results_dir", nargs="?", default="results", help="Directory containing summary.json files.")
    args = parser.parse_args()

    root = Path(args.results_dir)
    summaries = load_summaries(root)
    if not summaries:
        print(f"No summary.json files found under {root}")
        return 1

    print_table(summaries)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
