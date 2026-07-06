import contextlib
import io
import json
import re
import tempfile
import unittest
from pathlib import Path

from bench_harness.sweep_report import (
    aggregate_cells,
    decade_ticks,
    load_cells,
    process_count,
    render_report,
    write_report,
)


def write_cell(root: Path, tier: str, concurrency: int, repeat: int, client: str,
               efficiency: float, eps: int = 500, ttfc: int = 200) -> None:
    cell = root / tier / f"c{concurrency}" / f"r{repeat}" / client
    cell.mkdir(parents=True, exist_ok=True)
    (cell / "summary.json").write_text(json.dumps({
        "language": "x", "implementation": client, "started_at": "2026-07-03T00:00:00Z",
        "config": {
            "base_url": "http://127.0.0.1:8080", "duration_seconds": 10.0,
            "warmup_seconds": 2.0, "concurrency": concurrency,
            "chunks_per_response": 512, "chunk_bytes": 8,
            "ttfc_ms": ttfc, "events_per_second": eps, "output_dir": "results",
        },
        "summary": {
            "duration_ms": 10000.0, "successful_requests": 100, "incomplete_requests": 0,
            "failed_requests": 0, "total_chunks": 51200, "total_bytes": 409600,
            "requests_per_second": 10.0, "chunks_per_second": eps * concurrency * efficiency,
            "mean_request_latency_ms": 1200.0, "p50_request_latency_ms": 1200.0,
            "p95_request_latency_ms": 1300.0, "p99_request_latency_ms": 1400.0,
            "mean_time_to_first_chunk_ms": 205.0, "p50_time_to_first_chunk_ms": 204.0,
            "p95_time_to_first_chunk_ms": 210.0, "p99_time_to_first_chunk_ms": 220.0,
            "p50_max_gap_ms": 3.0, "p95_max_gap_ms": 5.0, "p99_max_gap_ms": 8.0,
            "max_max_gap_ms": 9.0, "p50_stream_stretch": 1.01, "p95_stream_stretch": 1.02,
            "p99_stream_stretch": 1.05, "ideal_events_per_second": float(eps * concurrency),
            "efficiency": efficiency,
        },
    }))
    (cell / "server_stats.json").write_text(json.dumps({
        "requests_started": 100, "requests_completed": 100, "events_emitted": 51200,
        "slip_p50_ms": 0.1, "slip_p95_ms": 0.5, "slip_p99_ms": 1.0, "slip_max_ms": 2.0,
    }))
    (cell / "cpu.json").write_text(json.dumps({
        "server": {"mean_percent": 40.0, "max_percent": 60.0, "samples": 10},
        "client": {"mean_percent": 80.0, "max_percent": 95.0, "samples": 10},
    }))


class SweepReportTests(unittest.TestCase):
    def test_load_cells_parses_tree_coordinates(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_cell(root, "eps500", 4, 0, "python", 0.99)
            write_cell(root, "eps500", 4, 1, "python", 0.97)
            write_cell(root, "eps500", 16, 0, "go", 1.0)

            cells = load_cells(root)

        self.assertEqual(len(cells), 3)
        first = min(cells, key=lambda c: (c["concurrency"], c["repeat"]))
        self.assertEqual(first["tier"], "eps500")
        self.assertEqual(first["concurrency"], 4)
        self.assertEqual(first["client"], "python")
        self.assertIn("slip_p99_ms", first["server_stats"])

    def test_aggregate_cells_averages_repeats(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_cell(root, "eps500", 4, 0, "python", 0.99)
            write_cell(root, "eps500", 4, 1, "python", 0.97)

            groups = aggregate_cells(load_cells(root))

        entry = groups[("eps500", "python", 4)]
        self.assertAlmostEqual(entry["efficiency_mean"], 0.98)
        self.assertAlmostEqual(entry["efficiency_min"], 0.97)
        self.assertAlmostEqual(entry["efficiency_max"], 0.99)
        self.assertEqual(entry["repeats"], 2)

    def test_render_and_write_report(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "run"
            write_cell(root, "eps500", 4, 0, "python", 0.99)
            write_cell(root, "eps500", 4, 0, "drain", 1.0)
            write_cell(root, "max", 4, 0, "go", 0.0, eps=0, ttfc=0)
            (root / "sweep.json").write_text(json.dumps({
                "config": {"duration_seconds": 10.0, "concurrencies": [1, 4, 16]},
                "stops": {"eps500:python": {"concurrency": 4, "reason": "efficiency 0.5 below 0.9"}},
            }))

            output = Path(tmpdir) / "report" / "index.html"
            written = write_report(root, output)
            html = written.read_text()

        self.assertIn("<svg", html)
        self.assertIn("Run scope", html)
        self.assertIn("Planned rungs missing from this run: 1, 16", html)
        self.assertIn("python", html)
        self.assertIn("drain", html)
        self.assertIn("Delivery efficiency", html)
        self.assertIn("Observed events/sec", html)   # max tier fallback chart
        self.assertIn("efficiency 0.5 below 0.9", html)  # knee table
        self.assertIn("<table", html)                # relief rule: table view

    def test_dilution_suspect_flags_on_schedule_low_efficiency(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            # Low efficiency but perfect streams (fixture stretch 1.02, TTFC excess 10ms)
            write_cell(root, "eps500", 256, 0, "drain", 0.80)
            write_cell(root, "eps500", 4, 0, "go", 0.99)

            groups = aggregate_cells(load_cells(root))
            html = render_report(root, load_cells(root), {})

        self.assertTrue(groups[("eps500", "drain", 256)]["dilution_suspect"])
        self.assertFalse(groups[("eps500", "go", 4)]["dilution_suspect"])
        self.assertIn("window dilution", html)

    def test_write_report_merges_multiple_run_dirs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            run_a = Path(tmpdir) / "run-a"
            run_b = Path(tmpdir) / "run-b"
            write_cell(run_a, "eps500", 4, 0, "go", 0.99)
            (run_a / "sweep.json").write_text(json.dumps({
                "stops": {"eps500:go": {"concurrency": 4, "reason": "reason-a"}},
            }))
            write_cell(run_b, "eps500", 16, 0, "python", 0.95)
            (run_b / "sweep.json").write_text(json.dumps({
                "stops": {"eps500:python": {"concurrency": 16, "reason": "reason-b"}},
            }))

            output = Path(tmpdir) / "report" / "index.html"
            html = write_report([run_a, run_b], output).read_text()

        self.assertIn("run-a + ", html)          # both dirs named in the header
        self.assertIn("reason-a", html)          # stops unioned
        self.assertIn("reason-b", html)
        self.assertIn(">16<", html)              # cells from both runs present

    def test_aggregate_cells_warns_on_disagreeing_configs(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_cell(root, "eps500", 4, 0, "python", 0.99, ttfc=200)
            write_cell(root, "eps500", 4, 1, "python", 0.97, ttfc=300)

            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                groups = aggregate_cells(load_cells(root))

        # Still aggregates rather than crashing.
        self.assertIn(("eps500", "python", 4), groups)
        warning = stderr.getvalue()
        self.assertIn("ttfc_ms", warning)
        self.assertIn("eps500", warning)
        self.assertIn("python", warning)

    def test_aggregate_cells_silent_when_configs_agree(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_cell(root, "eps500", 4, 0, "python", 0.99)
            write_cell(root, "eps500", 4, 1, "python", 0.97)

            stderr = io.StringIO()
            with contextlib.redirect_stderr(stderr):
                aggregate_cells(load_cells(root))

        self.assertEqual(stderr.getvalue(), "")

    def test_direct_labels_do_not_collide(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "run"
            for client in ("python", "go", "rust-reqwest", "rust-hyper"):
                write_cell(root, "eps500", 4, 0, client, 0.99)

            html = render_report(root, load_cells(root), {})

        svg_fragments = html.split("<svg")[1:]
        self.assertTrue(svg_fragments)
        # Heatmaps, scatters, and band multiples legitimately carry no direct
        # labels; the collision rule applies to every chart that has them, and
        # at least one chart in the report must.
        labeled_fragments = 0
        for fragment in svg_fragments:
            y_values = [
                float(y)
                for y in re.findall(r'<text x="[^"]+" y="([0-9.]+)" class="direct-label"', fragment)
            ]
            if not y_values:
                continue
            labeled_fragments += 1
            y_values.sort()
            for prev, curr in zip(y_values, y_values[1:]):
                self.assertGreaterEqual(curr - prev, 13.5)
        self.assertGreaterEqual(labeled_fragments, 1)


class NewChartTests(unittest.TestCase):
    def test_process_count_from_registry(self):
        self.assertEqual(process_count("python-openai-mp"), 12)
        self.assertEqual(process_count("python-deferred-mp"), 12)
        self.assertEqual(process_count("python-openai"), 1)
        self.assertEqual(process_count("go"), 1)
        self.assertEqual(process_count("unknown-legacy-client"), 1)

    def test_decade_ticks_span_range(self):
        self.assertEqual(decade_ticks(1.0, 3000.0), [1.0, 10.0, 100.0, 1000.0, 3000.0])
        self.assertEqual(decade_ticks(1.0, 10.0), [1.0, 10.0])

    def _paced_report(self, stops=None):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "run"
            for concurrency, efficiency in ((64, 0.99), (128, 0.85)):
                write_cell(root, "eps250", concurrency, 0, "python-openai", efficiency, eps=250)
                write_cell(root, "eps250", concurrency, 0, "python-openai-mp", 0.99, eps=250)
                write_cell(root, "eps250", concurrency, 0, "go", 0.999, eps=250)
            write_cell(root, "eps250", 256, 0, "python-openai-mp", 0.98, eps=250)
            write_cell(root, "eps250", 256, 0, "go", 0.999, eps=250)
            sweep_meta = {"stops": stops or {}}
            return render_report(root, load_cells(root), sweep_meta)

    def test_new_sections_render(self):
        html = self._paced_report()
        self.assertIn("Client knee map", html)
        self.assertIn("Absolute throughput", html)
        self.assertIn("p99 max inter-chunk gap", html)
        self.assertIn("CPU cost per delivered throughput", html)
        self.assertIn("Failure modes", html)
        self.assertIn("TTFC excess vs per-process load", html)
        self.assertIn("TTFC excess percentiles per client", html)
        self.assertIn("Server schedule slip", html)
        self.assertIn("Server CPU", html)

    def test_heatmap_marks_knee_and_pruned_cells(self):
        html = self._paced_report(stops={
            "eps250:python-openai": {"concurrency": 128, "reason": "efficiency 0.85 below 0.9"},
        })
        self.assertIn("STOPPED: efficiency 0.85 below 0.9", html)  # knee cell tooltip
        self.assertIn("pruned by stop at c=128", html)             # c=256 cell hatched out
        self.assertIn("heat-knee", html)
        # Stop ✕ marker drawn on the line charts too.
        self.assertIn('class="stopx series-python-openai"', html)

    def test_cpu_chart_excludes_multiprocess_clients(self):
        html = self._paced_report()
        cpu_svg = next(
            fragment for fragment in html.split("<svg")[1:]
            if "CPU cost per delivered throughput" in fragment
        )
        self.assertIn("series-python-openai", cpu_svg)
        self.assertNotIn("series-python-openai-mp", cpu_svg)

    def test_collapse_curve_uses_per_process_load(self):
        html = self._paced_report()
        self.assertIn("(12 proc)", html)   # mp point annotated with process count
        # c=128 across 12 workers ≈ 11 streams per process.
        self.assertIn("11 streams/proc", html)
        collapse_svg = next(
            fragment for fragment in html.split("<svg")[1:]
            if "per-process load" in fragment
        )
        self.assertNotIn("series-go", collapse_svg)  # python variants only

    def test_band_grid_draws_percentile_polygon(self):
        html = self._paced_report()
        self.assertIn('class="band fill-python-openai"', html)
        self.assertIn("p50 4.0 / p95 10.0 / p99 20.0 ms", html)  # fixture percentiles - 200ms ttfc

    def test_unpaced_tier_keeps_simple_layout(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir) / "run"
            write_cell(root, "max", 4, 0, "go", 0.0, eps=0, ttfc=0)
            html = render_report(root, load_cells(root), {})
        self.assertIn("Observed events/sec", html)
        self.assertNotIn("Client knee map", html)
        self.assertNotIn("Failure modes", html)


if __name__ == "__main__":
    unittest.main()
