import json
import tempfile
import unittest
from pathlib import Path

from scripts.generate_report import compute_rows, find_latest_results_dir, load_summaries, render_report


def write_summary(path: Path, language: str, implementation: str, req_s: float, chunks_s: float) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "language": language,
                "implementation": implementation,
                "started_at": "2026-06-30T00:00:00Z",
                "config": {
                    "base_url": "http://127.0.0.1:8080",
                    "duration_seconds": 2.0,
                    "warmup_seconds": 0.5,
                    "concurrency": 2,
                    "chunks_per_response": 3,
                    "chunk_bytes": 8,
                    "ttfc_ms": 200,
                    "events_per_second": 500,
                    "output_dir": "results",
                },
                "summary": {
                    "duration_ms": 10.0,
                    "successful_requests": 10,
                    "failed_requests": 0,
                    "incomplete_requests": 0,
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
                    "p50_max_gap_ms": 1.0,
                    "p95_max_gap_ms": 2.0,
                    "p99_max_gap_ms": 3.0,
                    "max_max_gap_ms": 4.0,
                    "p50_stream_stretch": 1.0,
                    "p95_stream_stretch": 1.1,
                    "p99_stream_stretch": 1.2,
                    "ideal_events_per_second": 1000.0,
                    "efficiency": 0.95,
                },
            }
        )
    )


class GenerateReportTests(unittest.TestCase):
    def test_find_latest_results_dir_chooses_newest_timestamp_child(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            (root / "20260629T100000Z").mkdir()
            (root / "not-a-run").mkdir()
            (root / "20260630T090000Z").mkdir()

            self.assertEqual(find_latest_results_dir(root), root / "20260630T090000Z")

    def test_load_summaries_finds_nested_summary_files(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            write_summary(root / "python" / "summary.json", "python", "asyncio-httpx", 100.0, 300.0)
            write_summary(root / "go" / "summary.json", "go", "net-http-goroutines", 200.0, 600.0)

            summaries = load_summaries(root)

        self.assertEqual([item["implementation"] for item in summaries], ["asyncio-httpx", "net-http-goroutines"])
        self.assertTrue(all("_path" in item for item in summaries))

    def test_compute_rows_sorts_clients_and_computes_speedups(self):
        summaries = [
            make_summary("rust", "hyper-tokio", 300.0, 900.0),
            make_summary("go", "net-http-goroutines", 400.0, 1200.0),
            make_summary("python", "asyncio-httpx", 100.0, 300.0),
            make_summary("rust", "reqwest-tokio", 200.0, 600.0),
        ]

        rows = compute_rows(summaries)

        self.assertEqual(
            [row["label"] for row in rows],
            ["Python asyncio-httpx", "Go net-http-goroutines", "Rust reqwest-tokio", "Rust hyper-tokio"],
        )
        self.assertEqual(rows[1]["speedup_vs_python"], 4.0)
        self.assertEqual(rows[3]["speedup_vs_rust_reqwest"], 1.5)

    def test_render_report_contains_charts_without_scaling_notes(self):
        summaries = [
            make_summary("python", "asyncio-httpx", 100.0, 300.0),
            make_summary("go", "net-http-goroutines", 400.0, 1200.0),
            make_summary("rust", "reqwest-tokio", 200.0, 600.0),
            make_summary("rust", "hyper-tokio", 300.0, 900.0),
        ]

        html = render_report(Path("results/20260630T090000Z"), summaries)

        self.assertIn("Throughput", html)
        self.assertIn("Request Latency", html)
        self.assertIn("Time To First Chunk", html)
        self.assertIn("Python asyncio-httpx", html)
        self.assertIn("<svg", html)
        self.assertIn("Efficiency", html)
        self.assertNotIn("How Each Client Scales", html)

    def test_render_report_groups_latency_percentiles_per_client_lane(self):
        summaries = [
            make_summary("python", "asyncio-httpx", 100.0, 300.0),
            make_summary("go", "net-http-goroutines", 400.0, 1200.0),
            make_summary("rust", "reqwest-tokio", 200.0, 600.0),
            make_summary("rust", "hyper-tokio", 300.0, 900.0),
        ]

        html = render_report(Path("results/20260630T090000Z"), summaries)

        self.assertIn("Request latency distribution", html)
        self.assertIn("Time To First Chunk distribution", html)
        self.assertIn('class="latency-lane"', html)
        self.assertIn('data-percentile="p99"', html)
        self.assertIn("p50-p95 range", html)
        self.assertNotIn("Python asyncio-httpx p50", html)


def make_summary(language: str, implementation: str, req_s: float, chunks_s: float) -> dict:
    return {
        "language": language,
        "implementation": implementation,
        "started_at": "2026-06-30T00:00:00Z",
        "config": {
            "base_url": "http://127.0.0.1:8080",
            "duration_seconds": 2.0,
            "warmup_seconds": 0.5,
            "concurrency": 2,
            "chunks_per_response": 3,
            "chunk_bytes": 8,
            "ttfc_ms": 200,
            "events_per_second": 500,
            "output_dir": "results",
        },
        "summary": {
            "duration_ms": 10.0,
            "successful_requests": 10,
            "failed_requests": 0,
            "incomplete_requests": 0,
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
            "p50_max_gap_ms": 1.0,
            "p95_max_gap_ms": 2.0,
            "p99_max_gap_ms": 3.0,
            "max_max_gap_ms": 4.0,
            "p50_stream_stretch": 1.0,
            "p95_stream_stretch": 1.1,
            "p99_stream_stretch": 1.2,
            "ideal_events_per_second": 1000.0,
            "efficiency": 0.95,
        },
    }


if __name__ == "__main__":
    unittest.main()
