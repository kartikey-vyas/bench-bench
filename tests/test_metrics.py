import unittest

from bench_harness.metrics import RequestMeasurement, aggregate_summary, percentile


class MetricsTests(unittest.TestCase):
    def test_percentile_uses_nearest_rank(self):
        self.assertEqual(percentile([10.0, 20.0, 30.0, 40.0], 0.50), 20.0)
        self.assertEqual(percentile([10.0, 20.0, 30.0, 40.0], 0.95), 40.0)

    def test_aggregate_summary_computes_rates_and_latency_percentiles(self):
        measurements = [
            RequestMeasurement(ok=True, latency_ms=10.0, first_chunk_ms=2.0, chunks=4, bytes=16),
            RequestMeasurement(ok=True, latency_ms=30.0, first_chunk_ms=4.0, chunks=4, bytes=16),
            RequestMeasurement(ok=False, latency_ms=50.0, first_chunk_ms=0.0, chunks=0, bytes=0),
        ]

        summary = aggregate_summary(measurements, duration_ms=20.0)

        self.assertEqual(summary["successful_requests"], 2)
        self.assertEqual(summary["failed_requests"], 1)
        self.assertEqual(summary["total_chunks"], 8)
        self.assertEqual(summary["total_bytes"], 32)
        self.assertEqual(summary["requests_per_second"], 100.0)
        self.assertEqual(summary["chunks_per_second"], 400.0)
        self.assertEqual(summary["mean_request_latency_ms"], 20.0)
        self.assertEqual(summary["p50_request_latency_ms"], 10.0)
        self.assertEqual(summary["p95_request_latency_ms"], 30.0)
        self.assertEqual(summary["mean_time_to_first_chunk_ms"], 3.0)


if __name__ == "__main__":
    unittest.main()
