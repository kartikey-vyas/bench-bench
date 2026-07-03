import unittest

from bench_harness.metrics import RequestMeasurement, aggregate_summary, percentile


def measurement(ok=True, latency_ms=10.0, first_chunk_ms=2.0, chunks=4, bytes=32,
                max_gap_ms=1.0, stream_ms=30.0):
    return RequestMeasurement(
        ok=ok, latency_ms=latency_ms, first_chunk_ms=first_chunk_ms,
        chunks=chunks, bytes=bytes, max_gap_ms=max_gap_ms, stream_ms=stream_ms,
    )


class MetricsTests(unittest.TestCase):
    def test_percentile_uses_nearest_rank(self):
        self.assertEqual(percentile([10.0, 20.0, 30.0, 40.0], 0.50), 20.0)
        self.assertEqual(percentile([10.0, 20.0, 30.0, 40.0], 0.95), 40.0)

    def test_aggregate_summary_classifies_and_computes_efficiency(self):
        measurements = [
            measurement(chunks=4, stream_ms=30.0, max_gap_ms=12.0),
            measurement(chunks=3),               # incomplete: ok but wrong count
            measurement(ok=False, chunks=0),     # failed
        ]

        summary = aggregate_summary(
            measurements, duration_ms=1000.0,
            expected_chunks=4, events_per_second=100, concurrency=2, ttfc_ms=0,
        )

        self.assertEqual(summary["successful_requests"], 1)
        self.assertEqual(summary["incomplete_requests"], 1)
        self.assertEqual(summary["failed_requests"], 1)
        self.assertEqual(summary["total_chunks"], 4)
        self.assertEqual(summary["chunks_per_second"], 4.0)
        # ideal_request_seconds = 0 + 3/100 = 0.03; ideal = 2*4/0.03 = 266.6667
        self.assertAlmostEqual(summary["ideal_events_per_second"], 266.6666666666667)
        self.assertAlmostEqual(summary["efficiency"], 0.015)
        # ideal stream = (4-1)/100*1000 = 30ms; stretch = 30/30 = 1.0
        self.assertAlmostEqual(summary["p50_stream_stretch"], 1.0)
        self.assertEqual(summary["p95_max_gap_ms"], 12.0)
        self.assertEqual(summary["max_max_gap_ms"], 12.0)

    def test_aggregate_summary_unpaced_has_zero_ideal_and_stretch(self):
        summary = aggregate_summary(
            [measurement()], duration_ms=1000.0,
            expected_chunks=4, events_per_second=0, concurrency=2, ttfc_ms=0,
        )
        self.assertEqual(summary["ideal_events_per_second"], 0.0)
        self.assertEqual(summary["efficiency"], 0.0)
        self.assertEqual(summary["p50_stream_stretch"], 0.0)
        self.assertNotIn("per_chunk_overhead_ms", summary)


if __name__ == "__main__":
    unittest.main()
