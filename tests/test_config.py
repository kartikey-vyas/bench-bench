import json
import tempfile
import unittest
from pathlib import Path

from bench_harness.config import WorkloadConfig


def workload_json() -> str:
    return (
        '{"base_url":"http://127.0.0.1:8080","duration_seconds":2.0,'
        '"warmup_seconds":0.5,"concurrency":2,"chunks_per_response":4,'
        '"chunk_bytes":8,"ttfc_ms":200,"events_per_second":500,'
        '"output_dir":"results"}'
    )


class WorkloadConfigTests(unittest.TestCase):
    def test_workload_config_loads_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "workload.json"
            config_path.write_text(workload_json())

            config = WorkloadConfig.from_path(config_path)

        self.assertEqual(config.duration_seconds, 2.0)
        self.assertEqual(config.warmup_seconds, 0.5)
        payload = config.request_payload(1, 7, "python")
        self.assertEqual(payload["chunks"], 4)
        self.assertEqual(payload["ttfc_ms"], 200)
        self.assertEqual(payload["events_per_second"], 500)
        self.assertEqual(payload["request_id"], "python-1-7")

    def test_rejects_zero_chunk_bytes(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "workload.json"
            config_path.write_text(workload_json().replace('"chunk_bytes":8', '"chunk_bytes":0'))
            with self.assertRaises(ValueError):
                WorkloadConfig.from_path(config_path)

    def test_checked_in_workloads_have_pacing_and_duration_fields(self):
        for path in Path("config").glob("workload.*.json"):
            with self.subTest(path=path):
                data = json.loads(path.read_text())
                self.assertIn("duration_seconds", data)
                self.assertIn("warmup_seconds", data)
                self.assertIn("ttfc_ms", data)
                self.assertIn("events_per_second", data)
                self.assertEqual(data["chunk_bytes"], 8)

    def test_checked_in_comparison_workloads_use_at_least_512_chunks(self):
        for name in ("workload.default.json", "workload.compare.json"):
            with self.subTest(name=name):
                data = json.loads((Path("config") / name).read_text())
                self.assertGreaterEqual(data["chunks_per_response"], 512)


if __name__ == "__main__":
    unittest.main()
