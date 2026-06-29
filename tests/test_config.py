import tempfile
import unittest
from pathlib import Path

from bench_harness.config import WorkloadConfig


class WorkloadConfigTests(unittest.TestCase):
    def test_workload_config_loads_json(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / "workload.json"
            config_path.write_text(
                '{"base_url":"http://127.0.0.1:8080","total_requests":3,'
                '"concurrency":2,"chunks_per_response":4,"chunk_bytes":8,'
                '"delay_us":0,"warmup_requests":1,"output_dir":"results"}'
            )

            config = WorkloadConfig.from_path(config_path)

        self.assertEqual(config.total_requests, 3)
        self.assertEqual(config.request_payload(7, "python")["chunks"], 4)
        self.assertEqual(config.request_payload(7, "python")["request_id"], "python-7")


if __name__ == "__main__":
    unittest.main()
