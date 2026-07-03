import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.run_sweep import (
    SweepConfig,
    SweepTier,
    build_workload,
    python_client_ready,
    rotated,
    stop_reason,
)


def sweep_config(**overrides):
    data = {
        "base_url": "http://127.0.0.1:8080",
        "tiers": ({"name": "eps100", "events_per_second": 100, "ttfc_ms": 200},),
        "concurrencies": (1, 4),
        "clients": ("drain", "python"),
        "duration_seconds": 2.0,
        "warmup_seconds": 0.5,
        "repeats": 2,
        "cooldown_seconds": 0.0,
        "chunks_per_response": 64,
        "chunk_bytes": 8,
        "stop_efficiency_below": 0.9,
        "stop_ttfc_excess_p95_ms": 100.0,
        "stop_failure_fraction": 0.001,
        "output_dir": "results",
    }
    data.update(overrides)
    tiers = tuple(SweepTier(**tier) for tier in data.pop("tiers"))
    return SweepConfig(tiers=tiers, **data)


def summary(successful=100, incomplete=0, failed=0, efficiency=0.99, p95_ttfc=210.0):
    return {
        "successful_requests": successful,
        "incomplete_requests": incomplete,
        "failed_requests": failed,
        "efficiency": efficiency,
        "p95_time_to_first_chunk_ms": p95_ttfc,
    }


class SweepConfigTests(unittest.TestCase):
    def test_from_path_loads_tiers_and_lists(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "sweep.json"
            path.write_text(json.dumps({
                "base_url": "http://127.0.0.1:8080",
                "tiers": [{"name": "max", "events_per_second": 0, "ttfc_ms": 0}],
                "concurrencies": [1, 2],
                "clients": ["python"],
                "duration_seconds": 1.0,
                "warmup_seconds": 0.0,
                "repeats": 1,
                "cooldown_seconds": 0.0,
                "chunks_per_response": 16,
                "chunk_bytes": 8,
                "stop_efficiency_below": 0.9,
                "stop_ttfc_excess_p95_ms": 100.0,
                "stop_failure_fraction": 0.001,
                "output_dir": "results",
            }))
            config = SweepConfig.from_path(path)
        self.assertEqual(config.tiers[0].name, "max")
        self.assertEqual(config.concurrencies, (1, 2))

    def test_build_workload_maps_tier_and_concurrency(self):
        sweep = sweep_config()
        workload = build_workload(sweep, sweep.tiers[0], 4)
        self.assertEqual(workload["concurrency"], 4)
        self.assertEqual(workload["events_per_second"], 100)
        self.assertEqual(workload["ttfc_ms"], 200)
        self.assertEqual(workload["duration_seconds"], 2.0)


class RotationTests(unittest.TestCase):
    def test_rotated_shifts_by_repeat(self):
        clients = ("a", "b", "c")
        self.assertEqual(rotated(clients, 0), ["a", "b", "c"])
        self.assertEqual(rotated(clients, 1), ["b", "c", "a"])
        self.assertEqual(rotated(clients, 3), ["a", "b", "c"])


class StopReasonTests(unittest.TestCase):
    def test_healthy_cell_returns_none(self):
        sweep = sweep_config()
        self.assertIsNone(stop_reason(sweep, sweep.tiers[0], [summary(), summary()]))

    def test_failure_fraction_triggers_stop(self):
        sweep = sweep_config()
        reason = stop_reason(sweep, sweep.tiers[0], [summary(failed=5)])
        self.assertIn("failure fraction", reason)

    def test_low_efficiency_triggers_stop(self):
        sweep = sweep_config()
        reason = stop_reason(sweep, sweep.tiers[0], [summary(efficiency=0.5)])
        self.assertIn("efficiency", reason)

    def test_ttfc_excess_triggers_stop(self):
        sweep = sweep_config()
        reason = stop_reason(sweep, sweep.tiers[0], [summary(p95_ttfc=400.0)])
        self.assertIn("TTFC", reason)

    def test_unpaced_tier_skips_efficiency_and_ttfc_rules(self):
        sweep = sweep_config(tiers=({"name": "max", "events_per_second": 0, "ttfc_ms": 0},))
        self.assertIsNone(
            stop_reason(sweep, sweep.tiers[0], [summary(efficiency=0.0, p95_ttfc=5000.0)])
        )

    def test_no_summaries_triggers_stop(self):
        sweep = sweep_config()
        self.assertIsNotNone(stop_reason(sweep, sweep.tiers[0], []))


class PythonClientReadyTests(unittest.TestCase):
    def test_missing_httpx_returns_false(self):
        with patch("scripts.run_sweep.subprocess.run") as mock_run:
            mock_run.return_value.returncode = 1
            self.assertFalse(python_client_ready())

    def test_importable_httpx_returns_true(self):
        with patch("scripts.run_sweep.subprocess.run") as mock_run:
            mock_run.return_value.returncode = 0
            self.assertTrue(python_client_ready())


if __name__ == "__main__":
    unittest.main()
