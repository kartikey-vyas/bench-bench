import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.run_sweep import (
    SweepConfig,
    SweepProgress,
    SweepTier,
    build_workload,
    format_duration,
    python_client_ready,
    resolve_stop_reason,
    rotated,
    server_command,
    stop_reason,
    taskset_prefix,
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

    def test_from_path_rejects_non_positive_worker_threads(self):
        for bad_value in (0, -1):
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
                    "server_worker_threads": bad_value,
                }))
                with self.assertRaises(ValueError):
                    SweepConfig.from_path(path)

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


class ResolveStopReasonTests(unittest.TestCase):
    def test_failed_runs_stop_even_with_healthy_summaries(self):
        # 2 of 3 repeats crashed/timed-out; the one summary that did land looks
        # perfectly healthy. Without counting failed runs this would escalate.
        sweep = sweep_config()
        reason = resolve_stop_reason(sweep, sweep.tiers[0], [summary()], failed_runs=2)
        self.assertEqual(reason, "2 run(s) produced no summary")

    def test_no_failed_runs_falls_back_to_stop_reason(self):
        sweep = sweep_config()
        self.assertIsNone(
            resolve_stop_reason(sweep, sweep.tiers[0], [summary(), summary()], failed_runs=0)
        )
        reason = resolve_stop_reason(sweep, sweep.tiers[0], [summary(efficiency=0.5)], failed_runs=0)
        self.assertIn("efficiency", reason)


class SweepProgressTests(unittest.TestCase):
    def test_total_counts_full_grid(self):
        # 1 tier x 2 concurrencies x 2 repeats x 2 clients = 8 client-runs
        progress = SweepProgress(sweep_config())
        self.assertEqual(progress.total, 8)
        self.assertEqual(progress.completed, 0)

    def test_drop_client_prunes_remaining_rungs(self):
        progress = SweepProgress(sweep_config())
        # stopped at rung index 0 of 2 -> loses 1 remaining rung x 2 repeats
        removed = progress.drop_client(rung_index=0)
        self.assertEqual(removed, 2)
        self.assertEqual(progress.total, 6)
        # stopped at the last rung -> nothing left to prune
        self.assertEqual(progress.drop_client(rung_index=1), 0)

    def test_eta_uses_mean_run_duration(self):
        progress = SweepProgress(sweep_config())
        self.assertIsNone(progress.eta_seconds())
        progress.finish_run(10.0)
        progress.finish_run(20.0)
        # mean 15s x 6 remaining runs
        self.assertAlmostEqual(progress.eta_seconds(), 90.0)

    def test_percent_complete(self):
        progress = SweepProgress(sweep_config())
        progress.finish_run(1.0)
        progress.finish_run(1.0)
        self.assertAlmostEqual(progress.percent(), 25.0)


class FormatDurationTests(unittest.TestCase):
    def test_seconds_minutes_hours(self):
        self.assertEqual(format_duration(45), "45s")
        self.assertEqual(format_duration(125), "2m05s")
        self.assertEqual(format_duration(3900), "1h05m")


class CpuAllocationTests(unittest.TestCase):
    def test_defaults_are_unset(self):
        sweep = sweep_config()
        self.assertIsNone(sweep.server_worker_threads)
        self.assertIsNone(sweep.server_cpus)
        self.assertIsNone(sweep.client_cpus)

    def test_server_command_without_allocation(self):
        sweep = sweep_config()
        command = server_command(sweep, {"server": Path("/bin/server")}, "127.0.0.1:8080")
        self.assertEqual(command, ["/bin/server", "--bind", "127.0.0.1:8080"])

    def test_server_command_with_worker_threads_and_pinning(self):
        sweep = sweep_config(server_worker_threads=8, server_cpus="0-7")
        with patch("scripts.run_sweep.shutil.which", return_value="/usr/bin/taskset"):
            command = server_command(sweep, {"server": Path("/bin/server")}, "127.0.0.1:8080")
        self.assertEqual(
            command,
            ["taskset", "-c", "0-7", "/bin/server", "--bind", "127.0.0.1:8080",
             "--worker-threads", "8"],
        )

    def test_taskset_prefix_degrades_without_taskset(self):
        with patch("scripts.run_sweep.shutil.which", return_value=None):
            self.assertEqual(taskset_prefix("0-7"), [])
        self.assertEqual(taskset_prefix(None), [])


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
