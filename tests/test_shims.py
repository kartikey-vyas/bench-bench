"""The sweep runner and sweep report generator moved to bench_harness.*;
scripts/run_sweep.py and scripts/generate_sweep_report.py stay as thin
back-compat shims (muscle memory, existing docs). This test is the single
place that keeps the shims honest: if a shim's `main` ever drifts from the
real implementation, this fails loudly instead of a human noticing months
later that `python scripts/run_sweep.py` silently does something different."""

import unittest

import bench_harness.sweep
import bench_harness.sweep_report
import scripts.run_sweep
import scripts.generate_sweep_report


class ShimIdentityTests(unittest.TestCase):
    def test_run_sweep_shim_is_bench_harness_sweep(self):
        self.assertIs(scripts.run_sweep.main, bench_harness.sweep.main)

    def test_generate_sweep_report_shim_is_bench_harness_sweep_report(self):
        self.assertIs(scripts.generate_sweep_report.main, bench_harness.sweep_report.main)


if __name__ == "__main__":
    unittest.main()
