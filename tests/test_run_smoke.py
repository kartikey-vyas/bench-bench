import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import run_smoke


class RunSmokeTests(unittest.TestCase):
    def test_main_returns_clear_error_when_server_toolchain_is_missing(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            args = [
                "run_smoke.py",
                "--config",
                "config/workload.smoke.json",
                "--results-dir",
                str(Path(tmpdir) / "results"),
            ]
            with patch("sys.argv", args), patch(
                "scripts.run_smoke.start_server",
                side_effect=RuntimeError("cargo is required to start the Rust synthetic server"),
            ), patch("sys.stderr") as stderr:
                exit_code = run_smoke.main()

        self.assertEqual(exit_code, 2)
        stderr.write.assert_called()


if __name__ == "__main__":
    unittest.main()
