import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts import run_smoke


def write_summary(run_dir: Path, client: str, keys: list[str]) -> None:
    client_dir = run_dir / client
    client_dir.mkdir(parents=True, exist_ok=True)
    (client_dir / "summary.json").write_text(json.dumps({
        "language": client,
        "implementation": client,
        "summary": {key: 0 for key in keys},
    }))


class SummariesShareSchemaTests(unittest.TestCase):
    def test_matching_schemas_pass(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir)
            write_summary(run_dir, "python", ["a", "b", "c"])
            write_summary(run_dir, "go", ["a", "b", "c"])
            self.assertTrue(run_smoke.summaries_share_schema(run_dir))

    def test_missing_key_fails(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            run_dir = Path(tmpdir)
            write_summary(run_dir, "python", ["a", "b", "c"])
            write_summary(run_dir, "go", ["a", "b"])
            with patch("sys.stderr"):
                self.assertFalse(run_smoke.summaries_share_schema(run_dir))

    def test_no_summaries_passes_vacuously(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            self.assertTrue(run_smoke.summaries_share_schema(Path(tmpdir)))


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
