import unittest
from pathlib import Path

from bench_harness.clients import CLIENTS, command, required_python_modules, style


class RegistryShapeTests(unittest.TestCase):
    def test_exactly_nine_known_clients(self):
        self.assertEqual(
            set(CLIENTS),
            {
                "drain",
                "python-openai",
                "python-openai-mp",
                "python",
                "python-deferred",
                "python-deferred-mp",
                "go",
                "rust-reqwest",
                "rust-hyper",
            },
        )

    def test_orders_are_unique_and_contiguous(self):
        orders = sorted(spec.order for spec in CLIENTS.values())
        self.assertEqual(orders, list(range(9)))

    def test_every_spec_has_colors_and_summary_identity(self):
        for name, spec in CLIENTS.items():
            self.assertTrue(spec.light, msg=f"{name} missing light color")
            self.assertTrue(spec.dark, msg=f"{name} missing dark color")
            self.assertTrue(spec.language, msg=f"{name} missing language")
            self.assertTrue(spec.implementation, msg=f"{name} missing implementation")


class CommandTests(unittest.TestCase):
    def test_python_command(self):
        argv = command(
            "python",
            binaries={},
            config_path=Path("/cfg.json"),
            out_dir=Path("/out"),
            python_executable="/usr/bin/python3.12",
        )
        self.assertEqual(
            argv,
            [
                "/usr/bin/python3.12", "-m", "bench_harness.python_client",
                "--config", "/cfg.json", "--output-dir", "/out",
            ],
        )

    def test_python_mp_command_carries_variant_args(self):
        argv = command(
            "python-openai-mp",
            binaries={},
            config_path=Path("/cfg.json"),
            out_dir=Path("/out"),
            python_executable="/usr/bin/python3.12",
        )
        self.assertEqual(
            argv,
            [
                "/usr/bin/python3.12", "-m", "bench_harness.python_mp",
                "--variant", "openai",
                "--config", "/cfg.json", "--output-dir", "/out",
            ],
        )

    def test_go_command(self):
        argv = command(
            "go",
            binaries={"go": Path("/bin/bench-go-client")},
            config_path=Path("/cfg.json"),
            out_dir=Path("/out"),
            python_executable="/usr/bin/python3.12",
        )
        self.assertEqual(
            argv,
            ["/bin/bench-go-client", "--config", "/cfg.json", "--output-dir", "/out"],
        )

    def test_rust_command(self):
        argv = command(
            "rust-reqwest",
            binaries={"rust": Path("/bin/rust-benchmark-client")},
            config_path=Path("/cfg.json"),
            out_dir=Path("/out"),
            python_executable="/usr/bin/python3.12",
        )
        self.assertEqual(
            argv,
            [
                "/bin/rust-benchmark-client",
                "--config", "/cfg.json", "--output-dir", "/out",
                "--client", "reqwest",
            ],
        )

    def test_unknown_client_raises_naming_known_clients(self):
        with self.assertRaises(ValueError) as ctx:
            command(
                "carrier-pigeon",
                binaries={},
                config_path=Path("/cfg.json"),
                out_dir=Path("/out"),
                python_executable="/usr/bin/python3.12",
            )
        message = str(ctx.exception)
        self.assertIn("carrier-pigeon", message)
        for name in CLIENTS:
            self.assertIn(name, message)


class RequiredPythonModulesTests(unittest.TestCase):
    def test_union_of_two_python_clients(self):
        self.assertEqual(
            required_python_modules(("python-openai", "python")),
            {"httpx", "openai"},
        )


class StyleFallbackTests(unittest.TestCase):
    def test_known_client_style(self):
        got = style("drain")
        self.assertEqual(got, {"light": "#898781", "dark": "#898781", "dash": "6 4"})

    def test_unknown_client_falls_back_to_neutral(self):
        got = style("rust-drain")  # stale name from an old result tree
        self.assertIn("light", got)
        self.assertIn("dark", got)
        self.assertIn("dash", got)


if __name__ == "__main__":
    unittest.main()
