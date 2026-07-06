"""Single source of truth for the seven benchmark clients.

Every consumer that previously hard-coded client names, python modules,
rust --client values, summary (language, implementation) identities, or
chart styling now derives from CLIENTS below. Add a client here once and
the sweep runner, smoke runner, and both static report generators pick it
up automatically.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class ClientSpec:
    name: str                     # sweep-config name and result-directory name
    kind: str                     # "python" | "go" | "rust"
    order: int                    # canonical display order
    language: str                 # summary.json "language"
    implementation: str           # summary.json "implementation"
    light: str                    # chart color, light mode
    dark: str                     # chart color, dark mode
    dash: str = ""                # SVG dash pattern ("" = solid)
    module: str | None = None     # python: module for `python -m`
    required_modules: tuple[str, ...] = ()  # python: import preflight
    rust_kind: str | None = None  # rust: --client value


CLIENTS: dict[str, ClientSpec] = {
    "drain": ClientSpec(
        name="drain", kind="rust", order=0,
        language="rust", implementation="drain-hyper",
        light="#898781", dark="#898781", dash="6 4",
        rust_kind="drain",
    ),
    "python-openai": ClientSpec(
        name="python-openai", kind="python", order=1,
        language="python", implementation="asyncio-openai-sdk",
        light="#4a3aa7", dark="#9085e9",
        module="bench_harness.python_openai_client",
        required_modules=("httpx", "openai"),
    ),
    "python": ClientSpec(
        name="python", kind="python", order=2,
        language="python", implementation="asyncio-httpx",
        light="#2a78d6", dark="#3987e5",
        module="bench_harness.python_client",
        required_modules=("httpx",),
    ),
    "python-deferred": ClientSpec(
        name="python-deferred", kind="python", order=3,
        language="python", implementation="asyncio-httpx-deferred",
        light="#e34948", dark="#e66767",
        module="bench_harness.python_deferred_client",
        required_modules=("httpx",),
    ),
    "go": ClientSpec(
        name="go", kind="go", order=4,
        language="go", implementation="net-http-goroutines",
        light="#1baf7a", dark="#199e70",
    ),
    "rust-reqwest": ClientSpec(
        name="rust-reqwest", kind="rust", order=5,
        language="rust", implementation="reqwest-tokio",
        light="#eda100", dark="#c98500",
        rust_kind="reqwest",
    ),
    "rust-hyper": ClientSpec(
        name="rust-hyper", kind="rust", order=6,
        language="rust", implementation="hyper-tokio",
        light="#008300", dark="#008300",
        rust_kind="hyper",
    ),
}

CLIENT_ORDER: list[str] = [spec.name for spec in sorted(CLIENTS.values(), key=lambda s: s.order)]

_NEUTRAL_STYLE = {"light": "#898781", "dark": "#898781", "dash": ""}


def command(
    name: str,
    binaries: dict[str, Path],
    config_path: Path,
    out_dir: Path,
    python_executable: str,
) -> list[str]:
    """Build the argv used to launch the given client's subprocess."""
    spec = CLIENTS.get(name)
    if spec is None:
        raise ValueError(f"unknown client {name!r}; known clients are {sorted(CLIENTS)}")

    if spec.kind == "python":
        return [
            python_executable, "-m", spec.module,
            "--config", str(config_path), "--output-dir", str(out_dir),
        ]
    if spec.kind == "go":
        return [str(binaries["go"]), "--config", str(config_path), "--output-dir", str(out_dir)]
    # rust
    return [
        str(binaries["rust"]),
        "--config", str(config_path), "--output-dir", str(out_dir),
        "--client", spec.rust_kind,
    ]


def required_python_modules(names) -> set[str]:
    """Union of python import requirements for the given client names."""
    return {
        module
        for name in names
        if name in CLIENTS
        for module in CLIENTS[name].required_modules
    }


def style(name: str) -> dict:
    """Chart style for a client name; falls back to a neutral style for
    unknown/legacy names (e.g. an old `rust-drain` result directory) so
    report generation never breaks on stale result trees."""
    spec = CLIENTS.get(name)
    if spec is None:
        return dict(_NEUTRAL_STYLE)
    return {"light": spec.light, "dark": spec.dark, "dash": spec.dash}
