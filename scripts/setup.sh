#!/usr/bin/env bash
# Bootstrap the benchmark toolchain: rustup/cargo, Go (>= 1.22), uv + venv,
# and (Linux) taskset. Idempotent — checks before installing, safe to re-run.
set -euo pipefail

MIN_GO_MINOR=22
OS="$(uname -s)"
SUDO=""
if [ "$(id -u)" -ne 0 ] && command -v sudo >/dev/null 2>&1; then
    SUDO="sudo"
fi

# Tools installed by this script land here; make them visible to later steps
# in this same run even if the user's shell profile hasn't picked them up yet.
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$HOME/go/bin:$PATH"

ok()   { printf '  \033[32mok\033[0m    %s\n' "$1"; }
inst() { printf '  \033[33minstall\033[0m %s\n' "$1"; }
warn() { printf '  \033[31mwarn\033[0m  %s\n' "$1"; }

linux_pkg_install() {
    package="$1"
    if command -v apt-get >/dev/null 2>&1; then
        $SUDO apt-get update -qq && $SUDO apt-get install -y "$package"
    elif command -v dnf >/dev/null 2>&1; then
        $SUDO dnf install -y "$package"
    elif command -v pacman >/dev/null 2>&1; then
        $SUDO pacman -S --noconfirm "$package"
    elif command -v apk >/dev/null 2>&1; then
        $SUDO apk add "$package"
    else
        warn "no supported package manager found; install $package manually"
        return 1
    fi
}

echo "== toolchain setup ($OS)"

# --- Rust via rustup -------------------------------------------------------
if command -v cargo >/dev/null 2>&1; then
    ok "rust: $(cargo --version)"
else
    inst "rustup (stable toolchain)"
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --profile minimal
    # shellcheck disable=SC1091
    . "$HOME/.cargo/env"
    ok "rust: $(cargo --version)"
fi

# --- Go >= 1.22 ------------------------------------------------------------
go_version_ok() {
    command -v go >/dev/null 2>&1 || return 1
    version="$(go version | sed -E 's/.*go([0-9]+)\.([0-9]+).*/\1 \2/')"
    major="${version% *}"
    minor="${version#* }"
    [ "$major" -gt 1 ] || { [ "$major" -eq 1 ] && [ "$minor" -ge "$MIN_GO_MINOR" ]; }
}

if go_version_ok; then
    ok "go: $(go version)"
else
    if command -v go >/dev/null 2>&1; then
        warn "go is older than 1.$MIN_GO_MINOR: $(go version)"
    fi
    if [ "$OS" = "Darwin" ]; then
        if command -v brew >/dev/null 2>&1; then
            inst "go (homebrew)"
            brew install go
        else
            warn "homebrew not found; install Go >= 1.$MIN_GO_MINOR from https://go.dev/dl/"
            exit 1
        fi
    else
        inst "go (distro package)"
        linux_pkg_install golang-go || linux_pkg_install golang || linux_pkg_install go || true
    fi
    if go_version_ok; then
        ok "go: $(go version)"
    else
        warn "distro Go is missing or older than 1.$MIN_GO_MINOR — install from https://go.dev/dl/:"
        warn "  curl -LO https://go.dev/dl/go1.23.4.linux-amd64.tar.gz"
        warn "  $SUDO rm -rf /usr/local/go && $SUDO tar -C /usr/local -xzf go1.23.4.linux-amd64.tar.gz"
        warn "  export PATH=/usr/local/go/bin:\$PATH"
        exit 1
    fi
fi

# --- uv + project venv -----------------------------------------------------
if command -v uv >/dev/null 2>&1; then
    ok "uv: $(uv --version)"
else
    inst "uv"
    curl -LsSf https://astral.sh/uv/install.sh | sh
    ok "uv: $(uv --version)"
fi

echo "  sync  python venv (httpx)"
uv sync
ok "python: $(.venv/bin/python --version) with httpx"

# --- Linux extras ----------------------------------------------------------
if [ "$OS" = "Linux" ]; then
    if command -v taskset >/dev/null 2>&1; then
        ok "taskset: available (cpu pinning enabled)"
    else
        inst "taskset (util-linux)"
        linux_pkg_install util-linux || warn "cpu pinning will be unavailable without taskset"
    fi
else
    warn "macOS: no taskset — server_cpus/client_cpus in sweep configs are ignored (unpinned)"
fi

echo "== setup complete. Sanity gate: make sweep-smoke"
