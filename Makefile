PYTHON := $(shell test -x .venv/bin/python && echo .venv/bin/python || echo python3)

.PHONY: setup test test-python test-go test-rust smoke compare sweep sweep-smoke sweep-report

test: test-python test-go test-rust

test-python:
	$(PYTHON) -m unittest discover -s tests -v

test-go:
	cd go-client && go test ./...

test-rust:
	cargo test --manifest-path server-rust/Cargo.toml
	cargo test --manifest-path rust-client/Cargo.toml

smoke:
	$(PYTHON) scripts/run_smoke.py --config config/workload.smoke.json

compare:
	$(PYTHON) scripts/compare_results.py results

# Override per run: make sweep CONFIG=config/sweep.linux.json
CONFIG ?= config/sweep.default.json
# Optional run dirs to merge: make sweep-report RUNS="results/<a> results/<b>"
RUNS ?=

sweep:
	$(PYTHON) -m bench_harness.sweep --config $(CONFIG)

sweep-smoke:
	$(PYTHON) -m bench_harness.sweep --config config/sweep.smoke.json

sweep-report:
	$(PYTHON) -m bench_harness.sweep_report $(RUNS)

setup:
	bash scripts/setup.sh
