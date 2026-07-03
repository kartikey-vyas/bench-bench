PYTHON := $(shell test -x .venv/bin/python && echo .venv/bin/python || echo python3)

.PHONY: test test-python test-go test-rust smoke compare sweep sweep-smoke sweep-report

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

sweep:
	$(PYTHON) scripts/run_sweep.py --config config/sweep.default.json

sweep-smoke:
	$(PYTHON) scripts/run_sweep.py --config config/sweep.smoke.json

sweep-report:
	$(PYTHON) scripts/generate_sweep_report.py
