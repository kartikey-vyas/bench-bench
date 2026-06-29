.PHONY: test test-python test-go test-rust smoke compare

test: test-python test-go test-rust

test-python:
	python3 -m unittest discover -s tests -v

test-go:
	cd go-client && go test ./...

test-rust:
	cargo test --manifest-path server-rust/Cargo.toml
	cargo test --manifest-path rust-client/Cargo.toml

smoke:
	python3 scripts/run_smoke.py --config config/workload.smoke.json

compare:
	python3 scripts/compare_results.py results
