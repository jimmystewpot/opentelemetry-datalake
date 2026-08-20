.PHONY: check test fmt bench clippy coverage all e2e-test e2e-tests

all: fmt clippy test bench

fmt:
	cargo fmt --all -- --check

clippy:
	cargo clippy --all-targets -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc

test:
	cargo test --workspace

coverage:
	cargo llvm-cov --workspace --lcov --output-path lcov.info

bench:
	cargo bench --workspace

e2e-test:
	./tests/e2e/run.sh

e2e-tests: e2e-test

