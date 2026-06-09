.PHONY: check test fmt bench clippy all e2e-test e2e-tests

all: fmt clippy test bench

fmt:
	cargo fmt --all -- --check

clippy:
	cargo clippy --all-targets --all-features -- -D warnings -W clippy::pedantic -A clippy::missing_errors_doc

test:
	cargo test --workspace

bench:
	cargo bench --workspace

e2e-test:
	./tests/e2e/run.sh

e2e-tests: e2e-test
