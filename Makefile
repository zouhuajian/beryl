.PHONY: fmt fmt-check check clippy test metadata verify

fmt:
	cargo fmt --all

fmt-check:
	cargo fmt --all --check

metadata:
	cargo metadata --format-version 1 --no-deps

check:
	cargo check --workspace

clippy:
	cargo clippy --workspace --all-targets -- -D warnings

test:
	cargo test --workspace

verify: fmt-check metadata check clippy test
