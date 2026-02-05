CARGO_TARGET_DIR ?= $(CURDIR)/target

.PHONY: all setup fmt lint build build-release check test unit-test miri clean doc doc-open

all: lint test

# Setup dev toolchain
setup:
	rustup component add rustfmt clippy
	cargo install taplo-cli --locked
	cargo install typos-cli --locked
	cargo install cargo-machete --locked
	cargo install cargo-nextest --locked

# Formatting
fmt:
	cargo fmt --all
	taplo fmt

# Linting
lint: fmt
	cargo clippy --workspace --all-targets -- -D warnings
	cargo machete
	cargo doc --workspace --no-deps
	typos

# Type checking
check:
	cargo check --workspace --all-targets

# Build
build:
	cargo build --workspace

build-release:
	cargo build --workspace --release

# Testing
test: unit-test

unit-test:
	ulimit -n 10000 2>/dev/null || true; \
	ulimit -s 16384 2>/dev/null || true; \
	RUST_LOG="ERROR" cargo nextest run --workspace

miri:
	cargo miri setup
	MIRIFLAGS="-Zmiri-disable-isolation" cargo miri test --no-default-features

# Cleanup
clean:
	cargo clean
	rm -rf ./_meta*/ ./_logs*/

# Documentation
doc:
	cargo doc --workspace --no-deps

doc-open:
	cargo doc --workspace --no-deps --open

