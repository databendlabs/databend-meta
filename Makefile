CARGO_TARGET_DIR ?= $(CURDIR)/target

.PHONY: all setup fmt lint build build-release check test unit-test miri coverage coverage-html clean doc doc-open compat-history raft-protocol-compat-lint raft-protocol-compat-build test-raft-protocol-compat

all: lint test

# Setup dev toolchain
setup:
	rustup component add rustfmt clippy
	cargo install taplo-cli --locked
	cargo install typos-cli --locked
	cargo install cargo-machete --locked
	cargo install cargo-nextest --locked
	cargo install cargo-llvm-cov --locked

# Formatting
fmt:
	cargo fmt --all
	taplo fmt

# Linting
lint: fmt compat-history raft-protocol-compat-lint
	cargo clippy --workspace --all-targets -- -D warnings
	cargo machete
	cargo doc --workspace --no-deps
	typos

# Type checking
check:
	cargo check --workspace --all-targets

# Build
build: compat-history
	cargo build --workspace

build-release:
	cargo build --workspace --release

# Testing
test: compat-history unit-test

unit-test:
	ulimit -n 10000 2>/dev/null || true; \
	ulimit -s 16384 2>/dev/null || true; \
	RUST_LOG="ERROR" cargo nextest run --workspace

# Coverage report (requires cargo-llvm-cov: `make setup`)
coverage:
	cargo llvm-cov nextest --workspace

coverage-html:
	cargo llvm-cov nextest --workspace --html
	@echo "Report: target/llvm-cov/html/index.html"

miri:
	cargo miri setup
	MIRIFLAGS="-Zmiri-disable-isolation" cargo miri test --no-default-features

# Update compatibility history
compat-history:
	python3 docs/update-compat-history.py

# Raft protocol backward compatibility test
#
# Builds bin-current/ (the working source tree) plus every bin-v*/ workspace
# (each pinned to a released tag). New old versions added under
# crates/tests/raft-protocol-compat/bin-v<TAG>/ are picked up automatically.
raft-protocol-compat-lint:
	@set -e; for d in crates/tests/raft-protocol-compat/bin-current crates/tests/raft-protocol-compat/bin-v*; do \
		[ -d "$$d" ] || continue; \
		echo "==> Linting $$d"; \
		cargo fmt --manifest-path $$d/Cargo.toml -- --check; \
		cargo clippy --manifest-path $$d/Cargo.toml --all-targets -- -D warnings; \
	done

raft-protocol-compat-build:
	@set -e; for d in crates/tests/raft-protocol-compat/bin-current crates/tests/raft-protocol-compat/bin-v*; do \
		[ -d "$$d" ] || continue; \
		echo "==> Building $$d"; \
		cargo build --manifest-path $$d/Cargo.toml; \
	done

test-raft-protocol-compat: raft-protocol-compat-build
	python3 crates/tests/raft-protocol-compat/test_meta_meta.py --skip-build

# Cleanup
clean:
	cargo clean
	rm -rf ./_meta*/ ./_logs*/

# Documentation
doc:
	cargo doc --workspace --no-deps

doc-open:
	cargo doc --workspace --no-deps --open
