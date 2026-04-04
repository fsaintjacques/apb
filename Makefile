DUCKDB_LIB_DIR ?= $(shell brew --prefix duckdb)/lib
DUCKDB_INC_DIR ?= $(shell brew --prefix duckdb)/include

export DUCKDB_LIB_DIR
export DUCKDB_INC_DIR

CARGO_FLAGS ?=
FEATURES ?= duckdb

.PHONY: build release test lint format check-format check-lint check clean

build:
	cargo build --features $(FEATURES) $(CARGO_FLAGS)

release:
	cargo build --release --features $(FEATURES) $(CARGO_FLAGS)

test:
	cargo test --features $(FEATURES) $(CARGO_FLAGS)

lint:
	cargo clippy --features $(FEATURES) $(CARGO_FLAGS) --fix --allow-dirty -- -D warnings

format:
	cargo fmt

check-format:
	cargo fmt --check

check-lint:
	cargo clippy --features $(FEATURES) $(CARGO_FLAGS) -- -D warnings

check: check-format check-lint test

clean:
	cargo clean
