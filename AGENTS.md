# AGENTS.md

## Project

Arrow to Protobuf transcoder. Converts columnar Arrow data into row-wise
protobuf messages using dynamic binary descriptors — no code generation.

## Workspace layout

```
apb/
├── crates/apb-core/    # Library: schema mapping, transcoding, generation, validation
│                       # No I/O, no async. Pure transforms on Arrow arrays + descriptors.
└── crates/apb-cli/     # Binary: CLI with DuckDB + Arrow IPC input
```

The CLI binary is named `apb`.

## Build and test

```bash
# Build (IPC-only)
cargo build -p apb-cli

# Build with DuckDB (requires system libduckdb)
cargo build -p apb-cli --features duckdb

# Run all tests (no features needed — apb-core has no optional deps)
cargo test --workspace
```

DuckDB is optional and only used in `apb-cli`. All core logic is tested
without it. The system may not have `libduckdb` installed — don't use
`--features duckdb` in test commands unless explicitly asked.

## Module conventions

Each `apb-core` module follows the same structure:

- `mod.rs` — public API and logic
- `model.rs` — types, errors, options structs
- `tests.rs` or `#[cfg(test)] mod tests` in `mod.rs` — unit tests

Tests use inline `#[cfg(test)]` in small modules, separate `tests.rs` in
larger ones (mapping, transcode, validation).

## Key modules (apb-core)

| Module | Purpose |
|--------|---------|
| `descriptor` | Load `FileDescriptorSet` bytes → `ProtoSchema` (wraps `prost_reflect::DescriptorPool`) |
| `mapping` | Arrow schema + proto message → `FieldMapping` (infer or explicit mode) |
| `generate` | Arrow schema → `prost_types::FileDescriptorProto` (reverse of descriptor loading) |
| `transcode` | `FieldMapping` → `Transcoder` → batch encoding to protobuf wire format |
| `types` | Arrow ↔ proto type compatibility checks and coercion risk classification |
| `validation` | `FieldMapping` → human/JSON report with errors and warnings |

## Test fixtures

Proto fixtures live in `crates/apb-core/fixtures/`. Each `.proto` has a
corresponding `.bin` (compiled `FileDescriptorSet`). Regenerate with:

```bash
cd crates/apb-core/fixtures && ./regenerate.sh
```

Requires `protoc` on PATH. The `.bin` files are committed because CI may
not have `protoc`.

## Code style

- No async anywhere — the library is sync by design.
- Errors use `thiserror` derive. Each module has its own error enum.
- Arrow types: `arrow-array`, `arrow-schema`, `arrow-buffer` v58.
- Proto types: `prost` / `prost-types` for generated structs, `prost-reflect` for runtime descriptor introspection.
- Re-exported Arrow types in `lib.rs`: `Array`, `RecordBatch`.
- Private modules with `pub use` re-exports in `mod.rs` — don't reach into submodules directly.

## Architecture notes

Two-stage pipeline:

1. **Schema Mapping** — `infer_mapping` or `explicit_mapping` produces a
   `FieldMapping`. This resolves field bindings, type checks, and coercion
   decisions. Done once per schema pair.

2. **Transcoding** — `Transcoder::new(&mapping)` precomputes an `EncodingPlan`
   with pre-encoded wire tags, encoder selection, and null handling. The
   transcoder is `Sync` and reused across batches. It writes directly to a
   `Vec<u8>` output buffer.

The `generate` module inverts stage 1: given an Arrow schema, it produces a
`FileDescriptorProto` that can be serialized and fed back into stage 1.

## Protobuf well-known types

`google.protobuf.Timestamp` and `google.protobuf.Duration` are handled
specially. `prost_reflect::DescriptorPool::global()` includes these
descriptors. The `ProtoSchema::from_bytes` constructor uses a fresh pool
(not global), but embeds the `apb.proto` extension automatically.

When working with generated descriptors that reference well-known types,
use `DescriptorPool::global()` as the base pool rather than
`ProtoSchema::from_bytes`.

## Commit Conventions

- Use conventional commits: `feat:`, `fix:`, `test:`, `refactor:`, `style:`, `docs:`
- Update README.md when changing user-facing syntax
- Each commit should pass `make check` independently
