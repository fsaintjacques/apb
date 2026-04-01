# apb — Arrow to Protobuf Transcoder

## Overview

`apb` is a Rust library and CLI that transcodes Arrow record batches into
protobuf messages. It targets teams that define protobuf schemas and need to
convert columnar Arrow data into row-wise protobuf messages with minimal
friction.

The library works with dynamic protobuf binary descriptors
(`FileDescriptorSet`) — no code generation, no compile-time schema dependency.
Consuming teams own their `.proto` definitions; they do not own the deployment
of `apb`.

## Crate Structure

```
apb/
├── apb-core       # Library: schema mapping, transcoding, validation
│                   #   No I/O, no async. Accepts Arrow arrays + descriptors,
│                   #   produces bytes/arrays. Embeddable anywhere.
└── apb-cli        # Binary: CLI with DuckDB + IPC input
```

`apb-core` is the primary library crate. It has no network or filesystem
dependencies — callers provide `FileDescriptorSet` bytes and `RecordBatch`es,
and get back encoded output.

`apb-cli` is the CLI binary with two input modes (DuckDB SQL queries and Arrow
IPC streams) and three output formats. DuckDB is an optional feature.

## Architecture

The core library is split into two stages:

```
┌──────────────────┐       ┌─────────────┐       ┌──────────────┐
│ Proto Descriptor │──────▶│   Schema    │──────▶│  Transcoder  │
│ + Arrow Schema   │       │   Mapping   │       │              │
└──────────────────┘       └─────────────┘       └──────────────┘
                           FieldMapping           Batch + FieldMapping
                                                  ──▶ Output
```

### Stage 1: Schema Mapping

Produces a `FieldMapping` — a resolved, validated binding from Arrow columns to
protobuf fields. Two modes:

#### Infer mode

Automatic mapping with a well-defined precedence:

1. **Annotations** — proto field options (`(apb).arrow_name`, `(apb).coerce`)
   that explicitly declare the Arrow column binding. Always takes priority.
2. **Name match** — strict, case-sensitive match between Arrow field name and
   proto field name. No normalization, no fuzzy matching.

#### Explicit mode

The caller provides the full mapping directly (e.g. from a config file or API).
No inference is performed. Scalar fields only.

### Stage 2: Transcoding

A `Transcoder` is built once from a `FieldMapping`. Construction precomputes
all schema-dependent work:

- Pre-encoded proto wire tags (field number + wire type) as byte slices
- Column index → proto field bindings as a flat array (no per-row lookup)
- Resolved encoder function per field (including coercion)
- Flattened nested message serialization plan
- Null handling strategy per field

The `Transcoder` is then called per batch with no further schema resolution.
Its lifetime is bound to the stream — one transcoder per stream.

```rust
let transcoder = Transcoder::new(&mapping)?;
transcoder.transcode_delimited(&batch, &mut output)?;
```

## Output Formats

### Varint-delimited stream

Standard protobuf length-delimited encoding. Each row becomes one
length-prefixed message written sequentially into a byte buffer.

### Arrow BinaryArray

All serialized messages are written into one contiguous buffer with an offsets
array, forming a `BinaryArray`. Zero per-row allocation. Can be forwarded
through Arrow Flight or written to Parquet as a binary column.

### Proto JSON (via CLI)

Newline-delimited JSON using prost-reflect's proto-canonical JSON
serialization. Useful for debugging and inspection.

## Type System

### Strict by default

Arrow types must match their corresponding proto wire types exactly (or be in
a safe, lossless set). Mismatches are mapping errors caught at validation time.

### Lossless mappings

| Arrow               | Proto          |
|---------------------|---------------|
| Boolean             | bool          |
| Int32               | int32, sint32, sfixed32 |
| Int64               | int64, sint64, sfixed64 |
| UInt32              | uint32, fixed32 |
| UInt64              | uint64, fixed64 |
| Float32             | float         |
| Float64             | double        |
| Utf8 / LargeUtf8    | string        |
| Binary / LargeBinary | bytes        |
| Int32               | enum          |
| Timestamp(*, *)     | google.protobuf.Timestamp |
| Duration(*)         | google.protobuf.Duration |

### Opt-in coercion

Coercion is available per-field via annotation or globally via `--coerce`:

- Integer narrowing/widening (Int64 → int32, truncation check)
- Float narrowing (Float64 → float, precision loss)
- String/bytes crossover (Utf8 → bytes, Binary → string)
- String → enum (runtime name lookup)
- Temporal → integer (Timestamp → int64, Date32 → int32)

### String → enum encoding

String values are looked up in a precomputed `HashMap<String, i32>` of enum
variant names. Unknown values are handled by `--unknown-enum`:

- `error` — fail the batch (default)
- `default` — write 0 (proto3 zero variant)
- `skip` — omit the field

### Dictionary arrays

`DictionaryArray` is resolved to its value type before compatibility checking.
Dictionary-encoded strings are common in Arrow data from Parquet and BigQuery.

## Nested Types

Supported at arbitrary depth:

| Arrow             | Proto              | Notes                                      |
|-------------------|--------------------|-------------------------------------------|
| `StructArray`     | nested message     | Recursive field matching                   |
| `ListArray`       | `repeated` field   | Packed for numeric scalars                 |
| `MapArray`        | `map<K,V>`         | Proto map key constraints apply            |
| `StructArray`     | `oneof`            | Nullable children, at most one non-null    |

### Oneof mapping

A proto `oneof` maps to an Arrow `StructArray` named after the oneof group.
Each child column corresponds to a variant. The transcoder validates that at
most one child is non-null per row.

## CLI

### Input modes

| Mode | Flag | Description |
|------|------|-------------|
| DuckDB query | `--query "SELECT ..."` | Any SQL — parquet, csv, BigQuery via extensions |
| Arrow IPC | `--ipc <path>` | File or stdin (`--ipc -`) |

DuckDB is an optional feature (`--features duckdb` or `--features duckdb-bundled`).
Without it, only IPC input is available.

### `apb validate`

Colored side-by-side view showing Arrow columns mapped to proto fields in
proto field number order. IDL-style proto field display. Nested fields
indented. Unmapped fields highlighted. Exit code 1 on errors, 0 otherwise.
`--strict` promotes warnings to errors.

### `apb transcode`

Stream batches from input, transcode, write output.

| Flag | Description |
|------|-------------|
| `--out-format` | `proto-delimited` (default), `proto-jsonl`, `arrow-ipc` |
| `--out <path>` | Output file (default: stdout) |
| `--coerce` | Enable all type coercions globally |
| `--unknown-enum` | `error`, `default`, or `skip` for invalid enum strings |
| `-v` / `-vv` | Verbose logging (info / debug) |

## Error Handling

Fail the entire batch on any row-level error. The error message is actionable:
which row, which field, what went wrong, and what was expected.

## Validation Report

The `validate` command produces a structured report usable both for humans
(colored terminal output) and machines (JSON via `--format json`). The report
includes:

- Mapped fields with binding method and type mode
- Unmapped Arrow fields (no proto counterpart)
- Unmapped proto fields (missing from query)
- Type errors with reasons
- Structural errors (oneof not a struct, etc.)
- Nested sub-reports for composite fields

## Future

- `apb generate` — Arrow schema → proto descriptor/IDL generation
- C ABI (`apb-cabi` crate) for embedding in non-Rust applications
- Proto → Arrow reverse transcoding (decode)
- Performance tuning (SIMD, vectorized encoding)
