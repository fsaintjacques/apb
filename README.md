# apb - Arrow to Protobuf transcoder

Converts columnar Arrow data into row-wise protobuf messages using dynamic
binary descriptors — no code generation, no compile-time schema dependency.

## Quick start

```bash
# Build (IPC-only, no DuckDB)
cargo build -p apb-cli --release

# Build with DuckDB support (requires system libduckdb)
cargo build -p apb-cli --release --features duckdb

# Build with bundled DuckDB (no system install needed)
cargo build -p apb-cli --release --features duckdb-bundled
```

## Usage

### Validate a mapping

```bash
apb validate \
  --descriptor schema.bin \
  --message mypackage.MyMessage \
  --query "SELECT * FROM read_parquet('data.parquet') LIMIT 0"
```

Produces a colored side-by-side report showing which Arrow columns map to
which proto fields, with mismatches highlighted. Nested fields are indented.
Exit code 1 on errors, 0 otherwise.

### Transcode data

```bash
# DuckDB query → proto JSON (for inspection)
apb transcode \
  --descriptor schema.bin \
  --message mypackage.MyMessage \
  --query "SELECT * FROM read_parquet('data.parquet')" \
  --out-format proto-jsonl

# Arrow IPC stdin → proto binary (for production)
cat data.arrow | apb transcode \
  --descriptor schema.bin \
  --message mypackage.MyMessage \
  --ipc - \
  --out-format proto-delimited \
  --out output.pb

# BigQuery via DuckDB extension
apb transcode \
  --descriptor schema.bin \
  --message mypackage.MyMessage \
  --query "LOAD bigquery; SELECT * FROM bigquery_scan('project.dataset.table', billing_project='my-project')" \
  --out-format proto-jsonl
```

### Input modes

| Mode | Flag | Description |
|------|------|-------------|
| DuckDB query | `--query "SELECT ..."` | Any SQL — parquet, csv, BigQuery via extensions |
| Arrow IPC | `--ipc <path>` | File or stdin (`--ipc -`) |

DuckDB is an optional feature (`--features duckdb` or `--features duckdb-bundled`).
Without it, only IPC input is available.

### Output formats

| Format | Flag | Description |
|--------|------|-------------|
| Proto binary | `--out-format proto-delimited` | Varint-delimited stream (default) |
| Proto JSON | `--out-format proto-jsonl` | Newline-delimited JSON |
| Arrow IPC | `--out-format arrow-ipc` | BinaryArray column in IPC stream |

### Flags

| Flag | Description |
|------|-------------|
| `--coerce` | Enable all type coercions (e.g. string → enum) |
| `--unknown-enum error\|default\|skip` | Behavior for unknown enum strings |
| `-v` / `-vv` | Verbose logging (info / debug) |
| `--strict` | (validate) Promote unmapped field warnings to errors |
| `--format json` | (validate) Output report as JSON for CI |

## Library usage

```rust
use apb_core::descriptor::ProtoSchema;
use apb_core::mapping::{infer_mapping, InferOptions};
use apb_core::transcode::Transcoder;

let schema = ProtoSchema::from_bytes(&descriptor_bytes)?;
let msg = schema.message("mypackage.MyMessage")?;
let mapping = infer_mapping(&arrow_schema, &msg, &InferOptions::default())?;
let transcoder = Transcoder::new(&mapping)?;

let mut output = Vec::new();
transcoder.transcode_delimited(&batch, &mut output)?;
```

`Transcoder` takes `&self` — it is `Sync` and can be shared across threads.

## Schema mapping

Fields are matched by name (strict, case-sensitive). If names don't match,
use SQL to rename:

```sql
SELECT uid AS user_id, total AS amount FROM my_table
```

### Infer mode

Automatic mapping with a well-defined precedence:

1. **Annotations** — proto field options (`(apb).arrow_name`, `(apb).coerce`)
   that explicitly declare the Arrow column binding. Always takes priority.
2. **Name match** — strict, case-sensitive match between Arrow field name and
   proto field name. No normalization, no fuzzy matching.

### Explicit mode

The caller provides the full mapping directly (e.g. from a config file or API).
No inference is performed. Scalar fields only.

## Type system

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

## Nested types

Supported at arbitrary depth:

| Arrow             | Proto              | Notes                                      |
|-------------------|--------------------|-------------------------------------------|
| `StructArray`     | nested message     | Recursive field matching                   |
| `ListArray`       | `repeated` field   | Packed for numeric scalars                 |
| `MapArray`        | `map<K,V>`         | Proto map key constraints apply            |
| `StructArray`     | `oneof`            | Nullable children, at most one non-null    |

A proto `oneof` maps to an Arrow `StructArray` named after the oneof group.
Each child column corresponds to a variant. The transcoder validates that at
most one child is non-null per row.

## Architecture

```
apb/
├── apb-core       # Library: schema mapping, transcoding, validation
│                   #   No I/O, no async. Accepts Arrow arrays + descriptors,
│                   #   produces bytes/arrays. Embeddable anywhere.
└── apb-cli        # Binary: CLI with DuckDB + IPC input
```

The core library has two stages:

```
┌──────────────────┐       ┌─────────────┐       ┌──────────────┐
│ Proto Descriptor │──────>│   Schema    │──────>│  Transcoder  │
│ + Arrow Schema   │       │   Mapping   │       │              │
└──────────────────┘       └─────────────┘       └──────────────┘
                           FieldMapping           Batch + FieldMapping
                                                  ──> Output
```

**Stage 1: Schema Mapping** — produces a `FieldMapping`, a resolved, validated
binding from Arrow columns to protobuf fields.

**Stage 2: Transcoding** — a `Transcoder` is built once from a `FieldMapping`.
Construction precomputes all schema-dependent work: pre-encoded wire tags,
column index bindings, resolved encoder per field, nested message plans, and
null handling strategy. The transcoder is then called per batch with no further
schema resolution.

### Error handling

Fails the entire batch on any row-level error. The error message is actionable:
which row, which field, what went wrong, and what was expected.

## Future

- `apb generate` — Arrow schema → proto descriptor/IDL generation
- C ABI (`apb-cabi` crate) for embedding in non-Rust applications
- Proto → Arrow reverse transcoding (decode)
