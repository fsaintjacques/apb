# apb

Arrow to Protobuf transcoder library and cli. Converts columnar Arrow data
into row-wise protobuf messages using dynamic binary descriptors.

## Quick start

```bash
# Build (IPC-only, no DuckDB)
cargo build -p apb-cli --release

# Build with DuckDB support
cargo build -p apb-cli --release --features duckdb

# Build with bundled DuckDB (no system install needed)
cargo build -p apb-cli --release --features duckdb-bundled
```

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

## Usage

### Validate a mapping

```bash
apb validate \
  --descriptor schema.bin \
  --message mypackage.MyMessage \
  --query "SELECT * FROM read_parquet('data.parquet') LIMIT 0"
```

Produces a colored side-by-side report showing which Arrow columns map to
which proto fields, with mismatches highlighted.

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

## Schema mapping

Fields are matched by name (strict, case-sensitive). If names don't match,
use SQL to rename:

```sql
SELECT uid AS user_id, total AS amount FROM my_table
```

For fields that need type coercion (e.g. string columns to proto enums), use
`--coerce` or add proto annotations:

```proto
message Event {
  string user_id = 1 [(apb.apb).arrow_name = "uid"];
  int32 count = 2 [(apb.apb).coerce = true];
}
```

## Supported types

All proto scalar types, nested messages (arbitrary depth), repeated fields
(packed for numerics), map fields, oneof groups, enums (int32 and string),
`google.protobuf.Timestamp`, and `google.protobuf.Duration`.


## Architecture

See [doc/HIGHLEVEL.md](doc/HIGHLEVEL.md) for design details and
[doc/PLAN.md](doc/PLAN.md) for implementation history.
