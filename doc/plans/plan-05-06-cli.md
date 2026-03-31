# Plan 05+06 — CLI (merged)

## Goal

Build the `apb-cli` binary with two input modes (DuckDB query, Arrow IPC
stream) and three output formats. Drop the `apb-source` crate — DuckDB
replaces file/remote source adapters, and Arrow IPC stdin covers piping.

## Crate changes

```
crates/
├── apb-core/        # unchanged
├── apb-source/      # DELETE — no longer needed
└── apb-cli/
    ├── Cargo.toml
    └── src/
        ├── main.rs        # entrypoint, clap
        ├── input.rs       # DuckDB + IPC input sources
        ├── output.rs      # output format writers
        ├── validate.rs    # validate subcommand
        └── transcode.rs   # transcode subcommand
```

## Dependencies

| Crate        | Purpose                    |
|--------------|----------------------------|
| `apb-core`   | Mapping, transcoding, validation |
| `clap`       | Argument parsing (derive)  |
| `duckdb`     | DuckDB query engine (Arrow batches) |
| `arrow-ipc`  | IPC stream reader + writer |
| `arrow-array` | RecordBatch, Array types  |
| `arrow-schema` | Schema types             |
| `prost-reflect` | proto-jsonl via DynamicMessage |
| `serde_json` | JSON output for validate   |

## Input modes

### DuckDB query (`--query`)

```
apb transcode \
  --descriptor desc.bin \
  --message mypackage.MyMessage \
  --query "SELECT * FROM read_parquet('data.parquet')"
```

- Opens an in-memory DuckDB connection
- Executes the SQL query
- Iterates Arrow batches via `stmt.query_arrow([])`
- Covers: parquet, csv, json (local + GCS/S3 via httpfs extension)

DuckDB is also used for integration tests — generate test data with SQL,
transcode it, decode with prost-reflect, verify values.

### Arrow IPC stream (`--ipc`)

```
# From file
apb transcode --descriptor desc.bin --message pkg.Msg --ipc data.arrow

# From stdin (pipe from Flight, BQ, or another tool)
cat data.arrow | apb transcode --descriptor desc.bin --message pkg.Msg --ipc -
```

- Uses `arrow-ipc` `StreamReader`
- `-` or omitted path reads from stdin
- Zero dependencies beyond `arrow-ipc`

## CLI structure

```
apb <subcommand> [options]

Common options:
  --descriptor <path>     Proto descriptor file (FileDescriptorSet binary)
  --message <name>        Fully qualified proto message name

Subcommands:
  validate    Validate a mapping between an Arrow schema and proto message
  transcode   Read Arrow data, transcode to protobuf, write output
```

### `apb validate`

```
apb validate \
  --descriptor desc.bin \
  --message mypackage.MyMessage \
  --query "SELECT * FROM read_parquet('data.parquet') LIMIT 0"  # schema only
  [--ipc schema.arrow]                  # or from IPC file
  [--strict]                            # promote warnings to errors
  [--format human|json]                 # output format (default: human)
```

Flow:
1. Load descriptor, look up message.
2. Get Arrow schema from query (LIMIT 0) or IPC file.
3. Run `validate(arrow_schema, message, options)`.
4. Render report (human or JSON).
5. Exit 0 (ok/warnings) or 1 (errors). `--strict` makes warnings exit 1.

### `apb transcode`

```
apb transcode \
  --descriptor desc.bin \
  --message mypackage.MyMessage \
  --query "SELECT * FROM read_parquet('data.parquet')"  # or --ipc
  [--out-format proto-delimited|proto-jsonl|arrow-ipc]  # default: proto-delimited
  [--out path/to/output]                                # default: stdout
```

Flow:
1. Load descriptor, look up message.
2. Connect to input (DuckDB or IPC reader), get schema.
3. Infer mapping.
4. Build `Transcoder` from mapping.
5. Stream batches → transcode → write output.
6. On error, print actionable message to stderr, exit 1.

## Output formats

| Format                        | Flag              |
|-------------------------------|-------------------|
| Protobuf binary (delimited)   | `--out-format=proto-delimited` (default) |
| Protobuf JSON (newline-delimited) | `--out-format=proto-jsonl` |
| Arrow IPC (binary column)     | `--out-format=arrow-ipc`     |

### Proto-jsonl

Uses prost-reflect `DynamicMessage` to decode each proto message and serialize
to JSON. Known limitation: encode-then-decode round-trip is slower than
direct Arrow→JSON.

### Arrow IPC output

Wraps `BinaryArray` output in a single-column RecordBatch (column: `message`),
writes via `arrow-ipc` `StreamWriter`.

## Stderr output

Progress to stderr (clean stdout for piping):
- `Loaded descriptor: 5 messages`
- `Mapping: 10/12 fields mapped`
- `Transcoding: 1,000,000 rows in 42 batches`
- `Done.`

Suppress with `--quiet`.

## Descriptor loading

For v1, descriptors are loaded from local files only. The user fetches from
GCS/S3/HTTP themselves (e.g. `gsutil cp gs://... desc.bin`). This avoids
adding reqwest/object_store dependencies to the CLI.

Future: add `--descriptor gs://...` support.

## Tasks

1. **Delete apb-source** — remove the stub crate.

2. **CLI scaffold** — clap derive, subcommands, common options.

3. **DuckDB input** — open connection, execute query, iterate Arrow batches.

4. **IPC input** — `StreamReader` from file or stdin.

5. **Validate subcommand** — load descriptor, get schema, run validation,
   render report, set exit code.

6. **Transcode subcommand** — load descriptor, get schema, build mapping +
   transcoder, stream batches, write output.

7. **Output writers** — proto-delimited, proto-jsonl, arrow-ipc.

8. **Integration tests** — use DuckDB to generate test data:
   - Create table with all scalar types, transcode, decode, verify.
   - Nested struct, repeated, map fields.
   - Pipe IPC from stdin.
   - Validate with type mismatches → correct report.
   - End-to-end: DuckDB → proto-delimited → decode with prost.

## Done when

- `apb validate` produces correct reports from DuckDB query or IPC
- `apb transcode` reads from DuckDB or IPC, writes all three output formats
- Integration tests pass end-to-end
- `--quiet` suppresses stderr
- Error messages are actionable
