# Plan 06 — CLI

## Goal

Build the `apb-cli` binary that wires together `apb-core` and `apb-source`
into two user-facing subcommands: `validate` and `transcode`.

## Files

```
crates/apb-cli/
├── Cargo.toml
└── src/
    ├── main.rs        # entrypoint, clap arg parsing
    ├── validate.rs    # validate subcommand
    ├── transcode.rs   # transcode subcommand
    └── output.rs      # output format writers
```

## Dependencies

| Crate        | Purpose                    |
|--------------|----------------------------|
| `apb-core`   | Mapping, transcoding, validation |
| `apb-source` | Descriptor + Arrow sources |
| `clap`       | Argument parsing (derive)  |
| `tokio`      | Async runtime              |
| `serde_json` | JSON output for validate   |
| `arrow-ipc`  | IPC file writer for arrow-ipc output |

## CLI structure

```
apb <subcommand> [options]

Common options:
  --descriptor <uri>      Proto descriptor URI (file, https, gs, s3)
  --message <name>        Fully qualified proto message name

Subcommands:
  validate    Validate a mapping between an Arrow schema and proto message
  transcode   Read Arrow data, transcode to protobuf, write output
```

### `apb validate`

```
apb validate \
  --descriptor gs://bucket/descriptor.bin \
  --message mypackage.MyMessage \
  --arrow-schema <source>               # Arrow schema source (see below)
  [--strict]                             # promote warnings to errors
  [--format human|json]                  # output format (default: human)
```

Arrow schema source (one of):
- `--arrow-schema-ipc <path>` — read schema from an IPC file (no data needed)
- `--arrow-schema-json <path>` — read schema from Arrow JSON schema file
- `--flight <url>` — connect to Flight endpoint to get schema
- `--bq <table>` — connect to BQ to get table schema

The first two are offline and don't require connecting to a live source —
essential for CI validation where the data source may not be accessible.

Flow:
1. Fetch descriptor via `apb-source`.
2. Parse into `ProtoSchema`, look up message.
3. Obtain Arrow schema (from file or live source).
4. Run `validate(arrow_schema, message, options)`.
5. Render the `MappingReport` (human or JSON).
6. Exit 0 (ok/warnings) or 1 (errors). `--strict` makes warnings exit 1.

### `apb transcode`

```
apb transcode \
  --descriptor gs://bucket/descriptor.bin \
  --message mypackage.MyMessage \
  --flight grpc://host:port             # input source (one of)
  --bq project.dataset.table            # input source (one of)
  --ipc path/to/file.arrow              # input source (one of, - for stdin)
  [--out-format proto-delimited|proto-jsonl|arrow-ipc]  # default: proto-delimited
  [--out path/to/output]                # default: stdout
```

Flow:
1. Fetch descriptor via `apb-source`.
2. Parse into `ProtoSchema`, look up message.
3. Connect to Arrow source, get schema.
4. Infer mapping (or load explicit mapping — future flag).
5. Build `Transcoder` from mapping.
6. Stream batches → transcode → write output.
7. On error, print actionable message to stderr, exit 1.

### Output writers

```rust
trait OutputWriter {
    /// Write a transcoded batch.
    fn write_batch(&mut self, batch: &RecordBatch, transcoder: &Transcoder) -> Result<()>;
    /// Flush and finalize output.
    fn finish(&mut self) -> Result<()>;
}
```

**`DelimitedWriter`** — writes varint-delimited proto to a `Write` sink
(stdout or file). Calls `transcoder.transcode_delimited` per batch, writes
bytes.

**`JsonlWriter`** — encodes each row as a JSON object using prost-reflect's
JSON serialization (`MessageDescriptor` + raw proto bytes → JSON). Writes one
JSON object per line.

**`ArrowIpcWriter`** — wraps `arrow-ipc` `FileWriter`. Calls
`transcoder.transcode_arrow` per batch, wraps the `BinaryArray` in a single-
column `RecordBatch` (column name: `message`), writes via IPC writer.

## Proto JSON output

Proto-jsonl output requires deserializing the proto bytes back into a
dynamic message for JSON rendering. This uses `prost-reflect`:

```rust
let message = DynamicMessage::decode(message_descriptor, &proto_bytes)?;
let json = message.to_json();
```

This is inherently slower than binary output — it encodes to proto then
decodes back to produce JSON. Acceptable for debugging and low-throughput use
cases. Document this tradeoff.

**Known limitation:** An alternative is to skip proto encoding entirely and go
Arrow → JSON directly using the proto schema for field names/numbers. This
would avoid the encode-then-decode round-trip and is a planned future
optimization. For v1, the encode-then-decode path is acceptable since
proto-jsonl is a debugging/inspection tool, not a high-throughput path.

## Stderr output

Progress and diagnostics go to stderr so stdout is clean for piping:

- `Fetching descriptor from gs://...`
- `Connected to Flight endpoint grpc://...`
- `Schema: 12 Arrow fields, 10 proto fields`
- `Mapping: 10 mapped, 2 unmapped Arrow, 0 unmapped proto`
- `Transcoding: 1,000,000 rows in 42 batches`
- `Done.`

Suppress with `--quiet`.

## Tasks

1. **Clap arg parsing** — Define CLI structure with clap derive macros.
   Common options, validate subcommand, transcode subcommand.

2. **Validate subcommand** — Fetch descriptor, connect to source for schema,
   run validation, render report, set exit code.

3. **Transcode subcommand** — Fetch descriptor, connect to source, build
   mapping + transcoder, stream batches through, write output.

4. **DelimitedWriter** — Write proto-delimited to stdout/file.

5. **JsonlWriter** — Encode each row as JSON via prost-reflect
   `DynamicMessage`. One JSON object per line.

6. **ArrowIpcWriter** — Wrap `BinaryArray` output in IPC file format.

7. **Stderr progress** — Status messages to stderr. `--quiet` flag.

8. **Error handling** — Catch all errors, print actionable messages to stderr,
   exit 1. Include context: which stage failed (fetch, connect, map,
   transcode), what went wrong, what to check.

9. **Tests**
   - End-to-end: IPC file → proto-delimited stdout. Decode output with
     prost, verify values.
   - End-to-end: IPC file → proto-jsonl stdout. Parse JSON lines, verify
     field names and values.
   - End-to-end: IPC file → arrow-ipc file. Read back with IPC reader,
     verify BinaryArray contents.
   - Validate: correct report output for a clean mapping.
   - Validate: correct error output and exit code 1 for type mismatch.
   - Validate --strict: unmapped fields → exit code 1.
   - Validate --format json: parseable JSON output.
   - Missing descriptor → clear error message.
   - Invalid message name → clear error message.
   - --quiet suppresses stderr.

## Done when

- `apb validate` produces correct human and JSON reports with proper exit codes
- `apb transcode` reads from IPC/Flight/BQ and writes all three output formats
- Proto-jsonl output produces valid JSON with correct field names
- Arrow-ipc output is a valid IPC file with a BinaryArray column
- Stderr progress messages are informative and suppressible
- All end-to-end tests pass
- Error messages are actionable across all failure modes
