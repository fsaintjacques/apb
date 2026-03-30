# apb — Arrow to Protobuf Transcoder

## Overview

`apb` is a Rust library and CLI that transcodes Arrow record batches into
protobuf messages. It targets teams that define protobuf schemas and need to
convert columnar Arrow Flight streams into row-wise protobuf messages with
minimal friction.

The library works with dynamic protobuf binary descriptors
(`FileDescriptorSet`) — no code generation, no compile-time schema dependency.
Consuming teams own their `.proto` definitions; they do not own the deployment
of `apb`.

## Crate Structure

```
apb/
├── apb-core       # Library: schema mapping, transcoding, validation
│                   #   No I/O, no async. Accepts Arrow arrays + descriptors,
│                   #   produces bytes/arrays. This is what other Rust projects
│                   #   embed.
├── apb-source     # Library: descriptor + Arrow data source adapters
│                   #   (fs, https, object store, Flight, BQ Storage Read API)
│                   #   Async, pluggable. Optional dependency.
└── apb-cli        # Binary: CLI wrapping core + source
```

`apb-core` is the primary library crate. It has no network or filesystem
dependencies — callers provide `FileDescriptorSet` bytes and `RecordBatch`es,
and get back encoded output. This keeps the library embeddable in any context
(WASM, embedded services, other CLIs).

`apb-source` handles I/O: fetching descriptors from remote sources and reading
Arrow data from Flight/BQ/IPC. It depends on `apb-core`.

`apb-cli` is a thin binary that wires together `apb-core` and `apb-source`
behind a CLI interface.

## Architecture

The core library is split into two independent stages:

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

1. **Annotations** — proto field/message options that explicitly declare the
   Arrow column binding. Always takes priority when present.
2. **Name match** — strict, case-sensitive match between Arrow field name and
   proto field name. No normalization, no fuzzy matching. Used for fields
   without annotations.

#### Explicit mode

The caller provides the full mapping directly (e.g. from a config file or API).
No inference is performed.

### Stage 2: Transcoding

A `Transcoder` is built once from a `FieldMapping`. Construction precomputes
all schema-dependent work:

- Pre-encoded proto wire tags (field number + wire type) as byte slices
- Column index → proto field bindings as a flat array (no per-row lookup)
- Resolved encoder function per field (including coercion)
- Flattened nested message serialization plan
- Null handling strategy per field (from Arrow schema nullability)

The `Transcoder` is then called per batch with no further schema resolution.
Its lifetime is bound to the stream — one transcoder per stream.

```rust
let transcoder = Transcoder::new(&mapping)?;  // once per stream
transcoder.transcode(&batch, &mut output)?;   // per batch, cheap
```

The transcoder is independent of how the mapping was derived.

## Output Formats

### Varint-delimited stream

Standard protobuf length-delimited encoding. Each row becomes one
length-prefixed message written sequentially into a single byte buffer.

### Arrow-native binary column

All serialized messages are written into one contiguous buffer with an offsets
array, forming a `BinaryArray`. This avoids per-row allocation and allows the
output to be forwarded through Arrow Flight or written to Parquet without
copying.

## Type System

### Strict by default

Arrow types must match their corresponding proto wire types exactly (or be in a
safe, lossless set — e.g. Arrow `Int64` → proto `int64`). Mismatches are
mapping errors caught at validation time.

### Opt-in coercion

Coercion (e.g. Arrow `Int64` → proto `int32`) is available per-field via
annotation. The field owner explicitly opts in and accepts the risk of
truncation or precision loss.

## Nested Types

Supported from v1 with arbitrary depth.

| Arrow             | Proto              | Notes                                      |
|-------------------|--------------------|-------------------------------------------|
| `StructArray`     | nested message     |                                            |
| `ListArray`       | `repeated` field   |                                            |
| `MapArray`        | `map<K,V>`         | Proto map key constraints apply            |
| `StructArray`     | `oneof`            | Nullable children, at most one non-null per row |

### Oneof mapping

A proto `oneof` maps to an Arrow `StructArray` where each child is a nullable
column corresponding to a oneof variant. The transcoder validates that at most
one child is non-null per row.

## Descriptor Loading

The library accepts a `FileDescriptorSet` as parsed bytes — no I/O concerns.

The CLI handles fetching from pluggable sources:

| Source         | Scheme       |
|---------------|-------------|
| Local file    | `file://`   |
| HTTPS         | `https://`  |
| Object store  | `gs://`, `s3://` |

Descriptors are fetched once at startup and held for the lifetime of the
process. No refresh or caching logic.

## CLI

The CLI is both a validation tool and a standalone transcoding pipeline.

### `apb validate`

Takes a proto descriptor and an Arrow schema, produces a diagnostic report:

- **Mapped fields** — which Arrow columns bind to which proto fields, and how
  (annotation vs name-match).
- **Unmapped Arrow fields** — columns with no proto counterpart (warning).
- **Unmapped proto fields** — proto fields with no Arrow counterpart (warning).
- **Type errors** — incompatible types without coercion annotation (error).
- **Oneof violations** — structural issues in oneof-mapped structs (error).

Enables consuming teams to run validation in CI when evolving their proto
schemas.

### `apb transcode`

Reads Arrow data from a source, transcodes to protobuf, writes to stdout or
file.

#### Input sources

| Source                        | Flag / scheme     |
|-------------------------------|-------------------|
| Arrow Flight endpoint         | `--flight <url>`  |
| BigQuery Storage Read API     | `--bq <table>`    |
| Arrow IPC file / stdin        | `--ipc <path>`    |

#### Output formats

| Format                        | Flag              |
|-------------------------------|-------------------|
| Protobuf binary (delimited)   | `--out-format=proto-delimited` (default) |
| Protobuf JSON (newline-delimited) | `--out-format=proto-jsonl` |
| Arrow IPC (binary column)     | `--out-format=arrow-ipc`     |

Output goes to stdout by default, or to a file with `--out <path>`.

## Error Handling

Fail the entire batch on any row-level error. The error message must be
actionable: which row, which field, what went wrong, and what the expected
type/value was.

## C ABI

`apb-core` is designed to be exposed as a C shared library via a thin
`apb-cabi` crate. The core's sync, no-I/O design means nothing async crosses
the FFI boundary.

### API surface

| Function | Signature sketch |
|----------|-----------------|
| Descriptor loading | `apb_descriptor_parse(buf, len) -> *mut Descriptor` |
| Schema mapping | `apb_mapping_infer(descriptor, arrow_schema) -> *mut Mapping` |
| Transcoder | `apb_transcoder_new(mapping) -> *mut Transcoder` |
| Transcode (delimited) | `apb_transcode_delimited(transcoder, batch, out_buf, out_len) -> status` |
| Transcode (arrow) | `apb_transcode_arrow(transcoder, batch, out_array, out_schema) -> status` |
| Validation | `apb_validate(descriptor, arrow_schema) -> *const c_char` (JSON report) |
| Error detail | `apb_last_error() -> *const c_char` |
| Free functions | `apb_*_free(ptr)` per opaque type |

### Arrow interop

Arrow data crosses the boundary via the
[Arrow C Data Interface](https://arrow.apache.org/docs/format/CDataInterface.html)
(`ArrowArray` + `ArrowSchema` structs). Zero-copy, supported by `arrow-rs` and
every major Arrow implementation (PyArrow, Go, C++, Java).

### Ownership

- Opaque pointers returned by `apb_*_new` / `apb_*_parse` are owned by the
  caller and must be freed with the corresponding `apb_*_free`.
- Output buffers for delimited mode are library-owned and valid until the next
  call or free. Caller must copy if needed beyond that.
- Error strings from `apb_last_error` are library-owned, valid until the next
  call on the same thread.

### Scope

Not in v1. The `apb-core` API is designed so that adding `apb-cabi` later
requires no changes to the core — just a thin wrapper crate.

## Scope

### v1

- Schema mapping: infer (name + annotations) and explicit modes
- Transcoding: all output formats (proto-delimited, proto-jsonl, arrow-ipc)
- Nested types: struct, list, map, oneof at arbitrary depth
- Strict type matching with opt-in coercion via annotation
- CLI: `validate` and `transcode` subcommands
- Input sources: Arrow Flight, BigQuery Storage Read API, Arrow IPC
- Descriptor loading: fs, https, object store (gs, s3)
- Batch-level error handling with actionable messages

### Future

- Performance tuning (SIMD, vectorized encoding)
- Observability and metrics
- Additional object store backends
- Proto → Arrow reverse transcoding
