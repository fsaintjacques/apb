# apb — Implementation Plan

## Chunks

| #  | Name                              | Depends on | Crate(s)     |
|----|-----------------------------------|------------|--------------|
| 01 | Project scaffold + descriptor parsing | —          | apb-core     |
| 02a | Type system                       | 01         | apb-core     |
| 02b | Schema mapping                    | 02a        | apb-core     |
| 02c | Validation report                 | 02b        | apb-core     |
| 03 | Transcoder — scalar fields        | 02b        | apb-core     |
| 04 | Transcoder — nested types         | 03         | apb-core     |
| 05 | Source adapters                   | 01         | apb-source   |
| 06 | CLI                               | 02c, 04, 05 | apb-cli      |

## Dependency graph

```
01
├──▶ 02a ──▶ 02b ──▶ 02c ─────────┐
│                 │                 │
│                 └──▶ 03 ──▶ 04 ──┤
└──▶ 05 ───────────────────────────┤
                                   ▼
                                  06
```

## Chunk summaries

### 01 — Project scaffold + descriptor parsing

Cargo workspace with `apb-core`, `apb-source`, `apb-cli` crates (source and
cli as stubs). Parse `FileDescriptorSet` via `prost-reflect` into an internal
type model for proto descriptors (messages, fields, types, oneof groups).

### 02a — Type system

Arrow↔proto type compatibility matrix. Define which Arrow types map to which
proto field types losslessly. Define coercion rules (opt-in per field). Produce
clear type error messages on mismatch. Thorough test coverage of the matrix.

### 02b — Schema mapping

`FieldMapping` type — the resolved binding from Arrow columns to proto fields.
Infer mode: annotations take priority, strict name-match fills gaps. Explicit
mode: caller provides the full mapping. Each binding is validated against the
type system from 02a.

### 02c — Validation report

`MappingReport` type with structured diagnostics: mapped fields (with binding
method), unmapped Arrow fields, unmapped proto fields, type errors, oneof
structural issues. Library returns structured data; a rendering layer produces
human-readable output for CLI use.

### 03 — Transcoder — scalar fields

`Transcoder` built once from a `FieldMapping`. Precomputes wire tags, column
bindings, encoder functions, null handling. Encodes all scalar proto types.
Two output modes: varint-delimited stream and Arrow `BinaryArray`. Batch-level
error handling with actionable messages (row, field, reason).

### 04 — Transcoder — nested types

Extend the transcoder with recursive serialization: `StructArray` → nested
message, `ListArray` → repeated, `MapArray` → proto map, `StructArray` → oneof
(with at-most-one-non-null validation). Arbitrary nesting depth via a flattened
serialization plan.

### 05 — Source adapters

`apb-source` crate. Pluggable descriptor fetching: fs, https, object store
(gs://, s3://). Arrow data readers: Arrow IPC (file/stdin), Arrow Flight
client, BigQuery Storage Read API client. Fetch once at startup, no caching.

### 06 — CLI

`apb-cli` binary. `apb validate`: takes descriptor URI + Arrow schema, prints
diagnostic report. `apb transcode`: reads from source (Flight/BQ/IPC),
transcodes to output format (proto-delimited, proto-jsonl, arrow-ipc), writes
to stdout or file. Wires `apb-core` and `apb-source` together.

## Detailed plans

Each chunk has a dedicated document:

- [plan-01-scaffold.md](plan-01-scaffold.md)
- [plan-02a-type-system.md](plan-02a-type-system.md)
- [plan-02b-schema-mapping.md](plan-02b-schema-mapping.md)
- [plan-02c-validation-report.md](plan-02c-validation-report.md)
- [plan-03-transcoder-scalars.md](plan-03-transcoder-scalars.md)
- [plan-04-transcoder-nested.md](plan-04-transcoder-nested.md)
- [plan-05-source-adapters.md](plan-05-source-adapters.md)
- [plan-06-cli.md](plan-06-cli.md)
