# Plan 02c — Validation Report

## Goal

Produce a structured diagnostic report from a schema mapping attempt. The
report is the primary tool for consuming teams to verify their proto changes
won't break transcoding. It must be usable both as a library type (for
programmatic inspection) and as human-readable CLI output.

## Files

```
crates/apb-core/src/
├── validation/
│   ├── mod.rs        # public API
│   ├── report.rs     # MappingReport, diagnostic types
│   └── render.rs     # human-readable + JSON rendering
└── lib.rs            # re-export validation module
```

## Core types

### `MappingReport`

```rust
pub struct MappingReport {
    /// Proto message fully qualified name.
    pub message_name: String,
    /// Successfully mapped fields.
    pub mapped: Vec<MappedField>,
    /// Arrow fields with no proto counterpart.
    pub unmapped_arrow: Vec<UnmappedArrowField>,
    /// Proto fields with no Arrow counterpart.
    pub unmapped_proto: Vec<UnmappedProtoField>,
    /// Type errors (incompatible or unapproved coercion).
    pub type_errors: Vec<FieldTypeError>,
    /// Structural errors (e.g. oneof target not a struct).
    pub structural_errors: Vec<StructuralError>,
    /// Reports for nested messages (recursive).
    pub nested: Vec<NestedReport>,
    /// Overall status.
    pub status: ReportStatus,
}

pub enum ReportStatus {
    /// All fields valid, no errors.
    Ok,
    /// Mapping succeeded but has warnings (unmapped fields).
    Warnings,
    /// Mapping has errors, transcoding will fail.
    Error,
}
```

### Diagnostics

```rust
pub struct MappedField {
    pub arrow_name: String,
    pub arrow_index: usize,
    pub proto_name: String,
    pub proto_number: u32,
    pub bind_method: BindMethod,
    pub type_check: TypeCheck,
    pub field_shape: FieldShapeSummary,
}

/// Simplified shape for display (no recursive data).
pub enum FieldShapeSummary {
    Scalar,
    Repeated,
    Map,
    Message,
    Oneof,
}

pub struct FieldTypeError {
    pub arrow_name: String,
    pub arrow_type: String,
    pub proto_name: String,
    pub proto_type: String,
    pub reason: TypeErrorReason,
}

pub struct StructuralError {
    pub path: String,
    pub message: String,
}

pub struct NestedReport {
    pub proto_field: String,
    pub report: Box<MappingReport>,
}
```

## Building the report

The report is built from a mapping attempt — not from a successful
`FieldMapping`. It captures both success and failure cases.

```rust
/// Validate an Arrow schema against a proto message and produce a report.
/// This never panics or returns Err — all problems are captured in the report.
pub fn validate(
    arrow_schema: &Schema,
    message: &MessageDescriptor,
    options: &InferOptions,
) -> MappingReport;
```

### Single code path with mapping

The mapping logic (02b) and validation share a single internal implementation.
`infer_mapping` and `validate` both call the same core function that collects
all diagnostics. The difference is presentation:

- `infer_mapping` → if errors exist, return `Err(MappingError)` with the first
  (or all) errors. If ok, return the `FieldMapping`.
- `validate` → always return a `MappingReport` with all diagnostics, plus the
  `FieldMapping` if successful.

```rust
/// Internal: shared mapping + diagnostic collection.
fn resolve_mapping(
    arrow_schema: &Schema,
    message: &MessageDescriptor,
    options: &InferOptions,
) -> (Option<FieldMapping>, MappingReport);
```

This avoids two code paths doing the same schema walking with different error
handling, which would inevitably diverge.

## Rendering

### Human-readable (for CLI)

```
Validation: mypackage.Event
Status: WARNINGS

Mapped fields (4):
  ✓ user_id       → user_id       (#1 string)   name-match
  ✓ uid           → user_id       (#1 string)   annotation
  ✓ amount        → amount        (#3 int64)    name-match
  ✓ payload       → payload       (#4 bytes)    name-match, coerce (utf8 → bytes)

Unmapped Arrow fields (1):
  ⚠ internal_ts   (index 5, Timestamp)

Unmapped proto fields (1):
  ⚠ deprecated_field (#7 string)

Type errors (1):
  ✗ event_data    → metadata      (#6) arrow:Decimal128 ↔ proto:bytes — incompatible

Structural errors (0):
  (none)
```

### JSON (for CI / programmatic use)

Serialize `MappingReport` directly via `serde`. Teams can parse this in CI
scripts to gate deployments.

```rust
impl MappingReport {
    pub fn to_json(&self) -> String;
    pub fn render_human(&self) -> String;
}
```

## Exit codes (for CLI integration)

The CLI `apb validate` command uses the report status to set the exit code:

| Status   | Exit code | Meaning |
|----------|-----------|---------|
| Ok       | 0         | Clean mapping, no issues. |
| Warnings | 0         | Mapping succeeded, unmapped fields present. |
| Error    | 1         | Mapping has errors, transcoding would fail. |

Warnings don't fail CI by default. A `--strict` flag can promote warnings to
errors (exit code 1).

## Tasks

1. **Report types** — `MappingReport`, `MappedField`, `FieldTypeError`,
   `StructuralError`, `NestedReport`, `ReportStatus`.

2. **Validate function** — Run full mapping logic, collect all diagnostics
   instead of short-circuiting. Recurse into nested messages.

3. **Human-readable rendering** — Tabular output with clear symbols
   (✓, ⚠, ✗), grouped by category.

4. **JSON rendering** — Serde serialization of `MappingReport`.

5. **Tests**
   - Clean mapping → status Ok, no errors/warnings.
   - Unmapped fields on both sides → status Warnings, correct lists.
   - Type error → status Error, error details include both type names.
   - Structural error (oneof not struct) → status Error.
   - Nested message errors carry the full field path.
   - Multiple errors in one schema → all collected, not just the first.
   - JSON round-trip: serialize then deserialize, verify contents.
   - Human rendering contains expected field names and symbols.

## Done when

- `validate` produces a complete `MappingReport` that never short-circuits
- Reports include all mapped fields, unmapped fields, type errors, and
  structural errors
- Human-readable output is clear and scannable
- JSON output is parseable for CI integration
- Nested messages produce nested reports with full paths
- All test cases pass
