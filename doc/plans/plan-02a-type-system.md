# Plan 02a — Type System

## Goal

Define the compatibility matrix between Arrow data types and protobuf field
types. This module answers one question: given an Arrow type and a proto field
type, are they compatible — and if not, is there a coercion available?

## Files

```
crates/apb-core/src/
├── types/
│   ├── mod.rs          # public API
│   ├── compatibility.rs # compatibility checks
│   └── coercion.rs     # opt-in coercion rules
└── lib.rs              # re-export types module
```

## Compatibility matrix

### Lossless (always allowed)

| Arrow type         | Proto type     | Wire type |
|--------------------|---------------|-----------|
| `Boolean`          | `bool`        | varint    |
| `Int32`            | `int32`       | varint    |
| `Int32`            | `sint32`      | varint (zigzag) |
| `Int32`            | `sfixed32`    | fixed32   |
| `Int64`            | `int64`       | varint    |
| `Int64`            | `sint64`      | varint (zigzag) |
| `Int64`            | `sfixed64`    | fixed64   |
| `UInt32`           | `uint32`      | varint    |
| `UInt32`           | `fixed32`     | fixed32   |
| `UInt64`           | `uint64`      | varint    |
| `UInt64`           | `fixed64`     | fixed64   |
| `Float32`          | `float`       | fixed32   |
| `Float64`          | `double`      | fixed64   |
| `Utf8` / `LargeUtf8` | `string`   | length-delimited |
| `Binary` / `LargeBinary` | `bytes` | length-delimited |
| `Int32`            | `enum`        | varint (runtime range check) |

### Coercion (opt-in via annotation)

| Arrow type | Proto type | Risk |
|-----------|-----------|------|
| `Int64`   | `int32` / `sint32` / `sfixed32` | Truncation |
| `Int32`   | `int64` / `sint64` / `sfixed64` | None (widening, but type mismatch without annotation) |
| `UInt64`  | `uint32` / `fixed32` | Truncation |
| `UInt32`  | `uint64` / `fixed64` | None (widening) |
| `Float64` | `float`   | Precision loss |
| `Utf8`    | `bytes`   | Semantic (valid UTF-8 → opaque bytes) |
| `Binary`  | `string`  | May fail at runtime (bytes may not be valid UTF-8) |
| `Int64`   | `enum`    | Truncation + runtime range check |
| `Utf8`    | `enum`    | Runtime name lookup (value must match variant name) |
| `Timestamp(*, *)` | `int64` / `sfixed64` | Semantic (raw epoch value, unit-dependent) |
| `Date32`  | `int32`   | Semantic (days since epoch) |
| `Date64`  | `int64`   | Semantic (millis since epoch) |

### Dictionary arrays

`DictionaryArray` is not a leaf type — it is resolved to its value type before
compatibility is checked. For example, `Dictionary(Int8, Utf8)` is checked as
`Utf8`. This is important because dictionary-encoded strings are common in
Arrow data from Parquet and BigQuery.

For proto `enum` fields specifically, `Dictionary(Int*, Utf8)` resolves to
the `Utf8 → enum` coercion path (name lookup).

### Well-known proto types

These map Arrow temporal types to proto messages (not scalars):

| Arrow type | Proto type | Compatibility |
|-----------|-----------|---------------|
| `Timestamp(*, *)` | `google.protobuf.Timestamp` | Compatible (lossless, unit converted to seconds + nanos) |
| `Duration(*)` | `google.protobuf.Duration` | Compatible (lossless, unit converted to seconds + nanos) |

These are handled as special cases in `check_compatibility` — an Arrow scalar
mapping to a specific proto message.

### Not supported

| Arrow type | Notes |
|-----------|-------|
| `Interval` | No natural proto equivalent. |
| `Decimal128`, `Decimal256` | No proto equivalent. |
| `Null` | No proto equivalent. |

## Core types

### `TypeCompatibility`

Result of checking an Arrow type against a proto field type.

```rust
pub enum TypeCompatibility {
    /// Types match losslessly.
    Compatible,
    /// Coercion is available but must be opted in.
    CoercionAvailable { risk: CoercionRisk },
    /// No mapping exists.
    Incompatible { reason: String },
}

pub enum CoercionRisk {
    /// Value may be truncated (e.g. i64 → i32).
    Truncation,
    /// Precision may be lost (e.g. f64 → f32).
    PrecisionLoss,
    /// May fail at runtime for some values (e.g. bytes → string, string → enum).
    RuntimeCheck,
    /// Semantic change, no data loss (e.g. utf8 → bytes, timestamp → int64).
    Semantic,
    /// No risk, widening conversion.
    Lossless,
}
```

### `TypeCheck`

The resolved decision for a single field binding. Carries the concrete Arrow
and proto types so downstream consumers (the transcoder's encoding plan) can
select the correct encoder function without re-resolving types.

```rust
pub struct TypeCheck {
    /// The Arrow DataType (resolved through Dictionary if needed).
    pub arrow_type: DataType,
    /// The proto Kind (scalar type) or enum descriptor.
    pub proto_kind: Kind,
    /// How the types relate.
    pub mode: TypeCheckMode,
}

pub enum TypeCheckMode {
    /// Use the lossless encoder.
    Direct,
    /// Use a coercion encoder. Only valid if annotation opts in.
    Coerce { risk: CoercionRisk },
}
```

### Public API

```rust
/// Check compatibility between an Arrow data type and a proto field type.
pub fn check_compatibility(
    arrow_type: &DataType,
    proto_kind: Kind,
) -> TypeCompatibility;

/// Resolve the type check for a field binding.
/// Returns error if incompatible, or if coercion is needed but not opted in.
pub fn resolve_type_check(
    arrow_type: &DataType,
    proto_kind: Kind,
    coercion_allowed: bool,
) -> Result<TypeCheck, TypeError>;
```

### `TypeError`

```rust
pub struct TypeError {
    pub arrow_type: DataType,
    pub proto_type: String,
    pub reason: TypeErrorReason,
}

pub enum TypeErrorReason {
    /// No mapping exists between these types.
    Incompatible,
    /// Coercion exists but was not opted in.
    CoercionNotEnabled { risk: CoercionRisk },
}
```

## Composite types

The type system also defines the structural rules for composite types. These
are checked during schema mapping (02b), not here — but the rules are
documented here for reference.

| Arrow | Proto | Rule |
|-------|-------|------|
| `Struct` | message | Recurse into children |
| `List` / `LargeList` | repeated field | Check element type compatibility |
| `Map` | map<K,V> | Check key + value type compatibility. Key must be scalar. |
| `Struct` (oneof) | oneof | Each child maps to a oneof variant. Checked structurally. |

The compatibility functions above handle the leaf (scalar) level. Composite
validation is the responsibility of the mapping layer.

## Tasks

1. **Compatibility matrix** — Implement `check_compatibility`. One match arm
   per Arrow↔proto pair. Return `Compatible`, `CoercionAvailable`, or
   `Incompatible`.

2. **Type resolution** — Implement `resolve_type_check`. Wraps compatibility
   check with the coercion opt-in gate.

3. **Error types** — `TypeError`, `TypeErrorReason`, `CoercionRisk`.

4. **Tests** — Exhaustive coverage of the matrix:
   - Every lossless pair returns `Compatible`.
   - Every coercion pair returns `CoercionAvailable` with correct risk.
   - Unsupported Arrow types return `Incompatible`.
   - `resolve_type_check` with `coercion_allowed=false` errors on coercion pairs.
   - `resolve_type_check` with `coercion_allowed=true` passes on coercion pairs.

## Done when

- `check_compatibility` covers all pairs in the matrix above
- `resolve_type_check` gates coercion on opt-in
- All test cases pass
- Error messages include both the Arrow type and proto type names
