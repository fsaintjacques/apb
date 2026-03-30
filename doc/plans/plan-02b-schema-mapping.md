# Plan 02b — Schema Mapping

## Goal

Produce a `FieldMapping` — the fully resolved, validated binding between an
Arrow schema and a proto message descriptor. This is the contract that the
transcoder consumes.

## Files

```
crates/apb-core/src/
├── mapping/
│   ├── mod.rs        # public API
│   ├── infer.rs      # infer mode (annotations + name-match)
│   ├── explicit.rs   # explicit mode
│   └── model.rs      # FieldMapping, FieldBinding types
└── lib.rs            # re-export mapping module
```

## Core types

### `FieldMapping`

The top-level resolved mapping. Immutable once built — handed to the
transcoder.

```rust
pub struct FieldMapping {
    /// Proto message fully qualified name.
    pub message_name: String,
    /// One binding per mapped regular field, in proto field number order.
    pub bindings: Vec<FieldBinding>,
    /// Oneof groups mapped from Arrow StructArray columns.
    /// Separate from bindings because a oneof is not a single proto field —
    /// it's a grouping whose variants are individual fields.
    pub oneofs: Vec<OneofMapping>,
    /// Arrow column indices that have no proto counterpart.
    pub unmapped_arrow: Vec<UnmappedArrowField>,
    /// Proto fields that have no Arrow counterpart.
    pub unmapped_proto: Vec<UnmappedProtoField>,
}
```

### `FieldBinding`

A single resolved Arrow column → proto field pair.

```rust
pub struct FieldBinding {
    /// Arrow column index in the RecordBatch.
    pub arrow_index: usize,
    /// Arrow field name (for error messages).
    pub arrow_name: String,
    /// Proto field number (for wire encoding).
    pub proto_number: u32,
    /// Proto field name (for error messages).
    pub proto_name: String,
    /// How the types relate.
    pub type_check: TypeCheck,
    /// How this binding was resolved.
    pub bind_method: BindMethod,
    /// Shape of the field (scalar, repeated, map, oneof, nested).
    pub field_shape: FieldShape,
}

pub enum BindMethod {
    /// Matched by proto annotation.
    Annotation,
    /// Matched by exact name.
    NameMatch,
    /// Provided explicitly by caller.
    Explicit,
}

pub enum FieldShape {
    /// Scalar proto field.
    Scalar,
    /// `repeated` field. Contains the element shape.
    /// Element is scalar → element_type_check + Scalar shape.
    /// Element is a message → element has a sub-mapping.
    Repeated {
        element_type_check: TypeCheck,
        element_shape: Box<FieldShape>,
    },
    /// `map<K,V>` field. Contains key and value type checks.
    /// Keys are always scalar. Values may be scalar or nested message.
    Map {
        key_type_check: TypeCheck,
        value_type_check: TypeCheck,
        value_shape: Box<FieldShape>,
    },
    /// Nested message. Contains the sub-mapping.
    Message(Box<FieldMapping>),
}
```

### `OneofMapping`

```rust
pub struct OneofMapping {
    /// Proto oneof name.
    pub oneof_name: String,
    /// Arrow column index of the StructArray that wraps this oneof.
    pub arrow_index: usize,
    /// Arrow field name (for error messages).
    pub arrow_name: String,
    /// The Arrow column is a StructArray. Each child maps to a variant.
    pub variants: Vec<OneofVariant>,
}

pub struct OneofVariant {
    /// Child index within the Arrow StructArray.
    pub arrow_child_index: usize,
    /// Proto field number of this variant.
    pub proto_number: u32,
    /// Proto field name of this variant.
    pub proto_name: String,
    /// Type check for this variant's value.
    pub type_check: TypeCheck,
    /// Shape of the variant (scalar or nested message).
    pub field_shape: Box<FieldShape>,
}
```

### Unmapped fields

```rust
pub struct UnmappedArrowField {
    pub index: usize,
    pub name: String,
}

pub struct UnmappedProtoField {
    pub number: u32,
    pub name: String,
}
```

## Infer mode

### Algorithm

```
infer(arrow_schema, message_descriptor, options) -> Result<FieldMapping>:

  for each proto field in message_descriptor:
    1. Check annotations — if field has an apb annotation naming an Arrow
       column, bind to that column. Resolve type via resolve_type_check.
    2. Else name-match — find an Arrow field with the exact same name.
       Resolve type via resolve_type_check.
    3. Else — add to unmapped_proto.

  for each Arrow field not yet bound:
    add to unmapped_arrow.

  for composite fields (message, repeated, map, oneof):
    recurse into children.

  return FieldMapping
```

### Annotations

Proto field options for apb are defined in a `.proto` extension:

```proto
import "google/protobuf/descriptor.proto";

message ApbFieldOptions {
  // Explicit Arrow column name to bind to.
  optional string arrow_name = 1;
  // Allow type coercion for this field.
  optional bool coerce = 2;
}

extend google.protobuf.FieldOptions {
  optional ApbFieldOptions apb = 50000; // extension number TBD
}
```

Usage:
```proto
message Event {
  string user_id = 1 [(apb).arrow_name = "uid", (apb).coerce = true];
}
```

Annotations are read from the `FieldDescriptor` options at mapping time via
prost-reflect.

#### Distribution

The `apb.proto` extension file must be shipped with the project so consuming
teams can import it. It lives at `proto/apb/apb.proto` in the repo and should
be published as:
- A file in the release artifacts (teams copy it into their proto include path)
- A Buf module (if using Buf Schema Registry)

The extension's `FileDescriptorSet` must also be included in the `apb-core`
crate (embedded bytes) so that `DescriptorPool` can resolve the custom options
when parsing user descriptors that import `apb.proto`.

### Options

```rust
pub struct InferOptions {
    /// If true, unmapped proto fields are allowed (default: true).
    /// If false, any unmapped proto field is an error.
    pub allow_unmapped_proto: bool,
    /// If true, unmapped Arrow fields are allowed (default: true).
    pub allow_unmapped_arrow: bool,
}
```

## Explicit mode

The caller provides a list of explicit bindings. No inference is performed.

```rust
pub struct ExplicitBinding {
    /// Arrow field name or index.
    pub arrow_field: ArrowFieldRef,
    /// Proto field name or number.
    pub proto_field: ProtoFieldRef,
    /// Allow coercion for this binding.
    pub coerce: bool,
}

pub enum ArrowFieldRef {
    Name(String),
    Index(usize),
}

pub enum ProtoFieldRef {
    Name(String),
    Number(u32),
}
```

The explicit mapper resolves each binding against the Arrow schema and proto
descriptor, runs type checks, and produces the same `FieldMapping` output.

## Errors

```rust
pub enum MappingError {
    /// Arrow field referenced by annotation or explicit binding not found.
    ArrowFieldNotFound { reference: String },
    /// Proto field referenced by explicit binding not found.
    ProtoFieldNotFound { reference: String },
    /// Type incompatibility.
    TypeError(TypeError),
    /// Duplicate binding — two proto fields mapped to the same Arrow column.
    DuplicateBinding {
        arrow_field: String,
        proto_fields: Vec<String>,
    },
    /// Oneof structural error — Arrow field is not a StructArray.
    OneofNotStruct { arrow_field: String, oneof_name: String },
    /// Recursive mapping error in a nested message.
    Nested {
        proto_field: String,
        source: Box<MappingError>,
    },
}
```

## Public API

```rust
/// Infer a mapping from an Arrow schema and a proto message descriptor.
pub fn infer_mapping(
    arrow_schema: &Schema,
    message: &MessageDescriptor,
    options: &InferOptions,
) -> Result<FieldMapping, MappingError>;

/// Build a mapping from explicit bindings.
pub fn explicit_mapping(
    arrow_schema: &Schema,
    message: &MessageDescriptor,
    bindings: &[ExplicitBinding],
) -> Result<FieldMapping, MappingError>;
```

## Tasks

1. **Model types** — `FieldMapping`, `FieldBinding`, `FieldShape`,
   `OneofMapping`, `BindMethod`, unmapped types.

2. **Infer mode — scalars** — Name-match for flat messages with scalar fields
   only. Type check each binding via the 02a type system.

3. **Infer mode — annotations** — Read apb field options from descriptors.
   Annotations take priority over name-match. Define the `.proto` extension.

4. **Infer mode — composites** — Handle nested messages (recurse), repeated
   fields, map fields, oneof groups.

5. **Explicit mode** — Resolve explicit bindings against schemas. Same type
   checking, same output type.

6. **Error handling** — `MappingError` with all variants. Nested errors
   carry the proto field path for context.

7. **Tests**
   - Flat message, all fields match by name → all bound.
   - Flat message, some fields don't match → correct unmapped lists.
   - Annotation overrides name for one field.
   - Annotation + name-match coexist (annotation wins).
   - Coercion annotation enables a type-mismatched binding.
   - Coercion without annotation → `TypeError`.
   - Duplicate binding → error.
   - Nested message → recursive `FieldMapping`.
   - Oneof → `OneofMapping` with variants.
   - Oneof target is not a StructArray → error.
   - Explicit mode with name and index refs.
   - Explicit mode referencing nonexistent fields → error.

## Done when

- `infer_mapping` and `explicit_mapping` produce correct `FieldMapping` for
  all supported field shapes
- Annotations are read from proto field options and take priority
- Type system is consulted for every binding
- All test cases pass
- Errors carry enough context to be actionable (field names, paths)
