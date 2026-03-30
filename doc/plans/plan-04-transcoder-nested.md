# Plan 04 — Transcoder — Nested Types

## Goal

Extend the `Transcoder` to handle composite field shapes: nested messages,
repeated fields, map fields, and oneof groups. After this chunk, the
transcoder supports the full proto type surface.

## Files

Changes to existing files in `crates/apb-core/src/transcode/`:

```
├── plan.rs         # extend EncodingPlan with composite encoders
├── encode.rs       # add composite encoding functions
├── nested.rs       # NEW: nested message serialization
├── repeated.rs     # NEW: repeated field serialization
├── map.rs          # NEW: map field serialization
├── oneof.rs        # NEW: oneof group serialization
└── mod.rs          # wire up new modules
```

## Design principle

Nested encoding reuses the same `EncodingPlan` approach as scalars. Each
composite field gets a `CompositeEncoder` instead of a `ScalarEncodeFn`. The
plan becomes a tree of encoders that mirrors the proto message structure,
flattened during construction.

## Encoding plan extensions

```rust
enum FieldEncoderKind {
    Scalar(ScalarEncodeFn),
    Message(MessageEncoder),
    Repeated(RepeatedEncoder),
    Map(MapEncoder),
    Oneof(OneofEncoder),
}

struct FieldEncoder {
    arrow_index: usize,
    tag: Vec<u8>,
    kind: FieldEncoderKind,
    nullable: bool,
}
```

### MessageEncoder

Encodes an Arrow `StructArray` as a nested proto message.

```rust
struct MessageEncoder {
    /// Sub-plan for the nested message's fields.
    sub_plan: EncodingPlan,
}
```

Encoding: serialize the nested message into the scratch buffer, then write it
as a length-delimited field (tag + varint length + message bytes).

### RepeatedEncoder

Encodes an Arrow `ListArray` as a proto `repeated` field.

```rust
struct RepeatedEncoder {
    /// Tag for each repeated element.
    element_tag: Vec<u8>,
    /// Encoder for each element.
    element_kind: Box<FieldEncoderKind>,
}
```

Encoding: iterate the list offsets for the current row. For each element,
write tag + encoded value. Scalar repeated fields could use packed encoding
(single length-delimited field containing all values back-to-back) — use
packed for numeric types, unpacked for length-delimited types (strings, bytes,
nested messages), per proto3 convention.

### MapEncoder

Encodes an Arrow `MapArray` as a proto `map<K,V>` field. Proto maps are
wire-encoded as `repeated` nested messages with `key` (field 1) and `value`
(field 2).

```rust
struct MapEncoder {
    /// Tag for the outer repeated field.
    entry_tag: Vec<u8>,
    /// Pre-encoded tag for key (field 1).
    key_tag: Vec<u8>,
    /// Pre-encoded tag for value (field 2).
    value_tag: Vec<u8>,
    /// Key encoder.
    key_kind: Box<FieldEncoderKind>,
    /// Value encoder.
    value_kind: Box<FieldEncoderKind>,
}
```

Encoding: iterate map entries for the current row. For each entry, write the
outer tag + length-delimited wrapper containing key field + value field.

### OneofEncoder

Encodes an Arrow `StructArray` (with nullable children) as a proto `oneof`.

```rust
struct OneofEncoder {
    /// One variant per child of the StructArray.
    variants: Vec<OneofVariantEncoder>,
}

struct OneofVariantEncoder {
    /// Child index within the Arrow StructArray.
    arrow_child_index: usize,
    /// Tag for this variant's proto field.
    tag: Vec<u8>,
    /// Encoder for this variant's value.
    kind: Box<FieldEncoderKind>,
}
```

Encoding: for the current row, iterate variants. Find the one non-null child.
Encode that child's value with its tag. If zero children are non-null, write
nothing (valid — oneof is unset). If more than one is non-null, return a
`TranscodeError`.

## Scratch buffer strategy

Nested messages need to be serialized to determine their length before writing
the length prefix. This requires a scratch buffer.

For deeply nested structures, use a stack of scratch buffers:

```rust
struct ScratchStack {
    buffers: Vec<Vec<u8>>,
    depth: usize,
}

impl ScratchStack {
    fn push(&mut self) -> &mut Vec<u8>;  // get a clean buffer at next depth
    fn pop(&mut self) -> &[u8];          // return the written bytes, reset
}
```

Each nesting level pushes a new scratch buffer. When the nested message is
complete, pop it and write the bytes (with length prefix) into the parent
buffer. Buffers are reused across rows — clear but don't dealloc.

Max depth is bounded by the proto schema. No runtime depth limit needed.

## Error handling extensions

```rust
// Add to TranscodeError:

/// Oneof has more than one variant set.
OneofMultipleSet {
    row: usize,
    oneof_name: String,
    set_variants: Vec<String>,
},

/// List element encoding failed.
RepeatedElementError {
    row: usize,
    list_field: String,
    element_index: usize,
    source: Box<TranscodeError>,
},

/// Map entry encoding failed.
MapEntryError {
    row: usize,
    map_field: String,
    entry_index: usize,
    source: Box<TranscodeError>,
},

/// Nested message encoding failed.
NestedMessageError {
    row: usize,
    proto_field: String,
    source: Box<TranscodeError>,
},
```

All errors carry the full path to the failure: row → field → element/entry →
nested field.

## Tasks

1. **Refactor FieldEncoder** — Replace `ScalarEncodeFn` with
   `FieldEncoderKind` enum. Existing scalar path continues to work.

2. **ScratchStack** — Reusable scratch buffer stack for nested length
   computation.

3. **MessageEncoder** — Encode `StructArray` as nested message. Recurse into
   sub-plan. Length-delimited wrapping via scratch buffer.

4. **RepeatedEncoder** — Encode `ListArray` as repeated field. Packed encoding
   for numeric scalars, unpacked for length-delimited types.

5. **MapEncoder** — Encode `MapArray` as repeated key-value entry messages.

6. **OneofEncoder** — Encode `StructArray` oneof with at-most-one-non-null
   validation per row. Error on multiple set variants.

7. **Plan construction** — Extend `EncodingPlan` building to handle composite
   `FieldShape` variants from the `FieldMapping`. Recurse for nested messages.

8. **Tests**
   - Nested message: encode struct with scalar children, decode with prost,
     verify values.
   - Deeply nested: 3+ levels of nesting, verify round-trip.
   - Repeated scalars: list of int32, verify packed encoding.
   - Repeated strings: list of strings, verify unpacked encoding.
   - Repeated messages: list of structs, verify each element decodes.
   - Map<string, int64>: verify key-value entry encoding.
   - Map<string, message>: verify nested value encoding.
   - Oneof with one variant set → correct field encoded.
   - Oneof with no variant set → no bytes written.
   - Oneof with two variants set → `OneofMultipleSet` error with row number
     and variant names.
   - Null nested message → skip entire field.
   - Null list → skip entire field (vs empty list → write nothing but field
     is present? Clarify: null = absent, empty list = zero elements).
   - Mixed batch: message with scalar + nested + repeated + oneof fields.
   - Both output modes (delimited + arrow) with nested types.

## Done when

- `Transcoder::new` accepts `FieldMapping` with all field shapes
- Nested messages encode with correct length prefixes at arbitrary depth
- Repeated fields use packed encoding for numeric types
- Map fields encode as repeated entry messages
- Oneof validates at-most-one-non-null per row
- All error variants carry full path context
- Round-trip tests pass (encode with apb, decode with prost)
- Both output modes produce correct results with composite types
