# Plan 07 — google.protobuf.Any packing

## Goal

First-class support for `google.protobuf.Any` fields, centered on the
**envelope + payload** pattern: an envelope message carries metadata columns
(timestamp, id, ...) plus one or more `Any` fields, each wrapping a typed
payload message. The Arrow-side user exposes a plain typed schema — apb
performs the double serialization (payload message → bytes → Any wrap) and
derives the `type_url` automatically. The user should never hand-author
`type_url` strings or pre-serialized bytes, except in the heterogeneous-data
escape hatch.

## Contract

1. **An `Any` field binds to exactly one Struct column.** No whole-schema
   ("root") form. This keeps each binding self-contained, so an envelope can
   carry any number of `Any` fields — each claims its own struct column —
   and envelope scalar columns never interact with payload resolution.

2. **Two forms, selected by declaration, never by shape sniffing:**

   - **Packed (primary).** The field declares its payload type `P` via the
     `(apb).any_pack` annotation or a caller-side option. The struct's
     children infer-map onto `P` using the normal rules. At encode time the
     row is serialized as `P` and wrapped as
     `Any { type_url: "<prefix>/<P full name>", value: <bytes> }`.

   - **Raw (escape hatch, heterogeneous data only).** No `any_pack`
     declaration. The struct must be exactly
     `Struct<type_url: Utf8, value: Binary>` — a passthrough of
     already-serialized payloads. Any other shape is a mapping error whose
     message suggests `(apb).any_pack`.

3. **`type_url` is always derived in packed form** — never data. Format is
   `"{prefix}/{P.full_name()}"` with prefix defaulting to
   `type.googleapis.com`, overridable via a mapping option. It is constant
   per binding and pre-encoded at plan time.

4. **One payload type per field (homogeneous columns).** Per-row
   heterogeneous dispatch is what raw form is for; a typed dispatch design
   (e.g. Arrow dense unions) is out of scope.

5. **Errors surface at mapping time**, not encode time: unknown `P`,
   annotation on a non-`Any` field, non-Struct column, wrong raw shape,
   inner mapping failure. The transcoder's per-row error surface is
   unchanged.

## Files

```
proto/apb/apb.proto                     # any_pack annotation (+ regenerate apb.bin)
crates/apb-core/src/mapping/model.rs    # FieldShape::AnyPacked
crates/apb-core/src/mapping/infer.rs    # Any detection + packed/raw resolution
crates/apb-core/src/transcode/plan.rs   # AnyPackedEncoder
crates/apb-core/src/transcode/mod.rs    # encode path
crates/apb-core/src/validation/         # report packed bindings, raw warnings
crates/apb-core/fixtures/any.proto      # envelope + payload fixtures (+ .bin)
crates/apb-cli/src/main.rs              # --any-pack, --any-url-prefix flags
```

## Annotation

```proto
message ApbFieldOptions {
  optional string arrow_name = 1;
  optional bool coerce = 2;
  // Fully-qualified payload message name for a google.protobuf.Any field.
  // The message must be resolvable in the same descriptor pool.
  optional string any_pack = 3;
}
```

Usage:

```proto
import "google/protobuf/any.proto";

message Event {
  string event_id = 1;
  google.protobuf.Timestamp created_at = 2;
  google.protobuf.Any payload = 3 [(apb).any_pack = "my.pkg.OrderPlaced"];
  google.protobuf.Any context = 4 [(apb).any_pack = "my.pkg.RequestContext"];
}
```

Caller-side equivalent for protos that can't be modified:

```rust
pub struct InferOptions {
    // ... existing fields ...
    /// Payload types for Any fields, keyed by fully-qualified proto field
    /// name (e.g. "my.pkg.Event.payload"). Overrides (apb).any_pack.
    pub any_pack: HashMap<String, String>,
    /// type_url prefix for packed Any fields.
    /// Default: "type.googleapis.com".
    pub any_url_prefix: String,
}
```

CLI: `--any-pack my.pkg.Event.payload=my.pkg.OrderPlaced` (repeatable),
`--any-url-prefix <prefix>`.

## Mapping

New `FieldShape` variant:

```rust
FieldShape::AnyPacked {
    /// Precomputed, e.g. "type.googleapis.com/my.pkg.OrderPlaced".
    type_url: String,
    /// Struct children → payload message P.
    inner: Box<FieldMapping>,
}
```

Detection must live in `resolve_nested_message` — not `resolve_field_shape` —
because `resolve_repeated` and `resolve_map` call `resolve_nested_message`
directly for message-kind elements/values, bypassing `resolve_field_shape`.
`resolve_nested_message` currently receives only the `MessageDescriptor` and
field name; change its signature to take the `FieldDescriptor` so it can read
the `any_pack` annotation and build the fully-qualified key for the caller
option. On `msg_desc.full_name() == "google.protobuf.Any"`:

- `any_pack` declared (option wins over annotation): resolve `P` in the
  field's `parent_pool()`; require the Arrow side to be a Struct; build
  `inner = infer_from_fields(struct_children, P)` (failures wrapped in
  `MappingError::Nested`); produce `FieldShape::AnyPacked`.
- No declaration: raw form. Require the struct to be exactly
  `{type_url: Utf8, value: Binary}` and map it through the existing generic
  `FieldShape::Message` path. Any other shape →
  `MappingError::AnyRawShapeMismatch` (new variant) with a hint about
  `(apb).any_pack`.

With detection in `resolve_nested_message`, packed Any composes everywhere
message resolution happens: nested envelopes and oneof variants (via
`resolve_field_shape`), and `repeated google.protobuf.Any` / map values (via
their direct calls).

The `TypeCheck` for an `AnyPacked` binding follows the nested-message
precedent: `Kind::Message(Any)`, `TypeCheckMode::Direct`.

New `MappingError` variants: `AnyPackTargetNotFound`,
`AnyPackOnNonAnyField`, `AnyRawShapeMismatch`.

### Descriptor-pool availability of P

`Any` does not reference its payload type, so a `FileDescriptorSet` compiled
from the envelope proto alone will not contain `P` — `--include_imports`
only pulls actual imports. The payload proto must be imported by the
envelope or explicitly compiled into the set. Document this in the README,
and make the `AnyPackTargetNotFound` message hint at it (e.g. "ensure the
payload proto is compiled into the descriptor set").

### Caller-option addressing limits

The `InferOptions::any_pack` key is the fully-qualified proto field name.
For an Any appearing as a map value, the real field is the synthetic
`...Entry.value` — v1 excludes map-value Any from the caller-side option;
use the annotation there instead.

## Encoding

```rust
struct AnyPackedEncoder {
    /// Pre-encoded complete type_url sub-field: tag 0x0a + len + url bytes.
    type_url_field: Vec<u8>,
    /// Pre-encoded tag for value (field 2, length-delimited): 0x12.
    value_tag: Vec<u8>,
    /// Sub-plan encoding struct children as P.
    payload_plan: EncodingPlan,
}
```

No scratch buffers — reuse the in-place
`begin_length_delimited`/`finish_length_delimited` nesting already used by
`MessageEncoder`:

1. write the field tag (length-delimited)
2. `begin_length_delimited` — Any body
3. write `type_url_field` (constant bytes)
4. write `value_tag`, `begin_length_delimited`, encode struct children via
   `payload_plan`, `finish_length_delimited`
5. `finish_length_delimited` — Any body

`FieldEncoderKind::AnyPacked` must be wired into every dispatch site that
handles message-kind values — missing one turns packed-Any-in-that-position
into a per-row runtime error:

1. top-level fields (`encode_field`)
2. oneof variants (`encode_oneof`)
3. repeated elements (the `_ =>` catch-all in `encode_repeated`)
4. map values (the `_ =>` catch-all in `encode_map`)

### Null semantics

- Null struct → envelope field omitted (consistent with nested messages).
- Non-null struct with all-null children → `Any { type_url, value: "" }` —
  an empty but *typed* payload. The `type_url` is always written when the
  field is present.
- Raw form: passthrough as-is, per the unverified-passthrough contract. A
  non-null struct with a null `type_url` child encodes an `Any` without a
  `type_url` — apb does not error; the validation report's raw-form note
  covers this.

### type_url prefix normalization

Normalize `any_url_prefix` at option-parse time: trim trailing slashes and
reject an empty prefix, so the result is always `"{prefix}/{full_name}"`
with exactly one separator.

## Validation

- Packed bindings render as `payload → Any[my.pkg.OrderPlaced]` with the
  inner mapping's report nested, same as nested messages today.
- Raw bindings render with a note that values pass through unverified (apb
  cannot check that `value` bytes match `type_url`).

## Out of scope (v1)

- Heterogeneous per-row payload types (dense-union dispatch) — raw form
  covers this.
- Explicit mapping mode (already rejects all composite shapes).
- Generate direction (Arrow schema → descriptor with Any fields). Note the
  existing `generate` module already helps the packed flow: a user can
  generate `P` from their Arrow schema and only hand-author the envelope.
- Per-field `type_url` prefix override (global option only).

## Tasks

1. **Annotation** — add `any_pack` to `apb.proto`, regenerate `apb.bin`,
   extend `read_apb_annotations`.

2. **Fixtures** — `any.proto`: envelope with two annotated Any fields +
   payload messages (including one payload containing a WKT timestamp and a
   nested message, to prove inner-plan reuse); an unannotated Any field for
   raw-form tests. Wire into `regenerate.sh`.

3. **Mapping: raw form** — Any detection in `resolve_nested_message`
   (signature change to take `FieldDescriptor`), raw-shape enforcement,
   `AnyRawShapeMismatch`. Tests: raw-shape acceptance, wrong children →
   error mentioning `any_pack`, non-Struct column → shape error.

4. **Mapping: packed form** — `FieldShape::AnyPacked`, `InferOptions`
   extensions (`any_pack` map, `any_url_prefix` with normalization),
   `AnyPackTargetNotFound`, `AnyPackOnNonAnyField`. Tests: packed mapping
   resolution, caller option overrides annotation, target missing from
   pool → error, annotation on non-Any field → error, prefix
   normalization.

5. **Encoding** — `AnyPackedEncoder` in the plan, pre-encoded `type_url`
   at plan time, wired into all four dispatch sites (field, oneof variant,
   repeated element, map value). Tests: packed round-trip (decode with
   prost-reflect, unpack `value` as `P`, verify fields and `type_url`),
   two Any fields with different payload types, payload containing
   Timestamp + nested message (inner plan reuse), null struct → field
   omitted, all-null children → typed empty payload, raw passthrough
   round-trip (pins the behavior that works implicitly today),
   `repeated google.protobuf.Any` from `List<Struct>`.

6. **Validation** — report rendering for packed and raw bindings, with
   report-output tests.

7. **CLI** — `--any-pack`, `--any-url-prefix`; end-to-end integration test
   covering the full envelope + payload flow.

## Done when

- An envelope with metadata columns + N packed Any struct columns encodes
  with derived `type_url`s and prost-reflect can unpack every payload.
- The user never provides `type_url` or serialized bytes in packed form.
- Raw form works only with the exact canonical shape and is clearly
  reported by validation.
- All mapping-time failures produce actionable errors; no new encode-time
  error paths.
- `cargo test --workspace` passes.
