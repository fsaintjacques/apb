# Plan 03 — Transcoder — Scalar Fields

## Goal

Build the `Transcoder` — a compiled, reusable object that converts Arrow
`RecordBatch`es into serialized protobuf. This chunk handles flat messages with
scalar fields only. Nested types are added in chunk 04.

## Files

```
crates/apb-core/src/
├── transcode/
│   ├── mod.rs          # public API: Transcoder
│   ├── plan.rs         # EncodingPlan, precomputed field encoders
│   ├── encode.rs       # scalar encoding functions (wire format)
│   ├── delimited.rs    # varint-delimited output
│   ├── arrow_out.rs    # Arrow BinaryArray output
│   └── wire.rs         # low-level proto wire format helpers
└── lib.rs              # re-export transcode module
```

## Wire format helpers

Low-level building blocks for writing raw protobuf wire format. No
dependencies on prost encoding — we write bytes directly.

```rust
/// Encode a varint into the buffer. Returns bytes written.
fn encode_varint(value: u64, buf: &mut Vec<u8>);

/// Encode a zigzag-encoded varint (for sint32/sint64).
fn encode_zigzag32(value: i32, buf: &mut Vec<u8>);
fn encode_zigzag64(value: i64, buf: &mut Vec<u8>);

/// Encode a fixed 4-byte value (float, sfixed32, fixed32).
fn encode_fixed32(value: u32, buf: &mut Vec<u8>);

/// Encode a fixed 8-byte value (double, sfixed64, fixed64).
fn encode_fixed64(value: u64, buf: &mut Vec<u8>);

/// Encode a length-delimited field (string, bytes).
fn encode_length_delimited(data: &[u8], buf: &mut Vec<u8>);

/// Encode a proto field tag (field_number << 3 | wire_type).
fn encode_tag(field_number: u32, wire_type: u8) -> Vec<u8>;
```

## Encoding plan

The `Transcoder` precomputes an `EncodingPlan` at construction time. The plan
is a flat list of field encoders — one per binding in the `FieldMapping`.

```rust
struct EncodingPlan {
    /// One encoder per bound field, ordered for sequential column access.
    field_encoders: Vec<FieldEncoder>,
}

struct FieldEncoder {
    /// Arrow column index.
    arrow_index: usize,
    /// Pre-encoded tag bytes (field number + wire type).
    tag: Vec<u8>,
    /// The encoding function to use.
    encode_fn: ScalarEncodeFn,
    /// Whether the column is nullable (check null bitmap).
    nullable: bool,
}

/// Function pointer type for scalar encoding.
/// Takes: array, row index, output buffer. Writes the value (no tag).
type ScalarEncodeFn = fn(array: &dyn Array, row: usize, buf: &mut Vec<u8>) -> Result<(), TranscodeError>;
```

### Encoder selection

At plan construction, each binding resolves to a concrete `ScalarEncodeFn`
based on the `TypeCheck`:

| TypeCheck | Arrow type | Proto type | Encoder |
|-----------|-----------|-----------|---------|
| Direct | Int32 | int32 | `encode_int32_varint` |
| Direct | Int32 | sint32 | `encode_int32_zigzag` |
| Direct | Int32 | sfixed32 | `encode_int32_fixed` |
| Direct | Int64 | int64 | `encode_int64_varint` |
| Direct | Float32 | float | `encode_float32` |
| Direct | Float64 | double | `encode_float64` |
| Direct | Boolean | bool | `encode_bool` |
| Direct | Utf8 | string | `encode_utf8` |
| Direct | Binary | bytes | `encode_binary` |
| Coerce | Int64 | int32 | `encode_int64_as_int32` (truncate) |
| Coerce | Int64 | enum | `encode_int64_as_enum` (truncate + range check) |
| Coerce | Utf8 | enum | `encode_utf8_as_enum` (name→number lookup) |
| Coerce | Timestamp | int64 | `encode_timestamp_as_int64` (raw epoch) |
| Coerce | Date32 | int32 | `encode_date32_as_int32` (days since epoch) |
| Direct | Int32 | enum | `encode_int32_as_enum` (range check) |
| ... | ... | ... | ... |

Each `encode_*` function downcasts the `&dyn Array` to the concrete Arrow
array type, reads the value at the given row, and writes the proto-encoded
value to the buffer.

### Enum encoding

Proto enum fields require runtime validation. At plan construction time, the
transcoder precomputes:

- **Int32/Int64 → enum**: a `HashSet<i32>` of valid enum numbers for range
  checking.
- **Utf8 → enum**: a `HashMap<String, i32>` mapping variant names to numbers
  for name lookup.

These are stored in the `FieldEncoder` and shared across rows. The encode
function looks up the value, writes the resolved number as a varint, or
returns a `TranscodeError` with the invalid value and the list of valid
options.

## Transcoder

```rust
pub struct Transcoder {
    plan: EncodingPlan,
}

impl Transcoder {
    /// Build a transcoder from a validated field mapping.
    /// Precomputes all wire tags and selects encoder functions.
    /// Returns error if the mapping contains unsupported field shapes
    /// (nested types — not yet implemented in this chunk).
    pub fn new(mapping: &FieldMapping) -> Result<Self, TranscodeError>;
}
```

## Output: varint-delimited

Each row is encoded as a complete proto message, then written with a varint
length prefix.

```rust
impl Transcoder {
    /// Transcode a batch into varint-delimited protobuf messages.
    /// Appends to `output`.
    pub fn transcode_delimited(
        &self,
        batch: &RecordBatch,
        output: &mut Vec<u8>,
    ) -> Result<(), TranscodeError>;
}
```

### Internal loop

```
for row in 0..batch.num_rows():
    row_start = output.len()
    // Reserve space for length prefix (patched after encoding)
    for encoder in plan.field_encoders:
        if encoder.nullable && array.is_null(row):
            continue  // skip null fields (proto default)
        output.extend(&encoder.tag)
        (encoder.encode_fn)(array, row, output)?
    patch length prefix at row_start
```

The length prefix is tricky because proto varints are variable-length. Two
strategies:

**A: Two-pass** — encode the message into a scratch buffer, then write
length + scratch to output. Extra copy but simple.

**B: Patch in place** — reserve max varint bytes (10), encode message, then
patch the actual length. Avoids the copy but requires memmove if the
reserved space was too large.

Start with **A** (scratch buffer per row). The scratch buffer is reused across
rows (clear, don't dealloc). Optimize to B later if profiling shows the copy
matters.

## Output: Arrow BinaryArray

All messages are written into one contiguous buffer. An offsets array tracks
where each message starts.

```rust
impl Transcoder {
    /// Transcode a batch into an Arrow BinaryArray.
    /// Each element is one serialized proto message.
    pub fn transcode_arrow(
        &self,
        batch: &RecordBatch,
    ) -> Result<BinaryArray, TranscodeError>;
}
```

### Internal loop

```
offsets: Vec<i32>   // length = num_rows + 1
payload: Vec<u8>    // contiguous message buffer

offsets.push(0)
for row in 0..batch.num_rows():
    for encoder in plan.field_encoders:
        if encoder.nullable && array.is_null(row):
            continue
        payload.extend(&encoder.tag)
        (encoder.encode_fn)(array, row, payload)?
    offsets.push(payload.len() as i32)

BinaryArray::from(OffsetsBuffer, payload)
```

No scratch buffer needed — each message is written directly into `payload` and
the offset marks the boundary. Simpler and faster than the delimited path.

## Error handling

```rust
pub enum TranscodeError {
    /// Field encoding failed for a specific row.
    FieldError {
        row: usize,
        arrow_field: String,
        proto_field: String,
        reason: String,
    },
    /// Mapping contains field shapes not supported by this transcoder
    /// (placeholder until chunk 04 adds nested support).
    UnsupportedFieldShape {
        proto_field: String,
        shape: String,
    },
    /// Coercion failed at runtime (e.g. value out of range).
    CoercionFailed {
        row: usize,
        arrow_field: String,
        proto_field: String,
        value: String,
        target_type: String,
    },
}
```

Errors identify the row, field, and reason — enough for the user to find and
fix the problem.

## Buffer management

- `transcode_delimited`: caller passes `&mut Vec<u8>`. Can pre-allocate based
  on batch size estimate. The transcoder appends, never clears.
- `transcode_arrow`: transcoder owns the internal buffers and returns a
  `BinaryArray`. Caller can estimate capacity via
  `Transcoder::estimate_capacity(num_rows)` (optional hint, not required).
- Scratch buffer for delimited two-pass: owned by the `Transcoder`, reused
  across rows within a batch and across batches.

## Tasks

1. **Wire format helpers** — `encode_varint`, `encode_zigzag`, `encode_fixed`,
   `encode_length_delimited`, `encode_tag`. Test each against known proto
   encoded values.

2. **Scalar encode functions** — One function per Arrow type × proto type
   combination in the matrix. Each downcasts the array and writes the value.

3. **Encoding plan** — Build `EncodingPlan` from `FieldMapping`. Select the
   correct `ScalarEncodeFn` per binding. Pre-encode tag bytes.

4. **Transcoder construction** — `Transcoder::new`. Reject nested field shapes
   (for now).

5. **Delimited output** — `transcode_delimited` with scratch buffer approach.
   Length prefix encoding.

6. **Arrow output** — `transcode_arrow` with offsets + payload buffers.

7. **Error handling** — `TranscodeError` with row/field context. Coercion
   runtime checks (e.g. i64 value > i32::MAX).

8. **Tests**
   - Round-trip: encode with apb, decode with prost. Verify field values.
   - All scalar types: one test per lossless type pair.
   - Nullable fields: null rows produce no bytes for that field.
   - Coercion: valid values encode correctly; out-of-range values → error
     with row number.
   - Empty batch → empty output.
   - Delimited output: multiple rows, verify each message is independently
     decodable with correct length prefix.
   - Arrow output: verify offsets and BinaryArray element count matches
     row count.
   - Error messages include row index and field names.

## Done when

- `Transcoder::new` builds from a scalar-only `FieldMapping`
- `transcode_delimited` produces correct varint-delimited output
- `transcode_arrow` produces a correct `BinaryArray`
- All scalar type pairs encode correctly (verified by decoding with prost)
- Null handling: null values are skipped
- Coercion runtime errors are caught with actionable messages
- All test cases pass
