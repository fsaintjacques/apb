//! Scalar encoding functions.
//!
//! Each function downcasts `&dyn Array` to the concrete Arrow array type,
//! reads the value at the given row, and writes the proto-encoded value
//! (without tag) to the buffer.

use arrow_array::*;

use super::wire;

/// Error from encoding a single field value.
#[derive(Debug, thiserror::Error)]
#[error("row {row}, field '{field}': {reason}")]
pub struct EncodeError {
    pub row: usize,
    pub field: String,
    pub reason: String,
}

/// Enum identifying which scalar encoding to apply.
/// Replaces function pointers to enable inlining in the hot loop.
#[derive(Debug, Clone, Copy)]
pub enum ScalarKind {
    // Direct
    Bool,
    Int32Varint,
    Int32Zigzag,
    Int32Fixed,
    Int64Varint,
    Int64Zigzag,
    Int64Fixed,
    UInt32Varint,
    UInt32Fixed,
    UInt64Varint,
    UInt64Fixed,
    Float32,
    Float64,
    Utf8,
    LargeUtf8,
    Binary,
    LargeBinary,
    Int32AsEnum,
    // Timestamps
    TimestampS,
    TimestampMs,
    TimestampUs,
    TimestampNs,
    // Durations
    DurationS,
    DurationMs,
    DurationUs,
    DurationNs,
    // Coercions
    Int64AsInt32Varint,
    Int64AsSint32,
    Int64AsSfixed32,
    Int32AsInt64Varint,
    Int32AsSint64,
    Int32AsSfixed64,
    UInt64AsUInt32Varint,
    UInt64AsFixed32,
    UInt32AsUInt64Varint,
    UInt32AsFixed64,
    // Signed ↔ unsigned crossover (two's-complement reinterpretation)
    Int32AsUInt32Varint,
    Int32AsFixed32,
    Int32AsUInt64Varint,
    Int32AsFixed64,
    Int64AsUInt32Varint,
    Int64AsFixed32,
    Int64AsUInt64Varint,
    Int64AsFixed64,
    UInt32AsInt32Varint,
    UInt32AsSint32,
    UInt32AsSfixed32,
    UInt32AsInt64Varint,
    UInt32AsSint64,
    UInt32AsSfixed64,
    UInt64AsInt32Varint,
    UInt64AsSint32,
    UInt64AsSfixed32,
    UInt64AsInt64Varint,
    UInt64AsSint64,
    UInt64AsSfixed64,
    Float64AsFloat32,
    Float32AsFloat64,
    Utf8AsBytes,
    BinaryAsString,
    Int64AsEnum,
    // String-encoded integers
    Utf8AsInt(IntTarget),
    LargeUtf8AsInt(IntTarget),
}

/// Integer wire encoding targeted by a string-encoded integer column.
///
/// Carried as a parameter of [`ScalarKind::Utf8AsInt`] rather than expanded
/// into flat variants, which would need 20 of them (2 string widths x 10
/// integer kinds).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IntTarget {
    Int32Varint,
    Sint32,
    Sfixed32,
    Int64Varint,
    Sint64,
    Sfixed64,
    UInt32Varint,
    Fixed32,
    UInt64Varint,
    Fixed64,
}

impl IntTarget {
    /// Proto wire type this target encodes to.
    pub fn wire_type(self) -> u8 {
        match self {
            Self::Sfixed32 | Self::Fixed32 => wire::WIRE_FIXED32,
            Self::Sfixed64 | Self::Fixed64 => wire::WIRE_FIXED64,
            _ => wire::WIRE_VARINT,
        }
    }

    /// Whether the target is an unsigned proto kind.
    fn is_unsigned(self) -> bool {
        matches!(
            self,
            Self::UInt32Varint | Self::Fixed32 | Self::UInt64Varint | Self::Fixed64
        )
    }
}

impl ScalarKind {
    /// Encode a scalar value at `row` from `array` into `buf`.
    #[inline]
    pub fn encode(
        self,
        array: &dyn arrow_array::Array,
        row: usize,
        buf: &mut Vec<u8>,
    ) -> Result<(), EncodeError> {
        match self {
            Self::Bool => encode_bool(array, row, buf),
            Self::Int32Varint => encode_int32_varint(array, row, buf),
            Self::Int32Zigzag => encode_int32_zigzag(array, row, buf),
            Self::Int32Fixed => encode_int32_fixed(array, row, buf),
            Self::Int64Varint => encode_int64_varint(array, row, buf),
            Self::Int64Zigzag => encode_int64_zigzag(array, row, buf),
            Self::Int64Fixed => encode_int64_fixed(array, row, buf),
            Self::UInt32Varint => encode_uint32_varint(array, row, buf),
            Self::UInt32Fixed => encode_uint32_fixed(array, row, buf),
            Self::UInt64Varint => encode_uint64_varint(array, row, buf),
            Self::UInt64Fixed => encode_uint64_fixed(array, row, buf),
            Self::Float32 => encode_float32(array, row, buf),
            Self::Float64 => encode_float64(array, row, buf),
            Self::Utf8 => encode_utf8(array, row, buf),
            Self::LargeUtf8 => encode_large_utf8(array, row, buf),
            Self::Binary => encode_binary(array, row, buf),
            Self::LargeBinary => encode_large_binary(array, row, buf),
            Self::Int32AsEnum => encode_int32_as_enum(array, row, buf),
            Self::TimestampS => encode_timestamp_s(array, row, buf),
            Self::TimestampMs => encode_timestamp_ms(array, row, buf),
            Self::TimestampUs => encode_timestamp_us(array, row, buf),
            Self::TimestampNs => encode_timestamp_ns(array, row, buf),
            Self::DurationS => encode_duration_s(array, row, buf),
            Self::DurationMs => encode_duration_ms(array, row, buf),
            Self::DurationUs => encode_duration_us(array, row, buf),
            Self::DurationNs => encode_duration_ns(array, row, buf),
            Self::Int64AsInt32Varint => encode_int64_as_int32_varint(array, row, buf),
            Self::Int64AsSint32 => encode_int64_as_sint32(array, row, buf),
            Self::Int64AsSfixed32 => encode_int64_as_sfixed32(array, row, buf),
            Self::Int32AsInt64Varint => encode_int32_as_int64_varint(array, row, buf),
            Self::Int32AsSint64 => encode_int32_as_sint64(array, row, buf),
            Self::Int32AsSfixed64 => encode_int32_as_sfixed64(array, row, buf),
            Self::UInt64AsUInt32Varint => encode_uint64_as_uint32_varint(array, row, buf),
            Self::UInt64AsFixed32 => encode_uint64_as_fixed32(array, row, buf),
            Self::UInt32AsUInt64Varint => encode_uint32_as_uint64_varint(array, row, buf),
            Self::UInt32AsFixed64 => encode_uint32_as_fixed64(array, row, buf),
            Self::Int32AsUInt32Varint => encode_int32_as_uint32_varint(array, row, buf),
            Self::Int32AsFixed32 => encode_int32_as_fixed32(array, row, buf),
            Self::Int32AsUInt64Varint => encode_int32_as_uint64_varint(array, row, buf),
            Self::Int32AsFixed64 => encode_int32_as_fixed64(array, row, buf),
            Self::Int64AsUInt32Varint => encode_int64_as_uint32_varint(array, row, buf),
            Self::Int64AsFixed32 => encode_int64_as_fixed32(array, row, buf),
            Self::Int64AsUInt64Varint => encode_int64_as_uint64_varint(array, row, buf),
            Self::Int64AsFixed64 => encode_int64_as_fixed64(array, row, buf),
            Self::UInt32AsInt32Varint => encode_uint32_as_int32_varint(array, row, buf),
            Self::UInt32AsSint32 => encode_uint32_as_sint32(array, row, buf),
            Self::UInt32AsSfixed32 => encode_uint32_as_sfixed32(array, row, buf),
            Self::UInt32AsInt64Varint => encode_uint32_as_int64_varint(array, row, buf),
            Self::UInt32AsSint64 => encode_uint32_as_sint64(array, row, buf),
            Self::UInt32AsSfixed64 => encode_uint32_as_sfixed64(array, row, buf),
            Self::UInt64AsInt32Varint => encode_uint64_as_int32_varint(array, row, buf),
            Self::UInt64AsSint32 => encode_uint64_as_sint32(array, row, buf),
            Self::UInt64AsSfixed32 => encode_uint64_as_sfixed32(array, row, buf),
            Self::UInt64AsInt64Varint => encode_uint64_as_int64_varint(array, row, buf),
            Self::UInt64AsSint64 => encode_uint64_as_sint64(array, row, buf),
            Self::UInt64AsSfixed64 => encode_uint64_as_sfixed64(array, row, buf),
            Self::Float64AsFloat32 => encode_float64_as_float32(array, row, buf),
            Self::Float32AsFloat64 => encode_float32_as_float64(array, row, buf),
            Self::Utf8AsBytes => encode_utf8_as_bytes(array, row, buf),
            Self::BinaryAsString => encode_binary_as_string(array, row, buf),
            Self::Int64AsEnum => encode_int64_as_int32_varint(array, row, buf),
            Self::Utf8AsInt(target) => {
                let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
                encode_str_as_int(arr.value(row), target, row, buf)
            }
            Self::LargeUtf8AsInt(target) => {
                let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
                encode_str_as_int(arr.value(row), target, row, buf)
            }
        }
    }
}

// === String-encoded integers ===
//
// proto3's canonical JSON encoding renders 64-bit integers as strings, so
// systems that decode proto into a structured view surface them as string
// columns. BigQuery is the common case here: a decoded `uint64` arrives as
// STRING and must reach an integer proto field without a SQL-side CAST.

/// Parse a string-encoded integer and write it with `target`'s wire encoding.
///
/// Unsigned targets parse as `u64` first and fall back to `i64` reinterpreted
/// as two's complement, matching the existing signed<->unsigned crossover
/// coercion (and the fact that some producers only have a signed integer
/// type). Narrowing to 32 bits is range-checked and fails the batch, mirroring
/// the `Int64 -> int32` coercion rather than silently truncating.
///
/// The parse is strict: no whitespace trimming, no empty-string-as-zero. A
/// caller that wants an empty string treated as absent should map it to NULL
/// in the source query, which is unambiguous.
fn encode_str_as_int(
    s: &str,
    target: IntTarget,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let err = |reason: String| EncodeError {
        row,
        field: String::new(),
        reason,
    };

    if target.is_unsigned() {
        let v = parse_unsigned(s)
            .ok_or_else(|| err(format!("string {s:?} is not a valid unsigned integer")))?;
        match target {
            IntTarget::UInt64Varint => wire::encode_varint(v, buf),
            IntTarget::Fixed64 => wire::encode_fixed64(v, buf),
            IntTarget::UInt32Varint => wire::encode_varint(narrow_u32(v, row)? as u64, buf),
            IntTarget::Fixed32 => wire::encode_fixed32(narrow_u32(v, row)?, buf),
            _ => unreachable!("is_unsigned() covers exactly these targets"),
        }
    } else {
        let v = s
            .parse::<i64>()
            .map_err(|_| err(format!("string {s:?} is not a valid signed integer")))?;
        match target {
            IntTarget::Int64Varint => wire::encode_varint(v as u64, buf),
            IntTarget::Sint64 => wire::encode_zigzag64(v, buf),
            IntTarget::Sfixed64 => wire::encode_fixed64(v as u64, buf),
            // Proto int32 negatives are sign-extended to 64 bits on the wire,
            // matching encode_int64_as_int32_varint.
            IntTarget::Int32Varint => wire::encode_varint(narrow_i32(v, row)? as u64, buf),
            IntTarget::Sint32 => wire::encode_zigzag32(narrow_i32(v, row)?, buf),
            IntTarget::Sfixed32 => wire::encode_fixed32(narrow_i32(v, row)? as u32, buf),
            _ => unreachable!("is_unsigned() covers the remaining targets"),
        }
    }
    Ok(())
}

/// Parse an unsigned integer, accepting a negative literal as its two's
/// complement reinterpretation (`"-1"` -> `u64::MAX`).
fn parse_unsigned(s: &str) -> Option<u64> {
    s.parse::<u64>()
        .ok()
        .or_else(|| s.parse::<i64>().ok().map(|v| v as u64))
}

/// Range-check a parsed value before narrowing to i32.
fn narrow_i32(v: i64, row: usize) -> Result<i32, EncodeError> {
    i32::try_from(v).map_err(|_| EncodeError {
        row,
        field: String::new(),
        reason: format!("value {v} out of range for int32"),
    })
}

/// Range-check a parsed value before narrowing to u32.
fn narrow_u32(v: u64, row: usize) -> Result<u32, EncodeError> {
    u32::try_from(v).map_err(|_| EncodeError {
        row,
        field: String::new(),
        reason: format!("value {v} out of range for uint32"),
    })
}

// === Boolean ===

pub fn encode_bool(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
    let v = if arr.value(row) { 1u64 } else { 0u64 };
    wire::encode_varint(v, buf);
    Ok(())
}

// === Int32 variants ===

pub fn encode_int32_varint(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
    wire::encode_varint(arr.value(row) as u32 as u64, buf);
    Ok(())
}

pub fn encode_int32_zigzag(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
    wire::encode_zigzag32(arr.value(row), buf);
    Ok(())
}

pub fn encode_int32_fixed(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
    wire::encode_fixed32(arr.value(row) as u32, buf);
    Ok(())
}

// === Int64 variants ===

pub fn encode_int64_varint(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
    wire::encode_varint(arr.value(row) as u64, buf);
    Ok(())
}

pub fn encode_int64_zigzag(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
    wire::encode_zigzag64(arr.value(row), buf);
    Ok(())
}

pub fn encode_int64_fixed(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
    wire::encode_fixed64(arr.value(row) as u64, buf);
    Ok(())
}

// === UInt32 variants ===

pub fn encode_uint32_varint(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
    wire::encode_varint(arr.value(row) as u64, buf);
    Ok(())
}

pub fn encode_uint32_fixed(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
    wire::encode_fixed32(arr.value(row), buf);
    Ok(())
}

// === UInt64 variants ===

pub fn encode_uint64_varint(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
    wire::encode_varint(arr.value(row), buf);
    Ok(())
}

pub fn encode_uint64_fixed(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
    wire::encode_fixed64(arr.value(row), buf);
    Ok(())
}

// === Float ===

pub fn encode_float32(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
    wire::encode_fixed32(arr.value(row).to_bits(), buf);
    Ok(())
}

pub fn encode_float64(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
    wire::encode_fixed64(arr.value(row).to_bits(), buf);
    Ok(())
}

// === String / Bytes ===

pub fn encode_utf8(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
    wire::encode_length_delimited(arr.value(row).as_bytes(), buf);
    Ok(())
}

pub fn encode_large_utf8(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
    wire::encode_length_delimited(arr.value(row).as_bytes(), buf);
    Ok(())
}

pub fn encode_binary(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
    wire::encode_length_delimited(arr.value(row), buf);
    Ok(())
}

pub fn encode_large_binary(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<LargeBinaryArray>().unwrap();
    wire::encode_length_delimited(arr.value(row), buf);
    Ok(())
}

// === Coercions ===

/// Helper: read i64, check i32 range, return narrowed value.
fn read_i64_as_i32(array: &dyn arrow_array::Array, row: usize) -> Result<i32, EncodeError> {
    let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
    let v = arr.value(row);
    if v < i32::MIN as i64 || v > i32::MAX as i64 {
        return Err(EncodeError {
            row,
            field: String::new(),
            reason: format!("value {v} out of range for int32"),
        });
    }
    Ok(v as i32)
}

/// Helper: read u64, check u32 range, return narrowed value.
fn read_u64_as_u32(array: &dyn arrow_array::Array, row: usize) -> Result<u32, EncodeError> {
    let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
    let v = arr.value(row);
    if v > u32::MAX as u64 {
        return Err(EncodeError {
            row,
            field: String::new(),
            reason: format!("value {v} out of range for uint32"),
        });
    }
    Ok(v as u32)
}

// Int64 → int32 (varint, sign-extended to 64 bits per proto spec)
pub fn encode_int64_as_int32_varint(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let v = read_i64_as_i32(array, row)?;
    // Proto int32 negative values are sign-extended to 64 bits on the wire.
    wire::encode_varint(v as u64, buf);
    Ok(())
}

// Int64 → sint32 (zigzag)
pub fn encode_int64_as_sint32(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let v = read_i64_as_i32(array, row)?;
    wire::encode_zigzag32(v, buf);
    Ok(())
}

// Int64 → sfixed32 (fixed 4 bytes)
pub fn encode_int64_as_sfixed32(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let v = read_i64_as_i32(array, row)?;
    wire::encode_fixed32(v as u32, buf);
    Ok(())
}

// Int32 → int64 (varint, widening)
pub fn encode_int32_as_int64_varint(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
    wire::encode_varint(arr.value(row) as i64 as u64, buf);
    Ok(())
}

// Int32 → sint64 (zigzag, widening)
pub fn encode_int32_as_sint64(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
    wire::encode_zigzag64(arr.value(row) as i64, buf);
    Ok(())
}

// Int32 → sfixed64 (fixed 8 bytes, widening)
pub fn encode_int32_as_sfixed64(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
    wire::encode_fixed64(arr.value(row) as i64 as u64, buf);
    Ok(())
}

// UInt64 → uint32 (varint, truncation check)
pub fn encode_uint64_as_uint32_varint(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let v = read_u64_as_u32(array, row)?;
    wire::encode_varint(v as u64, buf);
    Ok(())
}

// UInt64 → fixed32 (fixed 4 bytes, truncation check)
pub fn encode_uint64_as_fixed32(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let v = read_u64_as_u32(array, row)?;
    wire::encode_fixed32(v, buf);
    Ok(())
}

// UInt32 → uint64 (varint, widening)
pub fn encode_uint32_as_uint64_varint(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
    wire::encode_varint(arr.value(row) as u64, buf);
    Ok(())
}

// UInt32 → fixed64 (fixed 8 bytes, widening)
pub fn encode_uint32_as_fixed64(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
    wire::encode_fixed64(arr.value(row) as u64, buf);
    Ok(())
}

// === Signed → unsigned crossover (two's-complement reinterpretation) ===
//
// No range checks: values are bit-cast like a C cast, matching what a
// protobuf decoder does when a field changes between int and uint kinds.
// Negatives become large unsigned values, widening sign-extends, narrowing
// truncates.

// Int32 → uint32 (varint, -1 → u32::MAX)
pub fn encode_int32_as_uint32_varint(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
    wire::encode_varint(arr.value(row) as u32 as u64, buf);
    Ok(())
}

// Int32 → fixed32 (fixed 4 bytes, bit-identical)
pub fn encode_int32_as_fixed32(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
    wire::encode_fixed32(arr.value(row) as u32, buf);
    Ok(())
}

// Int32 → uint64 (varint, sign-extends: -1 → u64::MAX)
pub fn encode_int32_as_uint64_varint(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
    wire::encode_varint(arr.value(row) as i64 as u64, buf);
    Ok(())
}

// Int32 → fixed64 (fixed 8 bytes, sign-extends)
pub fn encode_int32_as_fixed64(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
    wire::encode_fixed64(arr.value(row) as i64 as u64, buf);
    Ok(())
}

// Int64 → uint32 (varint, truncates to low 32 bits)
pub fn encode_int64_as_uint32_varint(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
    wire::encode_varint(arr.value(row) as u32 as u64, buf);
    Ok(())
}

// Int64 → fixed32 (fixed 4 bytes, truncates to low 32 bits)
pub fn encode_int64_as_fixed32(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
    wire::encode_fixed32(arr.value(row) as u32, buf);
    Ok(())
}

// Int64 → uint64 (varint, -1 → u64::MAX)
pub fn encode_int64_as_uint64_varint(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
    wire::encode_varint(arr.value(row) as u64, buf);
    Ok(())
}

// Int64 → fixed64 (fixed 8 bytes, bit-identical)
pub fn encode_int64_as_fixed64(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
    wire::encode_fixed64(arr.value(row) as u64, buf);
    Ok(())
}

// === Unsigned → signed crossover (two's-complement reinterpretation) ===

// UInt32 → int32 (varint, u32::MAX → -1; negatives sign-extended to 64 bits
// on the wire per proto spec)
pub fn encode_uint32_as_int32_varint(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
    wire::encode_varint(arr.value(row) as i32 as i64 as u64, buf);
    Ok(())
}

// UInt32 → sint32 (zigzag, u32::MAX → -1)
pub fn encode_uint32_as_sint32(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
    wire::encode_zigzag32(arr.value(row) as i32, buf);
    Ok(())
}

// UInt32 → sfixed32 (fixed 4 bytes, bit-identical)
pub fn encode_uint32_as_sfixed32(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
    wire::encode_fixed32(arr.value(row), buf);
    Ok(())
}

// UInt32 → int64 (varint, lossless — every u32 fits in i64)
pub fn encode_uint32_as_int64_varint(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
    wire::encode_varint(arr.value(row) as u64, buf);
    Ok(())
}

// UInt32 → sint64 (zigzag, lossless)
pub fn encode_uint32_as_sint64(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
    wire::encode_zigzag64(arr.value(row) as i64, buf);
    Ok(())
}

// UInt32 → sfixed64 (fixed 8 bytes, lossless)
pub fn encode_uint32_as_sfixed64(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
    wire::encode_fixed64(arr.value(row) as u64, buf);
    Ok(())
}

// UInt64 → int32 (varint, truncates to low 32 bits; negatives sign-extended
// to 64 bits on the wire per proto spec)
pub fn encode_uint64_as_int32_varint(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
    wire::encode_varint(arr.value(row) as i32 as i64 as u64, buf);
    Ok(())
}

// UInt64 → sint32 (zigzag, truncates to low 32 bits)
pub fn encode_uint64_as_sint32(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
    wire::encode_zigzag32(arr.value(row) as i32, buf);
    Ok(())
}

// UInt64 → sfixed32 (fixed 4 bytes, truncates to low 32 bits)
pub fn encode_uint64_as_sfixed32(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
    wire::encode_fixed32(arr.value(row) as u32, buf);
    Ok(())
}

// UInt64 → int64 (varint, u64::MAX → -1; bit-identical on the wire)
pub fn encode_uint64_as_int64_varint(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
    wire::encode_varint(arr.value(row), buf);
    Ok(())
}

// UInt64 → sint64 (zigzag, u64::MAX → -1)
pub fn encode_uint64_as_sint64(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
    wire::encode_zigzag64(arr.value(row) as i64, buf);
    Ok(())
}

// UInt64 → sfixed64 (fixed 8 bytes, bit-identical)
pub fn encode_uint64_as_sfixed64(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
    wire::encode_fixed64(arr.value(row), buf);
    Ok(())
}

pub fn encode_float64_as_float32(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
    wire::encode_fixed32((arr.value(row) as f32).to_bits(), buf);
    Ok(())
}

pub fn encode_float32_as_float64(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
    wire::encode_fixed64((arr.value(row) as f64).to_bits(), buf);
    Ok(())
}

pub fn encode_utf8_as_bytes(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
    wire::encode_length_delimited(arr.value(row).as_bytes(), buf);
    Ok(())
}

pub fn encode_binary_as_string(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
    let bytes = arr.value(row);
    if std::str::from_utf8(bytes).is_err() {
        return Err(EncodeError {
            row,
            field: String::new(),
            reason: "binary value is not valid UTF-8".to_string(),
        });
    }
    wire::encode_length_delimited(bytes, buf);
    Ok(())
}

// === Enum encoding (Int32 → enum, runtime range check) ===

pub fn encode_int32_as_enum(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
    // We encode the value as a varint — range checking happens at the plan level
    // or is deferred to the consumer. Proto3 allows unknown enum values.
    wire::encode_varint(arr.value(row) as u32 as u64, buf);
    Ok(())
}

// === Well-known type encoders ===
//
// google.protobuf.Timestamp: seconds (field 1, int64 varint) + nanos (field 2, int32 varint)
// google.protobuf.Duration: same structure.
//
// The encoder writes a length-delimited message body (no outer tag — caller handles that).
// Field tags: seconds = (1 << 3 | 0) = 0x08, nanos = (2 << 3 | 0) = 0x10.

fn encode_seconds_nanos(seconds: i64, nanos: i32, buf: &mut Vec<u8>) {
    // Max message size: tag(1) + varint(10) + tag(1) + varint(5) = 17 bytes.
    // Use a stack buffer to avoid heap allocation.
    let mut msg = [0u8; 22];
    let mut len = 0;

    macro_rules! push {
        ($b:expr) => {
            msg[len] = $b;
            len += 1;
        };
    }

    if seconds != 0 {
        // Tag for field 1, varint = 0x08
        push!(0x08);
        let mut v = seconds as u64;
        while v >= 0x80 {
            push!((v as u8) | 0x80);
            v >>= 7;
        }
        push!(v as u8);
    }
    if nanos != 0 {
        // Tag for field 2, varint = 0x10
        push!(0x10);
        let mut v = nanos as u32 as u64;
        while v >= 0x80 {
            push!((v as u8) | 0x80);
            v >>= 7;
        }
        push!(v as u8);
    }

    wire::encode_length_delimited(&msg[..len], buf);
}

/// Split a value in sub-second units into (seconds, nanos) using Euclidean
/// division so that nanos is always non-negative (required by
/// google.protobuf.Timestamp spec).
#[inline]
fn timestamp_split(value: i64, units_per_second: i64, nanos_per_unit: i64) -> (i64, i32) {
    let seconds = value.div_euclid(units_per_second);
    let nanos = (value.rem_euclid(units_per_second) * nanos_per_unit) as i32;
    (seconds, nanos)
}

/// Split a value in sub-second units into (seconds, nanos) using truncation
/// toward zero so that nanos sign matches seconds sign (required by
/// google.protobuf.Duration spec).
#[inline]
fn duration_split(value: i64, units_per_second: i64, nanos_per_unit: i64) -> (i64, i32) {
    let seconds = value / units_per_second;
    let nanos = ((value % units_per_second) * nanos_per_unit) as i32;
    (seconds, nanos)
}

/// Arrow Timestamp(Second) → google.protobuf.Timestamp
pub fn encode_timestamp_s(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array
        .as_any()
        .downcast_ref::<TimestampSecondArray>()
        .unwrap();
    encode_seconds_nanos(arr.value(row), 0, buf);
    Ok(())
}

/// Arrow Timestamp(Millisecond) → google.protobuf.Timestamp
pub fn encode_timestamp_ms(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array
        .as_any()
        .downcast_ref::<TimestampMillisecondArray>()
        .unwrap();
    let (s, n) = timestamp_split(arr.value(row), 1_000, 1_000_000);
    encode_seconds_nanos(s, n, buf);
    Ok(())
}

/// Arrow Timestamp(Microsecond) → google.protobuf.Timestamp
pub fn encode_timestamp_us(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array
        .as_any()
        .downcast_ref::<TimestampMicrosecondArray>()
        .unwrap();
    let (s, n) = timestamp_split(arr.value(row), 1_000_000, 1_000);
    encode_seconds_nanos(s, n, buf);
    Ok(())
}

/// Arrow Timestamp(Nanosecond) → google.protobuf.Timestamp
pub fn encode_timestamp_ns(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array
        .as_any()
        .downcast_ref::<TimestampNanosecondArray>()
        .unwrap();
    let (s, n) = timestamp_split(arr.value(row), 1_000_000_000, 1);
    encode_seconds_nanos(s, n, buf);
    Ok(())
}

/// Arrow Duration(Second) → google.protobuf.Duration
pub fn encode_duration_s(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array
        .as_any()
        .downcast_ref::<DurationSecondArray>()
        .unwrap();
    encode_seconds_nanos(arr.value(row), 0, buf);
    Ok(())
}

/// Arrow Duration(Millisecond) → google.protobuf.Duration
pub fn encode_duration_ms(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array
        .as_any()
        .downcast_ref::<DurationMillisecondArray>()
        .unwrap();
    let (s, n) = duration_split(arr.value(row), 1_000, 1_000_000);
    encode_seconds_nanos(s, n, buf);
    Ok(())
}

/// Arrow Duration(Microsecond) → google.protobuf.Duration
pub fn encode_duration_us(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array
        .as_any()
        .downcast_ref::<DurationMicrosecondArray>()
        .unwrap();
    let (s, n) = duration_split(arr.value(row), 1_000_000, 1_000);
    encode_seconds_nanos(s, n, buf);
    Ok(())
}

/// Arrow Duration(Nanosecond) → google.protobuf.Duration
pub fn encode_duration_ns(
    array: &dyn arrow_array::Array,
    row: usize,
    buf: &mut Vec<u8>,
) -> Result<(), EncodeError> {
    let arr = array
        .as_any()
        .downcast_ref::<DurationNanosecondArray>()
        .unwrap();
    let (s, n) = duration_split(arr.value(row), 1_000_000_000, 1);
    encode_seconds_nanos(s, n, buf);
    Ok(())
}
