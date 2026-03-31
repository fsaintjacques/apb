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

/// Function type for scalar encoding.
/// Takes: array, row index, output buffer.
pub type ScalarEncodeFn =
    fn(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError>;

// === Boolean ===

pub fn encode_bool(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<BooleanArray>().unwrap();
    let v = if arr.value(row) { 1u64 } else { 0u64 };
    wire::encode_varint(v, buf);
    Ok(())
}

// === Int32 variants ===

pub fn encode_int32_varint(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
    wire::encode_varint(arr.value(row) as u32 as u64, buf);
    Ok(())
}

pub fn encode_int32_zigzag(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
    wire::encode_zigzag32(arr.value(row), buf);
    Ok(())
}

pub fn encode_int32_fixed(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
    wire::encode_fixed32(arr.value(row) as u32, buf);
    Ok(())
}

// === Int64 variants ===

pub fn encode_int64_varint(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
    wire::encode_varint(arr.value(row) as u64, buf);
    Ok(())
}

pub fn encode_int64_zigzag(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
    wire::encode_zigzag64(arr.value(row), buf);
    Ok(())
}

pub fn encode_int64_fixed(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Int64Array>().unwrap();
    wire::encode_fixed64(arr.value(row) as u64, buf);
    Ok(())
}

// === UInt32 variants ===

pub fn encode_uint32_varint(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
    wire::encode_varint(arr.value(row) as u64, buf);
    Ok(())
}

pub fn encode_uint32_fixed(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
    wire::encode_fixed32(arr.value(row), buf);
    Ok(())
}

// === UInt64 variants ===

pub fn encode_uint64_varint(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
    wire::encode_varint(arr.value(row), buf);
    Ok(())
}

pub fn encode_uint64_fixed(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<UInt64Array>().unwrap();
    wire::encode_fixed64(arr.value(row), buf);
    Ok(())
}

// === Float ===

pub fn encode_float32(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
    wire::encode_fixed32(arr.value(row).to_bits(), buf);
    Ok(())
}

pub fn encode_float64(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
    wire::encode_fixed64(arr.value(row).to_bits(), buf);
    Ok(())
}

// === String / Bytes ===

pub fn encode_utf8(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
    wire::encode_length_delimited(arr.value(row).as_bytes(), buf);
    Ok(())
}

pub fn encode_large_utf8(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<LargeStringArray>().unwrap();
    wire::encode_length_delimited(arr.value(row).as_bytes(), buf);
    Ok(())
}

pub fn encode_binary(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<BinaryArray>().unwrap();
    wire::encode_length_delimited(arr.value(row), buf);
    Ok(())
}

pub fn encode_large_binary(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
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
pub fn encode_int64_as_int32_varint(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let v = read_i64_as_i32(array, row)?;
    // Proto int32 negative values are sign-extended to 64 bits on the wire.
    wire::encode_varint(v as u64, buf);
    Ok(())
}

// Int64 → sint32 (zigzag)
pub fn encode_int64_as_sint32(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let v = read_i64_as_i32(array, row)?;
    wire::encode_zigzag32(v, buf);
    Ok(())
}

// Int64 → sfixed32 (fixed 4 bytes)
pub fn encode_int64_as_sfixed32(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let v = read_i64_as_i32(array, row)?;
    wire::encode_fixed32(v as u32, buf);
    Ok(())
}

// Int32 → int64 (varint, widening)
pub fn encode_int32_as_int64_varint(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
    wire::encode_varint(arr.value(row) as i64 as u64, buf);
    Ok(())
}

// Int32 → sint64 (zigzag, widening)
pub fn encode_int32_as_sint64(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
    wire::encode_zigzag64(arr.value(row) as i64, buf);
    Ok(())
}

// Int32 → sfixed64 (fixed 8 bytes, widening)
pub fn encode_int32_as_sfixed64(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Int32Array>().unwrap();
    wire::encode_fixed64(arr.value(row) as i64 as u64, buf);
    Ok(())
}

// UInt64 → uint32 (varint, truncation check)
pub fn encode_uint64_as_uint32_varint(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let v = read_u64_as_u32(array, row)?;
    wire::encode_varint(v as u64, buf);
    Ok(())
}

// UInt64 → fixed32 (fixed 4 bytes, truncation check)
pub fn encode_uint64_as_fixed32(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let v = read_u64_as_u32(array, row)?;
    wire::encode_fixed32(v, buf);
    Ok(())
}

// UInt32 → uint64 (varint, widening)
pub fn encode_uint32_as_uint64_varint(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
    wire::encode_varint(arr.value(row) as u64, buf);
    Ok(())
}

// UInt32 → fixed64 (fixed 8 bytes, widening)
pub fn encode_uint32_as_fixed64(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<UInt32Array>().unwrap();
    wire::encode_fixed64(arr.value(row) as u64, buf);
    Ok(())
}

pub fn encode_float64_as_float32(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Float64Array>().unwrap();
    wire::encode_fixed32((arr.value(row) as f32).to_bits(), buf);
    Ok(())
}

pub fn encode_float32_as_float64(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<Float32Array>().unwrap();
    wire::encode_fixed64((arr.value(row) as f64).to_bits(), buf);
    Ok(())
}

pub fn encode_utf8_as_bytes(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<StringArray>().unwrap();
    wire::encode_length_delimited(arr.value(row).as_bytes(), buf);
    Ok(())
}

pub fn encode_binary_as_string(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
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

pub fn encode_int32_as_enum(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
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
        ($b:expr) => { msg[len] = $b; len += 1; };
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
pub fn encode_timestamp_s(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<TimestampSecondArray>().unwrap();
    encode_seconds_nanos(arr.value(row), 0, buf);
    Ok(())
}

/// Arrow Timestamp(Millisecond) → google.protobuf.Timestamp
pub fn encode_timestamp_ms(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
    let (s, n) = timestamp_split(arr.value(row), 1_000, 1_000_000);
    encode_seconds_nanos(s, n, buf);
    Ok(())
}

/// Arrow Timestamp(Microsecond) → google.protobuf.Timestamp
pub fn encode_timestamp_us(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<TimestampMicrosecondArray>().unwrap();
    let (s, n) = timestamp_split(arr.value(row), 1_000_000, 1_000);
    encode_seconds_nanos(s, n, buf);
    Ok(())
}

/// Arrow Timestamp(Nanosecond) → google.protobuf.Timestamp
pub fn encode_timestamp_ns(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<TimestampNanosecondArray>().unwrap();
    let (s, n) = timestamp_split(arr.value(row), 1_000_000_000, 1);
    encode_seconds_nanos(s, n, buf);
    Ok(())
}

/// Arrow Duration(Second) → google.protobuf.Duration
pub fn encode_duration_s(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<DurationSecondArray>().unwrap();
    encode_seconds_nanos(arr.value(row), 0, buf);
    Ok(())
}

/// Arrow Duration(Millisecond) → google.protobuf.Duration
pub fn encode_duration_ms(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<DurationMillisecondArray>().unwrap();
    let (s, n) = duration_split(arr.value(row), 1_000, 1_000_000);
    encode_seconds_nanos(s, n, buf);
    Ok(())
}

/// Arrow Duration(Microsecond) → google.protobuf.Duration
pub fn encode_duration_us(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<DurationMicrosecondArray>().unwrap();
    let (s, n) = duration_split(arr.value(row), 1_000_000, 1_000);
    encode_seconds_nanos(s, n, buf);
    Ok(())
}

/// Arrow Duration(Nanosecond) → google.protobuf.Duration
pub fn encode_duration_ns(array: &dyn arrow_array::Array, row: usize, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
    let arr = array.as_any().downcast_ref::<DurationNanosecondArray>().unwrap();
    let (s, n) = duration_split(arr.value(row), 1_000_000_000, 1);
    encode_seconds_nanos(s, n, buf);
    Ok(())
}
