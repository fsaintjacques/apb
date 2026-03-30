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
