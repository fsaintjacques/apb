use std::sync::Arc;

use arrow_array::*;
use arrow_schema::{DataType, Field, Schema};
use prost_reflect::DynamicMessage;

use crate::descriptor::ProtoSchema;
use crate::mapping::{infer_mapping, InferOptions};
use super::*;

const SCALARS_BIN: &[u8] = include_bytes!("../../fixtures/scalars.bin");

fn scalars_schema() -> ProtoSchema {
    ProtoSchema::from_bytes(SCALARS_BIN).unwrap()
}

/// Helper: build a transcoder from Arrow schema + proto message name.
fn build_transcoder(
    arrow_schema: &Schema,
    proto_schema: &ProtoSchema,
    message_name: &str,
) -> Transcoder {
    let msg = proto_schema.message(message_name).unwrap();
    let mapping = infer_mapping(arrow_schema, &msg, &InferOptions::default()).unwrap();
    Transcoder::new(&mapping).unwrap()
}

/// Decode a single proto message from bytes using prost-reflect DynamicMessage.
fn decode_message(bytes: &[u8], schema: &ProtoSchema, message_name: &str) -> DynamicMessage {
    let msg_desc = schema.message(message_name).unwrap();
    DynamicMessage::decode(msg_desc, bytes).unwrap()
}

/// Decode a varint-delimited stream into individual message byte slices.
fn split_delimited(data: &[u8]) -> Vec<Vec<u8>> {
    let mut messages = Vec::new();
    let mut pos = 0;
    while pos < data.len() {
        let (len, bytes_read) = decode_varint(&data[pos..]);
        pos += bytes_read;
        messages.push(data[pos..pos + len as usize].to_vec());
        pos += len as usize;
    }
    messages
}

fn decode_varint(data: &[u8]) -> (u64, usize) {
    let mut value: u64 = 0;
    let mut shift = 0;
    for (i, &byte) in data.iter().enumerate() {
        value |= ((byte & 0x7F) as u64) << shift;
        if byte & 0x80 == 0 {
            return (value, i + 1);
        }
        shift += 7;
    }
    panic!("unterminated varint");
}

// ==================== Round-trip tests ====================

#[test]
fn roundtrip_bool() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("bool_field", DataType::Boolean, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(BooleanArray::from(vec![true, false]))],
    ).unwrap();

    let mut transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&mut batch.clone(), &mut output).unwrap();

    let messages = split_delimited(&output);
    assert_eq!(messages.len(), 2);

    let msg0 = decode_message(&messages[0], &schema, "fixtures.Scalars");
    assert_eq!(
        msg0.get_field_by_name("bool_field").unwrap().as_bool().unwrap(),
        true
    );

    let msg1 = decode_message(&messages[1], &schema, "fixtures.Scalars");
    assert_eq!(
        msg1.get_field_by_name("bool_field").unwrap().as_bool().unwrap(),
        false
    );
}

#[test]
fn roundtrip_int32() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("int32_field", DataType::Int32, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(Int32Array::from(vec![0, 42, -1, i32::MAX, i32::MIN]))],
    ).unwrap();

    let mut transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&mut batch.clone(), &mut output).unwrap();

    let messages = split_delimited(&output);
    assert_eq!(messages.len(), 5);

    let msg = decode_message(&messages[1], &schema, "fixtures.Scalars");
    assert_eq!(msg.get_field_by_name("int32_field").unwrap().as_i32().unwrap(), 42);

    let msg = decode_message(&messages[3], &schema, "fixtures.Scalars");
    assert_eq!(msg.get_field_by_name("int32_field").unwrap().as_i32().unwrap(), i32::MAX);
}

#[test]
fn roundtrip_int64() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("int64_field", DataType::Int64, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(Int64Array::from(vec![0i64, 123456789, -1]))],
    ).unwrap();

    let mut transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&mut batch.clone(), &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[1], &schema, "fixtures.Scalars");
    assert_eq!(msg.get_field_by_name("int64_field").unwrap().as_i64().unwrap(), 123456789);
}

#[test]
fn roundtrip_uint32() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("uint32_field", DataType::UInt32, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(UInt32Array::from(vec![0u32, 42, u32::MAX]))],
    ).unwrap();

    let mut transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&mut batch.clone(), &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[2], &schema, "fixtures.Scalars");
    assert_eq!(msg.get_field_by_name("uint32_field").unwrap().as_u32().unwrap(), u32::MAX);
}

#[test]
fn roundtrip_uint64() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("uint64_field", DataType::UInt64, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(UInt64Array::from(vec![0u64, u64::MAX]))],
    ).unwrap();

    let mut transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&mut batch.clone(), &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[1], &schema, "fixtures.Scalars");
    assert_eq!(msg.get_field_by_name("uint64_field").unwrap().as_u64().unwrap(), u64::MAX);
}

#[test]
fn roundtrip_float32() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("float_field", DataType::Float32, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(Float32Array::from(vec![3.14f32, 0.0, -1.5]))],
    ).unwrap();

    let mut transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&mut batch.clone(), &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[0], &schema, "fixtures.Scalars");
    let v = msg.get_field_by_name("float_field").unwrap().as_f32().unwrap();
    assert!((v - 3.14).abs() < 0.001);
}

#[test]
fn roundtrip_float64() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("double_field", DataType::Float64, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(Float64Array::from(vec![3.14159265358979]))],
    ).unwrap();

    let mut transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&mut batch.clone(), &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[0], &schema, "fixtures.Scalars");
    let v = msg.get_field_by_name("double_field").unwrap().as_f64().unwrap();
    assert!((v - 3.14159265358979).abs() < 1e-10);
}

#[test]
fn roundtrip_string() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("string_field", DataType::Utf8, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(StringArray::from(vec!["hello", "", "world"]))],
    ).unwrap();

    let mut transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&mut batch.clone(), &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[0], &schema, "fixtures.Scalars");
    assert_eq!(msg.get_field_by_name("string_field").unwrap().as_str().unwrap(), "hello");
}

#[test]
fn roundtrip_bytes() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("bytes_field", DataType::Binary, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(BinaryArray::from(vec![b"data".as_ref(), b"", b"\x00\x01"]))],
    ).unwrap();

    let mut transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&mut batch.clone(), &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[0], &schema, "fixtures.Scalars");
    assert_eq!(msg.get_field_by_name("bytes_field").unwrap().as_bytes().unwrap().as_ref(), b"data");
}

// ==================== Zigzag and fixed types ====================

#[test]
fn roundtrip_sint32() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("sint32_field", DataType::Int32, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(Int32Array::from(vec![0, -1, 1, i32::MIN, i32::MAX]))],
    ).unwrap();

    let mut transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&mut batch.clone(), &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[1], &schema, "fixtures.Scalars");
    assert_eq!(msg.get_field_by_name("sint32_field").unwrap().as_i32().unwrap(), -1);

    let msg = decode_message(&messages[3], &schema, "fixtures.Scalars");
    assert_eq!(msg.get_field_by_name("sint32_field").unwrap().as_i32().unwrap(), i32::MIN);
}

#[test]
fn roundtrip_sint64() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("sint64_field", DataType::Int64, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(Int64Array::from(vec![0i64, -1, i64::MIN]))],
    ).unwrap();

    let mut transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&mut batch.clone(), &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[1], &schema, "fixtures.Scalars");
    assert_eq!(msg.get_field_by_name("sint64_field").unwrap().as_i64().unwrap(), -1);
}

#[test]
fn roundtrip_sfixed32() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("sfixed32_field", DataType::Int32, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(Int32Array::from(vec![0, -1, 42]))],
    ).unwrap();

    let mut transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&mut batch.clone(), &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[1], &schema, "fixtures.Scalars");
    assert_eq!(msg.get_field_by_name("sfixed32_field").unwrap().as_i32().unwrap(), -1);
}

#[test]
fn roundtrip_sfixed64() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("sfixed64_field", DataType::Int64, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(Int64Array::from(vec![0i64, -1, 42]))],
    ).unwrap();

    let mut transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&mut batch.clone(), &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[1], &schema, "fixtures.Scalars");
    assert_eq!(msg.get_field_by_name("sfixed64_field").unwrap().as_i64().unwrap(), -1);
}

#[test]
fn roundtrip_fixed32() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("fixed32_field", DataType::UInt32, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(UInt32Array::from(vec![0u32, 42, u32::MAX]))],
    ).unwrap();

    let mut transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&mut batch.clone(), &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[1], &schema, "fixtures.Scalars");
    assert_eq!(msg.get_field_by_name("fixed32_field").unwrap().as_u32().unwrap(), 42);
}

#[test]
fn roundtrip_fixed64() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("fixed64_field", DataType::UInt64, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(UInt64Array::from(vec![0u64, 42, u64::MAX]))],
    ).unwrap();

    let mut transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&mut batch.clone(), &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[1], &schema, "fixtures.Scalars");
    assert_eq!(msg.get_field_by_name("fixed64_field").unwrap().as_u64().unwrap(), 42);
}

// ==================== Multiple fields ====================

#[test]
fn roundtrip_multiple_fields() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![
        Field::new("bool_field", DataType::Boolean, false),
        Field::new("int32_field", DataType::Int32, false),
        Field::new("string_field", DataType::Utf8, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![
            Arc::new(BooleanArray::from(vec![true])),
            Arc::new(Int32Array::from(vec![42])),
            Arc::new(StringArray::from(vec!["hello"])),
        ],
    ).unwrap();

    let mut transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&mut batch.clone(), &mut output).unwrap();

    let messages = split_delimited(&output);
    assert_eq!(messages.len(), 1);

    let msg = decode_message(&messages[0], &schema, "fixtures.Scalars");
    assert_eq!(msg.get_field_by_name("bool_field").unwrap().as_bool().unwrap(), true);
    assert_eq!(msg.get_field_by_name("int32_field").unwrap().as_i32().unwrap(), 42);
    assert_eq!(msg.get_field_by_name("string_field").unwrap().as_str().unwrap(), "hello");
}

// ==================== Null handling ====================

#[test]
fn null_fields_skipped() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![
        Field::new("int32_field", DataType::Int32, true),
        Field::new("string_field", DataType::Utf8, true),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![Some(42), None])),
            Arc::new(StringArray::from(vec![Some("hello"), None])),
        ],
    ).unwrap();

    let mut transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&mut batch.clone(), &mut output).unwrap();

    let messages = split_delimited(&output);
    assert_eq!(messages.len(), 2);

    // Row 0: both fields present.
    let msg0 = decode_message(&messages[0], &schema, "fixtures.Scalars");
    assert_eq!(msg0.get_field_by_name("int32_field").unwrap().as_i32().unwrap(), 42);

    // Row 1: both null → empty message (proto defaults).
    assert!(messages[1].is_empty());
}

// ==================== Empty batch ====================

#[test]
fn empty_batch() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("int32_field", DataType::Int32, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(Int32Array::from(Vec::<i32>::new()))],
    ).unwrap();

    let mut transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&mut batch.clone(), &mut output).unwrap();

    assert!(output.is_empty());
}

// ==================== Arrow output ====================

#[test]
fn arrow_output_basic() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![
        Field::new("int32_field", DataType::Int32, false),
        Field::new("string_field", DataType::Utf8, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![
            Arc::new(Int32Array::from(vec![1, 2, 3])),
            Arc::new(StringArray::from(vec!["a", "bb", "ccc"])),
        ],
    ).unwrap();

    let mut transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let result = transcoder.transcode_arrow(&batch).unwrap();

    assert_eq!(result.len(), 3);

    // Decode each element.
    for i in 0..3 {
        let bytes = result.value(i);
        let msg = decode_message(bytes, &schema, "fixtures.Scalars");
        assert_eq!(msg.get_field_by_name("int32_field").unwrap().as_i32().unwrap(), (i + 1) as i32);
    }
}

#[test]
fn arrow_output_empty_batch() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("int32_field", DataType::Int32, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(Int32Array::from(Vec::<i32>::new()))],
    ).unwrap();

    let mut transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let result = transcoder.transcode_arrow(&batch).unwrap();
    assert_eq!(result.len(), 0);
}

// ==================== Coercion ====================

#[test]
fn coercion_int64_to_int32_valid() {
    let schema = scalars_schema();
    let msg = schema.message("fixtures.Scalars").unwrap();

    let arrow_schema = Schema::new(vec![Field::new("int32_field", DataType::Int64, false)]);

    // Need explicit mapping with coerce=true since types don't match directly.
    use crate::mapping::{explicit_mapping, ExplicitBinding, ArrowFieldRef, ProtoFieldRef};
    let mapping = explicit_mapping(
        &arrow_schema,
        &msg,
        &[ExplicitBinding {
            arrow_field: ArrowFieldRef::Index(0),
            proto_field: ProtoFieldRef::Name("int32_field".to_string()),
            coerce: true,
        }],
    ).unwrap();

    let mut transcoder = Transcoder::new(&mapping).unwrap();

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![Arc::new(Int64Array::from(vec![42i64, -1]))],
    ).unwrap();

    let mut output = Vec::new();
    transcoder.transcode_delimited(&mut batch.clone(), &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg0 = decode_message(&messages[0], &schema, "fixtures.Scalars");
    assert_eq!(msg0.get_field_by_name("int32_field").unwrap().as_i32().unwrap(), 42);
}

#[test]
fn coercion_int64_to_int32_overflow() {
    let schema = scalars_schema();
    let msg = schema.message("fixtures.Scalars").unwrap();

    let arrow_schema = Schema::new(vec![Field::new("int32_field", DataType::Int64, false)]);

    use crate::mapping::{explicit_mapping, ExplicitBinding, ArrowFieldRef, ProtoFieldRef};
    let mapping = explicit_mapping(
        &arrow_schema,
        &msg,
        &[ExplicitBinding {
            arrow_field: ArrowFieldRef::Index(0),
            proto_field: ProtoFieldRef::Name("int32_field".to_string()),
            coerce: true,
        }],
    ).unwrap();

    let mut transcoder = Transcoder::new(&mapping).unwrap();

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![Arc::new(Int64Array::from(vec![i64::MAX]))],
    ).unwrap();

    let mut output = Vec::new();
    let result = transcoder.transcode_delimited(&mut batch.clone(), &mut output);
    assert!(result.is_err());

    let err = result.unwrap_err();
    let err_str = err.to_string();
    assert!(err_str.contains("row 0"), "error should mention row: {err_str}");
    assert!(err_str.contains("int32_field"), "error should mention field: {err_str}");
}
