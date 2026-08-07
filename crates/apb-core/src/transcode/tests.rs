use std::sync::Arc;

use arrow_array::*;
use arrow_schema::{DataType, Field, Schema};
use prost_reflect::DynamicMessage;

use super::*;
use crate::descriptor::ProtoSchema;
use crate::mapping::{infer_mapping, InferOptions};

use arrow_array::builder::*;
use arrow_schema::Fields;

const SCALARS_BIN: &[u8] = include_bytes!("../../fixtures/scalars.bin");
const NESTED_BIN: &[u8] = include_bytes!("../../fixtures/nested.bin");

fn scalars_schema() -> ProtoSchema {
    ProtoSchema::from_bytes(SCALARS_BIN).unwrap()
}

fn nested_schema() -> ProtoSchema {
    ProtoSchema::from_bytes(NESTED_BIN).unwrap()
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
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    assert_eq!(messages.len(), 2);

    let msg0 = decode_message(&messages[0], &schema, "fixtures.Scalars");
    assert_eq!(
        msg0.get_field_by_name("bool_field")
            .unwrap()
            .as_bool()
            .unwrap(),
        true
    );

    let msg1 = decode_message(&messages[1], &schema, "fixtures.Scalars");
    assert_eq!(
        msg1.get_field_by_name("bool_field")
            .unwrap()
            .as_bool()
            .unwrap(),
        false
    );
}

#[test]
fn roundtrip_int32() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("int32_field", DataType::Int32, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(Int32Array::from(vec![
            0,
            42,
            -1,
            i32::MAX,
            i32::MIN,
        ]))],
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    assert_eq!(messages.len(), 5);

    let msg = decode_message(&messages[1], &schema, "fixtures.Scalars");
    assert_eq!(
        msg.get_field_by_name("int32_field")
            .unwrap()
            .as_i32()
            .unwrap(),
        42
    );

    let msg = decode_message(&messages[3], &schema, "fixtures.Scalars");
    assert_eq!(
        msg.get_field_by_name("int32_field")
            .unwrap()
            .as_i32()
            .unwrap(),
        i32::MAX
    );
}

#[test]
fn roundtrip_int64() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("int64_field", DataType::Int64, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(Int64Array::from(vec![0i64, 123456789, -1]))],
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[1], &schema, "fixtures.Scalars");
    assert_eq!(
        msg.get_field_by_name("int64_field")
            .unwrap()
            .as_i64()
            .unwrap(),
        123456789
    );
}

#[test]
fn roundtrip_uint32() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("uint32_field", DataType::UInt32, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(UInt32Array::from(vec![0u32, 42, u32::MAX]))],
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[2], &schema, "fixtures.Scalars");
    assert_eq!(
        msg.get_field_by_name("uint32_field")
            .unwrap()
            .as_u32()
            .unwrap(),
        u32::MAX
    );
}

#[test]
fn roundtrip_uint64() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("uint64_field", DataType::UInt64, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(UInt64Array::from(vec![0u64, u64::MAX]))],
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[1], &schema, "fixtures.Scalars");
    assert_eq!(
        msg.get_field_by_name("uint64_field")
            .unwrap()
            .as_u64()
            .unwrap(),
        u64::MAX
    );
}

#[test]
fn roundtrip_float32() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("float_field", DataType::Float32, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(Float32Array::from(vec![3.14f32, 0.0, -1.5]))],
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[0], &schema, "fixtures.Scalars");
    let v = msg
        .get_field_by_name("float_field")
        .unwrap()
        .as_f32()
        .unwrap();
    assert!((v - 3.14).abs() < 0.001);
}

#[test]
fn roundtrip_float64() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("double_field", DataType::Float64, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(Float64Array::from(vec![3.14159265358979]))],
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[0], &schema, "fixtures.Scalars");
    let v = msg
        .get_field_by_name("double_field")
        .unwrap()
        .as_f64()
        .unwrap();
    assert!((v - 3.14159265358979).abs() < 1e-10);
}

#[test]
fn roundtrip_string() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("string_field", DataType::Utf8, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(StringArray::from(vec!["hello", "", "world"]))],
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[0], &schema, "fixtures.Scalars");
    assert_eq!(
        msg.get_field_by_name("string_field")
            .unwrap()
            .as_str()
            .unwrap(),
        "hello"
    );
}

#[test]
fn roundtrip_bytes() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("bytes_field", DataType::Binary, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(BinaryArray::from(vec![
            b"data".as_ref(),
            b"",
            b"\x00\x01",
        ]))],
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[0], &schema, "fixtures.Scalars");
    assert_eq!(
        msg.get_field_by_name("bytes_field")
            .unwrap()
            .as_bytes()
            .unwrap()
            .as_ref(),
        b"data"
    );
}

// ==================== Zigzag and fixed types ====================

#[test]
fn roundtrip_sint32() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("sint32_field", DataType::Int32, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(Int32Array::from(vec![
            0,
            -1,
            1,
            i32::MIN,
            i32::MAX,
        ]))],
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[1], &schema, "fixtures.Scalars");
    assert_eq!(
        msg.get_field_by_name("sint32_field")
            .unwrap()
            .as_i32()
            .unwrap(),
        -1
    );

    let msg = decode_message(&messages[3], &schema, "fixtures.Scalars");
    assert_eq!(
        msg.get_field_by_name("sint32_field")
            .unwrap()
            .as_i32()
            .unwrap(),
        i32::MIN
    );
}

#[test]
fn roundtrip_sint64() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("sint64_field", DataType::Int64, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(Int64Array::from(vec![0i64, -1, i64::MIN]))],
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[1], &schema, "fixtures.Scalars");
    assert_eq!(
        msg.get_field_by_name("sint64_field")
            .unwrap()
            .as_i64()
            .unwrap(),
        -1
    );
}

#[test]
fn roundtrip_sfixed32() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("sfixed32_field", DataType::Int32, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(Int32Array::from(vec![0, -1, 42]))],
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[1], &schema, "fixtures.Scalars");
    assert_eq!(
        msg.get_field_by_name("sfixed32_field")
            .unwrap()
            .as_i32()
            .unwrap(),
        -1
    );
}

#[test]
fn roundtrip_sfixed64() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("sfixed64_field", DataType::Int64, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(Int64Array::from(vec![0i64, -1, 42]))],
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[1], &schema, "fixtures.Scalars");
    assert_eq!(
        msg.get_field_by_name("sfixed64_field")
            .unwrap()
            .as_i64()
            .unwrap(),
        -1
    );
}

#[test]
fn roundtrip_fixed32() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("fixed32_field", DataType::UInt32, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(UInt32Array::from(vec![0u32, 42, u32::MAX]))],
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[1], &schema, "fixtures.Scalars");
    assert_eq!(
        msg.get_field_by_name("fixed32_field")
            .unwrap()
            .as_u32()
            .unwrap(),
        42
    );
}

#[test]
fn roundtrip_fixed64() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("fixed64_field", DataType::UInt64, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(UInt64Array::from(vec![0u64, 42, u64::MAX]))],
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[1], &schema, "fixtures.Scalars");
    assert_eq!(
        msg.get_field_by_name("fixed64_field")
            .unwrap()
            .as_u64()
            .unwrap(),
        42
    );
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
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    assert_eq!(messages.len(), 1);

    let msg = decode_message(&messages[0], &schema, "fixtures.Scalars");
    assert_eq!(
        msg.get_field_by_name("bool_field")
            .unwrap()
            .as_bool()
            .unwrap(),
        true
    );
    assert_eq!(
        msg.get_field_by_name("int32_field")
            .unwrap()
            .as_i32()
            .unwrap(),
        42
    );
    assert_eq!(
        msg.get_field_by_name("string_field")
            .unwrap()
            .as_str()
            .unwrap(),
        "hello"
    );
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
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    assert_eq!(messages.len(), 2);

    // Row 0: both fields present.
    let msg0 = decode_message(&messages[0], &schema, "fixtures.Scalars");
    assert_eq!(
        msg0.get_field_by_name("int32_field")
            .unwrap()
            .as_i32()
            .unwrap(),
        42
    );

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
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

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
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
    let result = transcoder.transcode_arrow(&batch).unwrap();

    assert_eq!(result.len(), 3);

    // Decode each element.
    for i in 0..3 {
        let bytes = result.value(i);
        let msg = decode_message(bytes, &schema, "fixtures.Scalars");
        assert_eq!(
            msg.get_field_by_name("int32_field")
                .unwrap()
                .as_i32()
                .unwrap(),
            (i + 1) as i32
        );
    }
}

#[test]
fn arrow_output_empty_batch() {
    let schema = scalars_schema();
    let arrow_schema = Schema::new(vec![Field::new("int32_field", DataType::Int32, false)]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![Arc::new(Int32Array::from(Vec::<i32>::new()))],
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Scalars");
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
    use crate::mapping::{explicit_mapping, ArrowFieldRef, ExplicitBinding, ProtoFieldRef};
    let mapping = explicit_mapping(
        &arrow_schema,
        &msg,
        &[ExplicitBinding {
            arrow_field: ArrowFieldRef::Index(0),
            proto_field: ProtoFieldRef::Name("int32_field".to_string()),
            coerce: true,
        }],
    )
    .unwrap();

    let transcoder = Transcoder::new(&mapping).unwrap();

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![Arc::new(Int64Array::from(vec![42i64, -1]))],
    )
    .unwrap();

    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg0 = decode_message(&messages[0], &schema, "fixtures.Scalars");
    assert_eq!(
        msg0.get_field_by_name("int32_field")
            .unwrap()
            .as_i32()
            .unwrap(),
        42
    );
}

#[test]
fn coercion_int64_to_int32_overflow() {
    let schema = scalars_schema();
    let msg = schema.message("fixtures.Scalars").unwrap();

    let arrow_schema = Schema::new(vec![Field::new("int32_field", DataType::Int64, false)]);

    use crate::mapping::{explicit_mapping, ArrowFieldRef, ExplicitBinding, ProtoFieldRef};
    let mapping = explicit_mapping(
        &arrow_schema,
        &msg,
        &[ExplicitBinding {
            arrow_field: ArrowFieldRef::Index(0),
            proto_field: ProtoFieldRef::Name("int32_field".to_string()),
            coerce: true,
        }],
    )
    .unwrap();

    let transcoder = Transcoder::new(&mapping).unwrap();

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![Arc::new(Int64Array::from(vec![i64::MAX]))],
    )
    .unwrap();

    let mut output = Vec::new();
    let result = transcoder.transcode_delimited(&batch, &mut output);
    assert!(result.is_err());

    let err = result.unwrap_err();
    let err_str = err.to_string();
    assert!(
        err_str.contains("row 0"),
        "error should mention row: {err_str}"
    );
    assert!(
        err_str.contains("int32_field"),
        "error should mention field: {err_str}"
    );
}

// ==================== Signed ↔ unsigned coercions ====================

/// Helper: transcode a single-column batch into `fixtures.Scalars` with an
/// explicit coerce=true binding onto `proto_field`, returning the encoded
/// messages.
fn transcode_coerced(proto_field: &str, array: ArrayRef) -> Vec<Vec<u8>> {
    use crate::mapping::{explicit_mapping, ArrowFieldRef, ExplicitBinding, ProtoFieldRef};

    let schema = scalars_schema();
    let msg = schema.message("fixtures.Scalars").unwrap();
    let arrow_schema = Schema::new(vec![Field::new(
        proto_field,
        array.data_type().clone(),
        false,
    )]);
    let mapping = explicit_mapping(
        &arrow_schema,
        &msg,
        &[ExplicitBinding {
            arrow_field: ArrowFieldRef::Index(0),
            proto_field: ProtoFieldRef::Name(proto_field.to_string()),
            coerce: true,
        }],
    )
    .unwrap();
    let transcoder = Transcoder::new(&mapping).unwrap();

    let batch = RecordBatch::try_new(Arc::new(arrow_schema), vec![array]).unwrap();
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();
    split_delimited(&output)
}

/// Helper: decode row `i` of `msgs` and return the named field's value.
fn coerced_field(msgs: &[Vec<u8>], i: usize, field: &str) -> prost_reflect::Value {
    let schema = scalars_schema();
    let m = decode_message(&msgs[i], &schema, "fixtures.Scalars");
    m.get_field_by_name(field).unwrap().into_owned()
}

#[test]
fn coercion_int64_to_uint64_reinterprets() {
    let msgs = transcode_coerced(
        "uint64_field",
        Arc::new(Int64Array::from(vec![42i64, i64::MAX, -1])),
    );
    assert_eq!(coerced_field(&msgs, 0, "uint64_field").as_u64(), Some(42));
    assert_eq!(
        coerced_field(&msgs, 1, "uint64_field").as_u64(),
        Some(i64::MAX as u64)
    );
    // -1 reinterprets as two's complement.
    assert_eq!(
        coerced_field(&msgs, 2, "uint64_field").as_u64(),
        Some(u64::MAX)
    );
}

#[test]
fn coercion_int64_to_uint32_truncates() {
    let msgs = transcode_coerced(
        "uint32_field",
        Arc::new(Int64Array::from(vec![7i64, u32::MAX as i64 + 1, -1])),
    );
    assert_eq!(coerced_field(&msgs, 0, "uint32_field").as_u32(), Some(7));
    // Values above u32::MAX keep only the low 32 bits.
    assert_eq!(coerced_field(&msgs, 1, "uint32_field").as_u32(), Some(0));
    assert_eq!(
        coerced_field(&msgs, 2, "uint32_field").as_u32(),
        Some(u32::MAX)
    );
}

#[test]
fn coercion_int64_to_fixed32_truncates() {
    let msgs = transcode_coerced("fixed32_field", Arc::new(Int64Array::from(vec![9i64, -9])));
    assert_eq!(coerced_field(&msgs, 0, "fixed32_field").as_u32(), Some(9));
    assert_eq!(
        coerced_field(&msgs, 1, "fixed32_field").as_u32(),
        Some(-9i64 as u32)
    );
}

#[test]
fn coercion_int32_to_uint64_sign_extends() {
    let msgs = transcode_coerced(
        "uint64_field",
        Arc::new(Int32Array::from(vec![i32::MAX, -1])),
    );
    assert_eq!(
        coerced_field(&msgs, 0, "uint64_field").as_u64(),
        Some(i32::MAX as u64)
    );
    // Widening from a signed source sign-extends before reinterpreting.
    assert_eq!(
        coerced_field(&msgs, 1, "uint64_field").as_u64(),
        Some(u64::MAX)
    );
}

#[test]
fn coercion_uint64_to_int64_reinterprets() {
    let msgs = transcode_coerced(
        "int64_field",
        Arc::new(UInt64Array::from(vec![i64::MAX as u64, u64::MAX])),
    );
    assert_eq!(
        coerced_field(&msgs, 0, "int64_field").as_i64(),
        Some(i64::MAX)
    );
    assert_eq!(coerced_field(&msgs, 1, "int64_field").as_i64(), Some(-1));
}

#[test]
fn coercion_uint64_to_sint32_truncates() {
    let msgs = transcode_coerced(
        "sint32_field",
        Arc::new(UInt64Array::from(vec![5u64, i32::MAX as u64 + 1])),
    );
    assert_eq!(coerced_field(&msgs, 0, "sint32_field").as_i32(), Some(5));
    // Low 32 bits reinterpreted as i32: i32::MAX + 1 wraps to i32::MIN.
    assert_eq!(
        coerced_field(&msgs, 1, "sint32_field").as_i32(),
        Some(i32::MIN)
    );
}

#[test]
fn coercion_uint32_to_int64_lossless() {
    let msgs = transcode_coerced("int64_field", Arc::new(UInt32Array::from(vec![u32::MAX])));
    assert_eq!(
        coerced_field(&msgs, 0, "int64_field").as_i64(),
        Some(u32::MAX as i64)
    );
}

#[test]
fn coercion_uint32_to_int32_reinterprets() {
    let msgs = transcode_coerced(
        "int32_field",
        Arc::new(UInt32Array::from(vec![7u32, u32::MAX])),
    );
    assert_eq!(coerced_field(&msgs, 0, "int32_field").as_i32(), Some(7));
    assert_eq!(coerced_field(&msgs, 1, "int32_field").as_i32(), Some(-1));
}

// ==================== Nested message ====================

#[test]
fn roundtrip_nested_message() {
    let schema = nested_schema();
    let arrow_schema = Schema::new(vec![Field::new(
        "inner",
        DataType::Struct(Fields::from(vec![
            Field::new("value", DataType::Utf8, false),
            Field::new("count", DataType::Int32, false),
        ])),
        true,
    )]);

    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("value", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["hello", "world"])) as Arc<dyn Array>,
        ),
        (
            Arc::new(Field::new("count", DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![1, 2])) as Arc<dyn Array>,
        ),
    ]);

    let batch =
        RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![Arc::new(struct_array)]).unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Nested");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    assert_eq!(messages.len(), 2);

    let msg0 = decode_message(&messages[0], &schema, "fixtures.Nested");
    let inner0 = msg0.get_field_by_name("inner").unwrap();
    let inner0_msg = inner0.as_message().unwrap();
    assert_eq!(
        inner0_msg
            .get_field_by_name("value")
            .unwrap()
            .as_str()
            .unwrap(),
        "hello"
    );
    assert_eq!(
        inner0_msg
            .get_field_by_name("count")
            .unwrap()
            .as_i32()
            .unwrap(),
        1
    );
}

// ==================== Repeated scalar (packed) ====================

#[test]
fn roundtrip_repeated_int32() {
    let schema = nested_schema();
    let arrow_schema = Schema::new(vec![Field::new(
        "tags",
        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
        true,
    )]);

    let mut builder = ListBuilder::new(Int32Builder::new());
    builder.values().append_value(1);
    builder.values().append_value(2);
    builder.values().append_value(3);
    builder.append(true);
    builder.values().append_value(10);
    builder.append(true);
    let list_array = builder.finish();

    let batch =
        RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![Arc::new(list_array)]).unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Nested");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    assert_eq!(messages.len(), 2);

    let msg0 = decode_message(&messages[0], &schema, "fixtures.Nested");
    let tags: Vec<i32> = msg0
        .get_field_by_name("tags")
        .unwrap()
        .as_list()
        .unwrap()
        .iter()
        .map(|v| v.as_i32().unwrap())
        .collect();
    assert_eq!(tags, vec![1, 2, 3]);

    let msg1 = decode_message(&messages[1], &schema, "fixtures.Nested");
    let tags: Vec<i32> = msg1
        .get_field_by_name("tags")
        .unwrap()
        .as_list()
        .unwrap()
        .iter()
        .map(|v| v.as_i32().unwrap())
        .collect();
    assert_eq!(tags, vec![10]);
}

// ==================== Repeated message ====================

#[test]
fn roundtrip_repeated_message() {
    let schema = nested_schema();
    let arrow_schema = Schema::new(vec![Field::new(
        "items",
        DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![
                Field::new("value", DataType::Utf8, false),
                Field::new("count", DataType::Int32, false),
            ])),
            true,
        ))),
        true,
    )]);

    let inner_fields = Fields::from(vec![
        Field::new("value", DataType::Utf8, false),
        Field::new("count", DataType::Int32, false),
    ]);
    let struct_builder = StructBuilder::from_fields(inner_fields, 4);
    let mut list_builder = ListBuilder::new(struct_builder);

    // Row 0: two items.
    list_builder
        .values()
        .field_builder::<StringBuilder>(0)
        .unwrap()
        .append_value("a");
    list_builder
        .values()
        .field_builder::<Int32Builder>(1)
        .unwrap()
        .append_value(1);
    list_builder.values().append(true);
    list_builder
        .values()
        .field_builder::<StringBuilder>(0)
        .unwrap()
        .append_value("b");
    list_builder
        .values()
        .field_builder::<Int32Builder>(1)
        .unwrap()
        .append_value(2);
    list_builder.values().append(true);
    list_builder.append(true);

    let list_array = list_builder.finish();

    let batch =
        RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![Arc::new(list_array)]).unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Nested");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    assert_eq!(messages.len(), 1);

    let msg0 = decode_message(&messages[0], &schema, "fixtures.Nested");
    let items = msg0.get_field_by_name("items").unwrap();
    let items_list = items.as_list().unwrap();
    assert_eq!(items_list.len(), 2);
    assert_eq!(
        items_list[0]
            .as_message()
            .unwrap()
            .get_field_by_name("value")
            .unwrap()
            .as_str()
            .unwrap(),
        "a"
    );
    assert_eq!(
        items_list[1]
            .as_message()
            .unwrap()
            .get_field_by_name("count")
            .unwrap()
            .as_i32()
            .unwrap(),
        2
    );
}

// ==================== Map ====================

#[test]
fn roundtrip_map() {
    let schema = nested_schema();

    let key_builder = StringBuilder::new();
    let value_builder = Int64Builder::new();
    let mut map_builder = MapBuilder::new(None, key_builder, value_builder);

    map_builder.keys().append_value("a");
    map_builder.values().append_value(1);
    map_builder.keys().append_value("b");
    map_builder.values().append_value(2);
    map_builder.append(true).unwrap();

    let map_array = map_builder.finish();

    // Derive the schema from the actual array to avoid field name mismatches.
    let arrow_schema = Schema::new(vec![Field::new(
        "metadata",
        map_array.data_type().clone(),
        true,
    )]);

    let batch =
        RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![Arc::new(map_array)]).unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Nested");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    assert_eq!(messages.len(), 1);

    let msg0 = decode_message(&messages[0], &schema, "fixtures.Nested");
    let metadata = msg0.get_field_by_name("metadata").unwrap();
    let map = metadata.as_map().unwrap();
    assert_eq!(map.len(), 2);
}

// ==================== Oneof ====================

#[test]
fn roundtrip_oneof_one_set() {
    let schema = nested_schema();
    let arrow_schema = Schema::new(vec![Field::new(
        "choice",
        DataType::Struct(Fields::from(vec![
            Field::new("text_value", DataType::Utf8, true),
            Field::new("int_value", DataType::Int32, true),
        ])),
        true,
    )]);

    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("text_value", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("hello"), None])) as Arc<dyn Array>,
        ),
        (
            Arc::new(Field::new("int_value", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![None, Some(42)])) as Arc<dyn Array>,
        ),
    ]);

    let batch =
        RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![Arc::new(struct_array)]).unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Nested");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    assert_eq!(messages.len(), 2);

    // Row 0: text_value set.
    let msg0 = decode_message(&messages[0], &schema, "fixtures.Nested");
    assert_eq!(
        msg0.get_field_by_name("text_value")
            .unwrap()
            .as_str()
            .unwrap(),
        "hello"
    );

    // Row 1: int_value set.
    let msg1 = decode_message(&messages[1], &schema, "fixtures.Nested");
    assert_eq!(
        msg1.get_field_by_name("int_value")
            .unwrap()
            .as_i32()
            .unwrap(),
        42
    );
}

#[test]
fn roundtrip_oneof_none_set() {
    let schema = nested_schema();
    let arrow_schema = Schema::new(vec![Field::new(
        "choice",
        DataType::Struct(Fields::from(vec![
            Field::new("text_value", DataType::Utf8, true),
            Field::new("int_value", DataType::Int32, true),
        ])),
        true,
    )]);

    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("text_value", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![None::<&str>])) as Arc<dyn Array>,
        ),
        (
            Arc::new(Field::new("int_value", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![None::<i32>])) as Arc<dyn Array>,
        ),
    ]);

    let batch =
        RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![Arc::new(struct_array)]).unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Nested");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    assert_eq!(messages.len(), 1);
    // Empty message — no oneof variant set.
    assert!(messages[0].is_empty());
}

#[test]
fn oneof_multiple_set_error() {
    let schema = nested_schema();
    let arrow_schema = Schema::new(vec![Field::new(
        "choice",
        DataType::Struct(Fields::from(vec![
            Field::new("text_value", DataType::Utf8, true),
            Field::new("int_value", DataType::Int32, true),
        ])),
        true,
    )]);

    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("text_value", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("hello")])) as Arc<dyn Array>,
        ),
        (
            Arc::new(Field::new("int_value", DataType::Int32, true)),
            Arc::new(Int32Array::from(vec![Some(42)])) as Arc<dyn Array>,
        ),
    ]);

    let batch =
        RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![Arc::new(struct_array)]).unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Nested");
    let mut output = Vec::new();
    let result = transcoder.transcode_delimited(&batch, &mut output);
    assert!(result.is_err());

    let err = result.unwrap_err();
    assert!(matches!(err, TranscodeError::OneofMultipleSet { .. }));
    let err_str = err.to_string();
    assert!(err_str.contains("row 0"));
    assert!(err_str.contains("choice"));
}

// ==================== Mixed batch ====================

#[test]
fn roundtrip_mixed_batch() {
    let schema = nested_schema();
    let arrow_schema = Schema::new(vec![
        Field::new(
            "inner",
            DataType::Struct(Fields::from(vec![
                Field::new("value", DataType::Utf8, false),
                Field::new("count", DataType::Int32, false),
            ])),
            true,
        ),
        Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        ),
        Field::new("status", DataType::Int32, false),
    ]);

    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("value", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["test"])) as Arc<dyn Array>,
        ),
        (
            Arc::new(Field::new("count", DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![99])) as Arc<dyn Array>,
        ),
    ]);

    let mut list_builder = ListBuilder::new(Int32Builder::new());
    list_builder.values().append_value(10);
    list_builder.values().append_value(20);
    list_builder.append(true);
    let list_array = list_builder.finish();

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![
            Arc::new(struct_array),
            Arc::new(list_array),
            Arc::new(Int32Array::from(vec![1])), // STATUS_ACTIVE
        ],
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Nested");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    assert_eq!(messages.len(), 1);

    let msg = decode_message(&messages[0], &schema, "fixtures.Nested");
    let inner = msg
        .get_field_by_name("inner")
        .unwrap()
        .as_message()
        .unwrap()
        .clone();
    assert_eq!(
        inner.get_field_by_name("value").unwrap().as_str().unwrap(),
        "test"
    );
    assert_eq!(
        inner.get_field_by_name("count").unwrap().as_i32().unwrap(),
        99
    );

    let tags: Vec<i32> = msg
        .get_field_by_name("tags")
        .unwrap()
        .as_list()
        .unwrap()
        .iter()
        .map(|v| v.as_i32().unwrap())
        .collect();
    assert_eq!(tags, vec![10, 20]);
}

// ==================== Arrow output with nested ====================

#[test]
fn arrow_output_nested() {
    let schema = nested_schema();
    let arrow_schema = Schema::new(vec![Field::new(
        "inner",
        DataType::Struct(Fields::from(vec![
            Field::new("value", DataType::Utf8, false),
            Field::new("count", DataType::Int32, false),
        ])),
        true,
    )]);

    let struct_array = StructArray::from(vec![
        (
            Arc::new(Field::new("value", DataType::Utf8, false)),
            Arc::new(StringArray::from(vec!["x", "y"])) as Arc<dyn Array>,
        ),
        (
            Arc::new(Field::new("count", DataType::Int32, false)),
            Arc::new(Int32Array::from(vec![5, 10])) as Arc<dyn Array>,
        ),
    ]);

    let batch =
        RecordBatch::try_new(Arc::new(arrow_schema.clone()), vec![Arc::new(struct_array)]).unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Nested");
    let result = transcoder.transcode_arrow(&batch).unwrap();

    assert_eq!(result.len(), 2);

    let msg0 = decode_message(result.value(0), &schema, "fixtures.Nested");
    let inner0 = msg0
        .get_field_by_name("inner")
        .unwrap()
        .as_message()
        .unwrap()
        .clone();
    assert_eq!(
        inner0.get_field_by_name("value").unwrap().as_str().unwrap(),
        "x"
    );
}

// ==================== Well-known types ====================

const WELLKNOWN_BIN: &[u8] = include_bytes!("../../fixtures/wellknown.bin");

fn wellknown_schema() -> ProtoSchema {
    ProtoSchema::from_bytes(WELLKNOWN_BIN).unwrap()
}

#[test]
fn roundtrip_timestamp_microsecond() {
    use arrow_schema::TimeUnit;

    let schema = wellknown_schema();
    let arrow_schema = Schema::new(vec![
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        ),
        Field::new("name", DataType::Utf8, false),
    ]);

    // 2024-01-15 10:30:00 UTC = 1705311000 seconds = 1705311000000000 microseconds
    let ts_us: i64 = 1_705_311_000_000_000;

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![ts_us])),
            Arc::new(StringArray::from(vec!["test"])),
        ],
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.WithWellKnown");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    assert_eq!(messages.len(), 1);

    let msg = decode_message(&messages[0], &schema, "fixtures.WithWellKnown");
    let created_at = msg.get_field_by_name("created_at").unwrap();
    let ts_msg = created_at.as_message().unwrap();
    let seconds = ts_msg
        .get_field_by_name("seconds")
        .unwrap()
        .as_i64()
        .unwrap();
    let nanos = ts_msg.get_field_by_name("nanos").unwrap().as_i32().unwrap();
    assert_eq!(seconds, 1_705_311_000);
    assert_eq!(nanos, 0);
}

#[test]
fn roundtrip_timestamp_nanosecond_with_nanos() {
    use arrow_schema::TimeUnit;

    let schema = wellknown_schema();
    let arrow_schema = Schema::new(vec![
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("name", DataType::Utf8, false),
    ]);

    // 1705311000 seconds + 123456789 nanos
    let ts_ns: i64 = 1_705_311_000_123_456_789;

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![
            Arc::new(TimestampNanosecondArray::from(vec![ts_ns])),
            Arc::new(StringArray::from(vec!["test"])),
        ],
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.WithWellKnown");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[0], &schema, "fixtures.WithWellKnown");
    let ts_msg = msg
        .get_field_by_name("created_at")
        .unwrap()
        .as_message()
        .unwrap()
        .clone();
    assert_eq!(
        ts_msg
            .get_field_by_name("seconds")
            .unwrap()
            .as_i64()
            .unwrap(),
        1_705_311_000
    );
    assert_eq!(
        ts_msg.get_field_by_name("nanos").unwrap().as_i32().unwrap(),
        123_456_789
    );
}

#[test]
fn roundtrip_timestamp_millisecond() {
    use arrow_schema::TimeUnit;

    let schema = wellknown_schema();
    let arrow_schema = Schema::new(vec![
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("name", DataType::Utf8, false),
    ]);

    // 1705311000 seconds + 500ms
    let ts_ms: i64 = 1_705_311_000_500;

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![ts_ms])),
            Arc::new(StringArray::from(vec!["test"])),
        ],
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.WithWellKnown");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[0], &schema, "fixtures.WithWellKnown");
    let ts_msg = msg
        .get_field_by_name("created_at")
        .unwrap()
        .as_message()
        .unwrap()
        .clone();
    assert_eq!(
        ts_msg
            .get_field_by_name("seconds")
            .unwrap()
            .as_i64()
            .unwrap(),
        1_705_311_000
    );
    assert_eq!(
        ts_msg.get_field_by_name("nanos").unwrap().as_i32().unwrap(),
        500_000_000
    );
}

#[test]
fn roundtrip_duration_microsecond() {
    use arrow_schema::TimeUnit;

    let schema = wellknown_schema();
    let arrow_schema = Schema::new(vec![
        Field::new("elapsed", DataType::Duration(TimeUnit::Microsecond), false),
        Field::new("name", DataType::Utf8, false),
    ]);

    // 5 seconds + 123456 microseconds = 5.123456 seconds
    let dur_us: i64 = 5_123_456;

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![
            Arc::new(DurationMicrosecondArray::from(vec![dur_us])),
            Arc::new(StringArray::from(vec!["test"])),
        ],
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.WithWellKnown");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[0], &schema, "fixtures.WithWellKnown");
    let dur_msg = msg
        .get_field_by_name("elapsed")
        .unwrap()
        .as_message()
        .unwrap()
        .clone();
    assert_eq!(
        dur_msg
            .get_field_by_name("seconds")
            .unwrap()
            .as_i64()
            .unwrap(),
        5
    );
    assert_eq!(
        dur_msg
            .get_field_by_name("nanos")
            .unwrap()
            .as_i32()
            .unwrap(),
        123_456_000
    );
}

#[test]
fn roundtrip_timestamp_negative_pre_epoch() {
    use arrow_schema::TimeUnit;

    let schema = wellknown_schema();
    let arrow_schema = Schema::new(vec![
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Millisecond, None),
            false,
        ),
        Field::new("name", DataType::Utf8, false),
    ]);

    // -1500ms = -2 seconds + 500_000_000 nanos (Euclidean)
    let ts_ms: i64 = -1500;

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![
            Arc::new(TimestampMillisecondArray::from(vec![ts_ms])),
            Arc::new(StringArray::from(vec!["pre-epoch"])),
        ],
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.WithWellKnown");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[0], &schema, "fixtures.WithWellKnown");
    let ts_msg = msg
        .get_field_by_name("created_at")
        .unwrap()
        .as_message()
        .unwrap()
        .clone();
    let seconds = ts_msg
        .get_field_by_name("seconds")
        .unwrap()
        .as_i64()
        .unwrap();
    let nanos = ts_msg.get_field_by_name("nanos").unwrap().as_i32().unwrap();

    // nanos must be non-negative per google.protobuf.Timestamp spec
    assert_eq!(seconds, -2);
    assert_eq!(nanos, 500_000_000);
}

#[test]
fn roundtrip_timestamp_second() {
    use arrow_schema::TimeUnit;

    let schema = wellknown_schema();
    let arrow_schema = Schema::new(vec![
        Field::new(
            "created_at",
            DataType::Timestamp(TimeUnit::Second, None),
            false,
        ),
        Field::new("name", DataType::Utf8, false),
    ]);

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![
            Arc::new(TimestampSecondArray::from(vec![1_705_311_000i64])),
            Arc::new(StringArray::from(vec!["test"])),
        ],
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.WithWellKnown");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[0], &schema, "fixtures.WithWellKnown");
    let ts_msg = msg
        .get_field_by_name("created_at")
        .unwrap()
        .as_message()
        .unwrap()
        .clone();
    assert_eq!(
        ts_msg
            .get_field_by_name("seconds")
            .unwrap()
            .as_i64()
            .unwrap(),
        1_705_311_000
    );
}

#[test]
fn roundtrip_duration_negative() {
    use arrow_schema::TimeUnit;

    let schema = wellknown_schema();
    let arrow_schema = Schema::new(vec![
        Field::new("elapsed", DataType::Duration(TimeUnit::Millisecond), false),
        Field::new("name", DataType::Utf8, false),
    ]);

    // -2500ms = -2 seconds, -500_000_000 nanos (truncation toward zero for Duration)
    let dur_ms: i64 = -2500;

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![
            Arc::new(DurationMillisecondArray::from(vec![dur_ms])),
            Arc::new(StringArray::from(vec!["neg"])),
        ],
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.WithWellKnown");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[0], &schema, "fixtures.WithWellKnown");
    let dur_msg = msg
        .get_field_by_name("elapsed")
        .unwrap()
        .as_message()
        .unwrap()
        .clone();
    let seconds = dur_msg
        .get_field_by_name("seconds")
        .unwrap()
        .as_i64()
        .unwrap();
    let nanos = dur_msg
        .get_field_by_name("nanos")
        .unwrap()
        .as_i32()
        .unwrap();

    // Duration: nanos sign matches seconds sign
    assert_eq!(seconds, -2);
    assert_eq!(nanos, -500_000_000);
}

#[test]
fn roundtrip_duration_nanosecond() {
    use arrow_schema::TimeUnit;

    let schema = wellknown_schema();
    let arrow_schema = Schema::new(vec![
        Field::new("elapsed", DataType::Duration(TimeUnit::Nanosecond), false),
        Field::new("name", DataType::Utf8, false),
    ]);

    // 3 seconds + 141592653 nanos
    let dur_ns: i64 = 3_141_592_653;

    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema.clone()),
        vec![
            Arc::new(DurationNanosecondArray::from(vec![dur_ns])),
            Arc::new(StringArray::from(vec!["pi"])),
        ],
    )
    .unwrap();

    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.WithWellKnown");
    let mut output = Vec::new();
    transcoder.transcode_delimited(&batch, &mut output).unwrap();

    let messages = split_delimited(&output);
    let msg = decode_message(&messages[0], &schema, "fixtures.WithWellKnown");
    let dur_msg = msg
        .get_field_by_name("elapsed")
        .unwrap()
        .as_message()
        .unwrap()
        .clone();
    assert_eq!(
        dur_msg
            .get_field_by_name("seconds")
            .unwrap()
            .as_i64()
            .unwrap(),
        3
    );
    assert_eq!(
        dur_msg
            .get_field_by_name("nanos")
            .unwrap()
            .as_i32()
            .unwrap(),
        141_592_653
    );
}

// ==================== google.protobuf.Any ====================

const ANY_BIN: &[u8] = include_bytes!("../../fixtures/any.bin");

fn any_schema() -> ProtoSchema {
    ProtoSchema::from_bytes(ANY_BIN).unwrap()
}

fn order_placed_fields() -> Fields {
    Fields::from(vec![
        Field::new("order_id", DataType::Utf8, true),
        Field::new("amount_cents", DataType::Int64, true),
    ])
}

fn order_placed_struct(
    order_ids: Vec<Option<&str>>,
    amounts: Vec<Option<i64>>,
    validity: Option<Vec<bool>>,
) -> StructArray {
    let order_id: ArrayRef = Arc::new(StringArray::from(order_ids));
    let amount: ArrayRef = Arc::new(Int64Array::from(amounts));
    let nulls = validity.map(arrow_buffer::NullBuffer::from);
    StructArray::new(order_placed_fields(), vec![order_id, amount], nulls)
}

/// Extract (type_url, value) from an Any field of a decoded message.
fn get_any(msg: &DynamicMessage, field: &str) -> (String, Vec<u8>) {
    let v = msg.get_field_by_name(field).unwrap();
    let m = match v.as_ref() {
        prost_reflect::Value::Message(m) => m.clone(),
        other => panic!("expected message for '{field}', got {other:?}"),
    };
    let url = match m.get_field_by_name("type_url").unwrap().as_ref() {
        prost_reflect::Value::String(s) => s.clone(),
        other => panic!("expected string type_url, got {other:?}"),
    };
    let value = match m.get_field_by_name("value").unwrap().as_ref() {
        prost_reflect::Value::Bytes(b) => b.to_vec(),
        other => panic!("expected bytes value, got {other:?}"),
    };
    (url, value)
}

#[test]
fn any_packed_round_trip() {
    let schema = any_schema();
    let arrow_schema = Schema::new(vec![
        Field::new("event_id", DataType::Utf8, false),
        Field::new("payload", DataType::Struct(order_placed_fields()), true),
    ]);
    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Envelope");

    let payload = order_placed_struct(vec![Some("ord-1")], vec![Some(1299)], None);
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![
            Arc::new(StringArray::from(vec!["evt-1"])),
            Arc::new(payload),
        ],
    )
    .unwrap();

    let out = transcoder.transcode_arrow(&batch).unwrap();
    let msg = decode_message(out.value(0), &schema, "fixtures.Envelope");

    let (url, value) = get_any(&msg, "payload");
    assert_eq!(url, "type.googleapis.com/fixtures.OrderPlaced");

    let inner = decode_message(&value, &schema, "fixtures.OrderPlaced");
    assert_eq!(
        inner.get_field_by_name("order_id").unwrap().as_ref(),
        &prost_reflect::Value::String("ord-1".to_string()),
    );
    assert_eq!(
        inner.get_field_by_name("amount_cents").unwrap().as_ref(),
        &prost_reflect::Value::I64(1299),
    );
}

#[test]
fn any_packed_two_fields_with_wkt_payload() {
    let schema = any_schema();

    let origin_fields = Fields::from(vec![
        Field::new("service", DataType::Utf8, true),
        Field::new("region", DataType::Int32, true),
    ]);
    let context_fields = Fields::from(vec![
        Field::new(
            "received_at",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, None),
            true,
        ),
        Field::new("trace_id", DataType::Utf8, true),
        Field::new("origin", DataType::Struct(origin_fields.clone()), true),
    ]);
    let arrow_schema = Schema::new(vec![
        Field::new("payload", DataType::Struct(order_placed_fields()), true),
        Field::new("context", DataType::Struct(context_fields.clone()), true),
    ]);
    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Envelope");

    let origin = StructArray::new(
        origin_fields,
        vec![
            Arc::new(StringArray::from(vec!["checkout"])) as ArrayRef,
            Arc::new(Int32Array::from(vec![7])),
        ],
        None,
    );
    let context = StructArray::new(
        context_fields,
        vec![
            Arc::new(TimestampMicrosecondArray::from(vec![
                1_720_000_000_123_456i64,
            ])) as ArrayRef,
            Arc::new(StringArray::from(vec!["trace-42"])),
            Arc::new(origin),
        ],
        None,
    );
    let payload = order_placed_struct(vec![Some("ord-2")], vec![Some(50)], None);
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![Arc::new(payload), Arc::new(context)],
    )
    .unwrap();

    let out = transcoder.transcode_arrow(&batch).unwrap();
    let msg = decode_message(out.value(0), &schema, "fixtures.Envelope");

    let (payload_url, _) = get_any(&msg, "payload");
    assert_eq!(payload_url, "type.googleapis.com/fixtures.OrderPlaced");

    let (context_url, context_value) = get_any(&msg, "context");
    assert_eq!(context_url, "type.googleapis.com/fixtures.RequestContext");

    let ctx = decode_message(&context_value, &schema, "fixtures.RequestContext");
    assert_eq!(
        ctx.get_field_by_name("trace_id").unwrap().as_ref(),
        &prost_reflect::Value::String("trace-42".to_string()),
    );
    // received_at: 1_720_000_000_123_456 us → 1_720_000_000 s + 123_456_000 ns
    let received = match ctx.get_field_by_name("received_at").unwrap().as_ref() {
        prost_reflect::Value::Message(m) => m.clone(),
        other => panic!("expected Timestamp message, got {other:?}"),
    };
    assert_eq!(
        received.get_field_by_name("seconds").unwrap().as_ref(),
        &prost_reflect::Value::I64(1_720_000_000),
    );
    assert_eq!(
        received.get_field_by_name("nanos").unwrap().as_ref(),
        &prost_reflect::Value::I32(123_456_000),
    );
    let origin = match ctx.get_field_by_name("origin").unwrap().as_ref() {
        prost_reflect::Value::Message(m) => m.clone(),
        other => panic!("expected Origin message, got {other:?}"),
    };
    assert_eq!(
        origin.get_field_by_name("service").unwrap().as_ref(),
        &prost_reflect::Value::String("checkout".to_string()),
    );
}

#[test]
fn any_packed_null_struct_omitted() {
    let schema = any_schema();
    let arrow_schema = Schema::new(vec![Field::new(
        "payload",
        DataType::Struct(order_placed_fields()),
        true,
    )]);
    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Envelope");

    let payload = order_placed_struct(vec![None], vec![None], Some(vec![false]));
    let batch = RecordBatch::try_new(Arc::new(arrow_schema), vec![Arc::new(payload)]).unwrap();

    let out = transcoder.transcode_arrow(&batch).unwrap();
    assert!(out.value(0).is_empty());
    let msg = decode_message(out.value(0), &schema, "fixtures.Envelope");
    assert!(!msg.has_field_by_name("payload"));
}

#[test]
fn any_packed_all_null_children_typed_empty() {
    let schema = any_schema();
    let arrow_schema = Schema::new(vec![Field::new(
        "payload",
        DataType::Struct(order_placed_fields()),
        true,
    )]);
    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Envelope");

    // Struct row is valid but every child is null → empty, typed payload.
    let payload = order_placed_struct(vec![None], vec![None], None);
    let batch = RecordBatch::try_new(Arc::new(arrow_schema), vec![Arc::new(payload)]).unwrap();

    let out = transcoder.transcode_arrow(&batch).unwrap();
    let msg = decode_message(out.value(0), &schema, "fixtures.Envelope");
    assert!(msg.has_field_by_name("payload"));
    let (url, value) = get_any(&msg, "payload");
    assert_eq!(url, "type.googleapis.com/fixtures.OrderPlaced");
    assert!(value.is_empty());
}

#[test]
fn any_packed_long_payload_length_backpatch() {
    let schema = any_schema();
    let arrow_schema = Schema::new(vec![Field::new(
        "payload",
        DataType::Struct(order_placed_fields()),
        true,
    )]);
    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Envelope");

    // > 128 bytes forces the multi-byte varint shift in both nested lengths.
    let long_id = "x".repeat(300);
    let payload = order_placed_struct(vec![Some(&long_id)], vec![Some(1)], None);
    let batch = RecordBatch::try_new(Arc::new(arrow_schema), vec![Arc::new(payload)]).unwrap();

    let out = transcoder.transcode_arrow(&batch).unwrap();
    let msg = decode_message(out.value(0), &schema, "fixtures.Envelope");
    let (_, value) = get_any(&msg, "payload");
    let inner = decode_message(&value, &schema, "fixtures.OrderPlaced");
    assert_eq!(
        inner.get_field_by_name("order_id").unwrap().as_ref(),
        &prost_reflect::Value::String(long_id),
    );
}

#[test]
fn any_raw_passthrough_round_trip() {
    use prost::Message as _;

    let schema = any_schema();

    // Pre-serialize an OrderPlaced payload, as a raw-mode user would.
    let order_desc = schema.message("fixtures.OrderPlaced").unwrap();
    let mut order = DynamicMessage::new(order_desc.clone());
    order
        .try_set_field_by_name(
            "order_id",
            prost_reflect::Value::String("ord-raw".to_string()),
        )
        .unwrap();
    let inner_bytes = order.encode_to_vec();

    let raw_fields = Fields::from(vec![
        Field::new("type_url", DataType::Utf8, true),
        Field::new("value", DataType::Binary, true),
    ]);
    let arrow_schema = Schema::new(vec![Field::new(
        "raw",
        DataType::Struct(raw_fields.clone()),
        true,
    )]);
    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Envelope");

    let raw = StructArray::new(
        raw_fields,
        vec![
            Arc::new(StringArray::from(vec![
                "type.googleapis.com/fixtures.OrderPlaced",
            ])) as ArrayRef,
            Arc::new(BinaryArray::from(vec![&inner_bytes[..]])),
        ],
        None,
    );
    let batch = RecordBatch::try_new(Arc::new(arrow_schema), vec![Arc::new(raw)]).unwrap();

    let out = transcoder.transcode_arrow(&batch).unwrap();
    let msg = decode_message(out.value(0), &schema, "fixtures.Envelope");
    let (url, value) = get_any(&msg, "raw");
    assert_eq!(url, "type.googleapis.com/fixtures.OrderPlaced");
    assert_eq!(value, inner_bytes);
}

#[test]
fn any_packed_repeated() {
    let schema = any_schema();
    let item_field = Arc::new(Field::new(
        "item",
        DataType::Struct(order_placed_fields()),
        true,
    ));
    let arrow_schema = Schema::new(vec![Field::new(
        "events",
        DataType::List(item_field.clone()),
        true,
    )]);
    let transcoder = build_transcoder(&arrow_schema, &schema, "fixtures.Envelope");

    let values = order_placed_struct(vec![Some("a"), Some("b")], vec![Some(1), Some(2)], None);
    let offsets = arrow_buffer::OffsetBuffer::new(vec![0i32, 2].into());
    let events = ListArray::new(item_field, offsets, Arc::new(values), None);
    let batch = RecordBatch::try_new(Arc::new(arrow_schema), vec![Arc::new(events)]).unwrap();

    let out = transcoder.transcode_arrow(&batch).unwrap();
    let msg = decode_message(out.value(0), &schema, "fixtures.Envelope");

    let events_value = msg.get_field_by_name("events").unwrap();
    let list = match events_value.as_ref() {
        prost_reflect::Value::List(items) => items.clone(),
        other => panic!("expected list, got {other:?}"),
    };
    assert_eq!(list.len(), 2);
    for (i, (item, expected_id)) in list.iter().zip(["a", "b"]).enumerate() {
        let any = match item {
            prost_reflect::Value::Message(m) => m.clone(),
            other => panic!("expected Any message, got {other:?}"),
        };
        let url = match any.get_field_by_name("type_url").unwrap().as_ref() {
            prost_reflect::Value::String(s) => s.clone(),
            other => panic!("expected string, got {other:?}"),
        };
        assert_eq!(url, "type.googleapis.com/fixtures.OrderPlaced");
        let value = match any.get_field_by_name("value").unwrap().as_ref() {
            prost_reflect::Value::Bytes(b) => b.to_vec(),
            other => panic!("expected bytes, got {other:?}"),
        };
        let inner = decode_message(&value, &schema, "fixtures.OrderPlaced");
        assert_eq!(
            inner.get_field_by_name("order_id").unwrap().as_ref(),
            &prost_reflect::Value::String(expected_id.to_string()),
            "element {i}",
        );
    }
}

// ============ String-encoded integers (Utf8 -> integer coercion) ============

fn coerce_options() -> InferOptions {
    InferOptions {
        coerce_all: true,
        ..InferOptions::default()
    }
}

fn build_coercing_transcoder(arrow_schema: &Schema, proto: &ProtoSchema, msg: &str) -> Transcoder {
    let desc = proto.message(msg).unwrap();
    let mapping = infer_mapping(arrow_schema, &desc, &coerce_options()).unwrap();
    Transcoder::new(&mapping).unwrap()
}

/// One-row batch with a single Utf8 column.
fn str_batch(column: &str, value: Option<&str>) -> RecordBatch {
    let schema = Schema::new(vec![Field::new(column, DataType::Utf8, true)]);
    RecordBatch::try_new(
        Arc::new(schema),
        vec![Arc::new(StringArray::from(vec![value])) as ArrayRef],
    )
    .unwrap()
}

#[test]
fn string_encoded_int64_requires_coercion() {
    // Without coerce the binding must be rejected, not silently dropped —
    // otherwise a schema drift from INT64 to STRING would quietly stop
    // populating the field.
    let proto = scalars_schema();
    let desc = proto.message("fixtures.Scalars").unwrap();
    let arrow_schema = Schema::new(vec![Field::new("int64_field", DataType::Utf8, true)]);

    let err = infer_mapping(&arrow_schema, &desc, &InferOptions::default())
        .expect_err("Utf8 -> int64 must not bind without coercion");
    let msg = err.to_string();
    assert!(msg.contains("coercion available"), "{msg}");
    assert!(msg.contains("not enabled"), "{msg}");
}

#[test]
fn string_encoded_integers_round_trip() {
    let proto = scalars_schema();

    // Each proto integer kind, exercising varint / zigzag / fixed paths.
    for (column, input, expect) in [
        ("int64_field", "9223372036854775807", i64::MAX),
        ("int64_field", "-1", -1),
        ("sint64_field", "-9007199254740993", -9007199254740993),
        ("sfixed64_field", "-42", -42),
        ("int32_field", "-2147483648", i32::MIN as i64),
        ("sint32_field", "-7", -7),
        ("sfixed32_field", "13", 13),
    ] {
        let batch = str_batch(column, Some(input));
        let t = build_coercing_transcoder(&batch.schema(), &proto, "fixtures.Scalars");
        let values = t.transcode_arrow(&batch).unwrap();
        let msg = decode_message(values.value(0), &proto, "fixtures.Scalars");
        // 32-bit proto fields decode to Value::I32, 64-bit to Value::I64.
        let v = msg.get_field_by_name(column).unwrap();
        let got = v.as_i64().or_else(|| v.as_i32().map(i64::from));
        assert_eq!(got, Some(expect), "{column} = {input}");
    }

    for (column, input, expect) in [
        ("uint64_field", "18446744073709551615", u64::MAX),
        ("fixed64_field", "12345678901234567890", 12345678901234567890),
        ("uint32_field", "4294967295", u32::MAX as u64),
        ("fixed32_field", "0", 0),
    ] {
        let batch = str_batch(column, Some(input));
        let t = build_coercing_transcoder(&batch.schema(), &proto, "fixtures.Scalars");
        let values = t.transcode_arrow(&batch).unwrap();
        let msg = decode_message(values.value(0), &proto, "fixtures.Scalars");
        let got = msg
            .get_field_by_name(column)
            .unwrap()
            .as_u64()
            .or_else(|| msg.get_field_by_name(column).unwrap().as_u32().map(u64::from));
        assert_eq!(got, Some(expect), "{column} = {input}");
    }
}

#[test]
fn negative_string_reinterprets_into_unsigned_field() {
    // Mirrors the existing Int64 -> uint64 crossover coercion: producers whose
    // only integer type is signed (BigQuery) must still be able to populate a
    // uint64 proto field.
    let proto = scalars_schema();
    let batch = str_batch("uint64_field", Some("-1"));
    let t = build_coercing_transcoder(&batch.schema(), &proto, "fixtures.Scalars");

    let values = t.transcode_arrow(&batch).unwrap();
    let msg = decode_message(values.value(0), &proto, "fixtures.Scalars");
    assert_eq!(
        msg.get_field_by_name("uint64_field").unwrap().as_u64(),
        Some(u64::MAX)
    );
}

#[test]
fn unparseable_string_fails_the_batch() {
    // Silent corruption is worse than a failed encode: a non-numeric string in
    // an integer column must surface, not become zero.
    let proto = scalars_schema();
    for bad in ["", "12abc", "1.5", " 7", "0x10"] {
        let batch = str_batch("int64_field", Some(bad));
        let t = build_coercing_transcoder(&batch.schema(), &proto, "fixtures.Scalars");
        let err = t
            .transcode_arrow(&batch)
            .expect_err(&format!("{bad:?} must fail"));
        assert!(
            err.to_string().contains("not a valid signed integer"),
            "{bad:?}: {err}"
        );
    }
}

#[test]
fn out_of_range_string_narrowing_fails() {
    // 32-bit targets are range-checked, matching the Int64 -> int32 coercion
    // rather than silently truncating.
    let proto = scalars_schema();
    let batch = str_batch("int32_field", Some("2147483648"));
    let t = build_coercing_transcoder(&batch.schema(), &proto, "fixtures.Scalars");

    let err = t.transcode_arrow(&batch).expect_err("must be out of range");
    assert!(err.to_string().contains("out of range"), "{err}");
}

#[test]
fn null_string_integer_stays_unset() {
    let proto = scalars_schema();
    let batch = str_batch("int64_field", None);
    let t = build_coercing_transcoder(&batch.schema(), &proto, "fixtures.Scalars");

    let values = t.transcode_arrow(&batch).unwrap();
    assert!(values.value(0).is_empty(), "null must encode no field");
}

// ===== String-encoded integers through nested + repeated messages =====

const NESTED_STRING_INTS_BIN: &[u8] = include_bytes!("../../fixtures/nested_string_ints.bin");

/// One `items` element holding one related id and one detail with one marker,
/// every 64-bit integer supplied as a string.
fn nested_string_ints_batch() -> RecordBatch {
    let marker_struct = StructArray::from(vec![
        (
            Arc::new(Field::new("marker_id", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("7")])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("marker_version", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("1500")])) as ArrayRef,
        ),
        (
            Arc::new(Field::new("expires_at_millis", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
        ),
    ]);
    let single = |values: ArrayRef| {
        ListArray::new(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            arrow_buffer::OffsetBuffer::new(vec![0, 1].into()),
            values,
            None,
        )
    };
    let marker_list = single(Arc::new(marker_struct) as ArrayRef);

    let detail_struct = StructArray::from(vec![
        (
            Arc::new(Field::new("markers", marker_list.data_type().clone(), true)),
            Arc::new(marker_list) as ArrayRef,
        ),
        (
            Arc::new(Field::new("weight", DataType::Float64, true)),
            Arc::new(Float64Array::from(vec![Some(0.75)])) as ArrayRef,
        ),
    ]);
    let detail_list = single(Arc::new(detail_struct) as ArrayRef);

    // `high` is NULL, exercising proto3 defaulting alongside the coercion.
    let pair_id = |low: &str| {
        StructArray::from(vec![
            (
                Arc::new(Field::new("low", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![Some(low)])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("high", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
            ),
        ])
    };
    let related_list = single(Arc::new(pair_id("98765432109")) as ArrayRef);

    let item_struct = StructArray::from(vec![
        (
            Arc::new(Field::new(
                "item_id",
                pair_id("12345678901").data_type().clone(),
                true,
            )),
            Arc::new(pair_id("12345678901")) as ArrayRef,
        ),
        (
            Arc::new(Field::new("group_id", DataType::Utf8, true)),
            Arc::new(StringArray::from(vec![Some("grp-0001")])) as ArrayRef,
        ),
        (
            Arc::new(Field::new(
                "related_ids",
                related_list.data_type().clone(),
                true,
            )),
            Arc::new(related_list) as ArrayRef,
        ),
        (
            Arc::new(Field::new("details", detail_list.data_type().clone(), true)),
            Arc::new(detail_list) as ArrayRef,
        ),
    ]);
    let item_list = single(Arc::new(item_struct) as ArrayRef);

    RecordBatch::try_from_iter(vec![("items", Arc::new(item_list) as ArrayRef)]).unwrap()
}

#[test]
fn string_encoded_integers_round_trip_through_nesting() {
    let proto = ProtoSchema::from_bytes(NESTED_STRING_INTS_BIN).unwrap();
    let desc = proto.message("fixtures.ItemList").unwrap();
    let batch = nested_string_ints_batch();

    let mapping = infer_mapping(&batch.schema(), &desc, &coerce_options()).unwrap();
    let values = Transcoder::new(&mapping)
        .unwrap()
        .transcode_arrow(&batch)
        .unwrap();
    let msg = decode_message(values.value(0), &proto, "fixtures.ItemList");

    let items = msg.get_field_by_name("items").unwrap();
    let item = &items.as_list().unwrap()[0];
    let item = item.as_message().unwrap();

    let item_id = item.get_field_by_name("item_id").unwrap();
    let item_id = item_id.as_message().unwrap();
    assert_eq!(
        item_id.get_field_by_name("low").unwrap().as_u64(),
        Some(12_345_678_901)
    );
    // NULL half stays at the proto3 default rather than failing the parse.
    assert_eq!(item_id.get_field_by_name("high").unwrap().as_u64(), Some(0));

    let related = item.get_field_by_name("related_ids").unwrap();
    assert_eq!(
        related.as_list().unwrap()[0]
            .as_message()
            .unwrap()
            .get_field_by_name("low")
            .unwrap()
            .as_u64(),
        Some(98_765_432_109)
    );

    // Two more levels down: repeated inside repeated.
    let details = item.get_field_by_name("details").unwrap();
    let detail = &details.as_list().unwrap()[0];
    let markers = detail
        .as_message()
        .unwrap()
        .get_field_by_name("markers")
        .unwrap();
    let marker = &markers.as_list().unwrap()[0];
    let marker = marker.as_message().unwrap();
    assert_eq!(
        marker.get_field_by_name("marker_id").unwrap().as_i64(),
        Some(7)
    );
    assert_eq!(
        marker.get_field_by_name("marker_version").unwrap().as_i64(),
        Some(1500)
    );
    // proto3 `optional` + NULL column => field stays absent.
    assert!(!marker.has_field_by_name("expires_at_millis"));
}

// ===== Proto map from a list of entry structs (engines with no MAP type) =====

/// `metadata` is `map<string, int64>` in fixtures.Nested. Build it the only way
/// an engine without a MAP type can: `ARRAY<STRUCT<key, value>>`, which arrives
/// as List<Struct<key: Utf8, value: Int64>>.
fn map_as_list_batch(entry_null: bool) -> RecordBatch {
    let entry_fields = Fields::from(vec![
        Field::new("key", DataType::Utf8, true),
        Field::new("value", DataType::Int64, true),
    ]);
    let entries = StructArray::new(
        entry_fields,
        vec![
            Arc::new(StringArray::from(vec![Some("alpha"), Some("beta")])) as ArrayRef,
            Arc::new(Int64Array::from(vec![Some(1), Some(2)])) as ArrayRef,
        ],
        // Optionally mark the second entry itself null.
        Some(arrow_buffer::NullBuffer::from(vec![true, !entry_null])),
    );
    let list = ListArray::new(
        Arc::new(Field::new("item", entries.data_type().clone(), true)),
        arrow_buffer::OffsetBuffer::new(vec![0, 2].into()),
        Arc::new(entries) as ArrayRef,
        None,
    );
    RecordBatch::try_from_iter(vec![("metadata", Arc::new(list) as ArrayRef)]).unwrap()
}

#[test]
fn proto_map_binds_from_list_of_entry_structs() {
    let proto = nested_schema();
    let batch = map_as_list_batch(false);
    let t = build_transcoder(&batch.schema(), &proto, "fixtures.Nested");

    let values = t.transcode_arrow(&batch).unwrap();
    let msg = decode_message(values.value(0), &proto, "fixtures.Nested");

    let map = msg.get_field_by_name("metadata").unwrap();
    let map = map.as_map().unwrap();
    assert_eq!(map.len(), 2);
    let get = |k: &str| {
        map.get(&prost_reflect::MapKey::String(k.to_string()))
            .and_then(|v| v.as_i64())
    };
    assert_eq!(get("alpha"), Some(1));
    assert_eq!(get("beta"), Some(2));
}

#[test]
fn list_backed_map_is_wire_identical_to_map_array() {
    // The whole premise of accepting the list form is that a proto map encodes
    // as repeated MapEntry{key=1,value=2}. Prove the two Arrow shapes produce
    // byte-identical output rather than merely "both decode".
    let proto = nested_schema();

    let list_batch = map_as_list_batch(false);
    let list_bytes = build_transcoder(&list_batch.schema(), &proto, "fixtures.Nested")
        .transcode_arrow(&list_batch)
        .unwrap();

    let mut builder = MapBuilder::new(None, StringBuilder::new(), Int64Builder::new());
    builder.keys().append_value("alpha");
    builder.values().append_value(1);
    builder.keys().append_value("beta");
    builder.values().append_value(2);
    builder.append(true).unwrap();
    let map_batch =
        RecordBatch::try_from_iter(vec![("metadata", Arc::new(builder.finish()) as ArrayRef)])
            .unwrap();
    let map_bytes = build_transcoder(&map_batch.schema(), &proto, "fixtures.Nested")
        .transcode_arrow(&map_batch)
        .unwrap();

    assert_eq!(list_bytes.value(0), map_bytes.value(0));
}

#[test]
fn null_list_entry_is_skipped_not_default_keyed() {
    // A null entry struct has no key, and proto map keys cannot be absent.
    // Encoding it would silently add an ""-keyed entry.
    let proto = nested_schema();
    let batch = map_as_list_batch(true);
    let t = build_transcoder(&batch.schema(), &proto, "fixtures.Nested");

    let values = t.transcode_arrow(&batch).unwrap();
    let msg = decode_message(values.value(0), &proto, "fixtures.Nested");

    let map = msg.get_field_by_name("metadata").unwrap();
    let map = map.as_map().unwrap();
    assert_eq!(map.len(), 1, "null entry must be dropped");
    assert!(map.contains_key(&prost_reflect::MapKey::String("alpha".into())));
    assert!(!map.contains_key(&prost_reflect::MapKey::String(String::new())));
}

#[test]
fn map_from_list_of_non_struct_is_rejected() {
    let proto = nested_schema();
    let desc = proto.message("fixtures.Nested").unwrap();
    let arrow_schema = Schema::new(vec![Field::new(
        "metadata",
        DataType::List(Arc::new(Field::new("item", DataType::Int64, true))),
        true,
    )]);

    let err = infer_mapping(&arrow_schema, &desc, &InferOptions::default())
        .expect_err("a list of scalars is not a map");
    assert!(err.to_string().contains("Struct(key, value)"), "{err}");
}
