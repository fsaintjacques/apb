//! End-to-end test of the envelope + payload Any flow through the CLI:
//! Arrow IPC in → `apb transcode` → varint-delimited protobuf out.

use std::fs;
use std::path::PathBuf;
use std::process::Command;
use std::sync::Arc;

use arrow_array::{ArrayRef, Int64Array, RecordBatch, StringArray, StructArray};
use arrow_schema::{DataType, Field, Fields, Schema};
use prost_reflect::{DescriptorPool, DynamicMessage, Value};

const ANY_BIN: &[u8] = include_bytes!("../../apb-core/fixtures/any.bin");

fn descriptor_path() -> String {
    concat!(env!("CARGO_MANIFEST_DIR"), "/../apb-core/fixtures/any.bin").to_string()
}

fn order_placed_fields() -> Fields {
    Fields::from(vec![
        Field::new("order_id", DataType::Utf8, true),
        Field::new("amount_cents", DataType::Int64, true),
    ])
}

fn order_placed_struct(order_id: &str, amount_cents: i64) -> StructArray {
    StructArray::new(
        order_placed_fields(),
        vec![
            Arc::new(StringArray::from(vec![order_id])) as ArrayRef,
            Arc::new(Int64Array::from(vec![amount_cents])),
        ],
        None,
    )
}

/// Write a one-row batch to an Arrow IPC stream file in a temp dir.
fn write_ipc(name: &str, batch: &RecordBatch) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("apb-any-e2e-{}", std::process::id()));
    fs::create_dir_all(&dir).unwrap();
    let path = dir.join(name);
    let file = fs::File::create(&path).unwrap();
    let mut writer = arrow_ipc::writer::StreamWriter::try_new(file, &batch.schema()).unwrap();
    writer.write(batch).unwrap();
    writer.finish().unwrap();
    path
}

/// Split a varint-delimited stream into message byte slices.
fn split_delimited(data: &[u8]) -> Vec<Vec<u8>> {
    let mut messages = Vec::new();
    let mut pos = 0;
    while pos < data.len() {
        let mut len: u64 = 0;
        let mut shift = 0;
        loop {
            let byte = data[pos];
            pos += 1;
            len |= ((byte & 0x7F) as u64) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }
        messages.push(data[pos..pos + len as usize].to_vec());
        pos += len as usize;
    }
    messages
}

fn decode_envelope(bytes: &[u8]) -> DynamicMessage {
    let fds =
        <prost_reflect::prost_types::FileDescriptorSet as prost_reflect::prost::Message>::decode(
            ANY_BIN,
        )
        .unwrap();
    let pool = DescriptorPool::from_file_descriptor_set(fds).unwrap();
    let desc = pool.get_message_by_name("fixtures.Envelope").unwrap();
    DynamicMessage::decode(desc, bytes).unwrap()
}

fn get_any(msg: &DynamicMessage, field: &str) -> (String, Vec<u8>) {
    let m = match msg.get_field_by_name(field).unwrap().as_ref() {
        Value::Message(m) => m.clone(),
        other => panic!("expected message for '{field}', got {other:?}"),
    };
    let url = match m.get_field_by_name("type_url").unwrap().as_ref() {
        Value::String(s) => s.clone(),
        other => panic!("expected string, got {other:?}"),
    };
    let value = match m.get_field_by_name("value").unwrap().as_ref() {
        Value::Bytes(b) => b.to_vec(),
        other => panic!("expected bytes, got {other:?}"),
    };
    (url, value)
}

fn run_transcode(args: &[&str]) -> Vec<u8> {
    let output = Command::new(env!("CARGO_BIN_EXE_apb"))
        .arg("transcode")
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "apb transcode failed: {}",
        String::from_utf8_lossy(&output.stderr),
    );
    output.stdout
}

/// Annotation-driven packing: the (apb).any_pack annotation on
/// fixtures.Envelope.payload drives the double serialization.
#[test]
fn transcode_packed_any_via_annotation() {
    let arrow_schema = Schema::new(vec![
        Field::new("event_id", DataType::Utf8, false),
        Field::new("payload", DataType::Struct(order_placed_fields()), true),
    ]);
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![
            Arc::new(StringArray::from(vec!["evt-1"])),
            Arc::new(order_placed_struct("ord-1", 1299)),
        ],
    )
    .unwrap();
    let ipc = write_ipc("annotation.arrow", &batch);

    let stdout = run_transcode(&[
        "--descriptor",
        &descriptor_path(),
        "--message",
        "fixtures.Envelope",
        "--ipc",
        ipc.to_str().unwrap(),
    ]);

    let messages = split_delimited(&stdout);
    assert_eq!(messages.len(), 1);
    let envelope = decode_envelope(&messages[0]);

    assert_eq!(
        envelope.get_field_by_name("event_id").unwrap().as_ref(),
        &Value::String("evt-1".to_string()),
    );
    let (url, value) = get_any(&envelope, "payload");
    assert_eq!(url, "type.googleapis.com/fixtures.OrderPlaced");

    let fds =
        <prost_reflect::prost_types::FileDescriptorSet as prost_reflect::prost::Message>::decode(
            ANY_BIN,
        )
        .unwrap();
    let pool = DescriptorPool::from_file_descriptor_set(fds).unwrap();
    let order_desc = pool.get_message_by_name("fixtures.OrderPlaced").unwrap();
    let order = DynamicMessage::decode(order_desc, &value[..]).unwrap();
    assert_eq!(
        order.get_field_by_name("order_id").unwrap().as_ref(),
        &Value::String("ord-1".to_string()),
    );
    assert_eq!(
        order.get_field_by_name("amount_cents").unwrap().as_ref(),
        &Value::I64(1299),
    );
}

/// Caller-driven packing: --any-pack packs the unannotated `raw` field and
/// --any-url-prefix overrides the derived type_url prefix.
#[test]
fn transcode_packed_any_via_flags() {
    let arrow_schema = Schema::new(vec![Field::new(
        "raw",
        DataType::Struct(order_placed_fields()),
        true,
    )]);
    let batch = RecordBatch::try_new(
        Arc::new(arrow_schema),
        vec![Arc::new(order_placed_struct("ord-2", 50))],
    )
    .unwrap();
    let ipc = write_ipc("flags.arrow", &batch);

    let stdout = run_transcode(&[
        "--descriptor",
        &descriptor_path(),
        "--message",
        "fixtures.Envelope",
        "--ipc",
        ipc.to_str().unwrap(),
        "--any-pack",
        "fixtures.Envelope.raw=fixtures.OrderPlaced",
        "--any-url-prefix",
        "example.com/",
    ]);

    let messages = split_delimited(&stdout);
    assert_eq!(messages.len(), 1);
    let envelope = decode_envelope(&messages[0]);

    let (url, value) = get_any(&envelope, "raw");
    assert_eq!(url, "example.com/fixtures.OrderPlaced");
    assert!(!value.is_empty());
}
