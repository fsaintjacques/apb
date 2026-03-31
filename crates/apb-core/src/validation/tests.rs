use arrow_schema::{DataType, Field, Fields, Schema};

use crate::descriptor::ProtoSchema;
use crate::mapping::InferOptions;
use super::*;

const SCALARS_BIN: &[u8] = include_bytes!("../../fixtures/scalars.bin");
const NESTED_BIN: &[u8] = include_bytes!("../../fixtures/nested.bin");

fn scalars_schema() -> ProtoSchema {
    ProtoSchema::from_bytes(SCALARS_BIN).unwrap()
}

fn nested_schema() -> ProtoSchema {
    ProtoSchema::from_bytes(NESTED_BIN).unwrap()
}

#[test]
fn validate_clean_mapping() {
    let schema = scalars_schema();
    let msg = schema.message("fixtures.Scalars").unwrap();

    let arrow_schema = Schema::new(vec![
        Field::new("bool_field", DataType::Boolean, false),
        Field::new("int32_field", DataType::Int32, false),
        Field::new("int64_field", DataType::Int64, false),
        Field::new("uint32_field", DataType::UInt32, false),
        Field::new("uint64_field", DataType::UInt64, false),
        Field::new("sint32_field", DataType::Int32, false),
        Field::new("sint64_field", DataType::Int64, false),
        Field::new("fixed32_field", DataType::UInt32, false),
        Field::new("fixed64_field", DataType::UInt64, false),
        Field::new("sfixed32_field", DataType::Int32, false),
        Field::new("sfixed64_field", DataType::Int64, false),
        Field::new("float_field", DataType::Float32, false),
        Field::new("double_field", DataType::Float64, false),
        Field::new("string_field", DataType::Utf8, false),
        Field::new("bytes_field", DataType::Binary, false),
    ]);

    let report = validate(&arrow_schema, &msg, &InferOptions::default());

    assert_eq!(report.status, ReportStatus::Ok);
    assert_eq!(report.mapped.len(), 15);
    assert!(report.unmapped_arrow.is_empty());
    assert!(report.unmapped_proto.is_empty());
    assert!(report.type_errors.is_empty());
    assert!(report.structural_errors.is_empty());
}

#[test]
fn validate_unmapped_fields_warnings() {
    let schema = scalars_schema();
    let msg = schema.message("fixtures.Scalars").unwrap();

    let arrow_schema = Schema::new(vec![
        Field::new("bool_field", DataType::Boolean, false),
        Field::new("extra_col", DataType::Utf8, false),
    ]);

    let report = validate(&arrow_schema, &msg, &InferOptions::default());

    assert_eq!(report.status, ReportStatus::Warnings);
    assert_eq!(report.mapped.len(), 1);
    assert_eq!(report.unmapped_arrow.len(), 1);
    assert_eq!(report.unmapped_arrow[0].name, "extra_col");
    assert_eq!(report.unmapped_proto.len(), 14);
    assert!(report.type_errors.is_empty());
}

#[test]
fn validate_type_error() {
    let schema = scalars_schema();
    let msg = schema.message("fixtures.Scalars").unwrap();

    // bool_field is proto bool, but we provide Int32.
    let arrow_schema = Schema::new(vec![
        Field::new("bool_field", DataType::Int32, false),
        Field::new("string_field", DataType::Float64, false),
    ]);

    let report = validate(&arrow_schema, &msg, &InferOptions::default());

    assert_eq!(report.status, ReportStatus::Error);
    // Both type errors should be collected, not just the first.
    assert_eq!(report.type_errors.len(), 2);
    assert!(report.type_errors.iter().any(|e| e.arrow_name == "bool_field"));
    assert!(report.type_errors.iter().any(|e| e.arrow_name == "string_field"));
}

#[test]
fn validate_multiple_errors_all_collected() {
    let schema = scalars_schema();
    let msg = schema.message("fixtures.Scalars").unwrap();

    // Multiple type mismatches — all should appear in the report.
    let arrow_schema = Schema::new(vec![
        Field::new("bool_field", DataType::Utf8, false),
        Field::new("int32_field", DataType::Utf8, false),
        Field::new("float_field", DataType::Utf8, false),
    ]);

    let report = validate(&arrow_schema, &msg, &InferOptions::default());

    assert_eq!(report.status, ReportStatus::Error);
    assert_eq!(report.type_errors.len(), 3);
}

#[test]
fn validate_oneof_not_struct() {
    let schema = nested_schema();
    let msg = schema.message("fixtures.Nested").unwrap();

    let arrow_schema = Schema::new(vec![
        Field::new("choice", DataType::Utf8, true),
    ]);

    let report = validate(&arrow_schema, &msg, &InferOptions::default());

    assert_eq!(report.status, ReportStatus::Error);
    assert!(!report.structural_errors.is_empty());
    assert!(report.structural_errors[0].message.contains("oneof"));
}

#[test]
fn validate_nested_message_errors() {
    let schema = nested_schema();
    let msg = schema.message("fixtures.Nested").unwrap();

    // Provide inner as a struct but with wrong child types.
    let arrow_schema = Schema::new(vec![
        Field::new(
            "inner",
            DataType::Struct(Fields::from(vec![
                Field::new("value", DataType::Int32, false), // should be string
                Field::new("count", DataType::Utf8, false),  // should be int32
            ])),
            true,
        ),
    ]);

    let report = validate(&arrow_schema, &msg, &InferOptions::default());

    // Parent status should be Error because nested has errors.
    assert_eq!(report.status, ReportStatus::Error);

    // The nested report should contain the type errors.
    assert!(!report.nested.is_empty());
    let inner_report = &report.nested[0].report;
    assert_eq!(inner_report.status, ReportStatus::Error);
    assert_eq!(inner_report.type_errors.len(), 2);
}

#[test]
fn validate_strict_unmapped_proto() {
    let schema = scalars_schema();
    let msg = schema.message("fixtures.Scalars").unwrap();

    let arrow_schema = Schema::new(vec![
        Field::new("bool_field", DataType::Boolean, false),
    ]);

    let options = InferOptions {
        allow_unmapped_proto: false,
        allow_unmapped_arrow: true,
    };

    let report = validate(&arrow_schema, &msg, &options);

    assert_eq!(report.status, ReportStatus::Error);
    assert!(!report.structural_errors.is_empty());
    assert!(report.structural_errors[0].message.contains("unmapped proto"));
}

#[test]
fn validate_strict_unmapped_arrow() {
    let schema = scalars_schema();
    let msg = schema.message("fixtures.Scalars").unwrap();

    let arrow_schema = Schema::new(vec![
        Field::new("bool_field", DataType::Boolean, false),
        Field::new("extra", DataType::Utf8, false),
    ]);

    let options = InferOptions {
        allow_unmapped_proto: true,
        allow_unmapped_arrow: false,
    };

    let report = validate(&arrow_schema, &msg, &options);

    assert_eq!(report.status, ReportStatus::Error);
    assert!(!report.structural_errors.is_empty());
    assert!(report.structural_errors[0].message.contains("unmapped Arrow"));
}

#[test]
fn validate_json_roundtrip() {
    let schema = scalars_schema();
    let msg = schema.message("fixtures.Scalars").unwrap();

    let arrow_schema = Schema::new(vec![
        Field::new("bool_field", DataType::Boolean, false),
        Field::new("extra", DataType::Utf8, false),
    ]);

    let report = validate(&arrow_schema, &msg, &InferOptions::default());

    let json = report.to_json();
    let parsed: MappingReport = serde_json::from_str(&json).unwrap();

    assert_eq!(parsed.status, report.status);
    assert_eq!(parsed.mapped.len(), report.mapped.len());
    assert_eq!(parsed.unmapped_arrow.len(), report.unmapped_arrow.len());
    assert_eq!(parsed.unmapped_proto.len(), report.unmapped_proto.len());
    assert_eq!(parsed.message_name, report.message_name);
}

#[test]
fn validate_human_rendering_contains_field_names() {
    let schema = scalars_schema();
    let msg = schema.message("fixtures.Scalars").unwrap();

    let arrow_schema = Schema::new(vec![
        Field::new("bool_field", DataType::Boolean, false),
        Field::new("extra", DataType::Utf8, false),
    ]);

    let report = validate(&arrow_schema, &msg, &InferOptions::default());
    let rendered = report.render_human();

    assert!(rendered.contains("bool_field"), "should contain matched field name");
    assert!(rendered.contains("extra"), "should contain unmapped arrow field");
    assert!(rendered.contains("no proto field"), "should show unmapped arrow columns");
    assert!(rendered.contains("1/"), "should show mapped count");
}

#[test]
fn validate_human_rendering_shows_errors() {
    let schema = scalars_schema();
    let msg = schema.message("fixtures.Scalars").unwrap();

    // Bool field gets Int32 — type mismatch triggers a type error line.
    let arrow_schema = Schema::new(vec![
        Field::new("bool_field", DataType::Int32, false),
    ]);

    let report = validate(&arrow_schema, &msg, &InferOptions::default());
    let rendered = report.render_human();

    assert!(rendered.contains("bool_field"), "should contain field name");
    assert!(rendered.contains("missing"), "should show missing proto fields");
    // Type error for bool_field (Int32 vs bool) should show the reason.
    assert!(
        rendered.contains("no mapping") || rendered.contains("incompatible"),
        "should show type error reason: {rendered}"
    );
}
