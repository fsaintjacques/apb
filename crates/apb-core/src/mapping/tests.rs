use arrow_schema::{DataType, Field, Fields, Schema};
use std::sync::Arc;

use crate::descriptor::ProtoSchema;
use super::*;

const SCALARS_BIN: &[u8] = include_bytes!("../../fixtures/scalars.bin");
const NESTED_BIN: &[u8] = include_bytes!("../../fixtures/nested.bin");
const ANNOTATED_BIN: &[u8] = include_bytes!("../../fixtures/annotated.bin");

fn scalars_schema() -> ProtoSchema {
    ProtoSchema::from_bytes(SCALARS_BIN).unwrap()
}

fn nested_schema() -> ProtoSchema {
    ProtoSchema::from_bytes(NESTED_BIN).unwrap()
}

fn annotated_schema() -> ProtoSchema {
    ProtoSchema::from_bytes(ANNOTATED_BIN).unwrap()
}

// ==================== Infer: scalar name-match ====================

#[test]
fn infer_flat_all_match() {
    let schema = scalars_schema();
    let msg = schema.message("fixtures.Scalars").unwrap();

    let arrow_schema = Schema::new(vec![
        Field::new("bool_field", DataType::Boolean, false),
        Field::new("int32_field", DataType::Int32, false),
        Field::new("string_field", DataType::Utf8, false),
    ]);

    let mapping = infer_mapping(&arrow_schema, &msg, &InferOptions::default()).unwrap();

    assert_eq!(mapping.bindings.len(), 3);
    assert!(mapping.unmapped_arrow.is_empty());
    // 15 proto fields - 3 matched = 12 unmapped
    assert_eq!(mapping.unmapped_proto.len(), 12);

    // Check binding details.
    let bool_binding = mapping.bindings.iter().find(|b| b.proto_name == "bool_field").unwrap();
    assert_eq!(bool_binding.arrow_index, 0);
    assert_eq!(bool_binding.proto_number, 1);
    assert_eq!(bool_binding.bind_method, BindMethod::NameMatch);
}

#[test]
fn infer_flat_partial_match() {
    let schema = scalars_schema();
    let msg = schema.message("fixtures.Scalars").unwrap();

    let arrow_schema = Schema::new(vec![
        Field::new("bool_field", DataType::Boolean, false),
        Field::new("extra_column", DataType::Utf8, false),
    ]);

    let mapping = infer_mapping(&arrow_schema, &msg, &InferOptions::default()).unwrap();

    assert_eq!(mapping.bindings.len(), 1);
    assert_eq!(mapping.unmapped_arrow.len(), 1);
    assert_eq!(mapping.unmapped_arrow[0].name, "extra_column");
    assert_eq!(mapping.unmapped_proto.len(), 14);
}

#[test]
fn infer_no_match() {
    let schema = scalars_schema();
    let msg = schema.message("fixtures.Scalars").unwrap();

    let arrow_schema = Schema::new(vec![
        Field::new("foo", DataType::Int32, false),
    ]);

    let mapping = infer_mapping(&arrow_schema, &msg, &InferOptions::default()).unwrap();

    assert_eq!(mapping.bindings.len(), 0);
    assert_eq!(mapping.unmapped_arrow.len(), 1);
    assert_eq!(mapping.unmapped_proto.len(), 15);
}

#[test]
fn infer_type_mismatch_error() {
    let schema = scalars_schema();
    let msg = schema.message("fixtures.Scalars").unwrap();

    // bool_field is proto bool, but we provide Int32 Arrow type.
    let arrow_schema = Schema::new(vec![
        Field::new("bool_field", DataType::Int32, false),
    ]);

    let result = infer_mapping(&arrow_schema, &msg, &InferOptions::default());
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(matches!(err, MappingError::TypeError(_)));
}

#[test]
fn infer_disallow_unmapped_proto() {
    let schema = scalars_schema();
    let msg = schema.message("fixtures.Scalars").unwrap();

    let arrow_schema = Schema::new(vec![
        Field::new("bool_field", DataType::Boolean, false),
    ]);

    let options = InferOptions {
        allow_unmapped_proto: false,
        allow_unmapped_arrow: true,
    };

    let result = infer_mapping(&arrow_schema, &msg, &options);
    assert!(matches!(result, Err(MappingError::UnmappedProtoNotAllowed { .. })));
}

#[test]
fn infer_disallow_unmapped_arrow() {
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

    let result = infer_mapping(&arrow_schema, &msg, &options);
    assert!(matches!(result, Err(MappingError::UnmappedArrowNotAllowed { .. })));
}

// ==================== Infer: nested message ====================

#[test]
fn infer_nested_message() {
    let schema = nested_schema();
    let msg = schema.message("fixtures.Nested").unwrap();

    let arrow_schema = Schema::new(vec![
        Field::new("inner", DataType::Struct(Fields::from(vec![
            Field::new("value", DataType::Utf8, false),
            Field::new("count", DataType::Int32, false),
        ])), true),
    ]);

    let mapping = infer_mapping(&arrow_schema, &msg, &InferOptions::default()).unwrap();

    assert_eq!(mapping.bindings.len(), 1);
    let inner_binding = &mapping.bindings[0];
    assert_eq!(inner_binding.proto_name, "inner");
    match &inner_binding.field_shape {
        FieldShape::Message(sub) => {
            assert_eq!(sub.bindings.len(), 2);
            assert!(sub.unmapped_arrow.is_empty());
            assert!(sub.unmapped_proto.is_empty());
        }
        other => panic!("expected Message shape, got {:?}", other),
    }
}

// ==================== Infer: repeated ====================

#[test]
fn infer_repeated_scalar() {
    let schema = nested_schema();
    let msg = schema.message("fixtures.Nested").unwrap();

    let arrow_schema = Schema::new(vec![
        Field::new("tags", DataType::List(Arc::new(Field::new("item", DataType::Int32, false))), true),
    ]);

    let mapping = infer_mapping(&arrow_schema, &msg, &InferOptions::default()).unwrap();

    assert_eq!(mapping.bindings.len(), 1);
    let tags = &mapping.bindings[0];
    assert!(matches!(&tags.field_shape, FieldShape::Repeated { .. }));
}

#[test]
fn infer_repeated_message() {
    let schema = nested_schema();
    let msg = schema.message("fixtures.Nested").unwrap();

    let arrow_schema = Schema::new(vec![
        Field::new("items", DataType::List(Arc::new(Field::new(
            "item",
            DataType::Struct(Fields::from(vec![
                Field::new("value", DataType::Utf8, false),
                Field::new("count", DataType::Int32, false),
            ])),
            true,
        ))), true),
    ]);

    let mapping = infer_mapping(&arrow_schema, &msg, &InferOptions::default()).unwrap();

    assert_eq!(mapping.bindings.len(), 1);
    match &mapping.bindings[0].field_shape {
        FieldShape::Repeated { element_shape, .. } => {
            assert!(matches!(element_shape.as_ref(), FieldShape::Message(_)));
        }
        other => panic!("expected Repeated, got {:?}", other),
    }
}

// ==================== Infer: map ====================

#[test]
fn infer_map_field() {
    let schema = nested_schema();
    let msg = schema.message("fixtures.Nested").unwrap();

    let arrow_schema = Schema::new(vec![
        Field::new("metadata", DataType::Map(
            Arc::new(Field::new("entries", DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, false),
            ])), false)),
            false,
        ), true),
    ]);

    let mapping = infer_mapping(&arrow_schema, &msg, &InferOptions::default()).unwrap();

    assert_eq!(mapping.bindings.len(), 1);
    assert!(matches!(&mapping.bindings[0].field_shape, FieldShape::Map { .. }));
}

// ==================== Infer: oneof ====================

#[test]
fn infer_oneof() {
    let schema = nested_schema();
    let msg = schema.message("fixtures.Nested").unwrap();

    let arrow_schema = Schema::new(vec![
        Field::new("choice", DataType::Struct(Fields::from(vec![
            Field::new("text_value", DataType::Utf8, true),
            Field::new("int_value", DataType::Int32, true),
        ])), true),
    ]);

    let mapping = infer_mapping(&arrow_schema, &msg, &InferOptions::default()).unwrap();

    assert_eq!(mapping.oneofs.len(), 1);
    let oneof = &mapping.oneofs[0];
    assert_eq!(oneof.oneof_name, "choice");
    assert_eq!(oneof.arrow_name, "choice");
    // We provided 2 of 3 variants — message_value is missing.
    assert_eq!(oneof.variants.len(), 2);
    assert_eq!(oneof.variants[0].proto_name, "text_value");
    assert_eq!(oneof.variants[1].proto_name, "int_value");
}

#[test]
fn infer_oneof_not_struct_error() {
    let schema = nested_schema();
    let msg = schema.message("fixtures.Nested").unwrap();

    let arrow_schema = Schema::new(vec![
        Field::new("choice", DataType::Utf8, true),
    ]);

    let result = infer_mapping(&arrow_schema, &msg, &InferOptions::default());
    assert!(matches!(result, Err(MappingError::OneofNotStruct { .. })));
}

// ==================== Infer: enum ====================

#[test]
fn infer_enum_field() {
    let schema = nested_schema();
    let msg = schema.message("fixtures.Nested").unwrap();

    let arrow_schema = Schema::new(vec![
        Field::new("status", DataType::Int32, false),
    ]);

    let mapping = infer_mapping(&arrow_schema, &msg, &InferOptions::default()).unwrap();
    assert_eq!(mapping.bindings.len(), 1);
    assert_eq!(mapping.bindings[0].proto_name, "status");
}

// ==================== Explicit mode ====================

#[test]
fn explicit_by_name() {
    let schema = scalars_schema();
    let msg = schema.message("fixtures.Scalars").unwrap();

    let arrow_schema = Schema::new(vec![
        Field::new("my_bool", DataType::Boolean, false),
        Field::new("my_string", DataType::Utf8, false),
    ]);

    let bindings = vec![
        ExplicitBinding {
            arrow_field: ArrowFieldRef::Name("my_bool".to_string()),
            proto_field: ProtoFieldRef::Name("bool_field".to_string()),
            coerce: false,
        },
        ExplicitBinding {
            arrow_field: ArrowFieldRef::Name("my_string".to_string()),
            proto_field: ProtoFieldRef::Name("string_field".to_string()),
            coerce: false,
        },
    ];

    let mapping = explicit_mapping(&arrow_schema, &msg, &bindings).unwrap();

    assert_eq!(mapping.bindings.len(), 2);
    assert_eq!(mapping.bindings[0].proto_name, "bool_field");
    assert_eq!(mapping.bindings[0].arrow_name, "my_bool");
    assert_eq!(mapping.bindings[0].bind_method, BindMethod::Explicit);
}

#[test]
fn explicit_by_index_and_number() {
    let schema = scalars_schema();
    let msg = schema.message("fixtures.Scalars").unwrap();

    let arrow_schema = Schema::new(vec![
        Field::new("col0", DataType::Boolean, false),
    ]);

    let bindings = vec![ExplicitBinding {
        arrow_field: ArrowFieldRef::Index(0),
        proto_field: ProtoFieldRef::Number(1), // bool_field
        coerce: false,
    }];

    let mapping = explicit_mapping(&arrow_schema, &msg, &bindings).unwrap();
    assert_eq!(mapping.bindings.len(), 1);
    assert_eq!(mapping.bindings[0].proto_name, "bool_field");
}

#[test]
fn explicit_missing_arrow_field() {
    let schema = scalars_schema();
    let msg = schema.message("fixtures.Scalars").unwrap();

    let arrow_schema = Schema::new(Vec::<Field>::new());

    let bindings = vec![ExplicitBinding {
        arrow_field: ArrowFieldRef::Name("nonexistent".to_string()),
        proto_field: ProtoFieldRef::Name("bool_field".to_string()),
        coerce: false,
    }];

    let result = explicit_mapping(&arrow_schema, &msg, &bindings);
    assert!(matches!(result, Err(MappingError::ArrowFieldNotFound { .. })));
}

#[test]
fn explicit_missing_proto_field() {
    let schema = scalars_schema();
    let msg = schema.message("fixtures.Scalars").unwrap();

    let arrow_schema = Schema::new(vec![
        Field::new("col0", DataType::Boolean, false),
    ]);

    let bindings = vec![ExplicitBinding {
        arrow_field: ArrowFieldRef::Name("col0".to_string()),
        proto_field: ProtoFieldRef::Name("nonexistent".to_string()),
        coerce: false,
    }];

    let result = explicit_mapping(&arrow_schema, &msg, &bindings);
    assert!(matches!(result, Err(MappingError::ProtoFieldNotFound { .. })));
}

#[test]
fn explicit_coercion() {
    let schema = scalars_schema();
    let msg = schema.message("fixtures.Scalars").unwrap();

    // Int64 → int32 requires coercion.
    let arrow_schema = Schema::new(vec![
        Field::new("col0", DataType::Int64, false),
    ]);

    // Without coerce → error.
    let bindings_no_coerce = vec![ExplicitBinding {
        arrow_field: ArrowFieldRef::Index(0),
        proto_field: ProtoFieldRef::Name("int32_field".to_string()),
        coerce: false,
    }];
    assert!(explicit_mapping(&arrow_schema, &msg, &bindings_no_coerce).is_err());

    // With coerce → success.
    let bindings_coerce = vec![ExplicitBinding {
        arrow_field: ArrowFieldRef::Index(0),
        proto_field: ProtoFieldRef::Name("int32_field".to_string()),
        coerce: true,
    }];
    let mapping = explicit_mapping(&arrow_schema, &msg, &bindings_coerce).unwrap();
    assert_eq!(mapping.bindings.len(), 1);
}

#[test]
fn explicit_duplicate_arrow() {
    let schema = scalars_schema();
    let msg = schema.message("fixtures.Scalars").unwrap();

    let arrow_schema = Schema::new(vec![
        Field::new("col0", DataType::Boolean, false),
    ]);

    let bindings = vec![
        ExplicitBinding {
            arrow_field: ArrowFieldRef::Index(0),
            proto_field: ProtoFieldRef::Name("bool_field".to_string()),
            coerce: false,
        },
        ExplicitBinding {
            arrow_field: ArrowFieldRef::Index(0),
            proto_field: ProtoFieldRef::Name("int32_field".to_string()),
            coerce: false,
        },
    ];

    let result = explicit_mapping(&arrow_schema, &msg, &bindings);
    assert!(matches!(result, Err(MappingError::DuplicateBinding { .. })));
}

#[test]
fn explicit_duplicate_proto() {
    let schema = scalars_schema();
    let msg = schema.message("fixtures.Scalars").unwrap();

    let arrow_schema = Schema::new(vec![
        Field::new("col0", DataType::Boolean, false),
        Field::new("col1", DataType::Boolean, false),
    ]);

    let bindings = vec![
        ExplicitBinding {
            arrow_field: ArrowFieldRef::Index(0),
            proto_field: ProtoFieldRef::Name("bool_field".to_string()),
            coerce: false,
        },
        ExplicitBinding {
            arrow_field: ArrowFieldRef::Index(1),
            proto_field: ProtoFieldRef::Number(1), // bool_field again
            coerce: false,
        },
    ];

    let result = explicit_mapping(&arrow_schema, &msg, &bindings);
    assert!(matches!(result, Err(MappingError::DuplicateProtoBinding { .. })));
}

#[test]
fn explicit_nested_message_unsupported() {
    let schema = nested_schema();
    let msg = schema.message("fixtures.Nested").unwrap();

    let arrow_schema = Schema::new(vec![
        Field::new("inner", DataType::Struct(Fields::from(vec![
            Field::new("value", DataType::Utf8, false),
        ])), true),
    ]);

    let bindings = vec![ExplicitBinding {
        arrow_field: ArrowFieldRef::Name("inner".to_string()),
        proto_field: ProtoFieldRef::Name("inner".to_string()),
        coerce: false,
    }];

    let result = explicit_mapping(&arrow_schema, &msg, &bindings);
    assert!(matches!(result, Err(MappingError::UnsupportedExplicitNested { .. })));
}

#[test]
fn infer_repeated_large_list() {
    let schema = nested_schema();
    let msg = schema.message("fixtures.Nested").unwrap();

    let arrow_schema = Schema::new(vec![
        Field::new("tags", DataType::LargeList(Arc::new(Field::new("item", DataType::Int32, false))), true),
    ]);

    let mapping = infer_mapping(&arrow_schema, &msg, &InferOptions::default()).unwrap();
    assert_eq!(mapping.bindings.len(), 1);
    assert!(matches!(&mapping.bindings[0].field_shape, FieldShape::Repeated { .. }));
}

// ==================== Infer: annotations ====================

#[test]
fn infer_annotation_arrow_name() {
    let schema = annotated_schema();
    let msg = schema.message("fixtures.Annotated").unwrap();

    // Arrow column is "uid", but proto field is "user_id" with (apb).arrow_name = "uid".
    let arrow_schema = Schema::new(vec![
        Field::new("uid", DataType::Utf8, false),
        Field::new("name", DataType::Utf8, false),
    ]);

    let mapping = infer_mapping(&arrow_schema, &msg, &InferOptions::default()).unwrap();

    // user_id should bind via annotation, name via name-match.
    let uid_binding = mapping.bindings.iter().find(|b| b.proto_name == "user_id").unwrap();
    assert_eq!(uid_binding.arrow_name, "uid");
    assert_eq!(uid_binding.bind_method, BindMethod::Annotation);

    let name_binding = mapping.bindings.iter().find(|b| b.proto_name == "name").unwrap();
    assert_eq!(name_binding.arrow_name, "name");
    assert_eq!(name_binding.bind_method, BindMethod::NameMatch);
}

#[test]
fn infer_annotation_missing_arrow_field_error() {
    let schema = annotated_schema();
    let msg = schema.message("fixtures.Annotated").unwrap();

    // Annotation says arrow_name = "uid", but no such Arrow field exists.
    let arrow_schema = Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
    ]);

    let result = infer_mapping(&arrow_schema, &msg, &InferOptions::default());
    assert!(matches!(result, Err(MappingError::ArrowFieldNotFound { .. })));
}

#[test]
fn infer_annotation_coerce() {
    let schema = annotated_schema();
    let msg = schema.message("fixtures.Annotated").unwrap();

    // count is proto int32, provide Int64 Arrow type.
    // Coercion is enabled via (apb).coerce = true on the proto field.
    // Must also provide "uid" since user_id has annotation arrow_name="uid".
    let arrow_schema = Schema::new(vec![
        Field::new("uid", DataType::Utf8, false),
        Field::new("count", DataType::Int64, false),
    ]);

    let mapping = infer_mapping(&arrow_schema, &msg, &InferOptions::default()).unwrap();

    let count_binding = mapping.bindings.iter().find(|b| b.proto_name == "count").unwrap();
    assert_eq!(count_binding.arrow_name, "count");
    // Should succeed because coercion is enabled via annotation.
    assert!(matches!(
        count_binding.type_check.mode,
        crate::types::TypeCheckMode::Coerce { .. }
    ));
}

#[test]
fn infer_annotation_priority_over_name() {
    let schema = annotated_schema();
    let msg = schema.message("fixtures.Annotated").unwrap();

    // Both "uid" (annotation target) and "user_id" (name-match) exist.
    // Annotation should win — bind to "uid".
    let arrow_schema = Schema::new(vec![
        Field::new("user_id", DataType::Utf8, false),
        Field::new("uid", DataType::Utf8, false),
    ]);

    let mapping = infer_mapping(&arrow_schema, &msg, &InferOptions::default()).unwrap();

    let uid_binding = mapping.bindings.iter().find(|b| b.proto_name == "user_id").unwrap();
    assert_eq!(uid_binding.arrow_name, "uid");
    assert_eq!(uid_binding.bind_method, BindMethod::Annotation);

    // "user_id" Arrow column should be unmapped.
    assert!(mapping.unmapped_arrow.iter().any(|u| u.name == "user_id"));
}
