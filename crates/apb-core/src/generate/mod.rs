mod model;

pub use model::GenerateError;

use std::collections::HashSet;

use arrow_schema::{DataType, Field, Schema};
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto, MessageOptions};

use model::{MAX_FIELD_NUMBER, RESERVED_RANGE};

/// Generate a `FileDescriptorProto` from an Arrow schema.
///
/// Produces a proto3 file descriptor with a single top-level message.
/// Field numbers are auto-assigned sequentially starting at 1, skipping
/// the protobuf reserved range (19000–19999).
pub fn generate_file_descriptor(
    schema: &Schema,
    package: &str,
    message_name: &str,
) -> Result<FileDescriptorProto, GenerateError> {
    let mut ctx = GenContext::default();
    let message = generate_message(
        message_name,
        schema.fields().iter().map(|f| f.as_ref()),
        &mut ctx,
    )?;

    let mut file = FileDescriptorProto {
        name: Some(format!("{package}.proto")),
        package: Some(package.to_string()),
        syntax: Some("proto3".to_string()),
        message_type: vec![message],
        ..Default::default()
    };

    if ctx.needs_timestamp {
        file.dependency
            .push("google/protobuf/timestamp.proto".to_string());
    }
    if ctx.needs_duration {
        file.dependency
            .push("google/protobuf/duration.proto".to_string());
    }

    Ok(file)
}

/// Tracks state needed across recursive message generation.
#[derive(Default)]
struct GenContext {
    needs_timestamp: bool,
    needs_duration: bool,
}

/// Resolved proto type for a leaf (non-composite) Arrow DataType.
struct ResolvedType {
    proto_type: Type,
    type_name: Option<String>,
}

/// Resolve a leaf Arrow DataType to its proto type and optional type_name.
/// Handles scalars, Timestamp, and Duration. Returns None for composite types.
fn resolve_leaf_type(
    dt: &DataType,
    field_name: &str,
    ctx: &mut GenContext,
) -> Result<Option<ResolvedType>, GenerateError> {
    if is_scalar(dt) {
        let proto_type = scalar_to_proto_type(dt, field_name)?;
        return Ok(Some(ResolvedType {
            proto_type,
            type_name: None,
        }));
    }
    match dt {
        DataType::Timestamp(_, _) => {
            ctx.needs_timestamp = true;
            Ok(Some(ResolvedType {
                proto_type: Type::Message,
                type_name: Some(".google.protobuf.Timestamp".to_string()),
            }))
        }
        DataType::Duration(_) => {
            ctx.needs_duration = true;
            Ok(Some(ResolvedType {
                proto_type: Type::Message,
                type_name: Some(".google.protobuf.Duration".to_string()),
            }))
        }
        _ => Ok(None),
    }
}

/// Assign a field number, skipping the protobuf reserved range.
fn assign_field_number(index: usize, message_name: &str) -> Result<i32, GenerateError> {
    let mut number = (index as i32) + 1;
    // Skip the reserved range 19000–19999.
    if RESERVED_RANGE.contains(&number) {
        number = *RESERVED_RANGE.end() + 1 + (number - *RESERVED_RANGE.start());
    }
    if number > MAX_FIELD_NUMBER {
        return Err(GenerateError::TooManyFields {
            message_name: message_name.to_string(),
            count: index + 1,
        });
    }
    Ok(number)
}

/// Pick a unique nested message name within the parent scope.
fn unique_nested_name(base: &str, used: &mut HashSet<String>) -> String {
    let name = to_message_name(base);
    if used.insert(name.clone()) {
        return name;
    }
    // Disambiguate with a numeric suffix.
    for i in 2.. {
        let candidate = format!("{name}{i}");
        if used.insert(candidate.clone()) {
            return candidate;
        }
    }
    unreachable!()
}

fn generate_message<'a>(
    name: &str,
    fields: impl Iterator<Item = &'a Field>,
    ctx: &mut GenContext,
) -> Result<DescriptorProto, GenerateError> {
    let mut message = DescriptorProto {
        name: Some(name.to_string()),
        ..Default::default()
    };

    let mut used_names = HashSet::new();

    for (i, field) in fields.enumerate() {
        let number = assign_field_number(i, name)?;
        generate_field(field, number, &mut message, ctx, &mut used_names)?;
    }

    Ok(message)
}

fn generate_field(
    field: &Field,
    number: i32,
    parent: &mut DescriptorProto,
    ctx: &mut GenContext,
    used_names: &mut HashSet<String>,
) -> Result<(), GenerateError> {
    let data_type = resolve_dictionary(field.data_type());

    // Try leaf type first (scalars, Timestamp, Duration).
    if let Some(resolved) = resolve_leaf_type(data_type, field.name(), ctx)? {
        parent.field.push(FieldDescriptorProto {
            name: Some(field.name().to_string()),
            number: Some(number),
            r#type: Some(resolved.proto_type as i32),
            type_name: resolved.type_name,
            label: Some(Label::Optional as i32),
            ..Default::default()
        });
        return Ok(());
    }

    match data_type {
        // Struct -> nested message
        DataType::Struct(sub_fields) => {
            let nested_name = unique_nested_name(field.name(), used_names);
            let nested =
                generate_message(&nested_name, sub_fields.iter().map(|f| f.as_ref()), ctx)?;
            parent.nested_type.push(nested);
            parent.field.push(FieldDescriptorProto {
                name: Some(field.name().to_string()),
                number: Some(number),
                r#type: Some(Type::Message as i32),
                type_name: Some(nested_name),
                label: Some(Label::Optional as i32),
                ..Default::default()
            });
        }

        // List -> repeated
        DataType::List(inner) | DataType::LargeList(inner) => {
            let inner_dt = resolve_dictionary(inner.data_type());

            // Try leaf element type.
            if let Some(resolved) = resolve_leaf_type(inner_dt, field.name(), ctx)? {
                parent.field.push(FieldDescriptorProto {
                    name: Some(field.name().to_string()),
                    number: Some(number),
                    r#type: Some(resolved.proto_type as i32),
                    type_name: resolved.type_name,
                    label: Some(Label::Repeated as i32),
                    ..Default::default()
                });
            } else if let DataType::Struct(sub_fields) = inner_dt {
                let nested_name = unique_nested_name(field.name(), used_names);
                let nested =
                    generate_message(&nested_name, sub_fields.iter().map(|f| f.as_ref()), ctx)?;
                parent.nested_type.push(nested);
                parent.field.push(FieldDescriptorProto {
                    name: Some(field.name().to_string()),
                    number: Some(number),
                    r#type: Some(Type::Message as i32),
                    type_name: Some(nested_name),
                    label: Some(Label::Repeated as i32),
                    ..Default::default()
                });
            } else {
                return Err(GenerateError::UnsupportedType {
                    field_name: field.name().to_string(),
                    arrow_type: inner.data_type().clone(),
                });
            }
        }

        // Map<K,V> -> map<K,V> (synthetic MapEntry message)
        DataType::Map(entries_field, _sorted) => {
            let DataType::Struct(kv_fields) = entries_field.data_type() else {
                return Err(GenerateError::UnsupportedType {
                    field_name: field.name().to_string(),
                    arrow_type: field.data_type().clone(),
                });
            };

            let key_field = &kv_fields[0];
            let value_field = &kv_fields[1];

            let key_dt = resolve_dictionary(key_field.data_type());
            let key_type = scalar_to_proto_type(key_dt, field.name())?;

            let entry_name = unique_nested_name(&format!("{}_entry", field.name()), used_names);

            // Resolve value type.
            let value_dt = resolve_dictionary(value_field.data_type());
            let (value_type, value_type_name) = if let Some(resolved) =
                resolve_leaf_type(value_dt, field.name(), ctx)?
            {
                (resolved.proto_type, resolved.type_name)
            } else if let DataType::Struct(sub_fields) = value_dt {
                // Nested message for map value — placed inside the MapEntry.
                let value_msg_name =
                    unique_nested_name(&format!("{}_value", field.name()), used_names);
                let nested =
                    generate_message(&value_msg_name, sub_fields.iter().map(|f| f.as_ref()), ctx)?;
                parent.nested_type.push(nested);
                (Type::Message, Some(value_msg_name))
            } else {
                return Err(GenerateError::UnsupportedType {
                    field_name: field.name().to_string(),
                    arrow_type: value_field.data_type().clone(),
                });
            };

            let entry_message = DescriptorProto {
                name: Some(entry_name.clone()),
                field: vec![
                    FieldDescriptorProto {
                        name: Some("key".to_string()),
                        number: Some(1),
                        r#type: Some(key_type as i32),
                        label: Some(Label::Optional as i32),
                        ..Default::default()
                    },
                    FieldDescriptorProto {
                        name: Some("value".to_string()),
                        number: Some(2),
                        r#type: Some(value_type as i32),
                        type_name: value_type_name,
                        label: Some(Label::Optional as i32),
                        ..Default::default()
                    },
                ],
                options: Some(MessageOptions {
                    map_entry: Some(true),
                    ..Default::default()
                }),
                ..Default::default()
            };

            parent.nested_type.push(entry_message);
            parent.field.push(FieldDescriptorProto {
                name: Some(field.name().to_string()),
                number: Some(number),
                r#type: Some(Type::Message as i32),
                type_name: Some(entry_name),
                label: Some(Label::Repeated as i32),
                ..Default::default()
            });
        }

        _ => {
            return Err(GenerateError::UnsupportedType {
                field_name: field.name().to_string(),
                arrow_type: field.data_type().clone(),
            });
        }
    }

    Ok(())
}

fn resolve_dictionary(dt: &DataType) -> &DataType {
    match dt {
        DataType::Dictionary(_, value_type) => resolve_dictionary(value_type.as_ref()),
        other => other,
    }
}

fn is_scalar(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Boolean
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
    )
}

fn scalar_to_proto_type(dt: &DataType, field_name: &str) -> Result<Type, GenerateError> {
    match dt {
        DataType::Boolean => Ok(Type::Bool),
        DataType::Int32 => Ok(Type::Int32),
        DataType::Int64 => Ok(Type::Int64),
        DataType::UInt32 => Ok(Type::Uint32),
        DataType::UInt64 => Ok(Type::Uint64),
        DataType::Float32 => Ok(Type::Float),
        DataType::Float64 => Ok(Type::Double),
        DataType::Utf8 | DataType::LargeUtf8 => Ok(Type::String),
        DataType::Binary | DataType::LargeBinary => Ok(Type::Bytes),
        _ => Err(GenerateError::UnsupportedType {
            field_name: field_name.to_string(),
            arrow_type: dt.clone(),
        }),
    }
}

/// Convert a snake_case field name to PascalCase message name.
fn to_message_name(field_name: &str) -> String {
    field_name
        .split('_')
        .map(|part| {
            let mut chars = part.chars();
            match chars.next() {
                None => String::new(),
                Some(c) => {
                    let upper: String = c.to_uppercase().collect();
                    upper + chars.as_str()
                }
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{Field, Fields, Schema, TimeUnit};
    use std::sync::Arc;

    #[test]
    fn test_scalars() {
        let schema = Schema::new(vec![
            Field::new("a_bool", DataType::Boolean, false),
            Field::new("an_int32", DataType::Int32, false),
            Field::new("an_int64", DataType::Int64, false),
            Field::new("a_uint32", DataType::UInt32, false),
            Field::new("a_uint64", DataType::UInt64, false),
            Field::new("a_float", DataType::Float32, false),
            Field::new("a_double", DataType::Float64, false),
            Field::new("a_string", DataType::Utf8, false),
            Field::new("some_bytes", DataType::Binary, false),
        ]);

        let fd = generate_file_descriptor(&schema, "test", "Scalars").unwrap();
        assert_eq!(fd.package.as_deref(), Some("test"));
        assert_eq!(fd.syntax.as_deref(), Some("proto3"));
        assert!(fd.dependency.is_empty());

        let msg = &fd.message_type[0];
        assert_eq!(msg.name.as_deref(), Some("Scalars"));
        assert_eq!(msg.field.len(), 9);

        let expected: Vec<(&str, i32, Type)> = vec![
            ("a_bool", 1, Type::Bool),
            ("an_int32", 2, Type::Int32),
            ("an_int64", 3, Type::Int64),
            ("a_uint32", 4, Type::Uint32),
            ("a_uint64", 5, Type::Uint64),
            ("a_float", 6, Type::Float),
            ("a_double", 7, Type::Double),
            ("a_string", 8, Type::String),
            ("some_bytes", 9, Type::Bytes),
        ];

        for (field, (name, number, proto_type)) in msg.field.iter().zip(expected) {
            assert_eq!(field.name.as_deref(), Some(name));
            assert_eq!(field.number, Some(number));
            assert_eq!(field.r#type, Some(proto_type as i32));
        }
    }

    #[test]
    fn test_large_utf8_and_large_binary() {
        let schema = Schema::new(vec![
            Field::new("big_text", DataType::LargeUtf8, false),
            Field::new("big_blob", DataType::LargeBinary, false),
        ]);
        let fd = generate_file_descriptor(&schema, "test", "LargeTypes").unwrap();
        let msg = &fd.message_type[0];
        assert_eq!(msg.field[0].r#type, Some(Type::String as i32));
        assert_eq!(msg.field[1].r#type, Some(Type::Bytes as i32));
    }

    #[test]
    fn test_wellknown_timestamp() {
        let schema = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Microsecond, None),
            false,
        )]);
        let fd = generate_file_descriptor(&schema, "test", "WithTs").unwrap();
        assert!(fd
            .dependency
            .contains(&"google/protobuf/timestamp.proto".to_string()));
        assert!(!fd
            .dependency
            .contains(&"google/protobuf/duration.proto".to_string()));

        let field = &fd.message_type[0].field[0];
        assert_eq!(
            field.type_name.as_deref(),
            Some(".google.protobuf.Timestamp")
        );
        assert_eq!(field.r#type, Some(Type::Message as i32));
    }

    #[test]
    fn test_wellknown_duration() {
        let schema = Schema::new(vec![Field::new(
            "dur",
            DataType::Duration(TimeUnit::Nanosecond),
            false,
        )]);
        let fd = generate_file_descriptor(&schema, "test", "WithDur").unwrap();
        assert!(fd
            .dependency
            .contains(&"google/protobuf/duration.proto".to_string()));

        let field = &fd.message_type[0].field[0];
        assert_eq!(
            field.type_name.as_deref(),
            Some(".google.protobuf.Duration")
        );
    }

    #[test]
    fn test_no_imports_when_not_needed() {
        let schema = Schema::new(vec![Field::new("x", DataType::Int32, false)]);
        let fd = generate_file_descriptor(&schema, "test", "Simple").unwrap();
        assert!(fd.dependency.is_empty());
    }

    #[test]
    fn test_nested_struct() {
        let inner_fields = Fields::from(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Utf8, false),
        ]);
        let schema = Schema::new(vec![Field::new(
            "inner",
            DataType::Struct(inner_fields),
            true,
        )]);
        let fd = generate_file_descriptor(&schema, "test", "Outer").unwrap();
        let msg = &fd.message_type[0];

        assert_eq!(msg.nested_type.len(), 1);
        assert_eq!(msg.nested_type[0].name.as_deref(), Some("Inner"));
        assert_eq!(msg.nested_type[0].field.len(), 2);

        let field = &msg.field[0];
        assert_eq!(field.r#type, Some(Type::Message as i32));
        assert_eq!(field.type_name.as_deref(), Some("Inner"));
    }

    #[test]
    fn test_deeply_nested_struct() {
        let level2 = Fields::from(vec![Field::new("z", DataType::Int32, false)]);
        let level1 = Fields::from(vec![
            Field::new("y", DataType::Utf8, false),
            Field::new("deep", DataType::Struct(level2), false),
        ]);
        let schema = Schema::new(vec![Field::new("outer", DataType::Struct(level1), false)]);
        let fd = generate_file_descriptor(&schema, "test", "Deep").unwrap();
        let msg = &fd.message_type[0];

        // Top-level has one nested type: "Outer"
        assert_eq!(msg.nested_type.len(), 1);
        let outer_nested = &msg.nested_type[0];
        assert_eq!(outer_nested.name.as_deref(), Some("Outer"));
        // "Outer" has one nested type: "Deep"
        assert_eq!(outer_nested.nested_type.len(), 1);
        assert_eq!(outer_nested.nested_type[0].name.as_deref(), Some("Deep"));
        assert_eq!(outer_nested.nested_type[0].field.len(), 1);
    }

    #[test]
    fn test_repeated_scalar() {
        let schema = Schema::new(vec![Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
            false,
        )]);
        let fd = generate_file_descriptor(&schema, "test", "WithList").unwrap();
        let field = &fd.message_type[0].field[0];
        assert_eq!(field.name.as_deref(), Some("tags"));
        assert_eq!(field.r#type, Some(Type::String as i32));
        assert_eq!(field.label, Some(Label::Repeated as i32));
    }

    #[test]
    fn test_repeated_large_list() {
        let schema = Schema::new(vec![Field::new(
            "ids",
            DataType::LargeList(Arc::new(Field::new("item", DataType::Int64, false))),
            false,
        )]);
        let fd = generate_file_descriptor(&schema, "test", "WithLargeList").unwrap();
        let field = &fd.message_type[0].field[0];
        assert_eq!(field.name.as_deref(), Some("ids"));
        assert_eq!(field.r#type, Some(Type::Int64 as i32));
        assert_eq!(field.label, Some(Label::Repeated as i32));
    }

    #[test]
    fn test_repeated_message() {
        let inner = Fields::from(vec![Field::new("v", DataType::Int32, false)]);
        let schema = Schema::new(vec![Field::new(
            "items",
            DataType::List(Arc::new(Field::new("item", DataType::Struct(inner), false))),
            false,
        )]);
        let fd = generate_file_descriptor(&schema, "test", "WithRepeatedMsg").unwrap();
        let msg = &fd.message_type[0];

        assert_eq!(msg.nested_type.len(), 1);
        assert_eq!(msg.nested_type[0].name.as_deref(), Some("Items"));

        let field = &msg.field[0];
        assert_eq!(field.label, Some(Label::Repeated as i32));
        assert_eq!(field.r#type, Some(Type::Message as i32));
        assert_eq!(field.type_name.as_deref(), Some("Items"));
    }

    #[test]
    fn test_map_string_int32() {
        let entries = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int32, false),
            ])),
            false,
        );
        let schema = Schema::new(vec![Field::new(
            "labels",
            DataType::Map(Arc::new(entries), false),
            false,
        )]);
        let fd = generate_file_descriptor(&schema, "test", "WithMap").unwrap();
        let msg = &fd.message_type[0];

        assert_eq!(msg.nested_type.len(), 1);
        let entry = &msg.nested_type[0];
        assert_eq!(entry.name.as_deref(), Some("LabelsEntry"));
        assert_eq!(entry.options.as_ref().unwrap().map_entry, Some(true));
        assert_eq!(entry.field.len(), 2);
        assert_eq!(entry.field[0].name.as_deref(), Some("key"));
        assert_eq!(entry.field[0].r#type, Some(Type::String as i32));
        assert_eq!(entry.field[1].name.as_deref(), Some("value"));
        assert_eq!(entry.field[1].r#type, Some(Type::Int32 as i32));

        let field = &msg.field[0];
        assert_eq!(field.label, Some(Label::Repeated as i32));
        assert_eq!(field.type_name.as_deref(), Some("LabelsEntry"));
    }

    #[test]
    fn test_map_with_struct_value() {
        let value_fields = Fields::from(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Utf8, false),
        ]);
        let entries = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Struct(value_fields), false),
            ])),
            false,
        );
        let schema = Schema::new(vec![Field::new(
            "objects",
            DataType::Map(Arc::new(entries), false),
            false,
        )]);
        let fd = generate_file_descriptor(&schema, "test", "WithMapStruct").unwrap();
        let msg = &fd.message_type[0];

        // Should have both the value message and the MapEntry nested types.
        assert_eq!(msg.nested_type.len(), 2);
        let value_msg = msg
            .nested_type
            .iter()
            .find(|m| m.name.as_deref() == Some("ObjectsValue"))
            .expect("should have ObjectsValue nested type");
        assert_eq!(value_msg.field.len(), 2);

        let entry_msg = msg
            .nested_type
            .iter()
            .find(|m| m.name.as_deref() == Some("ObjectsEntry"))
            .expect("should have ObjectsEntry nested type");
        assert_eq!(entry_msg.options.as_ref().unwrap().map_entry, Some(true));
    }

    #[test]
    fn test_dictionary_resolved() {
        let schema = Schema::new(vec![Field::new(
            "status",
            DataType::Dictionary(Box::new(DataType::Int8), Box::new(DataType::Utf8)),
            false,
        )]);
        let fd = generate_file_descriptor(&schema, "test", "WithDict").unwrap();
        let field = &fd.message_type[0].field[0];
        assert_eq!(field.r#type, Some(Type::String as i32));
    }

    #[test]
    fn test_unsupported_type_error() {
        let schema = Schema::new(vec![Field::new("bad", DataType::Decimal128(10, 2), false)]);
        let err = generate_file_descriptor(&schema, "test", "Bad").unwrap_err();
        assert!(matches!(err, GenerateError::UnsupportedType { .. }));
    }

    #[test]
    fn test_duplicate_nested_names_disambiguated() {
        // Two struct fields that produce the same PascalCase name.
        let inner1 = Fields::from(vec![Field::new("a", DataType::Int32, false)]);
        let inner2 = Fields::from(vec![Field::new("b", DataType::Utf8, false)]);
        let schema = Schema::new(vec![
            Field::new("foo_bar", DataType::Struct(inner1), false),
            Field::new("foo__bar", DataType::Struct(inner2), false),
        ]);
        let fd = generate_file_descriptor(&schema, "test", "Dup").unwrap();
        let msg = &fd.message_type[0];

        assert_eq!(msg.nested_type.len(), 2);
        let names: Vec<_> = msg
            .nested_type
            .iter()
            .map(|m| m.name.as_deref().unwrap())
            .collect();
        // Second one should be disambiguated.
        assert_eq!(names[0], "FooBar");
        assert_eq!(names[1], "FooBar2");
    }

    #[test]
    fn test_reserved_field_numbers_skipped() {
        // Verify that assign_field_number skips 19000–19999.
        assert_eq!(assign_field_number(18998, "M").unwrap(), 18999);
        // Index 18999 would be field 19000, which is reserved → should jump.
        let n = assign_field_number(18999, "M").unwrap();
        assert_eq!(n, 20000);
        let n = assign_field_number(19000, "M").unwrap();
        assert_eq!(n, 20001);
    }

    #[test]
    fn test_to_message_name() {
        assert_eq!(to_message_name("foo_bar"), "FooBar");
        assert_eq!(to_message_name("hello"), "Hello");
        assert_eq!(to_message_name("a_b_c"), "ABC");
    }

    /// Serialize a FileDescriptorProto into FileDescriptorSet bytes suitable
    /// for loading into ProtoSchema.
    fn to_fds_bytes(fd: &FileDescriptorProto) -> Vec<u8> {
        use prost::Message;
        let fds = prost_types::FileDescriptorSet {
            file: vec![fd.clone()],
        };
        fds.encode_to_vec()
    }

    #[test]
    fn roundtrip_scalars() {
        use crate::descriptor::ProtoSchema;
        use crate::mapping::{infer_mapping, InferOptions};

        let schema = Schema::new(vec![
            Field::new("a_bool", DataType::Boolean, false),
            Field::new("an_int32", DataType::Int32, false),
            Field::new("an_int64", DataType::Int64, false),
            Field::new("a_uint32", DataType::UInt32, false),
            Field::new("a_uint64", DataType::UInt64, false),
            Field::new("a_float", DataType::Float32, false),
            Field::new("a_double", DataType::Float64, false),
            Field::new("a_string", DataType::Utf8, false),
            Field::new("some_bytes", DataType::Binary, false),
        ]);

        let fd = generate_file_descriptor(&schema, "test", "Scalars").unwrap();
        let bytes = to_fds_bytes(&fd);

        let proto_schema = ProtoSchema::from_bytes(&bytes).unwrap();
        let msg = proto_schema.message("test.Scalars").unwrap();

        let options = InferOptions::default();
        let mapping = infer_mapping(&schema, &msg, &options).unwrap();

        assert_eq!(mapping.bindings.len(), 9);
        assert!(mapping.unmapped_arrow.is_empty());
        assert!(mapping.unmapped_proto.is_empty());
    }

    #[test]
    fn roundtrip_nested_struct() {
        use crate::descriptor::ProtoSchema;
        use crate::mapping::{infer_mapping, InferOptions};

        let inner_fields = Fields::from(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Utf8, false),
        ]);
        let schema = Schema::new(vec![
            Field::new("name", DataType::Utf8, false),
            Field::new("inner", DataType::Struct(inner_fields), true),
        ]);

        let fd = generate_file_descriptor(&schema, "test", "Outer").unwrap();
        let bytes = to_fds_bytes(&fd);

        let proto_schema = ProtoSchema::from_bytes(&bytes).unwrap();
        let msg = proto_schema.message("test.Outer").unwrap();

        let mapping = infer_mapping(&schema, &msg, &InferOptions::default()).unwrap();
        assert_eq!(mapping.bindings.len(), 2);
        assert!(mapping.unmapped_arrow.is_empty());
        assert!(mapping.unmapped_proto.is_empty());
    }

    #[test]
    fn roundtrip_wellknown_types() {
        use crate::mapping::{infer_mapping, InferOptions};
        use prost_reflect::DescriptorPool;

        let schema = Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, None),
                false,
            ),
            Field::new("dur", DataType::Duration(TimeUnit::Nanosecond), false),
        ]);

        let fd = generate_file_descriptor(&schema, "test", "WithWellKnown").unwrap();

        // Use global pool which already has google well-known types,
        // then add our generated descriptor on top.
        let mut pool = DescriptorPool::global();
        pool.add_file_descriptor_proto(fd).unwrap();
        let msg = pool.get_message_by_name("test.WithWellKnown").unwrap();

        let mapping = infer_mapping(&schema, &msg, &InferOptions::default()).unwrap();
        assert_eq!(mapping.bindings.len(), 2);
        assert!(mapping.unmapped_arrow.is_empty());
        assert!(mapping.unmapped_proto.is_empty());
    }

    #[test]
    fn roundtrip_repeated_and_map() {
        use crate::descriptor::ProtoSchema;
        use crate::mapping::{infer_mapping, InferOptions};

        let entries = Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::Int64, false),
            ])),
            false,
        );
        let schema = Schema::new(vec![
            Field::new(
                "tags",
                DataType::List(Arc::new(Field::new("item", DataType::Utf8, false))),
                false,
            ),
            Field::new("counts", DataType::Map(Arc::new(entries), false), false),
        ]);

        let fd = generate_file_descriptor(&schema, "test", "Complex").unwrap();
        let bytes = to_fds_bytes(&fd);

        let proto_schema = ProtoSchema::from_bytes(&bytes).unwrap();
        let msg = proto_schema.message("test.Complex").unwrap();

        let mapping = infer_mapping(&schema, &msg, &InferOptions::default()).unwrap();
        assert_eq!(mapping.bindings.len(), 2);
        assert!(mapping.unmapped_arrow.is_empty());
        assert!(mapping.unmapped_proto.is_empty());
    }
}
