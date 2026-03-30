use std::collections::HashSet;

use arrow_schema::{DataType, Fields, Schema};
use prost_reflect::{Cardinality, FieldDescriptor, Kind, MessageDescriptor};

use super::model::*;
use crate::types::{check_compatibility, resolve_type_check, TypeCheck, TypeCheckMode, TypeCompatibility};

/// Options for infer mode.
#[derive(Debug, Clone)]
pub struct InferOptions {
    /// If true, unmapped proto fields are allowed (default: true).
    pub allow_unmapped_proto: bool,
    /// If true, unmapped Arrow fields are allowed (default: true).
    pub allow_unmapped_arrow: bool,
}

impl Default for InferOptions {
    fn default() -> Self {
        Self {
            allow_unmapped_proto: true,
            allow_unmapped_arrow: true,
        }
    }
}

/// The fully qualified name of the apb extension.
const APB_EXTENSION_NAME: &str = "apb.apb";

/// Read apb annotations from a proto field descriptor.
/// Returns (arrow_name_override, coerce_allowed).
fn read_apb_annotations(field: &FieldDescriptor) -> (Option<String>, bool) {
    let options = field.options();

    // Find the apb extension descriptor in the pool.
    let pool = field.parent_pool();
    let Some(ext) = pool.get_extension_by_name(APB_EXTENSION_NAME) else {
        return (None, false);
    };

    if !options.has_extension(&ext) {
        return (None, false);
    }

    let apb_value = options.get_extension(&ext);
    let msg = match apb_value.as_ref() {
        prost_reflect::Value::Message(m) => m,
        _ => return (None, false),
    };

    let arrow_name = msg
        .get_field_by_name("arrow_name")
        .and_then(|v| match v.as_ref() {
            prost_reflect::Value::String(s) if !s.is_empty() => Some(s.clone()),
            _ => None,
        });

    let coerce = msg
        .get_field_by_name("coerce")
        .and_then(|v| match v.as_ref() {
            prost_reflect::Value::Bool(b) => Some(*b),
            _ => None,
        })
        .unwrap_or(false);

    (arrow_name, coerce)
}

/// Infer a mapping from an Arrow schema and a proto message descriptor.
pub fn infer_mapping(
    arrow_schema: &Schema,
    message: &MessageDescriptor,
    options: &InferOptions,
) -> Result<FieldMapping, MappingError> {
    let mapping = infer_from_fields(
        arrow_schema.fields(),
        message,
        options,
    )?;
    Ok(mapping)
}

fn infer_from_fields(
    arrow_fields: &Fields,
    message: &MessageDescriptor,
    options: &InferOptions,
) -> Result<FieldMapping, MappingError> {
    let mut bindings = Vec::new();
    let mut oneofs = Vec::new();
    let mut unmapped_proto = Vec::new();
    let mut bound_arrow_indices = HashSet::new();

    // Track which oneof groups we've already processed.
    let mut processed_oneofs = HashSet::new();

    for proto_field in message.fields() {
        // Skip fields that belong to a real oneof — handled separately.
        // Synthetic oneofs (proto3 optional) have exactly one field and should
        // be treated as regular fields. Note: this heuristic misclassifies
        // a real oneof with a single variant as synthetic, but this edge case
        // is rare and mostly harmless (the field is still mapped correctly,
        // just without oneof validation).
        if let Some(oneof) = proto_field.containing_oneof() {
            if oneof.fields().len() > 1 {
                if processed_oneofs.insert(oneof.name().to_string()) {
                    match infer_oneof(arrow_fields, &oneof, &mut bound_arrow_indices, options) {
                        Ok(Some(oneof_mapping)) => oneofs.push(oneof_mapping),
                        Ok(None) => {
                            for variant in oneof.fields() {
                                unmapped_proto.push(UnmappedProtoField {
                                    number: variant.number(),
                                    name: variant.name().to_string(),
                                });
                            }
                        }
                        Err(e) => return Err(e),
                    }
                }
                continue;
            }
        }

        match infer_field(arrow_fields, &proto_field, &mut bound_arrow_indices, options) {
            Ok(Some(binding)) => bindings.push(binding),
            Ok(None) => {
                unmapped_proto.push(UnmappedProtoField {
                    number: proto_field.number(),
                    name: proto_field.name().to_string(),
                });
            }
            Err(e) => return Err(e),
        }
    }

    let unmapped_arrow: Vec<_> = arrow_fields
        .iter()
        .enumerate()
        .filter(|(i, _)| !bound_arrow_indices.contains(i))
        .map(|(i, f)| UnmappedArrowField {
            index: i,
            name: f.name().to_string(),
        })
        .collect();

    if !options.allow_unmapped_proto && !unmapped_proto.is_empty() {
        return Err(MappingError::UnmappedProtoNotAllowed {
            fields: unmapped_proto,
        });
    }

    if !options.allow_unmapped_arrow && !unmapped_arrow.is_empty() {
        return Err(MappingError::UnmappedArrowNotAllowed {
            fields: unmapped_arrow,
        });
    }

    // Sort bindings by proto field number for deterministic output.
    bindings.sort_by_key(|b| b.proto_number);

    Ok(FieldMapping {
        message_name: message.full_name().to_string(),
        bindings,
        oneofs,
        unmapped_arrow,
        unmapped_proto,
    })
}

/// Try to infer a binding for a single proto field.
/// Returns Ok(None) if no matching Arrow field is found.
fn infer_field(
    arrow_fields: &Fields,
    proto_field: &FieldDescriptor,
    bound_indices: &mut HashSet<usize>,
    options: &InferOptions,
) -> Result<Option<FieldBinding>, MappingError> {
    let (annotation_name, coerce) = read_apb_annotations(proto_field);

    // Step 1: find the Arrow field — annotation takes priority over name-match.
    let (arrow_index, arrow_field, bind_method) =
        if let Some(ref name) = annotation_name {
            match arrow_fields.iter().enumerate().find(|(_, f)| f.name() == name) {
                Some((i, f)) => (i, f, BindMethod::Annotation),
                None => return Err(MappingError::ArrowFieldNotFound {
                    reference: name.clone(),
                }),
            }
        } else {
            match arrow_fields
                .iter()
                .enumerate()
                .find(|(_, f)| f.name() == proto_field.name())
            {
                Some((i, f)) => (i, f, BindMethod::NameMatch),
                None => return Ok(None),
            }
        };

    // Check for duplicate binding.
    if !bound_indices.insert(arrow_index) {
        return Err(MappingError::DuplicateBinding {
            arrow_field: arrow_field.name().to_string(),
            proto_fields: vec![proto_field.name().to_string()],
        });
    }

    // Step 2: resolve the field shape and type check.
    let (type_check, field_shape) =
        resolve_field_shape(arrow_field.data_type(), proto_field, coerce, options)?;

    Ok(Some(FieldBinding {
        arrow_index,
        arrow_name: arrow_field.name().to_string(),
        proto_number: proto_field.number(),
        proto_name: proto_field.name().to_string(),
        type_check,
        bind_method,
        field_shape,
    }))
}

/// Resolve the FieldShape and TypeCheck for a field, handling scalars,
/// repeated, map, and nested messages.
fn resolve_field_shape(
    arrow_type: &DataType,
    proto_field: &FieldDescriptor,
    coerce: bool,
    options: &InferOptions,
) -> Result<(TypeCheck, FieldShape), MappingError> {
    if proto_field.is_map() {
        return resolve_map(arrow_type, proto_field, coerce, options);
    }

    if proto_field.cardinality() == Cardinality::Repeated {
        return resolve_repeated(arrow_type, proto_field, coerce, options);
    }

    match proto_field.kind() {
        Kind::Message(msg_desc) => {
            // Check if this is a well-known type that maps directly.
            let compat = check_compatibility(arrow_type, &proto_field.kind());
            if matches!(compat, TypeCompatibility::Compatible | TypeCompatibility::CoercionAvailable { .. }) {
                let tc = resolve_type_check(arrow_type, &proto_field.kind(), coerce)?;
                return Ok((tc, FieldShape::Scalar));
            }

            // Otherwise, it's a nested message — Arrow side must be a Struct.
            resolve_nested_message(arrow_type, &msg_desc, proto_field.name(), options)
        }
        _ => {
            let tc = resolve_type_check(arrow_type, &proto_field.kind(), coerce)?;
            Ok((tc, FieldShape::Scalar))
        }
    }
}

fn resolve_nested_message(
    arrow_type: &DataType,
    msg_desc: &MessageDescriptor,
    proto_field_name: &str,
    options: &InferOptions,
) -> Result<(TypeCheck, FieldShape), MappingError> {
    let struct_fields = match arrow_type {
        DataType::Struct(fields) => fields,
        _ => {
            return Err(MappingError::TypeShapeMismatch {
                field_name: proto_field_name.to_string(),
                expected: "Struct".to_string(),
                actual: format!("{arrow_type}"),
            });
        }
    };

    let sub_mapping = infer_from_fields(struct_fields, msg_desc, options)
        .map_err(|e| MappingError::Nested {
            proto_field: proto_field_name.to_string(),
            source: Box::new(e),
        })?;

    // TypeCheck for the message field itself — the type is "message", always Direct.
    let tc = TypeCheck {
        arrow_type: arrow_type.clone(),
        proto_kind: Kind::Message(msg_desc.clone()),
        mode: TypeCheckMode::Direct,
    };

    Ok((tc, FieldShape::Message(Box::new(sub_mapping))))
}

fn resolve_repeated(
    arrow_type: &DataType,
    proto_field: &FieldDescriptor,
    coerce: bool,
    options: &InferOptions,
) -> Result<(TypeCheck, FieldShape), MappingError> {
    let element_type = match arrow_type {
        DataType::List(f) | DataType::LargeList(f) => f.data_type(),
        _ => {
            return Err(MappingError::TypeShapeMismatch {
                field_name: proto_field.name().to_string(),
                expected: "List or LargeList".to_string(),
                actual: format!("{arrow_type}"),
            });
        }
    };

    let (element_tc, element_shape) = match proto_field.kind() {
        Kind::Message(msg_desc) => {
            // Check well-known type first.
            let compat = check_compatibility(element_type, &proto_field.kind());
            if matches!(compat, TypeCompatibility::Compatible | TypeCompatibility::CoercionAvailable { .. }) {
                let tc = resolve_type_check(element_type, &proto_field.kind(), coerce)?;
                (tc, FieldShape::Scalar)
            } else {
                resolve_nested_message(element_type, &msg_desc, proto_field.name(), options)?
            }
        }
        _ => {
            let tc = resolve_type_check(element_type, &proto_field.kind(), coerce)?;
            (tc, FieldShape::Scalar)
        }
    };

    // The outer TypeCheck represents the list→repeated binding.
    let outer_tc = TypeCheck {
        arrow_type: arrow_type.clone(),
        proto_kind: proto_field.kind().clone(),
        mode: TypeCheckMode::Direct,
    };

    Ok((
        outer_tc,
        FieldShape::Repeated {
            element_type_check: element_tc,
            element_shape: Box::new(element_shape),
        },
    ))
}

fn resolve_map(
    arrow_type: &DataType,
    proto_field: &FieldDescriptor,
    coerce: bool,
    options: &InferOptions,
) -> Result<(TypeCheck, FieldShape), MappingError> {
    let (key_type, value_type) = match arrow_type {
        DataType::Map(entry_field, _) => match entry_field.data_type() {
            DataType::Struct(fields) if fields.len() == 2 => {
                (fields[0].data_type(), fields[1].data_type())
            }
            _ => {
                return Err(MappingError::TypeShapeMismatch {
                    field_name: proto_field.name().to_string(),
                    expected: "Map with Struct(key, value)".to_string(),
                    actual: format!("{arrow_type}"),
                });
            }
        },
        _ => {
            return Err(MappingError::TypeShapeMismatch {
                field_name: proto_field.name().to_string(),
                expected: "Map".to_string(),
                actual: format!("{arrow_type}"),
            });
        }
    };

    // Proto map<K,V> — the map entry message has key (field 1) and value (field 2).
    let map_entry = match proto_field.kind() {
        Kind::Message(desc) => desc,
        _ => unreachable!("map field should have Message kind"),
    };

    let key_field = map_entry.map_entry_key_field();
    let value_field = map_entry.map_entry_value_field();

    let key_tc = resolve_type_check(key_type, &key_field.kind(), coerce)?;

    let (value_tc, value_shape) = match value_field.kind() {
        Kind::Message(msg_desc) => {
            let compat = check_compatibility(value_type, &value_field.kind());
            if matches!(compat, TypeCompatibility::Compatible | TypeCompatibility::CoercionAvailable { .. }) {
                let tc = resolve_type_check(value_type, &value_field.kind(), coerce)?;
                (tc, FieldShape::Scalar)
            } else {
                resolve_nested_message(value_type, &msg_desc, proto_field.name(), options)?
            }
        }
        _ => {
            let tc = resolve_type_check(value_type, &value_field.kind(), coerce)?;
            (tc, FieldShape::Scalar)
        }
    };

    let outer_tc = TypeCheck {
        arrow_type: arrow_type.clone(),
        proto_kind: proto_field.kind().clone(),
        mode: TypeCheckMode::Direct,
    };

    Ok((
        outer_tc,
        FieldShape::Map {
            key_type_check: key_tc,
            value_type_check: value_tc,
            value_shape: Box::new(value_shape),
        },
    ))
}

fn infer_oneof(
    arrow_fields: &Fields,
    oneof: &prost_reflect::OneofDescriptor,
    bound_indices: &mut HashSet<usize>,
    options: &InferOptions,
) -> Result<Option<OneofMapping>, MappingError> {
    // Look for an Arrow field named after the oneof.
    let (arrow_index, arrow_field) = match arrow_fields
        .iter()
        .enumerate()
        .find(|(_, f)| f.name() == oneof.name())
    {
        Some((i, f)) => (i, f),
        None => return Ok(None),
    };

    if !bound_indices.insert(arrow_index) {
        return Err(MappingError::DuplicateBinding {
            arrow_field: arrow_field.name().to_string(),
            proto_fields: vec![oneof.name().to_string()],
        });
    }

    // The Arrow field must be a Struct.
    let struct_fields = match arrow_field.data_type() {
        DataType::Struct(fields) => fields,
        _ => {
            return Err(MappingError::OneofNotStruct {
                arrow_field: arrow_field.name().to_string(),
                oneof_name: oneof.name().to_string(),
            });
        }
    };

    let mut variants = Vec::new();
    for variant_field in oneof.fields() {
        // Find the child in the Arrow struct by name.
        if let Some((child_index, child_field)) = struct_fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name() == variant_field.name())
        {
            let (_, coerce) = read_apb_annotations(&variant_field);
            let (tc, shape) =
                resolve_field_shape(child_field.data_type(), &variant_field, coerce, options)?;
            variants.push(OneofVariant {
                arrow_child_index: child_index,
                proto_number: variant_field.number(),
                proto_name: variant_field.name().to_string(),
                type_check: tc,
                field_shape: Box::new(shape),
            });
        }
        // Unmatched oneof variant children are silently ignored — not all
        // variants need to be present.
    }

    if variants.is_empty() {
        return Ok(None);
    }

    Ok(Some(OneofMapping {
        oneof_name: oneof.name().to_string(),
        arrow_index,
        arrow_name: arrow_field.name().to_string(),
        variants,
    }))
}
