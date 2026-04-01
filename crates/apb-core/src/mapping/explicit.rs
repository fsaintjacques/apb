use std::collections::HashSet;

use arrow_schema::Schema;
use prost_reflect::{Kind, MessageDescriptor};

use super::model::*;
use crate::types::{check_compatibility, resolve_type_check, TypeCheck, TypeCompatibility};

/// Reference to an Arrow field by name or index.
#[derive(Debug, Clone)]
pub enum ArrowFieldRef {
    Name(String),
    Index(usize),
}

/// Reference to a proto field by name or number.
#[derive(Debug, Clone)]
pub enum ProtoFieldRef {
    Name(String),
    Number(u32),
}

/// An explicit binding from an Arrow field to a proto field.
#[derive(Debug, Clone)]
pub struct ExplicitBinding {
    /// Arrow field reference.
    pub arrow_field: ArrowFieldRef,
    /// Proto field reference.
    pub proto_field: ProtoFieldRef,
    /// Allow coercion for this binding.
    pub coerce: bool,
}

/// Build a mapping from explicit bindings.
///
/// No inference is performed — only the specified bindings are created.
/// Arrow and proto fields not mentioned in bindings are reported as unmapped.
///
/// **Limitation:** Explicit mode only supports scalar fields and well-known
/// types (Timestamp, Duration). Composite fields (repeated, map, nested
/// messages) require infer mode.
pub fn explicit_mapping(
    arrow_schema: &Schema,
    message: &MessageDescriptor,
    bindings: &[ExplicitBinding],
) -> Result<FieldMapping, MappingError> {
    let mut field_bindings = Vec::new();
    let mut bound_arrow_indices = HashSet::new();
    let mut bound_proto_numbers = HashSet::new();

    for binding in bindings {
        // Resolve Arrow field.
        let (arrow_index, arrow_field) = match &binding.arrow_field {
            ArrowFieldRef::Name(name) => arrow_schema
                .fields()
                .iter()
                .enumerate()
                .find(|(_, f)| f.name() == name)
                .ok_or_else(|| MappingError::ArrowFieldNotFound {
                    reference: name.clone(),
                })?,
            ArrowFieldRef::Index(idx) => {
                let field = arrow_schema.fields().get(*idx).ok_or_else(|| {
                    MappingError::ArrowFieldNotFound {
                        reference: format!("index {idx}"),
                    }
                })?;
                (*idx, field)
            }
        };

        // Resolve proto field.
        let proto_field =
            match &binding.proto_field {
                ProtoFieldRef::Name(name) => message.get_field_by_name(name).ok_or_else(|| {
                    MappingError::ProtoFieldNotFound {
                        reference: name.clone(),
                    }
                })?,
                ProtoFieldRef::Number(num) => {
                    message
                        .get_field(*num)
                        .ok_or_else(|| MappingError::ProtoFieldNotFound {
                            reference: format!("field number {num}"),
                        })?
                }
            };

        // Check for duplicate Arrow field.
        if !bound_arrow_indices.insert(arrow_index) {
            return Err(MappingError::DuplicateBinding {
                arrow_field: arrow_field.name().to_string(),
                proto_fields: vec![proto_field.name().to_string()],
            });
        }
        // Check for duplicate proto field.
        if !bound_proto_numbers.insert(proto_field.number()) {
            return Err(MappingError::DuplicateProtoBinding {
                proto_field: proto_field.name().to_string(),
            });
        }

        // Resolve type.
        let (type_check, field_shape) =
            resolve_explicit_field(arrow_field.data_type(), &proto_field, binding.coerce)?;

        field_bindings.push(FieldBinding {
            arrow_index,
            arrow_name: arrow_field.name().to_string(),
            proto_number: proto_field.number(),
            proto_name: proto_field.name().to_string(),
            type_check,
            bind_method: BindMethod::Explicit,
            field_shape,
        });
    }

    // Sort by proto field number.
    field_bindings.sort_by_key(|b| b.proto_number);

    let unmapped_arrow: Vec<_> = arrow_schema
        .fields()
        .iter()
        .enumerate()
        .filter(|(i, _)| !bound_arrow_indices.contains(i))
        .map(|(i, f)| UnmappedArrowField {
            index: i,
            name: f.name().to_string(),
        })
        .collect();

    let unmapped_proto: Vec<_> = message
        .fields()
        .filter(|f| !bound_proto_numbers.contains(&f.number()))
        .map(|f| UnmappedProtoField {
            number: f.number(),
            name: f.name().to_string(),
        })
        .collect();

    Ok(FieldMapping {
        message_name: message.full_name().to_string(),
        bindings: field_bindings,
        oneofs: Vec::new(),
        unmapped_arrow,
        unmapped_proto,
    })
}

fn resolve_explicit_field(
    arrow_type: &arrow_schema::DataType,
    proto_field: &prost_reflect::FieldDescriptor,
    coerce: bool,
) -> Result<(TypeCheck, FieldShape), MappingError> {
    // For explicit mode, we only handle scalar fields for now.
    // Composite fields (repeated, map, nested) would need sub-bindings
    // which are not part of the explicit API yet.
    match proto_field.kind() {
        Kind::Message(_msg_desc) => {
            // Check if it's a well-known type.
            let compat = check_compatibility(arrow_type, &proto_field.kind());
            if matches!(
                compat,
                TypeCompatibility::Compatible | TypeCompatibility::CoercionAvailable { .. }
            ) {
                let tc = resolve_type_check(arrow_type, &proto_field.kind(), coerce)?;
                Ok((tc, FieldShape::Scalar))
            } else {
                // Nested message in explicit mode — not yet supported.
                Err(MappingError::UnsupportedExplicitNested {
                    proto_field: proto_field.name().to_string(),
                })
            }
        }
        _ => {
            let tc = resolve_type_check(arrow_type, &proto_field.kind(), coerce)?;
            Ok((tc, FieldShape::Scalar))
        }
    }
}
