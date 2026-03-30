//! Encoding plan — precomputed field encoders built from a FieldMapping.

use arrow_schema::DataType;
use prost_reflect::Kind;

use crate::mapping::{FieldBinding, FieldMapping, FieldShape};
use crate::types::TypeCheckMode;

use super::encode::{self, ScalarEncodeFn};
use super::wire;

/// A precomputed encoding plan for a message.
pub struct EncodingPlan {
    pub field_encoders: Vec<FieldEncoder>,
}

/// A single field encoder with precomputed tag and encode function.
pub struct FieldEncoder {
    /// Arrow column index.
    pub arrow_index: usize,
    /// Arrow field name (for error messages).
    pub arrow_name: String,
    /// Proto field name (for error messages).
    pub proto_name: String,
    /// Pre-encoded tag bytes (field number + wire type).
    pub tag: Vec<u8>,
    /// The encoding function to use.
    pub encode_fn: ScalarEncodeFn,
    /// Whether the column is nullable (check null bitmap).
    pub nullable: bool,
}

/// Error building the encoding plan.
#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error("unsupported field shape for '{proto_field}': {shape} (nested types not yet supported)")]
    UnsupportedFieldShape { proto_field: String, shape: String },

    #[error("no encoder for arrow type {arrow_type} → proto {proto_type}")]
    NoEncoder {
        arrow_type: String,
        proto_type: String,
    },
}

impl EncodingPlan {
    /// Build an encoding plan from a field mapping.
    pub fn from_mapping(mapping: &FieldMapping) -> Result<Self, PlanError> {
        let mut encoders = Vec::with_capacity(mapping.bindings.len());

        for binding in &mapping.bindings {
            let encoder = build_field_encoder(binding)?;
            encoders.push(encoder);
        }

        Ok(Self {
            field_encoders: encoders,
        })
    }
}

fn build_field_encoder(binding: &FieldBinding) -> Result<FieldEncoder, PlanError> {
    match &binding.field_shape {
        FieldShape::Scalar => {}
        other => {
            return Err(PlanError::UnsupportedFieldShape {
                proto_field: binding.proto_name.clone(),
                shape: format!("{other:?}"),
            });
        }
    }

    let (encode_fn, wire_type) = select_encoder(
        &binding.type_check.arrow_type,
        &binding.type_check.proto_kind,
        &binding.type_check.mode,
    )?;

    let tag = wire::encode_tag(binding.proto_number, wire_type);

    Ok(FieldEncoder {
        arrow_index: binding.arrow_index,
        arrow_name: binding.arrow_name.clone(),
        proto_name: binding.proto_name.clone(),
        tag,
        encode_fn,
        nullable: true, // Conservative — check null bitmap for all fields.
    })
}

/// Select the encoder function and wire type for a scalar field.
fn select_encoder(
    arrow_type: &DataType,
    proto_kind: &Kind,
    mode: &TypeCheckMode,
) -> Result<(ScalarEncodeFn, u8), PlanError> {
    use DataType::*;

    let result: (ScalarEncodeFn, u8) = match (arrow_type, proto_kind, mode) {
        // === Direct lossless ===
        (Boolean, Kind::Bool, TypeCheckMode::Direct) => {
            (encode::encode_bool, wire::WIRE_VARINT)
        }

        (Int32, Kind::Int32, TypeCheckMode::Direct) => {
            (encode::encode_int32_varint, wire::WIRE_VARINT)
        }
        (Int32, Kind::Sint32, TypeCheckMode::Direct) => {
            (encode::encode_int32_zigzag, wire::WIRE_VARINT)
        }
        (Int32, Kind::Sfixed32, TypeCheckMode::Direct) => {
            (encode::encode_int32_fixed, wire::WIRE_FIXED32)
        }

        (Int64, Kind::Int64, TypeCheckMode::Direct) => {
            (encode::encode_int64_varint, wire::WIRE_VARINT)
        }
        (Int64, Kind::Sint64, TypeCheckMode::Direct) => {
            (encode::encode_int64_zigzag, wire::WIRE_VARINT)
        }
        (Int64, Kind::Sfixed64, TypeCheckMode::Direct) => {
            (encode::encode_int64_fixed, wire::WIRE_FIXED64)
        }

        (UInt32, Kind::Uint32, TypeCheckMode::Direct) => {
            (encode::encode_uint32_varint, wire::WIRE_VARINT)
        }
        (UInt32, Kind::Fixed32, TypeCheckMode::Direct) => {
            (encode::encode_uint32_fixed, wire::WIRE_FIXED32)
        }

        (UInt64, Kind::Uint64, TypeCheckMode::Direct) => {
            (encode::encode_uint64_varint, wire::WIRE_VARINT)
        }
        (UInt64, Kind::Fixed64, TypeCheckMode::Direct) => {
            (encode::encode_uint64_fixed, wire::WIRE_FIXED64)
        }

        (Float32, Kind::Float, TypeCheckMode::Direct) => {
            (encode::encode_float32, wire::WIRE_FIXED32)
        }
        (Float64, Kind::Double, TypeCheckMode::Direct) => {
            (encode::encode_float64, wire::WIRE_FIXED64)
        }

        (Utf8, Kind::String, TypeCheckMode::Direct) => {
            (encode::encode_utf8, wire::WIRE_LENGTH_DELIMITED)
        }
        (LargeUtf8, Kind::String, TypeCheckMode::Direct) => {
            (encode::encode_large_utf8, wire::WIRE_LENGTH_DELIMITED)
        }
        (Binary, Kind::Bytes, TypeCheckMode::Direct) => {
            (encode::encode_binary, wire::WIRE_LENGTH_DELIMITED)
        }
        (LargeBinary, Kind::Bytes, TypeCheckMode::Direct) => {
            (encode::encode_large_binary, wire::WIRE_LENGTH_DELIMITED)
        }

        // Int32 → enum (direct, runtime range check is deferred)
        (Int32, Kind::Enum(_), TypeCheckMode::Direct) => {
            (encode::encode_int32_as_enum, wire::WIRE_VARINT)
        }

        // === Coercions: integer narrowing/widening ===
        // Each proto encoding variant (varint, zigzag, fixed) needs its own arm.
        (Int64, Kind::Int32, TypeCheckMode::Coerce { .. }) => {
            (encode::encode_int64_as_int32_varint, wire::WIRE_VARINT)
        }
        (Int64, Kind::Sint32, TypeCheckMode::Coerce { .. }) => {
            (encode::encode_int64_as_sint32, wire::WIRE_VARINT)
        }
        (Int64, Kind::Sfixed32, TypeCheckMode::Coerce { .. }) => {
            (encode::encode_int64_as_sfixed32, wire::WIRE_FIXED32)
        }
        (Int32, Kind::Int64, TypeCheckMode::Coerce { .. }) => {
            (encode::encode_int32_as_int64_varint, wire::WIRE_VARINT)
        }
        (Int32, Kind::Sint64, TypeCheckMode::Coerce { .. }) => {
            (encode::encode_int32_as_sint64, wire::WIRE_VARINT)
        }
        (Int32, Kind::Sfixed64, TypeCheckMode::Coerce { .. }) => {
            (encode::encode_int32_as_sfixed64, wire::WIRE_FIXED64)
        }
        (UInt64, Kind::Uint32, TypeCheckMode::Coerce { .. }) => {
            (encode::encode_uint64_as_uint32_varint, wire::WIRE_VARINT)
        }
        (UInt64, Kind::Fixed32, TypeCheckMode::Coerce { .. }) => {
            (encode::encode_uint64_as_fixed32, wire::WIRE_FIXED32)
        }
        (UInt32, Kind::Uint64, TypeCheckMode::Coerce { .. }) => {
            (encode::encode_uint32_as_uint64_varint, wire::WIRE_VARINT)
        }
        (UInt32, Kind::Fixed64, TypeCheckMode::Coerce { .. }) => {
            (encode::encode_uint32_as_fixed64, wire::WIRE_FIXED64)
        }
        (Float64, Kind::Float, TypeCheckMode::Coerce { .. }) => {
            (encode::encode_float64_as_float32, wire::WIRE_FIXED32)
        }
        (Float32, Kind::Double, TypeCheckMode::Coerce { .. }) => {
            (encode::encode_float32_as_float64, wire::WIRE_FIXED64)
        }
        (Utf8 | LargeUtf8, Kind::Bytes, TypeCheckMode::Coerce { .. }) => {
            (encode::encode_utf8_as_bytes, wire::WIRE_LENGTH_DELIMITED)
        }
        (Binary | LargeBinary, Kind::String, TypeCheckMode::Coerce { .. }) => {
            (encode::encode_binary_as_string, wire::WIRE_LENGTH_DELIMITED)
        }
        (Int64, Kind::Enum(_), TypeCheckMode::Coerce { .. }) => {
            (encode::encode_int64_as_int32_varint, wire::WIRE_VARINT)
        }

        _ => {
            return Err(PlanError::NoEncoder {
                arrow_type: format!("{arrow_type}"),
                proto_type: format!("{proto_kind:?}"),
            });
        }
    };

    Ok(result)
}
