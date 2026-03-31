//! Encoding plan — precomputed field encoders built from a FieldMapping.

use arrow_schema::DataType;
use prost_reflect::Kind;

use crate::mapping::{FieldBinding, FieldMapping, FieldShape, OneofMapping};
use crate::types::TypeCheckMode;

use super::encode::{self, ScalarEncodeFn};
use super::wire;

/// A precomputed encoding plan for a message.
pub struct EncodingPlan {
    /// All encoders (fields + oneofs) in proto field number order.
    pub encoders: Vec<EncoderEntry>,
}

/// A single entry in the encoding plan — either a regular field or a oneof group.
pub enum EncoderEntry {
    Field(FieldEncoder),
    Oneof(OneofEncoder),
}

impl EncoderEntry {
    /// The lowest proto field number for sorting.
    fn sort_key(&self) -> u32 {
        match self {
            EncoderEntry::Field(f) => f.proto_number,
            EncoderEntry::Oneof(o) => o.min_field_number,
        }
    }
}

/// A single field encoder with precomputed tag.
pub struct FieldEncoder {
    /// Arrow column index.
    pub arrow_index: usize,
    /// Arrow field name (for error messages).
    pub arrow_name: String,
    /// Proto field name (for error messages).
    pub proto_name: String,
    /// Proto field number (for sorting).
    pub proto_number: u32,
    /// Pre-encoded tag bytes (field number + wire type).
    pub tag: Vec<u8>,
    /// The encoding kind.
    pub kind: FieldEncoderKind,
    /// Whether the column is nullable.
    pub nullable: bool,
}

/// The kind of encoder for a field.
pub enum FieldEncoderKind {
    /// Scalar field — function pointer.
    Scalar(ScalarEncodeFn),
    /// Nested message — sub-plan for the struct's children.
    Message(MessageEncoder),
    /// Repeated field (list).
    Repeated(RepeatedEncoder),
    /// Map field.
    Map(MapEncoder),
}

/// Encodes a StructArray as a nested proto message.
pub struct MessageEncoder {
    pub sub_plan: EncodingPlan,
}

/// Encodes a ListArray as a proto repeated field.
pub struct RepeatedEncoder {
    /// Encoder for each element.
    pub element_kind: Box<FieldEncoderKind>,
    /// Wire type of each element (for tag encoding in unpacked mode).
    pub element_wire_type: u8,
    /// Proto field number (for element tags in unpacked mode).
    pub field_number: u32,
    /// Whether to use packed encoding (numeric scalars only).
    pub packed: bool,
    /// Pre-encoded tag for packed encoding (length-delimited).
    pub packed_tag: Vec<u8>,
}

/// Encodes a MapArray as proto map<K,V> (repeated entry messages).
pub struct MapEncoder {
    /// Pre-encoded tag for key (field 1).
    pub key_tag: Vec<u8>,
    /// Pre-encoded tag for value (field 2).
    pub value_tag: Vec<u8>,
    /// Key encoder.
    pub key_kind: Box<FieldEncoderKind>,
    /// Value encoder.
    pub value_kind: Box<FieldEncoderKind>,
}

/// Encodes a StructArray (with nullable children) as a proto oneof.
pub struct OneofEncoder {
    /// Arrow column index of the StructArray wrapping the oneof.
    pub arrow_index: usize,
    /// Oneof name (for error messages).
    pub oneof_name: String,
    /// Lowest proto field number among variants (for sorting).
    pub min_field_number: u32,
    /// One entry per variant.
    pub variants: Vec<OneofVariantEncoder>,
}

/// A single variant within a oneof encoder.
pub struct OneofVariantEncoder {
    /// Child index within the Arrow StructArray.
    pub arrow_child_index: usize,
    /// Proto field name (for error messages).
    pub proto_name: String,
    /// Pre-encoded tag for this variant.
    pub tag: Vec<u8>,
    /// Encoder for this variant's value.
    pub kind: Box<FieldEncoderKind>,
}

/// Error building the encoding plan.
#[derive(Debug, thiserror::Error)]
pub enum PlanError {
    #[error("no encoder for arrow type {arrow_type} → proto {proto_type}")]
    NoEncoder {
        arrow_type: String,
        proto_type: String,
    },
}

impl EncodingPlan {
    /// Build an encoding plan from a field mapping.
    pub fn from_mapping(mapping: &FieldMapping) -> Result<Self, PlanError> {
        let mut entries = Vec::with_capacity(mapping.bindings.len() + mapping.oneofs.len());

        for binding in &mapping.bindings {
            entries.push(EncoderEntry::Field(build_field_encoder(binding)?));
        }

        for oneof in &mapping.oneofs {
            entries.push(EncoderEntry::Oneof(build_oneof_encoder(oneof)?));
        }

        // Sort all entries by proto field number for canonical output order.
        entries.sort_by_key(|e| e.sort_key());

        Ok(Self { encoders: entries })
    }
}

fn build_field_encoder(binding: &FieldBinding) -> Result<FieldEncoder, PlanError> {
    let (kind, tag) = build_encoder_kind(
        &binding.field_shape,
        &binding.type_check.arrow_type,
        &binding.type_check.proto_kind,
        &binding.type_check.mode,
        binding.proto_number,
    )?;

    Ok(FieldEncoder {
        arrow_index: binding.arrow_index,
        arrow_name: binding.arrow_name.clone(),
        proto_name: binding.proto_name.clone(),
        proto_number: binding.proto_number,
        tag,
        kind,
        nullable: true,
    })
}

fn build_encoder_kind(
    shape: &FieldShape,
    arrow_type: &DataType,
    proto_kind: &Kind,
    mode: &TypeCheckMode,
    proto_number: u32,
) -> Result<(FieldEncoderKind, Vec<u8>), PlanError> {
    match shape {
        FieldShape::Scalar => {
            let (encode_fn, wire_type) = select_scalar_encoder(arrow_type, proto_kind, mode)?;
            let tag = wire::encode_tag(proto_number, wire_type);
            Ok((FieldEncoderKind::Scalar(encode_fn), tag))
        }
        FieldShape::Message(sub_mapping) => {
            let sub_plan = EncodingPlan::from_mapping(sub_mapping)?;
            let tag = wire::encode_tag(proto_number, wire::WIRE_LENGTH_DELIMITED);
            Ok((FieldEncoderKind::Message(MessageEncoder { sub_plan }), tag))
        }
        FieldShape::Repeated {
            element_type_check,
            element_shape,
        } => {
            let (element_kind, _element_tag) = build_encoder_kind(
                element_shape,
                &element_type_check.arrow_type,
                &element_type_check.proto_kind,
                &element_type_check.mode,
                proto_number,
            )?;

            // Determine if packed encoding should be used.
            // Packed is used for numeric scalars (varint, fixed32, fixed64).
            let (packed, element_wire_type) = match &element_kind {
                FieldEncoderKind::Scalar(_) => {
                    let (_, wt) = select_scalar_encoder(
                        &element_type_check.arrow_type,
                        &element_type_check.proto_kind,
                        &element_type_check.mode,
                    )?;
                    let is_packed = wt != wire::WIRE_LENGTH_DELIMITED;
                    (is_packed, wt)
                }
                _ => (false, wire::WIRE_LENGTH_DELIMITED),
            };

            let packed_tag = wire::encode_tag(proto_number, wire::WIRE_LENGTH_DELIMITED);

            let tag = wire::encode_tag(proto_number, wire::WIRE_LENGTH_DELIMITED);
            Ok((
                FieldEncoderKind::Repeated(RepeatedEncoder {
                    element_kind: Box::new(element_kind),
                    element_wire_type,
                    field_number: proto_number,
                    packed,
                    packed_tag,
                }),
                tag,
            ))
        }
        FieldShape::Map {
            key_type_check,
            value_type_check,
            value_shape,
        } => {
            let (key_kind, _) = build_encoder_kind(
                &FieldShape::Scalar,
                &key_type_check.arrow_type,
                &key_type_check.proto_kind,
                &key_type_check.mode,
                1, // map entry key is always field 1
            )?;
            let (value_kind, _) = build_encoder_kind(
                value_shape,
                &value_type_check.arrow_type,
                &value_type_check.proto_kind,
                &value_type_check.mode,
                2, // map entry value is always field 2
            )?;

            let key_tag = wire::encode_tag(1, scalar_wire_type(&key_type_check.arrow_type, &key_type_check.proto_kind, &key_type_check.mode)?);
            let value_tag = match value_shape.as_ref() {
                FieldShape::Scalar => wire::encode_tag(2, scalar_wire_type(&value_type_check.arrow_type, &value_type_check.proto_kind, &value_type_check.mode)?),
                _ => wire::encode_tag(2, wire::WIRE_LENGTH_DELIMITED),
            };

            let tag = wire::encode_tag(proto_number, wire::WIRE_LENGTH_DELIMITED);
            Ok((
                FieldEncoderKind::Map(MapEncoder {
                    key_tag,
                    value_tag,
                    key_kind: Box::new(key_kind),
                    value_kind: Box::new(value_kind),
                }),
                tag,
            ))
        }
    }
}

fn build_oneof_encoder(oneof: &OneofMapping) -> Result<OneofEncoder, PlanError> {
    let mut variants = Vec::new();
    for variant in &oneof.variants {
        let (kind, tag) = build_encoder_kind(
            &variant.field_shape,
            &variant.type_check.arrow_type,
            &variant.type_check.proto_kind,
            &variant.type_check.mode,
            variant.proto_number,
        )?;
        variants.push(OneofVariantEncoder {
            arrow_child_index: variant.arrow_child_index,
            proto_name: variant.proto_name.clone(),
            tag,
            kind: Box::new(kind),
        });
    }

    let min_field_number = oneof
        .variants
        .iter()
        .map(|v| v.proto_number)
        .min()
        .unwrap_or(u32::MAX);

    Ok(OneofEncoder {
        arrow_index: oneof.arrow_index,
        oneof_name: oneof.oneof_name.clone(),
        min_field_number,
        variants,
    })
}

fn scalar_wire_type(
    arrow_type: &DataType,
    proto_kind: &Kind,
    mode: &TypeCheckMode,
) -> Result<u8, PlanError> {
    let (_, wt) = select_scalar_encoder(arrow_type, proto_kind, mode)?;
    Ok(wt)
}

/// Select the encoder function and wire type for a scalar field.
fn select_scalar_encoder(
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

        // Int32 → enum
        (Int32, Kind::Enum(_), TypeCheckMode::Direct) => {
            (encode::encode_int32_as_enum, wire::WIRE_VARINT)
        }

        // === Well-known types (Arrow scalar → proto message) ===
        // These are length-delimited because they encode as proto messages.
        (Timestamp(arrow_schema::TimeUnit::Second, _), Kind::Message(desc), TypeCheckMode::Direct)
            if desc.full_name() == "google.protobuf.Timestamp" =>
        {
            (encode::encode_timestamp_s, wire::WIRE_LENGTH_DELIMITED)
        }
        (Timestamp(arrow_schema::TimeUnit::Millisecond, _), Kind::Message(desc), TypeCheckMode::Direct)
            if desc.full_name() == "google.protobuf.Timestamp" =>
        {
            (encode::encode_timestamp_ms, wire::WIRE_LENGTH_DELIMITED)
        }
        (Timestamp(arrow_schema::TimeUnit::Microsecond, _), Kind::Message(desc), TypeCheckMode::Direct)
            if desc.full_name() == "google.protobuf.Timestamp" =>
        {
            (encode::encode_timestamp_us, wire::WIRE_LENGTH_DELIMITED)
        }
        (Timestamp(arrow_schema::TimeUnit::Nanosecond, _), Kind::Message(desc), TypeCheckMode::Direct)
            if desc.full_name() == "google.protobuf.Timestamp" =>
        {
            (encode::encode_timestamp_ns, wire::WIRE_LENGTH_DELIMITED)
        }
        (Duration(arrow_schema::TimeUnit::Second), Kind::Message(desc), TypeCheckMode::Direct)
            if desc.full_name() == "google.protobuf.Duration" =>
        {
            (encode::encode_duration_s, wire::WIRE_LENGTH_DELIMITED)
        }
        (Duration(arrow_schema::TimeUnit::Millisecond), Kind::Message(desc), TypeCheckMode::Direct)
            if desc.full_name() == "google.protobuf.Duration" =>
        {
            (encode::encode_duration_ms, wire::WIRE_LENGTH_DELIMITED)
        }
        (Duration(arrow_schema::TimeUnit::Microsecond), Kind::Message(desc), TypeCheckMode::Direct)
            if desc.full_name() == "google.protobuf.Duration" =>
        {
            (encode::encode_duration_us, wire::WIRE_LENGTH_DELIMITED)
        }
        (Duration(arrow_schema::TimeUnit::Nanosecond), Kind::Message(desc), TypeCheckMode::Direct)
            if desc.full_name() == "google.protobuf.Duration" =>
        {
            (encode::encode_duration_ns, wire::WIRE_LENGTH_DELIMITED)
        }

        // === Coercions ===
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
