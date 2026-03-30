use crate::types::{TypeCheck, TypeError};

/// The fully resolved, validated binding between an Arrow schema and a proto
/// message descriptor. Immutable once built — handed to the transcoder.
#[derive(Debug, Clone)]
pub struct FieldMapping {
    /// Proto message fully qualified name.
    pub message_name: String,
    /// One binding per mapped regular field, in proto field number order.
    pub bindings: Vec<FieldBinding>,
    /// Oneof groups mapped from Arrow StructArray columns.
    pub oneofs: Vec<OneofMapping>,
    /// Arrow column indices that have no proto counterpart.
    pub unmapped_arrow: Vec<UnmappedArrowField>,
    /// Proto fields that have no Arrow counterpart.
    pub unmapped_proto: Vec<UnmappedProtoField>,
}

/// A single resolved Arrow column → proto field pair.
#[derive(Debug, Clone)]
pub struct FieldBinding {
    /// Arrow column index in the RecordBatch (or child index in a StructArray).
    pub arrow_index: usize,
    /// Arrow field name (for error messages).
    pub arrow_name: String,
    /// Proto field number (for wire encoding).
    pub proto_number: u32,
    /// Proto field name (for error messages).
    pub proto_name: String,
    /// How the types relate.
    pub type_check: TypeCheck,
    /// How this binding was resolved.
    pub bind_method: BindMethod,
    /// Shape of the field (scalar, repeated, map, nested).
    pub field_shape: FieldShape,
}

/// How a binding was resolved.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindMethod {
    /// Matched by proto annotation.
    Annotation,
    /// Matched by exact name.
    NameMatch,
    /// Provided explicitly by caller.
    Explicit,
}

/// The shape of a bound field.
#[derive(Debug, Clone)]
pub enum FieldShape {
    /// Scalar proto field.
    Scalar,
    /// `repeated` field.
    Repeated {
        element_type_check: TypeCheck,
        element_shape: Box<FieldShape>,
    },
    /// `map<K,V>` field.
    Map {
        key_type_check: TypeCheck,
        value_type_check: TypeCheck,
        value_shape: Box<FieldShape>,
    },
    /// Nested message. Contains the sub-mapping.
    Message(Box<FieldMapping>),
}

/// A proto oneof group mapped from an Arrow StructArray.
#[derive(Debug, Clone)]
pub struct OneofMapping {
    /// Proto oneof name.
    pub oneof_name: String,
    /// Arrow column index of the StructArray that wraps this oneof.
    pub arrow_index: usize,
    /// Arrow field name (for error messages).
    pub arrow_name: String,
    /// One entry per oneof variant.
    pub variants: Vec<OneofVariant>,
}

/// A single variant within a oneof.
#[derive(Debug, Clone)]
pub struct OneofVariant {
    /// Child index within the Arrow StructArray.
    pub arrow_child_index: usize,
    /// Proto field number of this variant.
    pub proto_number: u32,
    /// Proto field name of this variant.
    pub proto_name: String,
    /// Type check for this variant's value.
    pub type_check: TypeCheck,
    /// Shape of the variant (scalar or nested message).
    pub field_shape: Box<FieldShape>,
}

/// An Arrow field that has no proto counterpart.
#[derive(Debug, Clone)]
pub struct UnmappedArrowField {
    pub index: usize,
    pub name: String,
}

/// A proto field that has no Arrow counterpart.
#[derive(Debug, Clone)]
pub struct UnmappedProtoField {
    pub number: u32,
    pub name: String,
}

/// Errors from schema mapping.
#[derive(Debug, thiserror::Error)]
pub enum MappingError {
    #[error("arrow field not found: {reference}")]
    ArrowFieldNotFound { reference: String },

    #[error("proto field not found: {reference}")]
    ProtoFieldNotFound { reference: String },

    #[error("type error: {0}")]
    TypeError(#[from] TypeError),

    #[error("duplicate binding: arrow field '{arrow_field}' bound to multiple proto fields: {}", proto_fields.join(", "))]
    DuplicateBinding {
        arrow_field: String,
        proto_fields: Vec<String>,
    },

    #[error("oneof '{oneof_name}': arrow field '{arrow_field}' is not a Struct type")]
    OneofNotStruct {
        arrow_field: String,
        oneof_name: String,
    },

    #[error("in nested field '{proto_field}': {source}")]
    Nested {
        proto_field: String,
        source: Box<MappingError>,
    },

    #[error("unmapped proto fields not allowed: {}", fields.iter().map(|f| f.name.as_str()).collect::<Vec<_>>().join(", "))]
    UnmappedProtoNotAllowed { fields: Vec<UnmappedProtoField> },

    #[error("unmapped arrow fields not allowed: {}", fields.iter().map(|f| f.name.as_str()).collect::<Vec<_>>().join(", "))]
    UnmappedArrowNotAllowed { fields: Vec<UnmappedArrowField> },

    #[error("explicit mode does not support composite field binding for '{proto_field}' (repeated, map, and nested messages require infer mode)")]
    UnsupportedExplicitNested { proto_field: String },

    #[error("type shape mismatch for field '{field_name}': expected {expected}, got {actual}")]
    TypeShapeMismatch {
        field_name: String,
        expected: String,
        actual: String,
    },

    #[error("duplicate proto binding: proto field '{proto_field}' bound to multiple arrow fields")]
    DuplicateProtoBinding { proto_field: String },
}
