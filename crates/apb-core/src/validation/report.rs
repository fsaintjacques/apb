use serde::{Deserialize, Serialize};

use crate::mapping::{BindMethod, FieldMapping, FieldShape};
use crate::types::TypeCheckMode;

/// Overall status of a validation report.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReportStatus {
    /// All fields valid, no errors.
    Ok,
    /// Mapping succeeded but has warnings (unmapped fields).
    Warnings,
    /// Mapping has errors, transcoding will fail.
    Error,
}

/// A structured diagnostic report from a schema mapping attempt.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MappingReport {
    /// Proto message fully qualified name.
    pub message_name: String,
    /// Optional source name (table, query, file) for display.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_name: Option<String>,
    /// Successfully mapped fields.
    pub mapped: Vec<MappedField>,
    /// Arrow fields with no proto counterpart.
    pub unmapped_arrow: Vec<UnmappedArrowField>,
    /// Proto fields with no Arrow counterpart.
    pub unmapped_proto: Vec<UnmappedProtoField>,
    /// Type errors (incompatible or unapproved coercion).
    pub type_errors: Vec<FieldTypeError>,
    /// Structural errors (e.g. oneof target not a struct).
    pub structural_errors: Vec<StructuralError>,
    /// Reports for nested messages (recursive).
    pub nested: Vec<NestedReport>,
    /// Overall status.
    pub status: ReportStatus,
}

/// A successfully mapped field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MappedField {
    pub arrow_name: String,
    pub arrow_type: String,
    pub arrow_index: usize,
    pub proto_name: String,
    pub proto_number: u32,
    pub proto_type: String,
    pub bind_method: String,
    pub type_mode: String,
    pub field_shape: FieldShapeSummary,
}

/// Simplified shape for display (no recursive data).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldShapeSummary {
    Scalar,
    Repeated,
    Map,
    Message,
    Oneof,
}

/// An unmapped Arrow field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnmappedArrowField {
    pub name: String,
    pub arrow_type: String,
    pub index: usize,
}

/// An unmapped proto field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UnmappedProtoField {
    pub name: String,
    pub proto_type: String,
    pub number: u32,
    pub oneof_name: Option<String>,
}

/// A type error for a specific field.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FieldTypeError {
    pub arrow_name: String,
    pub arrow_type: String,
    pub proto_name: String,
    pub proto_number: u32,
    pub proto_type: String,
    pub reason: String,
}

/// A structural error (e.g. wrong Arrow type for a composite field).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StructuralError {
    pub path: String,
    pub message: String,
}

/// A nested report for a sub-message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NestedReport {
    pub proto_field: String,
    pub report: Box<MappingReport>,
}

impl MappingReport {
    /// Serialize the report to pretty-printed JSON.
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).expect("MappingReport should always serialize to JSON")
    }
}

/// Build a `MappingReport` from a successful `FieldMapping`.
pub(crate) fn report_from_mapping(mapping: &FieldMapping) -> MappingReport {
    let mut mapped = Vec::new();
    let mut nested_reports = Vec::new();

    for binding in &mapping.bindings {
        let (shape, sub_report) = summarize_shape(&binding.field_shape, &binding.proto_name);

        mapped.push(MappedField {
            arrow_name: binding.arrow_name.clone(),
            arrow_type: format_arrow_type(&binding.type_check.arrow_type),
            arrow_index: binding.arrow_index,
            proto_name: binding.proto_name.clone(),
            proto_number: binding.proto_number,
            proto_type: format_proto_kind(&binding.type_check.proto_kind, &shape),
            bind_method: format_bind_method(&binding.bind_method),
            type_mode: format_type_mode(&binding.type_check.mode),
            field_shape: shape,
        });

        if let Some(nr) = sub_report {
            nested_reports.push(nr);
        }
    }

    for oneof in &mapping.oneofs {
        for variant in &oneof.variants {
            let (shape, _) = summarize_shape(&variant.field_shape, &variant.proto_name);
            mapped.push(MappedField {
                arrow_name: format!("{}.{}", oneof.arrow_name, variant.proto_name),
                arrow_type: format_arrow_type(&variant.type_check.arrow_type),
                arrow_index: oneof.arrow_index,
                proto_name: variant.proto_name.clone(),
                proto_number: variant.proto_number,
                proto_type: format_proto_kind(&variant.type_check.proto_kind, &shape),
                bind_method: "oneof".to_string(),
                type_mode: format_type_mode(&variant.type_check.mode),
                field_shape: shape,
            });
        }
    }

    let unmapped_arrow: Vec<_> = mapping
        .unmapped_arrow
        .iter()
        .map(|f| UnmappedArrowField {
            name: f.name.clone(),
            arrow_type: String::new(), // not available from mapping alone
            index: f.index,
        })
        .collect();

    let unmapped_proto: Vec<_> = mapping
        .unmapped_proto
        .iter()
        .map(|f| UnmappedProtoField {
            name: f.name.clone(),
            proto_type: String::new(), // not available from mapping alone
            number: f.number,
            oneof_name: None,
        })
        .collect();

    let has_warnings = !unmapped_arrow.is_empty() || !unmapped_proto.is_empty();
    let nested_has_errors = nested_reports
        .iter()
        .any(|n| n.report.status == ReportStatus::Error);

    MappingReport {
        message_name: mapping.message_name.clone(),
        source_name: None,
        mapped,
        unmapped_arrow,
        unmapped_proto,
        type_errors: Vec::new(),
        structural_errors: Vec::new(),
        nested: nested_reports,
        status: if nested_has_errors {
            ReportStatus::Error
        } else if has_warnings {
            ReportStatus::Warnings
        } else {
            ReportStatus::Ok
        },
    }
}

pub(crate) fn format_arrow_type(dt: &arrow_schema::DataType) -> String {
    use arrow_schema::DataType::*;
    match dt {
        Boolean => "Boolean".into(),
        Int32 => "Int32".into(),
        Int64 => "Int64".into(),
        UInt32 => "UInt32".into(),
        UInt64 => "UInt64".into(),
        Float32 => "Float32".into(),
        Float64 => "Float64".into(),
        Utf8 | LargeUtf8 => "Utf8".into(),
        Binary | LargeBinary => "Binary".into(),
        Timestamp(u, _) => format!("Timestamp({u:?})"),
        Date32 => "Date32".into(),
        Date64 => "Date64".into(),
        Duration(u) => format!("Duration({u:?})"),
        Struct(_) => "Struct".into(),
        List(f) => format!("List<{}>", format_arrow_type(f.data_type())),
        LargeList(f) => format!("List<{}>", format_arrow_type(f.data_type())),
        Map(_, _) => "Map".into(),
        other => format!("{other}"),
    }
}

pub(crate) fn format_proto_kind(kind: &prost_reflect::Kind, shape: &FieldShapeSummary) -> String {
    use prost_reflect::Kind::*;
    let base = match kind {
        Bool => "bool".to_string(),
        Int32 => "int32".to_string(),
        Int64 => "int64".to_string(),
        Uint32 => "uint32".to_string(),
        Uint64 => "uint64".to_string(),
        Sint32 => "sint32".to_string(),
        Sint64 => "sint64".to_string(),
        Fixed32 => "fixed32".to_string(),
        Fixed64 => "fixed64".to_string(),
        Sfixed32 => "sfixed32".to_string(),
        Sfixed64 => "sfixed64".to_string(),
        Float => "float".to_string(),
        Double => "double".to_string(),
        String => "string".to_string(),
        Bytes => "bytes".to_string(),
        Message(desc) => desc.name().to_string(),
        Enum(desc) => format!("enum {}", desc.name()),
    };
    match shape {
        FieldShapeSummary::Repeated => format!("repeated {base}"),
        _ => base,
    }
}

fn summarize_shape(
    shape: &FieldShape,
    proto_name: &str,
) -> (FieldShapeSummary, Option<NestedReport>) {
    match shape {
        FieldShape::Scalar => (FieldShapeSummary::Scalar, None),
        FieldShape::Repeated { element_shape, .. } => {
            let sub = match element_shape.as_ref() {
                FieldShape::Message(sub_mapping) => Some(NestedReport {
                    proto_field: format!("{}[]", proto_name),
                    report: Box::new(report_from_mapping(sub_mapping)),
                }),
                _ => None,
            };
            (FieldShapeSummary::Repeated, sub)
        }
        FieldShape::Map { value_shape, .. } => {
            let sub = match value_shape.as_ref() {
                FieldShape::Message(sub_mapping) => Some(NestedReport {
                    proto_field: format!("{}[value]", proto_name),
                    report: Box::new(report_from_mapping(sub_mapping)),
                }),
                _ => None,
            };
            (FieldShapeSummary::Map, sub)
        }
        FieldShape::Message(sub_mapping) => {
            let sub = NestedReport {
                proto_field: proto_name.to_string(),
                report: Box::new(report_from_mapping(sub_mapping)),
            };
            (FieldShapeSummary::Message, Some(sub))
        }
    }
}

fn format_bind_method(method: &BindMethod) -> String {
    match method {
        BindMethod::Annotation => "annotation".to_string(),
        BindMethod::NameMatch => "name-match".to_string(),
        BindMethod::Explicit => "explicit".to_string(),
    }
}

fn format_type_mode(mode: &TypeCheckMode) -> String {
    match mode {
        TypeCheckMode::Direct => "direct".to_string(),
        TypeCheckMode::Coerce { risk } => format!("coerce ({})", risk),
    }
}
