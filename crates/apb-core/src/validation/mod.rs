mod render;
mod report;
#[cfg(test)]
mod tests;

pub use report::*;

use std::collections::HashSet;

use arrow_schema::{DataType, Fields, Schema};
use prost_reflect::{Cardinality, FieldDescriptor, Kind, MessageDescriptor};

use crate::mapping::{infer_mapping, InferOptions};
use crate::types::{check_compatibility, TypeCompatibility};

/// Validate an Arrow schema against a proto message and produce a report.
///
/// This never panics or returns `Err` — all problems are captured in the
/// report. It walks the full schema and collects all diagnostics instead of
/// short-circuiting on the first error.
pub fn validate(
    arrow_schema: &Schema,
    message: &MessageDescriptor,
    options: &InferOptions,
) -> MappingReport {
    // Try the normal mapping path first.
    let permissive = InferOptions {
        allow_unmapped_proto: true,
        allow_unmapped_arrow: true,
    };

    match infer_mapping(arrow_schema, message, &permissive) {
        Ok(mapping) => {
            let mut report = report_from_mapping(&mapping);

            // If the caller's options are stricter, check them and adjust status.
            if !options.allow_unmapped_proto && !report.unmapped_proto.is_empty() {
                report.status = ReportStatus::Error;
                report.structural_errors.push(StructuralError {
                    path: report.message_name.clone(),
                    message: format!(
                        "unmapped proto fields not allowed: {}",
                        report
                            .unmapped_proto
                            .iter()
                            .map(|f| f.name.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                });
            }
            if !options.allow_unmapped_arrow && !report.unmapped_arrow.is_empty() {
                report.status = ReportStatus::Error;
                report.structural_errors.push(StructuralError {
                    path: report.message_name.clone(),
                    message: format!(
                        "unmapped Arrow fields not allowed: {}",
                        report
                            .unmapped_arrow
                            .iter()
                            .map(|f| f.name.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ),
                });
            }

            report
        }
        Err(_err) => {
            // Mapping failed — build a report by walking the schema and
            // collecting all diagnostics.
            collect_all_diagnostics(arrow_schema.fields(), message)
        }
    }
}

/// Walk the schema exhaustively, collecting all errors instead of
/// short-circuiting. Used when `infer_mapping` fails on the first error.
fn collect_all_diagnostics(
    arrow_fields: &Fields,
    message: &MessageDescriptor,
) -> MappingReport {
    let mut mapped = Vec::new();
    let mut type_errors = Vec::new();
    let mut structural_errors = Vec::new();
    let mut nested_reports = Vec::new();
    let mut unmapped_proto = Vec::new();
    let mut bound_arrow_indices = HashSet::new();
    let mut processed_oneofs = HashSet::new();

    for proto_field in message.fields() {
        // Handle real oneofs.
        if let Some(oneof) = proto_field.containing_oneof() {
            if oneof.fields().len() > 1 {
                if processed_oneofs.insert(oneof.name().to_string()) {
                    validate_oneof(
                        arrow_fields,
                        &oneof,
                        &mut bound_arrow_indices,
                        &mut mapped,
                        &mut type_errors,
                        &mut structural_errors,
                        &mut unmapped_proto,
                    );
                }
                continue;
            }
        }

        validate_field(
            arrow_fields,
            &proto_field,
            &mut bound_arrow_indices,
            &mut mapped,
            &mut type_errors,
            &mut structural_errors,
            &mut nested_reports,
            &mut unmapped_proto,
        );
    }

    let unmapped_arrow: Vec<_> = arrow_fields
        .iter()
        .enumerate()
        .filter(|(i, _)| !bound_arrow_indices.contains(i))
        .map(|(_, f)| UnmappedField {
            name: f.name().to_string(),
            detail: format!("{}", f.data_type()),
        })
        .collect();

    let nested_has_errors = nested_reports
        .iter()
        .any(|n| n.report.status == ReportStatus::Error);
    let has_errors = !type_errors.is_empty() || !structural_errors.is_empty() || nested_has_errors;
    let has_warnings = !unmapped_arrow.is_empty() || !unmapped_proto.is_empty();

    MappingReport {
        message_name: message.full_name().to_string(),
        mapped,
        unmapped_arrow,
        unmapped_proto,
        type_errors,
        structural_errors,
        nested: nested_reports,
        status: if has_errors {
            ReportStatus::Error
        } else if has_warnings {
            ReportStatus::Warnings
        } else {
            ReportStatus::Ok
        },
    }
}

fn validate_field(
    arrow_fields: &Fields,
    proto_field: &FieldDescriptor,
    bound_indices: &mut HashSet<usize>,
    mapped: &mut Vec<MappedField>,
    type_errors: &mut Vec<FieldTypeError>,
    structural_errors: &mut Vec<StructuralError>,
    nested_reports: &mut Vec<NestedReport>,
    unmapped_proto: &mut Vec<UnmappedField>,
) {
    // Find Arrow field by name.
    let Some((arrow_index, arrow_field)) = arrow_fields
        .iter()
        .enumerate()
        .find(|(_, f)| f.name() == proto_field.name())
    else {
        unmapped_proto.push(UnmappedField {
            name: proto_field.name().to_string(),
            detail: format!("#{}", proto_field.number()),
        });
        return;
    };

    bound_indices.insert(arrow_index);

    // Check type compatibility.
    let compat = check_compatibility(arrow_field.data_type(), &proto_field.kind());
    match compat {
        TypeCompatibility::Compatible => {
            mapped.push(MappedField {
                arrow_name: arrow_field.name().to_string(),
                arrow_index,
                proto_name: proto_field.name().to_string(),
                proto_number: proto_field.number(),
                bind_method: "name-match".to_string(),
                type_mode: "direct".to_string(),
                field_shape: infer_shape_summary(proto_field),
            });

            // Recurse into nested messages.
            if let Kind::Message(msg_desc) = proto_field.kind() {
                if let DataType::Struct(sub_fields) = arrow_field.data_type() {
                    if proto_field.cardinality() != Cardinality::Repeated && !proto_field.is_map() {
                        let sub_report = collect_all_diagnostics(sub_fields, &msg_desc);
                        nested_reports.push(NestedReport {
                            proto_field: proto_field.name().to_string(),
                            report: Box::new(sub_report),
                        });
                    }
                }
            }
        }
        TypeCompatibility::CoercionAvailable { risk } => {
            type_errors.push(FieldTypeError {
                arrow_name: arrow_field.name().to_string(),
                arrow_type: format!("{}", arrow_field.data_type()),
                proto_name: proto_field.name().to_string(),
                proto_type: format!("{:?}", proto_field.kind()),
                reason: format!("coercion available ({risk}) but not enabled — add (apb).coerce = true"),
            });
        }
        TypeCompatibility::Incompatible { reason } => {
            // For nested messages, check if Arrow field is a Struct.
            if matches!(proto_field.kind(), Kind::Message(_))
                && proto_field.cardinality() != Cardinality::Repeated
                && !proto_field.is_map()
            {
                if let DataType::Struct(sub_fields) = arrow_field.data_type() {
                    if let Kind::Message(msg_desc) = proto_field.kind() {
                        // It's a nested message — recurse instead of reporting type error.
                        mapped.push(MappedField {
                            arrow_name: arrow_field.name().to_string(),
                            arrow_index,
                            proto_name: proto_field.name().to_string(),
                            proto_number: proto_field.number(),
                            bind_method: "name-match".to_string(),
                            type_mode: "direct".to_string(),
                            field_shape: FieldShapeSummary::Message,
                        });
                        let sub_report = collect_all_diagnostics(sub_fields, &msg_desc);
                        nested_reports.push(NestedReport {
                            proto_field: proto_field.name().to_string(),
                            report: Box::new(sub_report),
                        });
                        return;
                    }
                }
                // Arrow field is not a Struct but proto expects a message.
                structural_errors.push(StructuralError {
                    path: proto_field.name().to_string(),
                    message: format!(
                        "expected Struct for nested message, got {}",
                        arrow_field.data_type()
                    ),
                });
            } else {
                type_errors.push(FieldTypeError {
                    arrow_name: arrow_field.name().to_string(),
                    arrow_type: format!("{}", arrow_field.data_type()),
                    proto_name: proto_field.name().to_string(),
                    proto_type: format!("{:?}", proto_field.kind()),
                    reason,
                });
            }
        }
    }
}

fn validate_oneof(
    arrow_fields: &Fields,
    oneof: &prost_reflect::OneofDescriptor,
    bound_indices: &mut HashSet<usize>,
    mapped: &mut Vec<MappedField>,
    type_errors: &mut Vec<FieldTypeError>,
    structural_errors: &mut Vec<StructuralError>,
    unmapped_proto: &mut Vec<UnmappedField>,
) {
    let Some((arrow_index, arrow_field)) = arrow_fields
        .iter()
        .enumerate()
        .find(|(_, f)| f.name() == oneof.name())
    else {
        // No Arrow field for this oneof — all variants are unmapped.
        for variant in oneof.fields() {
            unmapped_proto.push(UnmappedField {
                name: variant.name().to_string(),
                detail: format!("#{} (oneof {})", variant.number(), oneof.name()),
            });
        }
        return;
    };

    bound_indices.insert(arrow_index);

    let struct_fields = match arrow_field.data_type() {
        DataType::Struct(fields) => fields,
        _ => {
            structural_errors.push(StructuralError {
                path: oneof.name().to_string(),
                message: format!(
                    "oneof '{}': expected Struct, got {}",
                    oneof.name(),
                    arrow_field.data_type()
                ),
            });
            return;
        }
    };

    for variant in oneof.fields() {
        if let Some((_child_index, child_field)) = struct_fields
            .iter()
            .enumerate()
            .find(|(_, f)| f.name() == variant.name())
        {
            let compat = check_compatibility(child_field.data_type(), &variant.kind());
            match compat {
                TypeCompatibility::Compatible => {
                    mapped.push(MappedField {
                        arrow_name: format!("{}.{}", oneof.name(), variant.name()),
                        arrow_index,
                        proto_name: variant.name().to_string(),
                        proto_number: variant.number(),
                        bind_method: "oneof".to_string(),
                        type_mode: "direct".to_string(),
                        field_shape: FieldShapeSummary::Oneof,
                    });
                }
                TypeCompatibility::CoercionAvailable { risk } => {
                    type_errors.push(FieldTypeError {
                        arrow_name: format!("{}.{}", oneof.name(), variant.name()),
                        arrow_type: format!("{}", child_field.data_type()),
                        proto_name: variant.name().to_string(),
                        proto_type: format!("{:?}", variant.kind()),
                        reason: format!("coercion available ({risk}) but not enabled"),
                    });
                }
                TypeCompatibility::Incompatible { reason } => {
                    type_errors.push(FieldTypeError {
                        arrow_name: format!("{}.{}", oneof.name(), variant.name()),
                        arrow_type: format!("{}", child_field.data_type()),
                        proto_name: variant.name().to_string(),
                        proto_type: format!("{:?}", variant.kind()),
                        reason,
                    });
                }
            }
        }
    }
}

fn infer_shape_summary(proto_field: &FieldDescriptor) -> FieldShapeSummary {
    if proto_field.is_map() {
        FieldShapeSummary::Map
    } else if proto_field.cardinality() == Cardinality::Repeated {
        FieldShapeSummary::Repeated
    } else if matches!(proto_field.kind(), Kind::Message(_)) {
        FieldShapeSummary::Message
    } else {
        FieldShapeSummary::Scalar
    }
}
