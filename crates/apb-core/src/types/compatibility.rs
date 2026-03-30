use arrow_schema::DataType;
use prost_reflect::Kind;

use super::coercion::CoercionRisk;

/// Result of checking compatibility between an Arrow type and a proto field type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeCompatibility {
    /// Types match losslessly.
    Compatible,
    /// Coercion is available but must be opted in.
    CoercionAvailable { risk: CoercionRisk },
    /// No mapping exists.
    Incompatible { reason: String },
}

/// The resolved type check for a single field binding.
#[derive(Debug, Clone)]
pub struct TypeCheck {
    /// The Arrow DataType (resolved through Dictionary if needed).
    pub arrow_type: DataType,
    /// The proto Kind.
    pub proto_kind: Kind,
    /// How the types relate.
    pub mode: TypeCheckMode,
}

/// Whether a type binding is direct or requires coercion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeCheckMode {
    /// Use the lossless encoder.
    Direct,
    /// Use a coercion encoder.
    Coerce { risk: CoercionRisk },
}

/// Error when types are incompatible or coercion is not enabled.
#[derive(Debug, Clone, thiserror::Error)]
#[error("type error: arrow {arrow_type} ↔ proto {proto_type}: {reason}")]
pub struct TypeError {
    pub arrow_type: String,
    pub proto_type: String,
    pub reason: TypeErrorReason,
}

/// Why a type check failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TypeErrorReason {
    /// No mapping exists between these types.
    Incompatible,
    /// Coercion exists but was not opted in.
    CoercionNotEnabled { risk: CoercionRisk },
}

impl std::fmt::Display for TypeErrorReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Incompatible => write!(f, "incompatible types"),
            Self::CoercionNotEnabled { risk } => {
                write!(f, "coercion available ({risk}) but not enabled")
            }
        }
    }
}

/// Resolve a Dictionary type to its value type. Non-dictionary types pass through.
fn resolve_dictionary(dt: &DataType) -> &DataType {
    match dt {
        DataType::Dictionary(_, value_type) => resolve_dictionary(value_type.as_ref()),
        other => other,
    }
}

fn kind_name(kind: &Kind) -> &'static str {
    match kind {
        Kind::Bool => "bool",
        Kind::Int32 => "int32",
        Kind::Int64 => "int64",
        Kind::Uint32 => "uint32",
        Kind::Uint64 => "uint64",
        Kind::Sint32 => "sint32",
        Kind::Sint64 => "sint64",
        Kind::Fixed32 => "fixed32",
        Kind::Fixed64 => "fixed64",
        Kind::Sfixed32 => "sfixed32",
        Kind::Sfixed64 => "sfixed64",
        Kind::Float => "float",
        Kind::Double => "double",
        Kind::String => "string",
        Kind::Bytes => "bytes",
        Kind::Message(_) => "message",
        Kind::Enum(_) => "enum",
    }
}

/// Check compatibility between an Arrow data type and a proto field type.
///
/// Dictionary types are resolved to their value type before checking.
/// Composite types (Struct, List, Map) are not handled here — they are
/// the responsibility of the mapping layer.
pub fn check_compatibility(arrow_type: &DataType, proto_kind: &Kind) -> TypeCompatibility {
    let resolved = resolve_dictionary(arrow_type);
    check_resolved(resolved, proto_kind)
}

fn check_resolved(arrow_type: &DataType, proto_kind: &Kind) -> TypeCompatibility {
    use DataType::*;
    use TypeCompatibility::*;

    match (arrow_type, proto_kind) {
        // === Lossless mappings ===
        (Boolean, Kind::Bool) => Compatible,

        (Int32, Kind::Int32) => Compatible,
        (Int32, Kind::Sint32) => Compatible,
        (Int32, Kind::Sfixed32) => Compatible,

        (Int64, Kind::Int64) => Compatible,
        (Int64, Kind::Sint64) => Compatible,
        (Int64, Kind::Sfixed64) => Compatible,

        (UInt32, Kind::Uint32) => Compatible,
        (UInt32, Kind::Fixed32) => Compatible,

        (UInt64, Kind::Uint64) => Compatible,
        (UInt64, Kind::Fixed64) => Compatible,

        (Float32, Kind::Float) => Compatible,
        (Float64, Kind::Double) => Compatible,

        (Utf8 | LargeUtf8, Kind::String) => Compatible,
        (Binary | LargeBinary, Kind::Bytes) => Compatible,

        // Int32 → enum: lossless (runtime range check inherent to enum encoding)
        (Int32, Kind::Enum(_)) => Compatible,

        // === Coercions: integer narrowing / widening ===
        (Int64, Kind::Int32 | Kind::Sint32 | Kind::Sfixed32) => {
            CoercionAvailable { risk: CoercionRisk::Truncation }
        }
        (Int32, Kind::Int64 | Kind::Sint64 | Kind::Sfixed64) => {
            CoercionAvailable { risk: CoercionRisk::Lossless }
        }
        (UInt64, Kind::Uint32 | Kind::Fixed32) => {
            CoercionAvailable { risk: CoercionRisk::Truncation }
        }
        (UInt32, Kind::Uint64 | Kind::Fixed64) => {
            CoercionAvailable { risk: CoercionRisk::Lossless }
        }

        // === Coercions: float narrowing ===
        (Float64, Kind::Float) => CoercionAvailable { risk: CoercionRisk::PrecisionLoss },

        // === Coercions: string/bytes crossover ===
        (Utf8 | LargeUtf8, Kind::Bytes) => CoercionAvailable { risk: CoercionRisk::Semantic },
        (Binary | LargeBinary, Kind::String) => {
            CoercionAvailable { risk: CoercionRisk::RuntimeCheck }
        }

        // === Coercions: enum ===
        (Int64, Kind::Enum(_)) => CoercionAvailable { risk: CoercionRisk::Truncation },
        (Utf8 | LargeUtf8, Kind::Enum(_)) => {
            CoercionAvailable { risk: CoercionRisk::RuntimeCheck }
        }

        // === Coercions: float widening ===
        (Float32, Kind::Double) => CoercionAvailable { risk: CoercionRisk::Lossless },

        // === Lossless: temporal → well-known proto types ===
        // Arrow Timestamp → google.protobuf.Timestamp: the transcoder converts
        // the Arrow value (in its TimeUnit) to seconds + nanos. Lossless.
        (Timestamp(_, _), Kind::Message(desc)) if desc.full_name() == "google.protobuf.Timestamp" => {
            Compatible
        }
        // Arrow Duration → google.protobuf.Duration: same seconds + nanos split.
        (Duration(_), Kind::Message(desc)) if desc.full_name() == "google.protobuf.Duration" => {
            Compatible
        }

        // === Coercions: temporal → integer ===
        // Note: timezone information (if present) is silently discarded.
        // The raw epoch value is used as-is; the unit (seconds, millis, micros,
        // nanos) is not converted — the consumer must know the unit.
        (Timestamp(_, _), Kind::Int64 | Kind::Sint64 | Kind::Sfixed64) => {
            CoercionAvailable { risk: CoercionRisk::Semantic }
        }
        (Date32, Kind::Int32 | Kind::Sint32 | Kind::Sfixed32) => {
            CoercionAvailable { risk: CoercionRisk::Semantic }
        }
        (Date64, Kind::Int64 | Kind::Sint64 | Kind::Sfixed64) => {
            CoercionAvailable { risk: CoercionRisk::Semantic }
        }

        // === Everything else is incompatible ===
        _ => Incompatible {
            reason: format!(
                "no mapping from arrow {} to proto {}",
                arrow_type,
                kind_name(proto_kind),
            ),
        },
    }
}

/// Resolve the type check for a field binding.
///
/// Returns `Ok(TypeCheck)` if the types are compatible (directly or via
/// opt-in coercion). Returns `Err(TypeError)` if incompatible or if
/// coercion is needed but `coercion_allowed` is false.
pub fn resolve_type_check(
    arrow_type: &DataType,
    proto_kind: &Kind,
    coercion_allowed: bool,
) -> Result<TypeCheck, TypeError> {
    let resolved = resolve_dictionary(arrow_type);
    let compat = check_resolved(resolved, proto_kind);

    match compat {
        TypeCompatibility::Compatible => Ok(TypeCheck {
            arrow_type: resolved.clone(),
            proto_kind: proto_kind.clone(),
            mode: TypeCheckMode::Direct,
        }),
        TypeCompatibility::CoercionAvailable { risk } => {
            if coercion_allowed {
                Ok(TypeCheck {
                    arrow_type: resolved.clone(),
                    proto_kind: proto_kind.clone(),
                    mode: TypeCheckMode::Coerce { risk },
                })
            } else {
                Err(TypeError {
                    arrow_type: format!("{resolved}"),
                    proto_type: kind_name(proto_kind).to_string(),
                    reason: TypeErrorReason::CoercionNotEnabled { risk },
                })
            }
        }
        TypeCompatibility::Incompatible { .. } => Err(TypeError {
            arrow_type: format!("{resolved}"),
            proto_type: kind_name(proto_kind).to_string(),
            reason: TypeErrorReason::Incompatible,
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_schema::{DataType, TimeUnit};
    use prost_reflect::{DescriptorPool, EnumDescriptor};

    fn make_pool(bytes: &[u8]) -> DescriptorPool {
        let fds =
            <prost_types::FileDescriptorSet as prost::Message>::decode(bytes).unwrap();
        DescriptorPool::from_file_descriptor_set(fds).unwrap()
    }

    fn test_enum_descriptor() -> EnumDescriptor {
        let pool = make_pool(include_bytes!("../../fixtures/nested.bin"));
        pool.get_enum_by_name("fixtures.Status").unwrap()
    }

    fn enum_kind() -> Kind {
        Kind::Enum(test_enum_descriptor())
    }

    fn wellknown_pool() -> DescriptorPool {
        make_pool(include_bytes!("../../fixtures/wellknown.bin"))
    }

    fn timestamp_message_kind() -> Kind {
        let pool = wellknown_pool();
        Kind::Message(pool.get_message_by_name("google.protobuf.Timestamp").unwrap())
    }

    fn duration_message_kind() -> Kind {
        let pool = wellknown_pool();
        Kind::Message(pool.get_message_by_name("google.protobuf.Duration").unwrap())
    }

    // ==================== Lossless pairs ====================

    #[test]
    fn compatible_bool() {
        assert_eq!(
            check_compatibility(&DataType::Boolean, &Kind::Bool),
            TypeCompatibility::Compatible,
        );
    }

    #[test]
    fn compatible_int32_variants() {
        for kind in [Kind::Int32, Kind::Sint32, Kind::Sfixed32] {
            assert_eq!(
                check_compatibility(&DataType::Int32, &kind),
                TypeCompatibility::Compatible,
                "Int32 → {kind:?}",
            );
        }
    }

    #[test]
    fn compatible_int64_variants() {
        for kind in [Kind::Int64, Kind::Sint64, Kind::Sfixed64] {
            assert_eq!(
                check_compatibility(&DataType::Int64, &kind),
                TypeCompatibility::Compatible,
                "Int64 → {kind:?}",
            );
        }
    }

    #[test]
    fn compatible_uint32_variants() {
        for kind in [Kind::Uint32, Kind::Fixed32] {
            assert_eq!(
                check_compatibility(&DataType::UInt32, &kind),
                TypeCompatibility::Compatible,
                "UInt32 → {kind:?}",
            );
        }
    }

    #[test]
    fn compatible_uint64_variants() {
        for kind in [Kind::Uint64, Kind::Fixed64] {
            assert_eq!(
                check_compatibility(&DataType::UInt64, &kind),
                TypeCompatibility::Compatible,
                "UInt64 → {kind:?}",
            );
        }
    }

    #[test]
    fn compatible_float32() {
        assert_eq!(
            check_compatibility(&DataType::Float32, &Kind::Float),
            TypeCompatibility::Compatible,
        );
    }

    #[test]
    fn compatible_float64() {
        assert_eq!(
            check_compatibility(&DataType::Float64, &Kind::Double),
            TypeCompatibility::Compatible,
        );
    }

    #[test]
    fn compatible_utf8_string() {
        assert_eq!(
            check_compatibility(&DataType::Utf8, &Kind::String),
            TypeCompatibility::Compatible,
        );
        assert_eq!(
            check_compatibility(&DataType::LargeUtf8, &Kind::String),
            TypeCompatibility::Compatible,
        );
    }

    #[test]
    fn compatible_binary_bytes() {
        assert_eq!(
            check_compatibility(&DataType::Binary, &Kind::Bytes),
            TypeCompatibility::Compatible,
        );
        assert_eq!(
            check_compatibility(&DataType::LargeBinary, &Kind::Bytes),
            TypeCompatibility::Compatible,
        );
    }

    #[test]
    fn compatible_int32_enum() {
        assert_eq!(
            check_compatibility(&DataType::Int32, &enum_kind()),
            TypeCompatibility::Compatible,
        );
    }

    // ==================== Coercion pairs ====================

    #[test]
    fn coercion_int64_to_int32_truncation() {
        for kind in [Kind::Int32, Kind::Sint32, Kind::Sfixed32] {
            assert_eq!(
                check_compatibility(&DataType::Int64, &kind),
                TypeCompatibility::CoercionAvailable {
                    risk: CoercionRisk::Truncation
                },
                "Int64 → {kind:?}",
            );
        }
    }

    #[test]
    fn coercion_int32_to_int64_lossless() {
        for kind in [Kind::Int64, Kind::Sint64, Kind::Sfixed64] {
            assert_eq!(
                check_compatibility(&DataType::Int32, &kind),
                TypeCompatibility::CoercionAvailable {
                    risk: CoercionRisk::Lossless
                },
                "Int32 → {kind:?}",
            );
        }
    }

    #[test]
    fn coercion_uint64_to_uint32_truncation() {
        for kind in [Kind::Uint32, Kind::Fixed32] {
            assert_eq!(
                check_compatibility(&DataType::UInt64, &kind),
                TypeCompatibility::CoercionAvailable {
                    risk: CoercionRisk::Truncation
                },
                "UInt64 → {kind:?}",
            );
        }
    }

    #[test]
    fn coercion_uint32_to_uint64_lossless() {
        for kind in [Kind::Uint64, Kind::Fixed64] {
            assert_eq!(
                check_compatibility(&DataType::UInt32, &kind),
                TypeCompatibility::CoercionAvailable {
                    risk: CoercionRisk::Lossless
                },
                "UInt32 → {kind:?}",
            );
        }
    }

    #[test]
    fn coercion_float64_to_float() {
        assert_eq!(
            check_compatibility(&DataType::Float64, &Kind::Float),
            TypeCompatibility::CoercionAvailable {
                risk: CoercionRisk::PrecisionLoss
            },
        );
    }

    #[test]
    fn coercion_utf8_to_bytes_semantic() {
        assert_eq!(
            check_compatibility(&DataType::Utf8, &Kind::Bytes),
            TypeCompatibility::CoercionAvailable {
                risk: CoercionRisk::Semantic
            },
        );
        assert_eq!(
            check_compatibility(&DataType::LargeUtf8, &Kind::Bytes),
            TypeCompatibility::CoercionAvailable {
                risk: CoercionRisk::Semantic
            },
        );
    }

    #[test]
    fn coercion_binary_to_string_runtime() {
        assert_eq!(
            check_compatibility(&DataType::Binary, &Kind::String),
            TypeCompatibility::CoercionAvailable {
                risk: CoercionRisk::RuntimeCheck
            },
        );
        assert_eq!(
            check_compatibility(&DataType::LargeBinary, &Kind::String),
            TypeCompatibility::CoercionAvailable {
                risk: CoercionRisk::RuntimeCheck
            },
        );
    }

    #[test]
    fn coercion_int64_to_enum_truncation() {
        assert_eq!(
            check_compatibility(&DataType::Int64, &enum_kind()),
            TypeCompatibility::CoercionAvailable {
                risk: CoercionRisk::Truncation
            },
        );
    }

    #[test]
    fn coercion_utf8_to_enum_runtime() {
        assert_eq!(
            check_compatibility(&DataType::Utf8, &enum_kind()),
            TypeCompatibility::CoercionAvailable {
                risk: CoercionRisk::RuntimeCheck
            },
        );
        assert_eq!(
            check_compatibility(&DataType::LargeUtf8, &enum_kind()),
            TypeCompatibility::CoercionAvailable {
                risk: CoercionRisk::RuntimeCheck
            },
        );
    }

    #[test]
    fn coercion_float32_to_double_lossless() {
        assert_eq!(
            check_compatibility(&DataType::Float32, &Kind::Double),
            TypeCompatibility::CoercionAvailable {
                risk: CoercionRisk::Lossless
            },
        );
    }

    #[test]
    fn coercion_timestamp_to_signed64_semantic() {
        let ts = DataType::Timestamp(TimeUnit::Microsecond, None);
        for kind in [Kind::Int64, Kind::Sint64, Kind::Sfixed64] {
            assert_eq!(
                check_compatibility(&ts, &kind),
                TypeCompatibility::CoercionAvailable {
                    risk: CoercionRisk::Semantic
                },
                "Timestamp → {kind:?}",
            );
        }
    }

    #[test]
    fn coercion_timestamp_with_tz_to_int64_semantic() {
        let ts = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
        assert_eq!(
            check_compatibility(&ts, &Kind::Int64),
            TypeCompatibility::CoercionAvailable {
                risk: CoercionRisk::Semantic
            },
        );
    }

    #[test]
    fn coercion_date32_to_signed32_semantic() {
        for kind in [Kind::Int32, Kind::Sint32, Kind::Sfixed32] {
            assert_eq!(
                check_compatibility(&DataType::Date32, &kind),
                TypeCompatibility::CoercionAvailable {
                    risk: CoercionRisk::Semantic
                },
                "Date32 → {kind:?}",
            );
        }
    }

    #[test]
    fn coercion_date64_to_signed64_semantic() {
        for kind in [Kind::Int64, Kind::Sint64, Kind::Sfixed64] {
            assert_eq!(
                check_compatibility(&DataType::Date64, &kind),
                TypeCompatibility::CoercionAvailable {
                    risk: CoercionRisk::Semantic
                },
                "Date64 → {kind:?}",
            );
        }
    }

    // ==================== Well-known types ====================

    #[test]
    fn compatible_timestamp_to_wellknown_timestamp() {
        let kind = timestamp_message_kind();
        for unit in [TimeUnit::Second, TimeUnit::Millisecond, TimeUnit::Microsecond, TimeUnit::Nanosecond] {
            let dt = DataType::Timestamp(unit, None);
            assert_eq!(
                check_compatibility(&dt, &kind),
                TypeCompatibility::Compatible,
                "Timestamp({unit:?}) → google.protobuf.Timestamp",
            );
        }
    }

    #[test]
    fn compatible_timestamp_with_tz_to_wellknown_timestamp() {
        let kind = timestamp_message_kind();
        let dt = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
        assert_eq!(
            check_compatibility(&dt, &kind),
            TypeCompatibility::Compatible,
        );
    }

    #[test]
    fn compatible_duration_to_wellknown_duration() {
        let kind = duration_message_kind();
        for unit in [TimeUnit::Second, TimeUnit::Millisecond, TimeUnit::Microsecond, TimeUnit::Nanosecond] {
            let dt = DataType::Duration(unit);
            assert_eq!(
                check_compatibility(&dt, &kind),
                TypeCompatibility::Compatible,
                "Duration({unit:?}) → google.protobuf.Duration",
            );
        }
    }

    #[test]
    fn incompatible_duration_to_random_message() {
        // Duration should not match arbitrary messages, only google.protobuf.Duration.
        let pool = wellknown_pool();
        let msg = pool.get_message_by_name("fixtures.WithWellKnown").unwrap();
        let kind = Kind::Message(msg);
        assert!(matches!(
            check_compatibility(&DataType::Duration(TimeUnit::Microsecond), &kind),
            TypeCompatibility::Incompatible { .. },
        ));
    }

    #[test]
    fn incompatible_timestamp_to_random_message() {
        let pool = wellknown_pool();
        let msg = pool.get_message_by_name("fixtures.WithWellKnown").unwrap();
        let kind = Kind::Message(msg);
        let dt = DataType::Timestamp(TimeUnit::Microsecond, None);
        assert!(matches!(
            check_compatibility(&dt, &kind),
            TypeCompatibility::Incompatible { .. },
        ));
    }

    // ==================== Incompatible pairs ====================

    #[test]
    fn incompatible_unsupported_arrow_types() {
        let unsupported = [
            DataType::Decimal128(10, 2),
            DataType::Decimal256(10, 2),
            DataType::Null,
        ];
        for dt in unsupported {
            let result = check_compatibility(&dt, &Kind::Int64);
            assert!(
                matches!(result, TypeCompatibility::Incompatible { .. }),
                "{dt} should be incompatible",
            );
        }
    }

    #[test]
    fn incompatible_type_mismatch() {
        // A few spot checks for clearly wrong pairings.
        assert!(matches!(
            check_compatibility(&DataType::Boolean, &Kind::Int32),
            TypeCompatibility::Incompatible { .. },
        ));
        assert!(matches!(
            check_compatibility(&DataType::Float32, &Kind::String),
            TypeCompatibility::Incompatible { .. },
        ));
        assert!(matches!(
            check_compatibility(&DataType::Utf8, &Kind::Double),
            TypeCompatibility::Incompatible { .. },
        ));
    }

    // ==================== Dictionary resolution ====================

    #[test]
    fn dictionary_resolves_to_value_type() {
        let dict = DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::Utf8),
        );
        assert_eq!(
            check_compatibility(&dict, &Kind::String),
            TypeCompatibility::Compatible,
        );
    }

    #[test]
    fn dictionary_utf8_to_enum() {
        let dict = DataType::Dictionary(
            Box::new(DataType::Int32),
            Box::new(DataType::Utf8),
        );
        assert_eq!(
            check_compatibility(&dict, &enum_kind()),
            TypeCompatibility::CoercionAvailable {
                risk: CoercionRisk::RuntimeCheck
            },
        );
    }

    // ==================== resolve_type_check ====================

    #[test]
    fn resolve_direct() {
        let result = resolve_type_check(&DataType::Int32, &Kind::Int32, false).unwrap();
        assert_eq!(result.mode, TypeCheckMode::Direct);
        assert_eq!(result.arrow_type, DataType::Int32);
    }

    #[test]
    fn resolve_coercion_allowed() {
        let result = resolve_type_check(&DataType::Int64, &Kind::Int32, true).unwrap();
        assert_eq!(
            result.mode,
            TypeCheckMode::Coerce {
                risk: CoercionRisk::Truncation
            },
        );
    }

    #[test]
    fn resolve_coercion_not_allowed() {
        let err = resolve_type_check(&DataType::Int64, &Kind::Int32, false).unwrap_err();
        assert_eq!(
            err.reason,
            TypeErrorReason::CoercionNotEnabled {
                risk: CoercionRisk::Truncation,
            },
        );
        assert!(err.arrow_type.contains("Int64"));
        assert!(err.proto_type.contains("int32"));
    }

    #[test]
    fn resolve_incompatible() {
        let err = resolve_type_check(&DataType::Boolean, &Kind::String, true).unwrap_err();
        assert_eq!(err.reason, TypeErrorReason::Incompatible);
    }

    #[test]
    fn resolve_dictionary_passthrough() {
        let dict = DataType::Dictionary(
            Box::new(DataType::Int8),
            Box::new(DataType::Utf8),
        );
        let result = resolve_type_check(&dict, &Kind::String, false).unwrap();
        assert_eq!(result.mode, TypeCheckMode::Direct);
        // The resolved arrow_type should be Utf8, not Dictionary.
        assert_eq!(result.arrow_type, DataType::Utf8);
    }
}
