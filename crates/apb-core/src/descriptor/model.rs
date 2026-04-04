use prost_reflect::{DescriptorPool, MessageDescriptor};

/// Errors from parsing or querying proto descriptors.
#[derive(Debug, thiserror::Error)]
pub enum DescriptorError {
    #[error("failed to decode descriptor: {0}")]
    DecodeFailed(#[from] prost_reflect::DescriptorError),

    #[error("message not found: {0}")]
    MessageNotFound(String),
}

/// Top-level container parsed from a serialized `FileDescriptorSet`.
///
/// Wraps a `prost_reflect::DescriptorPool` and provides message lookup by
/// fully qualified name.
pub struct ProtoSchema {
    pool: DescriptorPool,
}

/// Embedded apb.proto extension descriptor, so custom field options are
/// resolvable when parsing user descriptors that don't include apb.proto.
const APB_EXTENSION_BYTES: &[u8] = include_bytes!("../../../../proto/apb/apb.bin");

impl ProtoSchema {
    /// Parse from serialized `FileDescriptorSet` bytes.
    ///
    /// The apb extension descriptors are automatically added to the pool
    /// so that custom field options (e.g. `(apb).arrow_name`) are resolvable.
    /// If the user's descriptor already includes apb.proto (via
    /// `--include_imports`), the duplicate is silently ignored.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DescriptorError> {
        // Use decode_file_descriptor_set which takes raw bytes and preserves
        // extension data in field options (unlike from_file_descriptor_set
        // which goes through prost_types and strips unknown fields).
        //
        // Start from the global pool which includes Google well-known types
        // (Timestamp, Duration, etc.) so descriptors that import them work
        // without requiring --include_imports.
        let mut pool = DescriptorPool::global();
        // Load the apb extension so custom options are resolvable.
        pool.decode_file_descriptor_set(APB_EXTENSION_BYTES)?;
        // Then add user descriptors. If they already include well-known types
        // or apb.proto (via --include_imports), duplicates are silently skipped.
        pool.decode_file_descriptor_set(bytes)?;
        Ok(Self { pool })
    }

    /// Look up a message by fully qualified name (e.g. `"mypackage.MyMessage"`).
    pub fn message(&self, name: &str) -> Result<MessageDescriptor, DescriptorError> {
        self.pool
            .get_message_by_name(name)
            .ok_or_else(|| DescriptorError::MessageNotFound(name.to_string()))
    }

    /// Return a reference to the underlying `DescriptorPool`.
    pub fn pool(&self) -> &DescriptorPool {
        &self.pool
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use prost_reflect::{Cardinality, Kind};

    const SCALARS_BIN: &[u8] = include_bytes!("../../fixtures/scalars.bin");
    const NESTED_BIN: &[u8] = include_bytes!("../../fixtures/nested.bin");

    #[test]
    fn reject_garbage_bytes() {
        let result = ProtoSchema::from_bytes(b"not a valid protobuf");
        assert!(result.is_err());
    }

    #[test]
    fn reject_empty_bytes() {
        let schema = ProtoSchema::from_bytes(b"").unwrap();
        assert!(schema.message("Anything").is_err());
    }

    #[test]
    fn parse_scalars() {
        let schema = ProtoSchema::from_bytes(SCALARS_BIN).unwrap();
        let msg = schema.message("fixtures.Scalars").unwrap();

        assert_eq!(msg.name(), "Scalars");
        assert_eq!(msg.fields().len(), 15);

        // Spot-check a few fields.
        let bool_field = msg.get_field_by_name("bool_field").unwrap();
        assert_eq!(bool_field.number(), 1);
        assert!(matches!(bool_field.kind(), Kind::Bool));

        let string_field = msg.get_field_by_name("string_field").unwrap();
        assert_eq!(string_field.number(), 14);
        assert!(matches!(string_field.kind(), Kind::String));

        let double_field = msg.get_field_by_name("double_field").unwrap();
        assert_eq!(double_field.number(), 13);
        assert!(matches!(double_field.kind(), Kind::Double));
    }

    #[test]
    fn parse_nested_message() {
        let schema = ProtoSchema::from_bytes(NESTED_BIN).unwrap();
        let msg = schema.message("fixtures.Nested").unwrap();

        let inner_field = msg.get_field_by_name("inner").unwrap();
        assert!(matches!(inner_field.kind(), Kind::Message(_)));

        // Verify the nested message has the expected fields.
        if let Kind::Message(inner_desc) = inner_field.kind() {
            assert_eq!(inner_desc.name(), "Inner");
            assert_eq!(inner_desc.fields().len(), 2);
            assert!(inner_desc.get_field_by_name("value").is_some());
            assert!(inner_desc.get_field_by_name("count").is_some());
        }
    }

    #[test]
    fn parse_repeated_field() {
        let schema = ProtoSchema::from_bytes(NESTED_BIN).unwrap();
        let msg = schema.message("fixtures.Nested").unwrap();

        let tags = msg.get_field_by_name("tags").unwrap();
        assert_eq!(tags.cardinality(), Cardinality::Repeated);
        assert!(matches!(tags.kind(), Kind::Int32));

        let items = msg.get_field_by_name("items").unwrap();
        assert_eq!(items.cardinality(), Cardinality::Repeated);
        assert!(matches!(items.kind(), Kind::Message(_)));
    }

    #[test]
    fn parse_map_field() {
        let schema = ProtoSchema::from_bytes(NESTED_BIN).unwrap();
        let msg = schema.message("fixtures.Nested").unwrap();

        let metadata = msg.get_field_by_name("metadata").unwrap();
        assert!(metadata.is_map());
    }

    #[test]
    fn parse_oneof() {
        let schema = ProtoSchema::from_bytes(NESTED_BIN).unwrap();
        let msg = schema.message("fixtures.Nested").unwrap();

        let oneofs: Vec<_> = msg.oneofs().collect();
        assert_eq!(oneofs.len(), 1);
        assert_eq!(oneofs[0].name(), "choice");

        let variant_names: Vec<_> = oneofs[0].fields().map(|f| f.name().to_string()).collect();
        assert_eq!(
            variant_names,
            vec!["text_value", "int_value", "message_value"]
        );
    }

    #[test]
    fn parse_enum_field() {
        let schema = ProtoSchema::from_bytes(NESTED_BIN).unwrap();
        let msg = schema.message("fixtures.Nested").unwrap();

        let status = msg.get_field_by_name("status").unwrap();
        if let Kind::Enum(enum_desc) = status.kind() {
            assert_eq!(enum_desc.name(), "Status");
            let names: Vec<_> = enum_desc.values().map(|v| v.name().to_string()).collect();
            assert_eq!(
                names,
                vec!["STATUS_UNKNOWN", "STATUS_ACTIVE", "STATUS_INACTIVE"]
            );
        } else {
            panic!("expected enum kind");
        }
    }

    #[test]
    fn lookup_by_name_hit_and_miss() {
        let schema = ProtoSchema::from_bytes(SCALARS_BIN).unwrap();

        assert!(schema.message("fixtures.Scalars").is_ok());
        assert!(schema.message("fixtures.NonExistent").is_err());
        assert!(schema.message("wrong.package.Scalars").is_err());
    }

    #[test]
    fn inner_message_reachable() {
        let schema = ProtoSchema::from_bytes(NESTED_BIN).unwrap();
        assert!(schema.message("fixtures.Inner").is_ok());
    }

    /// Descriptors that import well-known types (e.g. google.protobuf.Timestamp)
    /// without embedding them must still load successfully, since the pool is
    /// seeded with the global well-known type definitions.
    #[test]
    fn well_known_type_imports_resolve() {
        use prost::Message;
        use prost_types::{
            field_descriptor_proto::{Label, Type},
            DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        };

        let fd = FileDescriptorProto {
            name: Some("test.proto".to_string()),
            package: Some("test".to_string()),
            syntax: Some("proto3".to_string()),
            dependency: vec!["google/protobuf/timestamp.proto".to_string()],
            message_type: vec![DescriptorProto {
                name: Some("Row".to_string()),
                field: vec![FieldDescriptorProto {
                    name: Some("created_at".to_string()),
                    number: Some(1),
                    label: Some(Label::Optional as i32),
                    r#type: Some(Type::Message as i32),
                    type_name: Some(".google.protobuf.Timestamp".to_string()),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        };

        let bytes = FileDescriptorSet { file: vec![fd] }.encode_to_vec();

        let schema = ProtoSchema::from_bytes(&bytes).unwrap();
        let msg = schema.message("test.Row").unwrap();
        let field = msg.get_field_by_name("created_at").unwrap();
        assert!(matches!(field.kind(), Kind::Message(_)));
    }
}
