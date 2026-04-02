use arrow_schema::DataType;

/// Errors that can occur during descriptor generation.
#[derive(Debug, thiserror::Error)]
pub enum GenerateError {
    #[error("unsupported Arrow type `{arrow_type}` for field `{field_name}`")]
    UnsupportedType {
        field_name: String,
        arrow_type: DataType,
    },

    #[error("too many fields ({count}) in message `{message_name}`: protobuf supports at most {MAX_FIELD_NUMBER}")]
    TooManyFields { message_name: String, count: usize },
}

/// Maximum valid protobuf field number.
pub const MAX_FIELD_NUMBER: i32 = 536_870_911; // 2^29 - 1

/// Protobuf reserved field number range (used internally by the implementation).
pub const RESERVED_RANGE: std::ops::RangeInclusive<i32> = 19_000..=19_999;
