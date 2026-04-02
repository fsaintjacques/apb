use arrow_schema::DataType;

/// Errors that can occur during descriptor generation.
#[derive(Debug, thiserror::Error)]
pub enum GenerateError {
    #[error("unsupported Arrow type `{arrow_type}` for field `{field_name}`")]
    UnsupportedType {
        field_name: String,
        arrow_type: DataType,
    },
}
