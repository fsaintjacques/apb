/// Behavior when a string value doesn't match any enum variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum UnknownEnumBehavior {
    /// Fail the batch with an error (default).
    #[default]
    Error,
    /// Write the proto3 default value (0).
    Default,
    /// Skip the field entirely (same as null).
    Skip,
}
