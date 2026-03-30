/// The risk level associated with a type coercion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CoercionRisk {
    /// Value may be truncated (e.g. i64 → i32).
    Truncation,
    /// Precision may be lost (e.g. f64 → f32).
    PrecisionLoss,
    /// May fail at runtime for some values (e.g. bytes → string, string → enum).
    RuntimeCheck,
    /// Semantic change, no data loss (e.g. utf8 → bytes, timestamp → int64).
    Semantic,
    /// No risk, widening conversion.
    Lossless,
}

impl std::fmt::Display for CoercionRisk {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Truncation => write!(f, "truncation"),
            Self::PrecisionLoss => write!(f, "precision loss"),
            Self::RuntimeCheck => write!(f, "runtime check"),
            Self::Semantic => write!(f, "semantic change"),
            Self::Lossless => write!(f, "lossless widening"),
        }
    }
}
