mod compatibility;
mod coercion;

pub use coercion::CoercionRisk;
pub use compatibility::{
    check_compatibility, resolve_type_check, TypeCheck, TypeCheckMode, TypeCompatibility,
    TypeError, TypeErrorReason,
};
