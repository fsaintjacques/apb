mod explicit;
mod infer;
mod model;
#[cfg(test)]
mod tests;

pub use explicit::{explicit_mapping, ArrowFieldRef, ExplicitBinding, ProtoFieldRef};
pub use infer::{infer_mapping, InferOptions};
pub use model::{
    BindMethod, FieldBinding, FieldMapping, FieldShape, MappingError, OneofMapping, OneofVariant,
    UnmappedArrowField, UnmappedProtoField,
};
