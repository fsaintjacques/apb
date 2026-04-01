mod encode;
mod enum_behavior;
mod plan;
#[cfg(test)]
mod tests;
pub mod wire;

pub use enum_behavior::UnknownEnumBehavior;

pub use plan::PlanError;

use arrow_array::{
    Array, BinaryArray, LargeListArray, LargeStringArray, ListArray, MapArray, RecordBatch,
    StringArray, StructArray,
};
use arrow_buffer::{Buffer, OffsetBuffer};

use crate::mapping::FieldMapping;
use plan::{
    EncoderEntry, EncodingPlan, EnumLookupEncoder, FieldEncoder, FieldEncoderKind, MapEncoder,
    MessageEncoder, OneofEncoder, RepeatedEncoder,
};

/// Error during transcoding.
#[derive(Debug, thiserror::Error)]
pub enum TranscodeError {
    #[error("plan error: {0}")]
    Plan(#[from] PlanError),

    #[error("row {row}, field '{arrow_field}' → '{proto_field}': {reason}")]
    FieldError {
        row: usize,
        arrow_field: String,
        proto_field: String,
        reason: String,
    },

    #[error("row {row}, oneof '{oneof_name}': multiple variants set: {}", set_variants.join(", "))]
    OneofMultipleSet {
        row: usize,
        oneof_name: String,
        set_variants: Vec<String>,
    },
}

/// A compiled transcoder that converts Arrow `RecordBatch`es into serialized
/// protobuf messages.
pub struct Transcoder {
    plan: EncodingPlan,
    unknown_enum: UnknownEnumBehavior,
}

impl Transcoder {
    /// Build a transcoder from a validated field mapping.
    /// Build a transcoder from a validated field mapping.
    pub fn new(mapping: &FieldMapping) -> Result<Self, TranscodeError> {
        let plan = EncodingPlan::from_mapping(mapping)?;
        Ok(Self {
            plan,
            unknown_enum: UnknownEnumBehavior::default(),
        })
    }

    /// Set the behavior for unknown enum string values.
    pub fn with_unknown_enum(mut self, behavior: UnknownEnumBehavior) -> Self {
        self.unknown_enum = behavior;
        // Update all enum lookup encoders in the plan.
        for entry in &mut self.plan.encoders {
            match entry {
                plan::EncoderEntry::Field(f) => set_enum_behavior(&mut f.kind, behavior),
                plan::EncoderEntry::Oneof(o) => {
                    for v in &mut o.variants {
                        set_enum_behavior(&mut v.kind, behavior);
                    }
                }
            }
        }
        self
    }

    /// Transcode a batch into varint-delimited protobuf messages.
    pub fn transcode_delimited(
        &self,
        batch: &RecordBatch,
        output: &mut Vec<u8>,
    ) -> Result<(), TranscodeError> {
        let mut msg_buf = Vec::with_capacity(256);

        for row in 0..batch.num_rows() {
            msg_buf.clear();
            encode_message_fields(&mut msg_buf, row, batch.columns(), &self.plan)?;

            wire::encode_varint(msg_buf.len() as u64, output);
            output.extend_from_slice(&msg_buf);
        }

        Ok(())
    }

    /// Transcode a batch into an Arrow `BinaryArray`.
    pub fn transcode_arrow(&self, batch: &RecordBatch) -> Result<BinaryArray, TranscodeError> {
        let num_rows = batch.num_rows();
        let mut msg_buf = Vec::with_capacity(256);
        let mut offsets: Vec<i32> = Vec::with_capacity(num_rows + 1);
        let mut payload: Vec<u8> = Vec::new();

        offsets.push(0);

        for row in 0..num_rows {
            msg_buf.clear();
            encode_message_fields(&mut msg_buf, row, batch.columns(), &self.plan)?;

            payload.extend_from_slice(&msg_buf);
            if payload.len() > i32::MAX as usize {
                return Err(TranscodeError::FieldError {
                    row,
                    arrow_field: String::new(),
                    proto_field: String::new(),
                    reason: "accumulated payload exceeds BinaryArray 2 GB limit".to_string(),
                });
            }
            offsets.push(payload.len() as i32);
        }

        let offsets = OffsetBuffer::new(offsets.into());
        let values = Buffer::from(payload);
        Ok(BinaryArray::new(offsets, values, None))
    }
}

/// Encode all fields of a message into `buf`.
fn encode_message_fields(
    buf: &mut Vec<u8>,
    row: usize,
    columns: &[std::sync::Arc<dyn Array>],
    plan: &EncodingPlan,
) -> Result<(), TranscodeError> {
    for entry in &plan.encoders {
        match entry {
            EncoderEntry::Field(encoder) => {
                let array = columns[encoder.arrow_index].as_ref();
                if encoder.nullable && array.is_null(row) {
                    continue;
                }
                encode_field(buf, row, array, encoder)?;
            }
            EncoderEntry::Oneof(oneof_enc) => {
                let struct_array = columns[oneof_enc.arrow_index]
                    .as_any()
                    .downcast_ref::<StructArray>()
                    .expect("oneof column should be StructArray");
                encode_oneof(buf, row, struct_array, oneof_enc)?;
            }
        }
    }

    Ok(())
}

/// Encode a single field value (with tag) into `buf`.
fn encode_field(
    buf: &mut Vec<u8>,
    row: usize,
    array: &dyn Array,
    encoder: &FieldEncoder,
) -> Result<(), TranscodeError> {
    match &encoder.kind {
        FieldEncoderKind::Scalar(kind) => {
            buf.extend_from_slice(&encoder.tag);
            kind.encode(array, row, buf)
                .map_err(|e| TranscodeError::FieldError {
                    row,
                    arrow_field: encoder.arrow_name.clone(),
                    proto_field: encoder.proto_name.clone(),
                    reason: e.reason,
                })?;
        }
        FieldEncoderKind::EnumLookup(lookup) => {
            let before = buf.len();
            buf.extend_from_slice(&encoder.tag);
            let wrote = encode_enum_lookup(
                buf,
                row,
                array,
                lookup,
                &encoder.arrow_name,
                &encoder.proto_name,
            )?;
            if !wrote {
                buf.truncate(before); // undo tag for Skip
            }
        }
        FieldEncoderKind::Message(msg_enc) => {
            buf.extend_from_slice(&encoder.tag);
            encode_nested_message_body(buf, row, array, msg_enc)?;
        }
        FieldEncoderKind::Repeated(rep_enc) => {
            encode_repeated(buf, row, array, rep_enc, &encoder.proto_name)?;
        }
        FieldEncoderKind::Map(map_enc) => {
            encode_map(buf, row, array, &encoder.tag, map_enc, &encoder.proto_name)?;
        }
    }
    Ok(())
}

/// Encode a string value as a proto enum via name lookup.
/// Returns `Ok(true)` if a value was written, `Ok(false)` if skipped.
fn encode_enum_lookup(
    buf: &mut Vec<u8>,
    row: usize,
    array: &dyn Array,
    lookup: &EnumLookupEncoder,
    arrow_name: &str,
    proto_name: &str,
) -> Result<bool, TranscodeError> {
    let value = if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
        arr.value(row)
    } else if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
        arr.value(row)
    } else {
        return Err(TranscodeError::FieldError {
            row,
            arrow_field: arrow_name.to_string(),
            proto_field: proto_name.to_string(),
            reason: "expected Utf8 or LargeUtf8 for enum lookup".to_string(),
        });
    };

    match lookup.name_to_number.get(value) {
        Some(number) => {
            wire::encode_varint(*number as u32 as u64, buf);
            Ok(true)
        }
        None => match lookup.unknown_behavior {
            UnknownEnumBehavior::Error => {
                let mut valid: Vec<_> = lookup.name_to_number.keys().cloned().collect();
                valid.sort();
                Err(TranscodeError::FieldError {
                    row,
                    arrow_field: arrow_name.to_string(),
                    proto_field: proto_name.to_string(),
                    reason: format!(
                        "unknown enum variant '{}' for {}. Valid: {:?}",
                        value, lookup.enum_name, valid,
                    ),
                })
            }
            UnknownEnumBehavior::Default => {
                wire::encode_varint(0, buf);
                Ok(true)
            }
            UnknownEnumBehavior::Skip => Ok(false),
        },
    }
}

fn set_enum_behavior(kind: &mut FieldEncoderKind, behavior: UnknownEnumBehavior) {
    match kind {
        FieldEncoderKind::EnumLookup(e) => e.unknown_behavior = behavior,
        FieldEncoderKind::Repeated(r) => set_enum_behavior(&mut r.element_kind, behavior),
        _ => {}
    }
}

/// Encode a nested message body as a length-delimited value (no tag).
///
/// Caller is responsible for writing the field tag before calling this.
fn encode_nested_message_body(
    buf: &mut Vec<u8>,
    row: usize,
    array: &dyn Array,
    msg_enc: &MessageEncoder,
) -> Result<(), TranscodeError> {
    let struct_array = array
        .as_any()
        .downcast_ref::<StructArray>()
        .expect("nested message column should be StructArray");

    let child_columns: Vec<_> = (0..struct_array.num_columns())
        .map(|i| struct_array.column(i).clone())
        .collect();

    let len_pos = wire::begin_length_delimited(buf);
    encode_message_fields(buf, row, &child_columns, &msg_enc.sub_plan)?;
    wire::finish_length_delimited(buf, len_pos);

    Ok(())
}

/// Encode a repeated field (ListArray → repeated proto field).
fn encode_repeated(
    buf: &mut Vec<u8>,
    row: usize,
    array: &dyn Array,
    rep_enc: &RepeatedEncoder,
    field_name: &str,
) -> Result<(), TranscodeError> {
    let (values, start, end) = if let Some(list) = array.as_any().downcast_ref::<ListArray>() {
        let offsets = list.offsets();
        let s = offsets[row] as usize;
        let e = offsets[row + 1] as usize;
        (list.values().as_ref(), s, e)
    } else if let Some(list) = array.as_any().downcast_ref::<LargeListArray>() {
        let offsets = list.offsets();
        let s = offsets[row] as usize;
        let e = offsets[row + 1] as usize;
        (list.values().as_ref(), s, e)
    } else {
        panic!("repeated field should be ListArray or LargeListArray");
    };

    if start == end {
        return Ok(());
    }

    if rep_enc.packed {
        // Packed encoding: single length-delimited field with all values.
        let kind = match &*rep_enc.element_kind {
            FieldEncoderKind::Scalar(k) => k,
            _ => unreachable!("packed encoding is only set for scalar elements"),
        };

        buf.extend_from_slice(&rep_enc.packed_tag);
        let len_pos = wire::begin_length_delimited(buf);
        for i in start..end {
            kind.encode(values, i, buf)
                .map_err(|e| TranscodeError::FieldError {
                    row,
                    arrow_field: field_name.to_string(),
                    proto_field: format!("{}[{}]", field_name, i - start),
                    reason: e.reason,
                })?;
        }
        wire::finish_length_delimited(buf, len_pos);
    } else {
        // Unpacked: each element gets its own tag.
        let element_tag = wire::encode_tag(rep_enc.field_number, rep_enc.element_wire_type);
        for i in start..end {
            match &*rep_enc.element_kind {
                FieldEncoderKind::Scalar(kind) => {
                    buf.extend_from_slice(&element_tag);
                    kind.encode(values, i, buf)
                        .map_err(|e| TranscodeError::FieldError {
                            row,
                            arrow_field: field_name.to_string(),
                            proto_field: format!("{}[{}]", field_name, i - start),
                            reason: e.reason,
                        })?;
                }
                FieldEncoderKind::Message(msg_enc) => {
                    buf.extend_from_slice(&element_tag);
                    encode_nested_message_body(buf, i, values, msg_enc)?;
                }
                _ => {
                    return Err(TranscodeError::FieldError {
                        row,
                        arrow_field: field_name.to_string(),
                        proto_field: field_name.to_string(),
                        reason: "unsupported repeated element type".to_string(),
                    });
                }
            }
        }
    }

    Ok(())
}

/// Encode a map field (MapArray → repeated entry messages).
fn encode_map(
    buf: &mut Vec<u8>,
    row: usize,
    array: &dyn Array,
    tag: &[u8],
    map_enc: &MapEncoder,
    field_name: &str,
) -> Result<(), TranscodeError> {
    let map_array = array
        .as_any()
        .downcast_ref::<MapArray>()
        .expect("map field should be MapArray");

    let offsets = map_array.offsets();
    let start = offsets[row] as usize;
    let end = offsets[row + 1] as usize;

    let keys = map_array.keys();
    let values = map_array.values();

    for i in start..end {
        buf.extend_from_slice(tag);
        let len_pos = wire::begin_length_delimited(buf);

        // Key (field 1) — proto map keys are always scalars.
        buf.extend_from_slice(&map_enc.key_tag);
        match &*map_enc.key_kind {
            FieldEncoderKind::Scalar(kind) => {
                kind.encode(keys.as_ref(), i, buf)
                    .map_err(|e| TranscodeError::FieldError {
                        row,
                        arrow_field: format!("{field_name}[{}].key", i - start),
                        proto_field: format!("{field_name}[{}].key", i - start),
                        reason: e.reason,
                    })?;
            }
            _ => unreachable!("proto map keys must be scalar types"),
        }

        // Value (field 2).
        if !values.is_null(i) {
            buf.extend_from_slice(&map_enc.value_tag);
            match &*map_enc.value_kind {
                FieldEncoderKind::Scalar(kind) => {
                    kind.encode(values.as_ref(), i, buf).map_err(|e| {
                        TranscodeError::FieldError {
                            row,
                            arrow_field: format!("{field_name}[{}].value", i - start),
                            proto_field: format!("{field_name}[{}].value", i - start),
                            reason: e.reason,
                        }
                    })?;
                }
                FieldEncoderKind::Message(msg_enc) => {
                    encode_nested_message_body(buf, i, values.as_ref(), msg_enc)?;
                }
                _ => {
                    return Err(TranscodeError::FieldError {
                        row,
                        arrow_field: field_name.to_string(),
                        proto_field: format!("{field_name}[{}].value", i - start),
                        reason: "unsupported map value type".to_string(),
                    });
                }
            }
        }

        wire::finish_length_delimited(buf, len_pos);
    }

    Ok(())
}

/// Encode a oneof group (StructArray with nullable children).
fn encode_oneof(
    buf: &mut Vec<u8>,
    row: usize,
    struct_array: &StructArray,
    oneof_enc: &OneofEncoder,
) -> Result<(), TranscodeError> {
    let mut set_variants = Vec::new();

    for variant in &oneof_enc.variants {
        let child = struct_array.column(variant.arrow_child_index);
        if !child.is_null(row) {
            set_variants.push(variant);
        }
    }

    match set_variants.len() {
        0 => Ok(()),
        1 => {
            let variant = set_variants[0];
            let child = struct_array.column(variant.arrow_child_index);
            buf.extend_from_slice(&variant.tag);
            match &*variant.kind {
                FieldEncoderKind::Scalar(kind) => {
                    kind.encode(child.as_ref(), row, buf).map_err(|e| {
                        TranscodeError::FieldError {
                            row,
                            arrow_field: variant.proto_name.clone(),
                            proto_field: variant.proto_name.clone(),
                            reason: e.reason,
                        }
                    })?;
                }
                FieldEncoderKind::EnumLookup(lookup) => {
                    let wrote = encode_enum_lookup(
                        buf,
                        row,
                        child.as_ref(),
                        lookup,
                        &variant.proto_name,
                        &variant.proto_name,
                    )?;
                    if !wrote {
                        // Undo the tag written before the match.
                        let tag_len = variant.tag.len();
                        buf.truncate(buf.len() - tag_len);
                    }
                }
                FieldEncoderKind::Message(msg_enc) => {
                    encode_nested_message_body(buf, row, child.as_ref(), msg_enc)?;
                }
                _ => {
                    return Err(TranscodeError::FieldError {
                        row,
                        arrow_field: variant.proto_name.clone(),
                        proto_field: variant.proto_name.clone(),
                        reason: "unsupported oneof variant type".to_string(),
                    });
                }
            }
            Ok(())
        }
        _ => Err(TranscodeError::OneofMultipleSet {
            row,
            oneof_name: oneof_enc.oneof_name.clone(),
            set_variants: set_variants.iter().map(|v| v.proto_name.clone()).collect(),
        }),
    }
}
