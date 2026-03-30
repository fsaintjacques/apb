mod encode;
mod plan;
#[cfg(test)]
mod tests;
pub mod wire;

pub use plan::PlanError;

use arrow_array::{Array, BinaryArray, RecordBatch};
use arrow_buffer::{Buffer, OffsetBuffer};

use crate::mapping::FieldMapping;
use plan::EncodingPlan;

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
}

/// A compiled transcoder that converts Arrow `RecordBatch`es into serialized
/// protobuf messages.
///
/// Built once from a `FieldMapping`. Precomputes wire tags, encoder functions,
/// and null handling. Reusable across batches within a stream.
pub struct Transcoder {
    plan: EncodingPlan,
    /// Scratch buffer for delimited output (reused across rows).
    scratch: Vec<u8>,
}

impl Transcoder {
    /// Build a transcoder from a validated field mapping.
    ///
    /// Returns error if the mapping contains unsupported field shapes
    /// (nested types are not yet supported).
    pub fn new(mapping: &FieldMapping) -> Result<Self, TranscodeError> {
        let plan = EncodingPlan::from_mapping(mapping)?;
        Ok(Self {
            plan,
            scratch: Vec::with_capacity(256),
        })
    }

    /// Transcode a batch into varint-delimited protobuf messages.
    ///
    /// Each row is encoded as a complete proto message with a varint length
    /// prefix. Appends to `output` — does not clear it.
    pub fn transcode_delimited(
        &mut self,
        batch: &RecordBatch,
        output: &mut Vec<u8>,
    ) -> Result<(), TranscodeError> {
        let arrays: Vec<_> = self
            .plan
            .field_encoders
            .iter()
            .map(|e| batch.column(e.arrow_index).as_ref())
            .collect();

        for row in 0..batch.num_rows() {
            self.scratch.clear();
            self.encode_row(row, &arrays)?;

            // Write length prefix + message bytes.
            wire::encode_varint(self.scratch.len() as u64, output);
            output.extend_from_slice(&self.scratch);
        }

        Ok(())
    }

    /// Transcode a batch into an Arrow `BinaryArray`.
    ///
    /// Each element is one serialized proto message. No length prefix.
    pub fn transcode_arrow(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<BinaryArray, TranscodeError> {
        let num_rows = batch.num_rows();
        let arrays: Vec<_> = self
            .plan
            .field_encoders
            .iter()
            .map(|e| batch.column(e.arrow_index).as_ref())
            .collect();

        let mut offsets: Vec<i32> = Vec::with_capacity(num_rows + 1);
        let mut payload: Vec<u8> = Vec::new();

        offsets.push(0);

        for row in 0..num_rows {
            self.scratch.clear();
            self.encode_row(row, &arrays)?;
            payload.extend_from_slice(&self.scratch);
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
        let array = BinaryArray::new(offsets, values, None);
        Ok(array)
    }

    /// Encode a single row into `self.scratch`.
    fn encode_row(
        &mut self,
        row: usize,
        arrays: &[&dyn Array],
    ) -> Result<(), TranscodeError> {
        for (i, encoder) in self.plan.field_encoders.iter().enumerate() {
            let array = arrays[i];

            // Skip null values — proto default.
            if encoder.nullable && array.is_null(row) {
                continue;
            }

            // Write tag.
            self.scratch.extend_from_slice(&encoder.tag);

            // Write value.
            (encoder.encode_fn)(array, row, &mut self.scratch).map_err(|e| {
                TranscodeError::FieldError {
                    row,
                    arrow_field: encoder.arrow_name.clone(),
                    proto_field: encoder.proto_name.clone(),
                    reason: e.reason,
                }
            })?;
        }

        Ok(())
    }
}
