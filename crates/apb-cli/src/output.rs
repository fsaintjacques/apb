use std::io::Write;
use std::sync::Arc;

use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use prost_reflect::{DynamicMessage, MessageDescriptor};

use apb_core::transcode::Transcoder;
use apb_core::{Array, RecordBatch};

/// Output format for transcoded data.
#[derive(Debug, Clone, clap::ValueEnum)]
pub enum OutputFormat {
    ProtoDelimited,
    ProtoJsonl,
    ArrowIpc,
}

/// Writer that handles output format.
pub enum OutputWriter {
    Delimited {
        writer: Box<dyn Write>,
    },
    Jsonl {
        writer: Box<dyn Write>,
        message_desc: MessageDescriptor,
    },
    ArrowIpc {
        writer: StreamWriter<Box<dyn Write>>,
    },
}

impl OutputWriter {
    pub fn new(
        format: &OutputFormat,
        writer: Box<dyn Write>,
        message_desc: &MessageDescriptor,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        match format {
            OutputFormat::ProtoDelimited => Ok(Self::Delimited { writer }),
            OutputFormat::ProtoJsonl => Ok(Self::Jsonl {
                writer,
                message_desc: message_desc.clone(),
            }),
            OutputFormat::ArrowIpc => {
                let schema = Schema::new(vec![Field::new("message", DataType::Binary, false)]);
                let ipc_writer = StreamWriter::try_new(writer, &schema)?;
                Ok(Self::ArrowIpc { writer: ipc_writer })
            }
        }
    }

    pub fn write_batch(
        &mut self,
        batch: &RecordBatch,
        transcoder: &mut Transcoder,
    ) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Self::Delimited { writer } => {
                let mut buf = Vec::new();
                transcoder.transcode_delimited(batch, &mut buf)?;
                writer.write_all(&buf)?;
            }
            Self::Jsonl {
                writer,
                message_desc,
            } => {
                let binary = transcoder.transcode_arrow(batch)?;
                for i in 0..binary.len() {
                    let bytes = binary.value(i);
                    let msg = DynamicMessage::decode(message_desc.clone(), bytes)?;
                    let mut serializer = serde_json::Serializer::new(Vec::new());
                    let opts = prost_reflect::SerializeOptions::new().use_proto_field_name(true);
                    msg.serialize_with_options(&mut serializer, &opts)?;
                    let json = String::from_utf8(serializer.into_inner())?;
                    writeln!(writer, "{json}")?;
                }
            }
            Self::ArrowIpc { writer } => {
                let binary = transcoder.transcode_arrow(batch)?;
                let out_batch = RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new(
                        "message",
                        DataType::Binary,
                        false,
                    )])),
                    vec![Arc::new(binary)],
                )?;
                writer.write(&out_batch)?;
            }
        }
        Ok(())
    }

    pub fn finish(self) -> Result<(), Box<dyn std::error::Error>> {
        match self {
            Self::Delimited { mut writer } => {
                writer.flush()?;
            }
            Self::Jsonl { mut writer, .. } => {
                writer.flush()?;
            }
            Self::ArrowIpc { mut writer } => {
                writer.finish()?;
            }
        }
        Ok(())
    }
}
