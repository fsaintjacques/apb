use std::io::{self, Read};
use std::path::Path;

use arrow_ipc::reader::StreamReader;
use arrow_schema::SchemaRef;

use apb_core::RecordBatch;

#[cfg(feature = "duckdb")]
use duckdb::Connection;

/// Opened input — schema already read, ready to iterate batches.
pub struct OpenInput {
    pub schema: SchemaRef,
    batches: Box<dyn Iterator<Item = Result<RecordBatch, Box<dyn std::error::Error>>>>,
}

impl OpenInput {
    pub fn into_batches(
        self,
    ) -> Box<dyn Iterator<Item = Result<RecordBatch, Box<dyn std::error::Error>>>> {
        self.batches
    }
}

/// Open a DuckDB query — executes once, returns schema + batches.
#[cfg(feature = "duckdb")]
pub fn open_duckdb(query: &str) -> Result<OpenInput, Box<dyn std::error::Error>> {
    let conn = Connection::open_in_memory()?;
    let mut stmt = conn.prepare(query)?;
    let arrow = stmt.query_arrow([])?;
    let schema = arrow.get_schema();
    let batches: Vec<RecordBatch> = arrow.collect();
    Ok(OpenInput {
        schema,
        batches: Box::new(batches.into_iter().map(Ok)),
    })
}

/// Open an Arrow IPC stream — reads from file or stdin.
pub fn open_ipc(path: &str) -> Result<OpenInput, Box<dyn std::error::Error>> {
    let reader: Box<dyn Read> = if path == "-" {
        Box::new(io::stdin())
    } else {
        Box::new(std::fs::File::open(Path::new(path))?)
    };
    let stream = StreamReader::try_new(reader, None)?;
    let schema = stream.schema();
    Ok(OpenInput {
        schema,
        batches: Box::new(
            stream.map(|r| r.map_err(|e| Box::new(e) as Box<dyn std::error::Error>)),
        ),
    })
}
