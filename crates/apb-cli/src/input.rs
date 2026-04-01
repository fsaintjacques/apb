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

/// Owns a DuckDB Connection and Statement, yielding RecordBatches lazily.
///
/// The Statement borrows from Connection, so we use a lifetime transmute to
/// keep both alive together. Safety relies on struct field drop order: `stmt`
/// is declared before `_conn`, so it is dropped first.
#[cfg(feature = "duckdb")]
struct DuckDbBatches {
    // Safety: drop order matters — stmt must be dropped before conn.
    // Fields are dropped in declaration order, so stmt (which borrows conn) is dropped first.
    stmt: Box<duckdb::Statement<'static>>,
    _conn: Box<Connection>,
    first: Option<RecordBatch>,
}

#[cfg(feature = "duckdb")]
impl DuckDbBatches {
    fn new(query: &str) -> Result<(SchemaRef, Self), Box<dyn std::error::Error>> {
        let conn = Box::new(Connection::open_in_memory()?);

        // Safety: We transmute the lifetime of the Statement to 'static.
        // This is safe because:
        // 1. The Connection is heap-allocated (Box) and won't move.
        // 2. We guarantee conn outlives stmt via struct drop order.
        // 3. The Statement is never exposed outside this struct.
        let conn_ref: &Connection = &conn;
        let conn_ref: &'static Connection = unsafe { std::mem::transmute(conn_ref) };
        let mut stmt = Box::new(conn_ref.prepare(query)?);
        stmt.execute([])?;

        // Fetch the first batch to derive the schema.
        let first = stmt.step().map(|s| RecordBatch::from(&s));
        let schema = first
            .as_ref()
            .map(|b| b.schema())
            .ok_or("query returned no results")?;

        Ok((
            schema,
            DuckDbBatches {
                stmt,
                _conn: conn,
                first,
            },
        ))
    }
}

#[cfg(feature = "duckdb")]
impl Iterator for DuckDbBatches {
    type Item = Result<RecordBatch, Box<dyn std::error::Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(batch) = self.first.take() {
            return Some(Ok(batch));
        }
        let struct_array = self.stmt.step()?;
        Some(Ok(RecordBatch::from(&struct_array)))
    }
}

/// Open a DuckDB query — executes once, returns schema + streaming batches.
#[cfg(feature = "duckdb")]
pub fn open_duckdb(query: &str) -> Result<OpenInput, Box<dyn std::error::Error>> {
    let (schema, batches) = DuckDbBatches::new(query)?;
    Ok(OpenInput {
        schema,
        batches: Box::new(batches),
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
        batches: Box::new(stream.map(|r| r.map_err(|e| Box::new(e) as Box<dyn std::error::Error>))),
    })
}
