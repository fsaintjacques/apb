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
/// The Statement borrows from Connection, and Arrow borrows from Statement,
/// so we pin both and use pointer-based self-referencing to keep them alive.
#[cfg(feature = "duckdb")]
struct DuckDbBatches {
    // Safety: drop order matters — stmt must be dropped before conn.
    // Fields are dropped in declaration order, so stmt (which borrows conn) is dropped first.
    stmt: Box<duckdb::Statement<'static>>,
    _conn: Box<Connection>,
    started: bool,
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

        let arrow = stmt.query_arrow([])?;
        let schema = arrow.get_schema();
        // Drop the Arrow iterator — we only needed it for the schema.
        // We'll re-step through results in Iterator::next().
        drop(arrow);

        Ok((
            schema,
            DuckDbBatches {
                stmt,
                _conn: conn,
                started: false,
            },
        ))
    }
}

#[cfg(feature = "duckdb")]
impl Iterator for DuckDbBatches {
    type Item = Result<RecordBatch, Box<dyn std::error::Error>>;

    fn next(&mut self) -> Option<Self::Item> {
        if !self.started {
            self.started = true;
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
