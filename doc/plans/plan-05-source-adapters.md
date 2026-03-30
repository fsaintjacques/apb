# Plan 05 — Source Adapters

## Goal

Build the `apb-source` crate: pluggable adapters for fetching proto
descriptors from remote sources and reading Arrow data from various inputs.
This crate is async and depends on `apb-core` for types only.

## Files

```
crates/apb-source/
├── Cargo.toml
└── src/
    ├── lib.rs
    ├── descriptor/
    │   ├── mod.rs        # DescriptorSource trait + resolver
    │   ├── fs.rs         # file:// and plain path
    │   ├── https.rs      # https://
    │   └── object.rs     # gs://, s3://
    └── arrow/
        ├── mod.rs        # ArrowSource trait + enum
        ├── ipc.rs        # Arrow IPC file / stdin
        ├── flight.rs     # Arrow Flight client
        └── bq.rs         # BigQuery Storage Read API
```

## Dependencies

| Crate            | Purpose                         |
|------------------|---------------------------------|
| `apb-core`       | `ProtoSchema`, Arrow types      |
| `tokio`          | Async runtime                   |
| `reqwest`        | HTTPS fetching                  |
| `object_store`   | GCS + S3 (via `object_store` crate from arrow-rs ecosystem) |
| `arrow-flight`   | Arrow Flight gRPC client        |
| `arrow-ipc`      | IPC file/stream reader          |
| `tonic`          | gRPC transport (for Flight + BQ)|
| `gcp-bigquery-client` or raw gRPC | BQ Storage Read API |

## Descriptor sources

### Trait

```rust
#[async_trait]
pub trait DescriptorSource {
    /// Fetch raw FileDescriptorSet bytes.
    async fn fetch(&self) -> Result<Vec<u8>, SourceError>;
}
```

### Resolver

Parse a URI string and return the appropriate source.

```rust
/// Resolve a URI to a descriptor source.
/// Supported schemes: file://, https://, gs://, s3://
/// Plain paths (no scheme) are treated as local files.
pub fn descriptor_source(uri: &str) -> Result<Box<dyn DescriptorSource>, SourceError>;
```

### Implementations

**`FsDescriptorSource`** — read a local file. Trivial: `tokio::fs::read`.

**`HttpsDescriptorSource`** — GET request via reqwest. No auth for v1. Accept
any 2xx, error on anything else.

**`ObjectStoreDescriptorSource`** — use `object_store` crate with `GcpStore`
or `AmazonS3` backend. The `object_store` crate handles auth via environment
(ADC for GCS, standard AWS env vars for S3).

## Arrow sources

### Trait

```rust
#[async_trait]
pub trait ArrowSource {
    /// Get the Arrow schema before reading data.
    async fn schema(&self) -> Result<Schema, SourceError>;

    /// Stream record batches.
    fn batches(&self) -> Pin<Box<dyn Stream<Item = Result<RecordBatch, SourceError>> + Send>>;
}
```

### Implementations

**`IpcArrowSource`** — read Arrow IPC file or stream from a path or stdin.
Uses `arrow-ipc` `FileReader` or `StreamReader`. Stdin is detected when path
is `-` or omitted.

**`FlightArrowSource`** — connect to an Arrow Flight endpoint. Takes a URI and
optionally a ticket or SQL command. Uses `arrow-flight` `FlightClient`.

```rust
pub struct FlightConfig {
    pub endpoint: String,
    pub ticket: Option<String>,
    pub sql: Option<String>,
    // Auth handled via channel interceptors (bearer token, mTLS, etc.)
}
```

**`BqArrowSource`** — BigQuery Storage Read API. Takes a table reference
(`project.dataset.table`), creates a read session, streams Arrow batches.

```rust
pub struct BqConfig {
    pub table: String,          // project.dataset.table
    pub row_filter: Option<String>,
    pub selected_fields: Option<Vec<String>>,
}
```

The BQ Storage Read API natively returns Arrow-serialized batches — no
conversion needed. Auth via Application Default Credentials.

## Error types

```rust
pub enum SourceError {
    /// URI scheme not recognized.
    UnsupportedScheme(String),
    /// Network or I/O error fetching descriptor.
    FetchFailed { uri: String, source: Box<dyn std::error::Error + Send + Sync> },
    /// Failed to connect to Arrow source.
    ConnectionFailed { target: String, source: Box<dyn std::error::Error + Send + Sync> },
    /// Error reading a batch from the stream.
    ReadError { source: Box<dyn std::error::Error + Send + Sync> },
    /// Source returned no schema.
    NoSchema,
}
```

## Tasks

1. **Crate setup** — `Cargo.toml` with feature flags for each source backend
   (so users can opt out of heavy dependencies like tonic/gRPC).

   ```toml
   [features]
   default = ["fs", "https"]
   fs = []
   https = ["reqwest"]
   gcs = ["object_store/gcp"]
   s3 = ["object_store/aws"]
   flight = ["arrow-flight", "tonic"]
   bq = ["tonic"]
   ```

2. **Descriptor sources** — `DescriptorSource` trait, URI resolver,
   fs/https/object-store implementations.

3. **IPC source** — `IpcArrowSource` for file and stdin. Simplest Arrow
   source — implement first.

4. **Flight source** — `FlightArrowSource`. Connect, get schema, stream
   batches.

5. **BQ source** — `BqArrowSource`. Create read session, stream batches.
   This is the most complex adapter due to BQ session management.

6. **Error types** — `SourceError` with context for each failure mode.

7. **Tests**
   - Descriptor: read a local `.bin` file.
   - Descriptor: HTTPS fetch (mock server or integration test behind feature
     flag).
   - IPC: read a fixture IPC file, verify schema + batch contents.
   - IPC: read from stdin (pipe a fixture file).
   - Flight: integration test against a local Flight server (behind feature
     flag).
   - BQ: integration test against real BQ (behind feature flag, requires
     credentials).
   - URI resolver: correct dispatch for file://, https://, gs://, s3://,
     plain path.
   - Unsupported scheme → error.

## Done when

- `descriptor_source("gs://bucket/desc.bin")` fetches and returns bytes
- `IpcArrowSource` reads Arrow IPC files and streams batches
- `FlightArrowSource` connects and streams batches from a Flight endpoint
- `BqArrowSource` creates a read session and streams batches
- Feature flags control which backends are compiled
- All unit tests pass; integration tests pass when credentials available
