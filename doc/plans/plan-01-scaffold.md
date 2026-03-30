# Plan 01 — Project Scaffold + Descriptor Parsing

## Goal

Set up the Cargo workspace and parse protobuf binary descriptors into an
internal type model that the rest of `apb-core` builds on.

## Workspace layout

```
Cargo.toml              # workspace root
proto/
└── apb/
    └── apb.proto        # ApbFieldOptions extension (for consuming teams)
crates/
├── apb-core/
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       └── descriptor/
│           ├── mod.rs
│           └── model.rs
├── apb-source/
│   ├── Cargo.toml
│   └── src/lib.rs      # stub
└── apb-cli/
    ├── Cargo.toml
    └── src/main.rs      # stub
```

## Dependencies

| Crate          | Purpose                                      |
|----------------|----------------------------------------------|
| `prost`        | Decode `FileDescriptorSet` from bytes         |
| `prost-types`  | `FileDescriptorProto`, `DescriptorProto`, etc |
| `prost-reflect` | Runtime reflection over descriptors          |
| `arrow`        | Arrow types (used in model, not yet for data) |
| `thiserror`    | Error types                                   |

## Internal type model

The raw prost-reflect descriptor tree is rich but awkward to work with
directly. We build a thin internal model that extracts what the mapping and
transcoding stages need.

### `ProtoSchema`

Top-level container parsed from a `FileDescriptorSet`. Holds all message
descriptors, indexed by fully qualified name.

```rust
pub struct ProtoSchema {
    // Backed by prost-reflect DescriptorPool for resolution
    pool: DescriptorPool,
}

impl ProtoSchema {
    /// Parse from serialized FileDescriptorSet bytes.
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, DescriptorError>;

    /// Look up a message by fully qualified name (e.g. "mypackage.MyMessage").
    pub fn message(&self, name: &str) -> Option<MessageDescriptor>;
}
```

### Key prost-reflect types we use directly

Rather than duplicating the full descriptor tree, we lean on `prost-reflect`'s
types where they're already ergonomic:

- `MessageDescriptor` — fields, oneofs, nested messages
- `FieldDescriptor` — name, number, type (Kind), cardinality
- `OneofDescriptor` — variant fields
- `Kind` — proto scalar types (Int32, String, Bytes, etc.)

We wrap only where we need to add apb-specific concerns (annotations,
resolved options).

### `DescriptorError`

```rust
pub enum DescriptorError {
    /// Failed to decode the binary FileDescriptorSet.
    DecodeFailed(prost::DecodeError),
    /// Failed to build the descriptor pool.
    PoolError(prost_reflect::DescriptorError),
    /// Requested message not found.
    MessageNotFound(String),
}
```

## Tasks

1. **Workspace setup** — Create `Cargo.toml` workspace root, three crate
   directories, stub `lib.rs`/`main.rs` files. Verify `cargo check` passes.

2. **Descriptor parsing** — Implement `ProtoSchema::from_bytes`. Parse raw
   bytes into `prost_reflect::DescriptorPool`. Expose message lookup by
   fully qualified name.

3. **Error types** — `DescriptorError` with variants for decode failure, pool
   construction failure, and missing message.

4. **Tests** — Use `protoc` in a build script or checked-in `.bin` fixtures to
   produce test `FileDescriptorSet` files. Test cases:
   - Parse a simple flat message (all scalar types).
   - Parse nested messages (message containing message).
   - Parse message with oneof.
   - Parse message with map field.
   - Parse message with repeated field.
   - Lookup by fully qualified name (hit and miss).
   - Reject garbage bytes.

## Test fixtures

Create a `crates/apb-core/fixtures/` directory with:

- `scalars.proto` — one message with every scalar type
- `nested.proto` — message containing nested messages, repeated, map, oneof
- Corresponding `.bin` files (serialized `FileDescriptorSet`)

Generate `.bin` files via:
```sh
protoc --descriptor_set_out=scalars.bin --include_imports scalars.proto
```

A `justfile` or shell script at the repo root can regenerate fixtures.

## Done when

- `cargo check --workspace` passes
- `ProtoSchema::from_bytes` parses a `FileDescriptorSet` and returns message
  descriptors
- All test cases above pass
- `apb-source` and `apb-cli` exist as stubs that compile
