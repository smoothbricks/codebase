//! Columnar event buffers and the Arrow IPC stream writer.
//!
//! - [`columns`]: base four-column and dynamic schema-driven storage, plus
//!   transactional variable-width reservations.
//! - [`schema`]: TypeScript-generated schema bytes and field metadata.
//! - [`record_batch`]: hand-emitted Dynamic RecordBatch FlatBuffers.
//! - [`ipc`]: Arrow IPC framing and in-place body construction.
//!
//! Column buffers are stored as little-endian byte vectors (the crate-family
//! convention): the IPC writer borrows `&[u8]` views of offsets/fixed-width
//! data without copies, and byte-backed storage provides them with zero
//! `unsafe`.
//!

pub mod columns;
pub mod ipc;
pub mod record_batch;
pub mod schema;

pub use columns::{
    ColumnStorage, ColumnType, DynamicColumns, EventColumns, MAX_EVENTS_PER_BATCH,
    MAX_STRING_BYTES, MAX_VALUE_BYTES, ParseError, VariableValueError, VariableValueReservation,
};
pub use ipc::{
    EOS_MARKER, IpcError, MIN_ARROW_OUTPUT_CAPACITY, required_arrow_ipc_len,
    write_arrow_ipc_from_borrowed_columns, write_arrow_ipc_from_columns_with_schema,
    write_arrow_ipc_from_dynamic_columns,
};
pub use record_batch::{
    BufferDesc, CONTINUATION_MARKER, DynamicBodyBuilder, DynamicColumn, FieldNode, MetadataError,
    MetadataLimits, MetadataStorage, align_to_8, compute_buffer_count, encode_record_batch_dynamic,
    record_batch_metadata_size,
};
pub use schema::{
    ArrowType, DynamicSchemaConfig, MAX_SCHEMA_FIELDS, SchemaError, SignalSchemaField,
};
