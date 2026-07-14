//! Columnar event buffers and the Arrow IPC stream writer.
//!
//! Ports, unified from the drifted columine/axe-runtime pairs (columine as
//! base per the port dossier; the axe delta was audited hunk-by-hunk):
//!
//! - `parsing/columns.zig` — [`columns`]: `EventColumns` (base 4-column),
//!   `ColumnStorage`/`DynamicColumns` (N-column extraction), and the
//!   transactional `VariableValueReservation`. The axe-runtime copy is the
//!   columine file minus the whole `EventColumns` base path (+ comment
//!   rewording); nothing else drifted.
//! - `arrow/dynamic_schema.zig` — [`schema`]: TS-generated schema bytes +
//!   `SignalSchemaField` metadata. Axe delta: deletion of
//!   `has_extraction_fields` (the base-path selector its EP no longer has).
//! - `arrow/dynamic_record_batch.zig` — [`record_batch`]: byte-identical
//!   between the two packages today (the 1-copy `recordBatchMetadataSize`
//!   optimization was forward-ported to columine before this port).
//! - `arrow/ipc_writer.zig` — [`ipc`]: axe delta is deletion of the base
//!   `writeArrowIpcFromColumnsWithSchema` path plus the in-place body build;
//!   the unified port keeps BOTH entry points and uses the in-place (1-copy)
//!   strategy for both, since output bytes are identical and the scratch-half
//!   relocation in columine's base path is un-forward-ported drift, not
//!   semantics (audited on telos thread t-5dacc729).
//!
//! Column buffers are stored as little-endian byte vectors (the crate-family
//! convention): the IPC writer needs `&[u8]` views of offsets/fixed-width
//! data without copies, and byte-backed storage provides them with zero
//! `unsafe`.

pub mod columns;
pub mod ipc;
pub mod record_batch;
pub mod schema;

pub use columns::{
    ColumnStorage, ColumnType, DynamicColumns, EventColumns, MAX_EVENTS_PER_BATCH,
    MAX_STRING_BYTES, MAX_VALUE_BYTES, ParseError, VariableValueError, VariableValueReservation,
};
pub use ipc::{
    EOS_MARKER, IpcError, IpcWriter, write_arrow_ipc_from_columns_with_schema,
    write_arrow_ipc_from_dynamic_columns,
};
pub use record_batch::{
    BufferDesc, CONTINUATION_MARKER, DynamicBodyBuilder, DynamicColumn, FieldNode, MetadataError,
    MetadataLimits, MetadataStorage, align_to_8, compute_buffer_count, encode_record_batch_dynamic,
    record_batch_metadata_size,
};
pub use schema::{ArrowType, DynamicSchemaConfig, SignalSchemaField};
