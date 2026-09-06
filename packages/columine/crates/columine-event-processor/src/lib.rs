//! Unified event processor: parse → columns → (dedup) → Arrow IPC.
//!
//! [`EpWiring`] parameterizes the two published artifact configurations:
//!
//! - **Consumer configuration**: dynamic-only extraction, exact deduplication
//!   against the seen-set ([`dedup`]), diagnostic bytes in the result
//!   header, JSON fallback-workspace growth, and no MessagePack workspace
//!   growth.
//! - **Columine configuration**: no deduplication, a base four-column path for
//!   four-field schemas, and workspace growth for both JSON and MessagePack.
//!
//! The `ep_*` wasm exports are thin bindings around this library core.

pub mod compact;
pub mod dedup;

pub use compact::{
    COMPACT_ABI_VERSION, COMPACT_BATCH_MAGIC, COMPACT_DESCRIPTOR_SIZE, COMPACT_DIAGNOSTIC_STAGE,
    COMPACT_HEADER_SIZE, CompactBatchView, CompactValidationError, compact_detail,
};
pub use dedup::{
    AdmissionRefusal, CollisionPolicy, EventKey, IdNamespace, Judgment, MAX_ID_BYTES, SeenSet,
};

use columine_arrow::{
    DynamicColumns, DynamicSchemaConfig, IpcError, MAX_EVENTS_PER_BATCH, MAX_VALUE_BYTES,
    MIN_ARROW_OUTPUT_CAPACITY, MetadataLimits, MetadataStorage, required_arrow_ipc_len,
    write_arrow_ipc_from_borrowed_columns, write_arrow_ipc_from_dynamic_columns,
};
use columine_parsing::{
    ExtractionConfig, json_extractor, json_scanner, msgpack_extractor, msgpack_scanner,
    validate::{
        BatchValidationError, SemanticSchemaSet, parse_schema_envelope, validate_json_batch,
        validate_msgpack_batch,
    },
};

/// Module version: 3 is the consumer artifact (two-phase exact dedup), 1
/// this crate's npm artifact.
pub const CONSUMER_VERSION: u32 = 3;
pub const COLUMINE_VERSION: u32 = 1;

/// Input format for `create_log_entry` (u8 values are ABI).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum InputFormat {
    Json = 0,
    /// Standard msgpack: array of map objects.
    Msgpack = 1,
    ArrowPassthrough = 2,
    /// Concatenated msgpack maps (no array wrapper).
    MsgpackStream = 3,
}

/// Result codes for the wasm exports (u32 values are ABI).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum ResultCode {
    Ok = 0,
    InvalidHandle = 1,
    ParseError = 2,
    EncodeError = 3,
    OutOfMemory = 4,
    InvalidFormat = 5,
    InvalidInput = 6,
    SchemaMismatch = 7,
    /// The seen-set refused an id: the diagnostic bytes name the cause
    /// ([`diagnostic_stage::DEDUP`]) and the set is untouched.
    AdmissionRefused = 8,
    /// No in-flight batch has the named id on that log: it was committed or
    /// abandoned already, or never judged.
    UnknownBatch = 9,
}

/// Event capacity every wasm `ep_create_*` handle is built with.
///
/// This is an ALLOCATION SIZE, not a validation ceiling, and the two must not
/// share a number. `DynamicColumns::new` allocates the whole column plane up
/// front: each variable-width column takes
/// `min(MAX_VALUE_BYTES, capacity * 128)` data bytes plus `(capacity + 1) * 4`
/// offset bytes. Measured against the shipped artifact with the seven-field
/// compact schema, creating one handle at capacity 65536 grows the wasm heap
/// from 145 to 532 pages — 24.19 MiB for a 26.06 MiB layout — while the same
/// schema at 256 needs 0.10 MiB and grows the heap by nothing. Width
/// multiplies it: 32 utf8 columns create cleanly at 61440 and trap at 61932,
/// where the plane crosses the artifact's `--max-memory=256MiB`.
///
/// Rows above this capacity are not lost: the compact path encodes a
/// caller-supplied batch of up to `MAX_EVENTS_PER_BATCH` rows without touching
/// the parse plane at all, so paying for 65536 parse rows per handle buys
/// nothing.
pub const WASM_EVENT_CAPACITY: u32 = 256;

/// Why an `ep_create_*` call produced no handle (u32 values are ABI).
///
/// Handles occupy 1..=255, so any other return value is a failure and these
/// name which one. Every cause used to collapse into a bare `0`: a schema type
/// mismatch was indistinguishable from a capacity refusal, from a null
/// pointer, and from an exhausted handle table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u32)]
pub enum CreateFailure {
    /// Null pointer, or a field count whose metadata length overflows.
    BadRequest = 0x8000_0001,
    /// `capacity == 0` or above the instance ceiling.
    Capacity = 0x8000_0002,
    /// `schema_bytes` is not one continuation-prefixed IPC Schema message.
    SchemaMessage = 0x8000_0003,
    /// More schema fields than `columine_arrow::MAX_SCHEMA_FIELDS`.
    SchemaTooManyFields = 0x8000_0004,
    /// A four-byte physical descriptor is not a valid `[type, nullable, 0, 0]`.
    SchemaFieldMetadata = 0x8000_0005,
    /// The schema message and the physical metadata table disagree on width.
    SchemaFieldCount = 0x8000_0006,
    /// A physical tag disagrees with that field's logical Arrow type.
    SchemaTypeMismatch = 0x8000_0007,
    /// A field's nullability disagrees, or a Null field is non-nullable.
    SchemaNullability = 0x8000_0008,
    /// The field-name blob is malformed or not one name per field.
    SchemaFieldNames = 0x8000_0009,
    /// Retained-metadata or extraction-config limits refused the schema.
    Init = 0x8000_000a,
    /// All 255 handle slots are in use.
    HandlesExhausted = 0x8000_000b,
}

impl CreateFailure {
    /// Every handle-creation failure, ascending by code. The codes are one
    /// contiguous block, which `parse_backend_ts_create_failures_match_rust`
    /// checks, so a variant missing from this list shows up as a short block
    /// there and as a surplus entry in the TypeScript decoder table.
    pub const ALL: &'static [CreateFailure] = &[
        Self::BadRequest,
        Self::Capacity,
        Self::SchemaMessage,
        Self::SchemaTooManyFields,
        Self::SchemaFieldMetadata,
        Self::SchemaFieldCount,
        Self::SchemaTypeMismatch,
        Self::SchemaNullability,
        Self::SchemaFieldNames,
        Self::Init,
        Self::HandlesExhausted,
    ];
}

impl From<columine_arrow::SchemaError> for CreateFailure {
    fn from(error: columine_arrow::SchemaError) -> Self {
        use columine_arrow::SchemaError as E;
        match error {
            E::InvalidMessage => Self::SchemaMessage,
            E::TooManyFields => Self::SchemaTooManyFields,
            E::InvalidFieldMetadata { .. } => Self::SchemaFieldMetadata,
            E::FieldCountMismatch { .. } => Self::SchemaFieldCount,
            E::TypeMismatch { .. } => Self::SchemaTypeMismatch,
            E::NullabilityMismatch { .. } => Self::SchemaNullability,
            E::InvalidFieldNames => Self::SchemaFieldNames,
        }
    }
}

impl From<EpInitError> for CreateFailure {
    fn from(_: EpInitError) -> Self {
        Self::Init
    }
}

/// Result header size (`ResultHeader`, 32 bytes: code u32 | arrow_ipc_offset
/// u32 | arrow_ipc_len u32 | events_processed u32 | duplicates_filtered u32 |
/// reserved [12]u8). Written as explicit LE bytes; layout pinned by test.
pub const RESULT_HEADER_SIZE: usize = 32;

pub const DIAGNOSTIC_ABI_VERSION: u8 = 1;

/// Extraction diagnostic packed into the header's reserved bytes
/// (consumer configuration; `ResultDiagnostic`, 12 bytes: version u8 | stage u8
/// | detail u8 | expected_type u8 | actual_type u8 | reserved0 u8 |
/// field_index u16 | row_index u16 | reserved1 u16).
///
/// JSON extraction threads the per-field diagnostic from the failure site;
/// the MessagePack extractor derives stage/detail from its error here.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ResultDiagnostic {
    pub stage: u8,
    pub detail: u8,
    pub expected_type: u8,
    pub actual_type: u8,
    pub field_index: u16,
    pub row_index: u16,
}

/// Diagnostic byte vocabularies and `NO_FIELD` live with the extractor that
/// populates them, keeping one source of truth for the ABI order.
pub use columine_parsing::json_extractor::{
    NO_FIELD, dedup_detail, diagnostic_detail, diagnostic_stage,
};

/// Parse-path wiring that distinguishes the two published artifact
/// configurations.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EpWiring {
    /// Exact dedup by event id against the seen-set (the consumer artifact
    /// wires it; columine does not).
    pub dedup: bool,
    /// Base 4-column scanner path for `field_count == 4` schemas
    /// (columine keeps it; the consumer artifact omits it).
    pub base_path: bool,
    /// Grow the msgpack workspace and retry on overflow (columine yes,
    /// consumer artifact no — its msgpack path errors without retry).
    pub msgpack_growth: bool,
    /// Write diagnostic bytes into the result header (consumer artifact yes).
    pub diagnostics: bool,
}

impl EpWiring {
    pub fn consumer_variant() -> Self {
        Self {
            dedup: true,
            base_path: false,
            msgpack_growth: false,
            diagnostics: true,
        }
    }

    pub fn columine() -> Self {
        Self {
            dedup: false,
            base_path: true,
            msgpack_growth: true,
            diagnostics: false,
        }
    }
}

const INITIAL_WORK_BUFFER_SIZE: usize = 64 * 1024;
const INITIAL_FALLBACK_WORK_BUFFER_SIZE: usize = 16 * 1024;
const MAX_WORK_BUFFER_SIZE: usize = MAX_VALUE_BYTES as usize;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EpInitError {
    Metadata(columine_arrow::MetadataError),
    Config(columine_parsing::ConfigError),
    SemanticSchema(columine_parsing::validate::SchemaParseError),
}

/// Structured diagnostic for a semantic payload refusal. The result header
/// carries the existing stage/detail lane; this owned value preserves the
/// path/message payload for native callers and ABI adapters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PayloadValidationDiagnostic {
    pub stage: u8,
    pub detail: u8,
    pub event_index: u32,
    pub signal_type: String,
    pub path: String,
    pub expected: String,
    pub observed: String,
}

/// The unified event processor core.
pub struct EventProcessor {
    pub wiring: EpWiring,
    pub schema_config: DynamicSchemaConfig,
    extraction_config: Option<ExtractionConfig>,
    semantic_schemas: Option<SemanticSchemaSet>,
    last_validation_diagnostic: Option<PayloadValidationDiagnostic>,
    dynamic_columns: DynamicColumns,
    /// True when the schema IS the base event log and this wiring keeps the
    /// scanner path, so the four-column scanners can write by index.
    use_base_scanners: bool,
    record_batch_metadata: MetadataStorage,
    /// One seen-set per destination log, opened by the log's own key. The
    /// processor is shared by every agent of a type, so the set that answers
    /// "was this id admitted to THIS log" cannot be the processor's: it is
    /// the log's, and a batch staged for one log never blocks another.
    logs: Vec<Option<SeenSet>>,
    free_logs: Vec<u32>,
    log_by_key: ptmcart_core::art::ArtMap<Box<[u8]>, u32>,
    policy: CollisionPolicy,
    column_capacity: u32,
    /// Signal types whose ids live in the ordinal namespace, sorted, so a
    /// row's namespace is one binary search over the type column.
    ordinal_id_types: Vec<Box<[u8]>>,
    /// Rows the discard policy keeps, one bit per row of the column plane;
    /// sized once with the columns so a batch allocates nothing to filter.
    keep_mask: Vec<u8>,
    /// Reusable MessagePack workspace for declared Binary values and
    /// internal batches.
    work_buffer: Vec<u8>,
    /// Independent workspace for an in-progress undeclared carrier map.
    fallback_work_buffer: Vec<u8>,
}

impl EventProcessor {
    /// Init with schema + field names (the primary path; names enable JSON
    /// key matching). `column_capacity` sizes the batch columns; the wasm
    /// exports build every handle at [`WASM_EVENT_CAPACITY`]. `policy` is
    /// what every seen-set opened on this processor does with a duplicate.
    pub fn new(
        wiring: EpWiring,
        column_capacity: u32,
        policy: CollisionPolicy,
        schema_config: DynamicSchemaConfig,
    ) -> Result<Self, EpInitError> {
        let semantic_schemas = schema_config
            .semantic_schema
            .as_deref()
            .map(parse_schema_envelope)
            .transpose()
            .map_err(EpInitError::SemanticSchema)?;
        let record_batch_metadata =
            MetadataStorage::for_fields(&schema_config.field_metadata, MetadataLimits::default())
                .map_err(EpInitError::Metadata)?;

        // Empty-names init keeps an empty extraction config: JSON extraction
        // cannot match keys to columns without field names.
        let extraction_config = if schema_config.field_names.is_empty() {
            None
        } else {
            let names: Vec<&str> = schema_config
                .field_names
                .iter()
                .map(String::as_str)
                .collect();
            Some(
                columine_parsing::build_extraction_config_with_semantic_schemas(
                    &schema_config.field_metadata,
                    &names,
                    semantic_schemas.as_ref(),
                    schema_config.payload_discriminator.as_ref(),
                )
                .map_err(EpInitError::Config)?,
            )
        };

        Ok(Self {
            use_base_scanners: wiring.base_path && schema_config.is_base_event_log,
            dynamic_columns: DynamicColumns::new(&schema_config.field_metadata, column_capacity),
            record_batch_metadata,
            logs: Vec::new(),
            free_logs: Vec::new(),
            log_by_key: ptmcart_core::art::ArtMap::new(),
            policy,
            column_capacity: column_capacity.min(MAX_EVENTS_PER_BATCH),
            ordinal_id_types: Vec::new(),
            keep_mask: vec![0; (column_capacity.min(MAX_EVENTS_PER_BATCH) as usize).div_ceil(8)],
            extraction_config,
            semantic_schemas,
            last_validation_diagnostic: None,
            schema_config,
            wiring,
            work_buffer: vec![0; INITIAL_WORK_BUFFER_SIZE],
            fallback_work_buffer: vec![0; INITIAL_FALLBACK_WORK_BUFFER_SIZE],
        })
    }

    /// Last semantic validation refusal, if the most recent append was refused
    /// by the schema-layer judgment.
    pub fn payload_validation_diagnostic(&self) -> Option<&PayloadValidationDiagnostic> {
        self.last_validation_diagnostic.as_ref()
    }

    /// Grow a workspace geometrically toward `MAX_WORK_BUFFER_SIZE`
    /// (`ensureWorkBufferCapacity`). Err means already at the cap.
    fn grow_work_buffer(&mut self, fallback: bool) -> Result<(), ()> {
        let buffer = if fallback {
            &mut self.fallback_work_buffer
        } else {
            &mut self.work_buffer
        };
        if buffer.len() >= MAX_WORK_BUFFER_SIZE {
            return Err(());
        }
        let target = (buffer.len() * 2).min(MAX_WORK_BUFFER_SIZE);
        buffer.resize(target, 0);
        Ok(())
    }

    /// Open (or find) the seen-set of the destination log named by `key`,
    /// with `ceiling` as the most ids it may hold; the slot it answers is
    /// what every dedup call names. A key already open keeps its set and its
    /// ceiling. `Err` when this wiring has no dedup.
    pub fn open_log(&mut self, key: &[u8], ceiling: u32) -> Result<u32, ResultCode> {
        if !self.wiring.dedup || key.is_empty() {
            return Err(ResultCode::InvalidInput);
        }
        if let Some(slot) = self.log_by_key.get(key) {
            return Ok(*slot);
        }
        let set = SeenSet::new(self.policy, ceiling, self.column_capacity);
        let slot = match self.free_logs.pop() {
            Some(slot) => {
                self.logs[slot as usize] = Some(set);
                slot
            }
            None => {
                let slot = u32::try_from(self.logs.len()).map_err(|_| ResultCode::OutOfMemory)?;
                self.logs.push(Some(set));
                slot
            }
        };
        self.log_by_key.insert(Box::from(key), slot);
        Ok(slot)
    }

    /// Close the seen-set opened for `key`, releasing what it holds.
    /// `InvalidInput` when no such log is open.
    pub fn close_log(&mut self, key: &[u8]) -> ResultCode {
        let Some(slot) = self.log_by_key.remove(key) else {
            return ResultCode::InvalidInput;
        };
        self.logs[slot as usize] = None;
        self.free_logs.push(slot);
        ResultCode::Ok
    }

    /// The seen-set at `slot`, if open.
    pub fn log(&self, slot: u32) -> Option<&SeenSet> {
        self.logs.get(slot as usize).and_then(Option::as_ref)
    }

    fn log_mut(&mut self, slot: u32) -> Option<&mut SeenSet> {
        self.logs.get_mut(slot as usize).and_then(Option::as_mut)
    }

    /// Process one input batch into `output` with no seen-set — validation
    /// and encoding only, the admission boundary's call:
    /// `[ResultHeader 32B][Arrow IPC stream]`. Returns the header's code.
    pub fn create_log_entry(
        &mut self,
        input: &[u8],
        format: InputFormat,
        output: &mut [u8],
    ) -> ResultCode {
        self.create_log_entry_for(None, input, format, output)
    }

    /// Process one input batch destined for the log at `log` (a slot from
    /// [`EventProcessor::open_log`]; `None` judges nothing): the batch is
    /// judged against that log's seen-set and staged there under the batch
    /// id the result header carries at [`BATCH_ID_OFFSET`], until
    /// [`EventProcessor::commit_batch`] or [`EventProcessor::abandon_batch`]
    /// names that id. Batches for one log may be in flight together.
    pub fn create_log_entry_for(
        &mut self,
        log: Option<u32>,
        input: &[u8],
        format: InputFormat,
        output: &mut [u8],
    ) -> ResultCode {
        if output.len() < RESULT_HEADER_SIZE {
            return ResultCode::OutOfMemory;
        }
        self.last_validation_diagnostic = None;
        let arrow_offset = RESULT_HEADER_SIZE as u32;
        if let Some((event_index, event_type, violation)) = self.validate_input(input, format) {
            self.last_validation_diagnostic = Some(PayloadValidationDiagnostic {
                stage: diagnostic_stage::VALIDATION,
                detail: diagnostic_detail::PAYLOAD_VIOLATION,
                event_index: event_index as u32,
                signal_type: event_type,
                path: violation.path,
                expected: violation.expected,
                observed: violation.observed,
            });
            if self.wiring.diagnostics {
                write_result_header_with_diagnostic(
                    output,
                    ResultCode::ParseError,
                    &ResultDiagnostic {
                        stage: diagnostic_stage::VALIDATION,
                        detail: diagnostic_detail::PAYLOAD_VIOLATION,
                        ..ResultDiagnostic::default()
                    },
                );
            } else {
                write_result_header(output, ResultCode::ParseError, 0, 0, 0, 0);
            }
            return ResultCode::ParseError;
        }

        // The scanners write the four base event-log columns by index, so the
        // schema must BE that event log — not merely have four fields.
        if self.use_base_scanners {
            return self.create_log_entry_base(input, format, output, arrow_offset);
        }
        if let Some(slot) = log
            && self.log(slot).is_none()
        {
            write_result_header(output, ResultCode::InvalidInput, 0, 0, 0, 0);
            return ResultCode::InvalidInput;
        }
        self.create_log_entry_dynamic(log, input, format, output, arrow_offset)
    }
    fn validate_input(
        &self,
        input: &[u8],
        format: InputFormat,
    ) -> Option<(usize, String, columine_parsing::validate::PayloadViolation)> {
        let schemas = self.semantic_schemas.as_ref()?;
        let result = match format {
            InputFormat::Json => validate_json_batch(input, schemas),
            InputFormat::Msgpack => validate_msgpack_batch(input, schemas, false),
            InputFormat::MsgpackStream => validate_msgpack_batch(input, schemas, true),
            InputFormat::ArrowPassthrough => return None,
        };
        match result {
            Err(BatchValidationError::Violation {
                event_index,
                event_type,
                violation,
            }) => Some((event_index, event_type, violation)),
            Err(BatchValidationError::InvalidInput) | Ok(()) => None,
        }
    }

    /// Encode one validated CPB1 column batch into `[ResultHeader][Arrow IPC]`.
    ///
    /// Validation and exact-size preflight finish before any Arrow output byte
    /// is mutated. On `EncodeError` caused by insufficient capacity, the
    /// header reports offset 32 and the exact required Arrow IPC length so the
    /// caller can retry with `32 + max(4096, arrow_len)` bytes.
    pub fn compact(&mut self, batch: &[u8], output: &mut [u8]) -> ResultCode {
        if output.len() < RESULT_HEADER_SIZE {
            return ResultCode::OutOfMemory;
        }
        let view = match CompactBatchView::parse(batch, &self.schema_config) {
            Ok(view) => view,
            Err(error) => {
                write_compact_result_header(output, error.code, 0, 0, 0, &error.diagnostic);
                return error.code;
            }
        };

        let required_ipc = match required_arrow_ipc_len(&self.schema_config, |index| {
            view.column(index, &self.schema_config)
        }) {
            Ok(required) => required,
            Err(_) => {
                let diagnostic = compact_encode_diagnostic();
                write_compact_result_header(output, ResultCode::EncodeError, 0, 0, 0, &diagnostic);
                return ResultCode::EncodeError;
            }
        };
        let Ok(required_ipc_u32) = u32::try_from(required_ipc) else {
            let diagnostic = compact_encode_diagnostic();
            write_compact_result_header(output, ResultCode::EncodeError, 0, 0, 0, &diagnostic);
            return ResultCode::EncodeError;
        };
        let Some(required_output) =
            RESULT_HEADER_SIZE.checked_add(required_ipc.max(MIN_ARROW_OUTPUT_CAPACITY))
        else {
            return ResultCode::OutOfMemory;
        };
        if output.len() < required_output {
            let diagnostic = compact_encode_diagnostic();
            write_compact_result_header(
                output,
                ResultCode::EncodeError,
                RESULT_HEADER_SIZE as u32,
                required_ipc_u32,
                0,
                &diagnostic,
            );
            return ResultCode::EncodeError;
        }

        let result = write_arrow_ipc_from_borrowed_columns(
            view.row_count(),
            &self.schema_config,
            &mut output[RESULT_HEADER_SIZE..],
            &mut self.record_batch_metadata,
            |index| view.column(index, &self.schema_config),
            |index| view.null_count(index),
        );
        match result {
            Ok(written) => {
                let written = u32::try_from(written)
                    .unwrap_or_else(|_| columine_types::die!("compact IPC length exceeds u32"));
                write_result_header(
                    output,
                    ResultCode::Ok,
                    RESULT_HEADER_SIZE as u32,
                    written,
                    view.row_count(),
                    0,
                );
                ResultCode::Ok
            }
            Err(
                IpcError::BufferTooSmall { .. } | IpcError::InvalidColumn | IpcError::SizeOverflow,
            ) => {
                let diagnostic = compact_encode_diagnostic();
                write_compact_result_header(
                    output,
                    ResultCode::EncodeError,
                    RESULT_HEADER_SIZE as u32,
                    required_ipc_u32,
                    0,
                    &diagnostic,
                );
                ResultCode::EncodeError
            }
        }
    }

    /// BASE PATH (columine npm variant): the base event-log scanners, which
    /// write the four `BASE_EVENT_LOG_FIELDS` columns by index. Selected only
    /// when the schema IS that event log, and it shares the one column store
    /// and the one IPC writer with the extraction path.
    fn create_log_entry_base(
        &mut self,
        input: &[u8],
        format: InputFormat,
        output: &mut [u8],
        arrow_offset: u32,
    ) -> ResultCode {
        let cols = &mut self.dynamic_columns;
        cols.reset();

        let parse_result = match format {
            InputFormat::Json => {
                json_scanner::parse_json_events(input, cols).map_err(|_| ResultCode::ParseError)
            }
            InputFormat::Msgpack => msgpack_scanner::parse_msgpack_events(input, cols)
                .map_err(|_| ResultCode::ParseError),
            InputFormat::MsgpackStream => msgpack_scanner::parse_msgpack_stream(input, cols)
                .map_err(|_| ResultCode::ParseError),
            // Arrow passthrough is unsupported on the base path.
            InputFormat::ArrowPassthrough => Err(ResultCode::InvalidFormat),
        };
        if parse_result.is_err() {
            write_result_header(output, ResultCode::ParseError, 0, 0, 0, 0);
            return ResultCode::ParseError;
        }

        // No dedup in columine — all events are processed.
        let processed = self.dynamic_columns.count;
        match write_arrow_ipc_from_dynamic_columns(
            &self.dynamic_columns,
            &self.schema_config,
            &mut output[arrow_offset as usize..],
            &mut self.record_batch_metadata,
        ) {
            Ok(len) => {
                write_result_header(
                    output,
                    ResultCode::Ok,
                    arrow_offset,
                    len as u32,
                    processed,
                    0,
                );
                ResultCode::Ok
            }
            Err(
                IpcError::BufferTooSmall { .. } | IpcError::InvalidColumn | IpcError::SizeOverflow,
            ) => {
                write_result_header(output, ResultCode::EncodeError, 0, 0, 0, 0);
                ResultCode::EncodeError
            }
        }
    }

    /// EXTRACTION PATH: extractors into `DynamicColumns`, optional dedup,
    /// dynamic writer.
    fn create_log_entry_dynamic(
        &mut self,
        log: Option<u32>,
        input: &[u8],
        format: InputFormat,
        output: &mut [u8],
        arrow_offset: u32,
    ) -> ResultCode {
        if format == InputFormat::ArrowPassthrough {
            write_result_header(output, ResultCode::InvalidFormat, 0, 0, 0, 0);
            return ResultCode::InvalidFormat;
        }
        let Some(config) = self.extraction_config.take() else {
            // No field names: JSON keys cannot be matched to columns.
            write_result_header(output, ResultCode::ParseError, 0, 0, 0, 0);
            return ResultCode::ParseError;
        };

        let mut extraction_diagnostic = json_extractor::ExtractionDiagnostic::default();
        let extract_result =
            self.extract_with_growth(input, format, &config, &mut extraction_diagnostic);
        self.extraction_config = Some(config);

        if let Err(err) = extract_result {
            let code = match err {
                json_extractor::ExtractionError::OutOfMemory => ResultCode::OutOfMemory,
                _ => ResultCode::ParseError,
            };
            if self.wiring.diagnostics {
                // JSON carries a per-field diagnostic; MessagePack derives
                // stage/detail from its error at this level.
                let diagnostic = if format == InputFormat::Json {
                    ResultDiagnostic {
                        stage: extraction_diagnostic.stage,
                        detail: extraction_diagnostic.detail,
                        expected_type: extraction_diagnostic.expected_type,
                        actual_type: extraction_diagnostic.actual_type,
                        field_index: extraction_diagnostic.field_index,
                        row_index: extraction_diagnostic.row_index,
                    }
                } else {
                    ResultDiagnostic {
                        stage: diagnostic_stage::MSGPACK,
                        detail: match err {
                            json_extractor::ExtractionError::OutOfMemory => {
                                diagnostic_detail::OUT_OF_MEMORY
                            }
                            json_extractor::ExtractionError::BufferOverflow => {
                                diagnostic_detail::BUFFER_OVERFLOW
                            }
                            _ => diagnostic_detail::INVALID_JSON,
                        },
                        expected_type: 0,
                        actual_type: 0,
                        field_index: NO_FIELD,
                        row_index: 0,
                    }
                };
                write_result_header_with_diagnostic(output, code, &diagnostic);
            } else {
                write_result_header(output, code, 0, 0, 0, 0);
            }
            return code;
        }

        // Dedup: judge event ids from column 0 (type from column 1) against
        // the destination log's seen-set; discarded rows leave the batch.
        let seen = log.and_then(|slot| self.logs.get_mut(slot as usize).and_then(Option::as_mut));
        let (batch, processed, duplicates) = match judge_batch(
            seen,
            &self.ordinal_id_types,
            &mut self.dynamic_columns,
            &mut self.keep_mask,
        ) {
            Ok(counts) => counts,
            Err(diagnostic) => {
                let code = ResultCode::AdmissionRefused;
                if self.wiring.diagnostics {
                    write_result_header_with_diagnostic(output, code, &diagnostic);
                } else {
                    write_result_header(output, code, 0, 0, 0, 0);
                }
                return code;
            }
        };

        match write_arrow_ipc_from_dynamic_columns(
            &self.dynamic_columns,
            &self.schema_config,
            &mut output[arrow_offset as usize..],
            &mut self.record_batch_metadata,
        ) {
            Ok(len) => {
                write_result_header(
                    output,
                    ResultCode::Ok,
                    arrow_offset,
                    len as u32,
                    processed,
                    duplicates,
                );
                write_batch_id(output, batch);
                ResultCode::Ok
            }
            Err(
                IpcError::BufferTooSmall { .. } | IpcError::InvalidColumn | IpcError::SizeOverflow,
            ) => {
                // The batch stays staged for nothing: the caller never
                // learns its id, so it is retracted here.
                if let Some(seen) = log.and_then(|slot| self.log_mut(slot)) {
                    let _ = seen.abandon(batch);
                }
                write_result_header(output, ResultCode::EncodeError, 0, 0, 0, 0);
                ResultCode::EncodeError
            }
        }
    }

    /// Extraction with workspace growth-and-retry
    /// (`extractJsonEventsWithWorkspaceGrowth` and columine's msgpack
    /// counterpart): on BufferOverflow the (fallback) workspace doubles and
    /// the batch re-extracts from a reset column set; at the cap the error
    /// surfaces.
    ///
    fn extract_with_growth(
        &mut self,
        input: &[u8],
        format: InputFormat,
        config: &ExtractionConfig,
        diagnostic: &mut json_extractor::ExtractionDiagnostic,
    ) -> Result<usize, json_extractor::ExtractionError> {
        loop {
            self.dynamic_columns.reset();
            let result = match format {
                InputFormat::Json => json_extractor::extract_json_events(
                    input,
                    config,
                    &mut self.dynamic_columns,
                    &mut self.fallback_work_buffer,
                    diagnostic,
                ),
                InputFormat::Msgpack | InputFormat::MsgpackStream => {
                    msgpack_extractor::extract_msgpack_events(
                        input,
                        config,
                        &mut self.dynamic_columns,
                        &mut self.work_buffer,
                        format == InputFormat::MsgpackStream,
                    )
                }
                InputFormat::ArrowPassthrough => {
                    columine_types::die!("passthrough handled by caller")
                }
            };
            match result {
                Err(json_extractor::ExtractionError::BufferOverflow) => {
                    let fallback = format == InputFormat::Json;
                    // Retry only workspace overflow; a column-limit overflow
                    // surfaces immediately.
                    let workspace_overflow = !fallback
                        || (diagnostic.stage == diagnostic_stage::MSGPACK
                            && diagnostic.detail == diagnostic_detail::BUFFER_OVERFLOW);
                    let may_grow = workspace_overflow && (fallback || self.wiring.msgpack_growth);
                    if !may_grow || self.grow_work_buffer(fallback).is_err() {
                        return Err(json_extractor::ExtractionError::BufferOverflow);
                    }
                }
                other => return other,
            }
        }
    }

    /// Bytes a checkpoint of the log's seen-set takes right now; 0 when no
    /// such log is open.
    pub fn checkpoint_len(&self, log: u32) -> usize {
        self.log(log).map_or(0, SeenSet::checkpoint_len)
    }

    /// Checkpoint the log's seen-set into `output`; 0 = error (no such log
    /// or buffer too small), matching `ep_checkpoint`'s sentinel.
    pub fn checkpoint(&self, log: u32, output: &mut [u8]) -> usize {
        let Some(seen) = self.log(log) else {
            return 0;
        };
        seen.checkpoint(output).unwrap_or(0)
    }

    /// Restore the log's seen-set from checkpoint bytes (`ep_restore`). The
    /// set keeps the ceiling it was opened with; a checkpoint that does not
    /// fit under it is refused with the set untouched.
    pub fn restore(&mut self, log: u32, input: &[u8]) -> ResultCode {
        let Some(seen) = self.log_mut(log) else {
            return ResultCode::InvalidInput;
        };
        match seen.restore(input) {
            Ok(()) => ResultCode::Ok,
            Err(dedup::checkpoint::DeserializeError::ExceedsCeiling) => {
                ResultCode::AdmissionRefused
            }
            Err(_) => ResultCode::ParseError,
        }
    }

    /// Packed dedup stats of the log's seen-set (`ep_get_stats`):
    /// `total_events as u32 | duplicates as u32 << 32` (u32 truncation is ABI).
    pub fn stats(&self, log: u32) -> u64 {
        let Some(seen) = self.log(log) else {
            return 0;
        };
        let total = seen.total_events as u32;
        let dupes = seen.duplicates_detected as u32;
        u64::from(total) | (u64::from(dupes) << 32)
    }

    /// Bind the log's in-flight `batch` to the tx the log assigned it.
    /// `InvalidInput` when no such log is open, `UnknownBatch` when the log
    /// has no such batch in flight.
    pub fn commit_batch(&mut self, log: u32, batch: u32, tx: u64) -> ResultCode {
        let Some(seen) = self.log_mut(log) else {
            return ResultCode::InvalidInput;
        };
        match seen.commit(batch, tx) {
            Ok(()) => ResultCode::Ok,
            Err(_) => ResultCode::UnknownBatch,
        }
    }

    /// Retract the log's in-flight `batch`: the append did not happen.
    pub fn abandon_batch(&mut self, log: u32, batch: u32) -> ResultCode {
        let Some(seen) = self.log_mut(log) else {
            return ResultCode::InvalidInput;
        };
        match seen.abandon(batch) {
            Ok(()) => ResultCode::Ok,
            Err(_) => ResultCode::UnknownBatch,
        }
    }

    /// Evict every admission of the log below the redelivery horizon; the
    /// count that left the set, 0 when no such log is open.
    pub fn cut_below(&mut self, log: u32, horizon: u64) -> u32 {
        self.log_mut(log).map_or(0, |seen| seen.cut_below(horizon))
    }

    /// The tx that admitted `id` to the log: the byproduct read of the
    /// seen-set.
    pub fn admitted_tx(&self, log: u32, id: &[u8], namespace: IdNamespace) -> AdmittedTx {
        let Some(seen) = self.log(log) else {
            return AdmittedTx::Absent;
        };
        match namespace {
            IdNamespace::Bytes => seen
                .admitted_tx(id)
                .map_or(AdmittedTx::Absent, AdmittedTx::At),
            IdNamespace::Ordinal if seen.contains_ordinal(id) => AdmittedTx::Member,
            IdNamespace::Ordinal => AdmittedTx::Absent,
        }
    }

    /// Declare the signal types whose ids live in the ordinal namespace:
    /// NUL-separated names, declared once per processor before any log is
    /// opened.
    pub fn declare_ordinal_id_types(&mut self, names: &[u8]) -> ResultCode {
        if !self.wiring.dedup
            || !self.ordinal_id_types.is_empty()
            || !self.logs.is_empty()
            || names.is_empty()
        {
            return ResultCode::InvalidInput;
        }
        let mut types: Vec<Box<[u8]>> = names
            .split(|byte| *byte == 0)
            .filter(|name| !name.is_empty())
            .map(Box::from)
            .collect();
        if types.is_empty() {
            return ResultCode::InvalidInput;
        }
        types.sort_unstable();
        types.dedup();
        self.ordinal_id_types = types;
        ResultCode::Ok
    }

    /// Judge the rows of a column plane that is not this processor's own —
    /// a transport lane that shreds its own frames — against the same
    /// seen-set, the same declared namespaces, and the same keep mask, so no
    /// two ingest paths can disagree about what a duplicate is.
    pub fn judge_rows(
        &mut self,
        log: Option<u32>,
        columns: &mut DynamicColumns,
    ) -> Result<(u32, u32, u32), ResultDiagnostic> {
        let seen = log.and_then(|slot| self.logs.get_mut(slot as usize).and_then(Option::as_mut));
        judge_batch(seen, &self.ordinal_id_types, columns, &mut self.keep_mask)
    }

    /// The namespace a row's type column selects.
    pub fn id_namespace_of(&self, signal_type: &[u8]) -> IdNamespace {
        id_namespace_of(&self.ordinal_id_types, signal_type)
    }

    /// The declared ordinal-namespace signal types, sorted.
    pub fn ordinal_id_types(&self) -> &[Box<[u8]>] {
        &self.ordinal_id_types
    }
}

/// Answer of [`EventProcessor::admitted_tx`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum AdmittedTx {
    /// Not in the set (or staged, which is not admitted).
    Absent,
    /// Admitted by the entry at this tx.
    At(u64),
    /// A member of the ordinal carrier, which keeps membership only.
    Member,
}

fn id_namespace_of(ordinal_id_types: &[Box<[u8]>], signal_type: &[u8]) -> IdNamespace {
    if ordinal_id_types.is_empty() {
        return IdNamespace::Bytes;
    }
    match ordinal_id_types.binary_search_by(|name| name.as_ref().cmp(signal_type)) {
        Ok(_) => IdNamespace::Ordinal,
        Err(_) => IdNamespace::Bytes,
    }
}

/// Judge every row of `columns` against the seen-set as one new batch:
/// `(batch, processed, duplicates)` on success, with discarded rows
/// compacted out of the batch and the batch staged under `batch`; on
/// refusal the diagnostic names the row and the cause, and the set is as it
/// was before the batch. `None` seen-set means no dedup is wired, and the
/// batch id is 0.
pub fn judge_batch(
    seen: Option<&mut SeenSet>,
    ordinal_id_types: &[Box<[u8]>],
    columns: &mut DynamicColumns,
    keep_mask: &mut [u8],
) -> Result<(u32, u32, u32), ResultDiagnostic> {
    let Some(seen) = seen else {
        return Ok((0, columns.count, 0));
    };
    let batch = seen.next_batch();
    let discard = seen.policy() == CollisionPolicy::Discard;
    let mut processed = 0u32;
    let mut duplicates = 0u32;
    keep_mask.fill(0);
    for row in 0..columns.count {
        let event_id = columns.columns[0]
            .read_variable(row)
            .unwrap_or_else(|| columine_types::die!("id column is not variable-width"));
        let namespace = if ordinal_id_types.is_empty() {
            IdNamespace::Bytes
        } else {
            let signal_type = columns.columns[1]
                .read_variable(row)
                .unwrap_or_else(|| columine_types::die!("type column is not variable-width"));
            id_namespace_of(ordinal_id_types, signal_type)
        };
        match seen.should_process(batch, event_id, namespace) {
            Ok(true) => {
                processed += 1;
                keep_mask[row as usize / 8] |= 1u8 << (row % 8);
            }
            Ok(false) => duplicates += 1,
            Err(refusal) => {
                // The batch is open only once a row was judged; a refusal
                // on its first row has nothing to retract.
                let _ = seen.abandon(batch);
                return Err(refusal_diagnostic(refusal, row));
            }
        }
    }
    if discard && duplicates > 0 {
        columns.retain_rows(keep_mask);
    }
    Ok((batch, processed, duplicates))
}

/// Offset of the batch id in a successful result header: the four bytes the
/// diagnostic lane occupies on failure, unused on success until now.
pub const BATCH_ID_OFFSET: usize = 20;

/// Record the batch id a successful judged entry was staged under.
pub fn write_batch_id(output: &mut [u8], batch: u32) {
    output[BATCH_ID_OFFSET..BATCH_ID_OFFSET + 4].copy_from_slice(&batch.to_le_bytes());
}

/// The batch id a successful judged entry was staged under (0 when no log
/// was named).
pub fn read_batch_id(output: &[u8]) -> u32 {
    u32::from_le_bytes([
        output[BATCH_ID_OFFSET],
        output[BATCH_ID_OFFSET + 1],
        output[BATCH_ID_OFFSET + 2],
        output[BATCH_ID_OFFSET + 3],
    ])
}

fn refusal_diagnostic(refusal: AdmissionRefusal, row: u32) -> ResultDiagnostic {
    let detail = match refusal {
        AdmissionRefusal::IdTooLong { .. } => dedup_detail::ID_TOO_LONG,
        AdmissionRefusal::CeilingReached { .. } => dedup_detail::CEILING_REACHED,
        AdmissionRefusal::NotAnOrdinal => dedup_detail::NOT_AN_ORDINAL,
        AdmissionRefusal::UnknownBatch { .. } => dedup_detail::BATCH_UNKNOWN,
    };
    ResultDiagnostic {
        stage: diagnostic_stage::DEDUP,
        detail,
        expected_type: 0,
        actual_type: 0,
        field_index: 0,
        row_index: u16::try_from(row).unwrap_or(u16::MAX),
    }
}

/// Write the 32-byte result header (`writeResultHeader`).
pub fn write_result_header(
    output: &mut [u8],
    code: ResultCode,
    arrow_offset: u32,
    arrow_len: u32,
    events_processed: u32,
    duplicates_filtered: u32,
) {
    output[0..4].copy_from_slice(&(code as u32).to_le_bytes());
    output[4..8].copy_from_slice(&arrow_offset.to_le_bytes());
    output[8..12].copy_from_slice(&arrow_len.to_le_bytes());
    output[12..16].copy_from_slice(&events_processed.to_le_bytes());
    output[16..20].copy_from_slice(&duplicates_filtered.to_le_bytes());
    output[20..32].fill(0);
}

fn write_diagnostic_bytes(output: &mut [u8], diagnostic: &ResultDiagnostic) {
    output[20] = DIAGNOSTIC_ABI_VERSION;
    output[21] = diagnostic.stage;
    output[22] = diagnostic.detail;
    output[23] = diagnostic.expected_type;
    output[24] = diagnostic.actual_type;
    output[25] = 0;
    output[26..28].copy_from_slice(&diagnostic.field_index.to_le_bytes());
    output[28..30].copy_from_slice(&diagnostic.row_index.to_le_bytes());
    output[30..32].fill(0);
}

pub fn write_compact_result_header(
    output: &mut [u8],
    code: ResultCode,
    arrow_offset: u32,
    arrow_len: u32,
    rows: u32,
    diagnostic: &ResultDiagnostic,
) {
    write_result_header(output, code, arrow_offset, arrow_len, rows, 0);
    write_diagnostic_bytes(output, diagnostic);
}

fn compact_encode_diagnostic() -> ResultDiagnostic {
    ResultDiagnostic {
        stage: COMPACT_DIAGNOSTIC_STAGE,
        detail: 0,
        expected_type: 0,
        actual_type: 0,
        field_index: NO_FIELD,
        row_index: 0,
    }
}

/// Write the header with the diagnostic packed into the reserved bytes
/// (`writeResultHeaderWithDiagnostic`, consumer artifact).
pub fn write_result_header_with_diagnostic(
    output: &mut [u8],
    code: ResultCode,
    diagnostic: &ResultDiagnostic,
) {
    write_result_header(output, code, 0, 0, 0, 0);
    write_diagnostic_bytes(output, diagnostic);
}

/// Read back the header fields (test/consumer view).
pub fn read_result_header(output: &[u8]) -> (u32, u32, u32, u32, u32) {
    let f = |at: usize| u32::from_le_bytes(output[at..at + 4].try_into().unwrap_or([0; 4]));
    (f(0), f(4), f(8), f(12), f(16))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Array, NullArray};
    use arrow_ipc::reader::StreamReader;
    use columine_arrow::{ArrowType, SignalSchemaField};
    use std::io::Cursor;

    fn base_fields() -> Vec<SignalSchemaField> {
        vec![
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new(ArrowType::Int64, false),
            SignalSchemaField::new(ArrowType::Binary, true),
        ]
    }

    fn schema_with_names(fields: &[SignalSchemaField], names: &[u8]) -> DynamicSchemaConfig {
        DynamicSchemaConfig::from_physical_fields_with_names(fields, names).unwrap()
    }

    #[test]
    fn compact_null_rows_ignore_parse_column_capacity_and_round_trip() {
        let fields = [SignalSchemaField::new(ArrowType::Null, true)];
        let schema = schema_with_names(&fields, b"null\0");
        let mut processor =
            EventProcessor::new(EpWiring::columine(), 1, CollisionPolicy::Latest, schema).unwrap();

        let mut request = vec![0u8; COMPACT_HEADER_SIZE + COMPACT_DESCRIPTOR_SIZE];
        request[0..4].copy_from_slice(&COMPACT_BATCH_MAGIC.to_le_bytes());
        request[4..6].copy_from_slice(&COMPACT_ABI_VERSION.to_le_bytes());
        request[6..8].copy_from_slice(&(COMPACT_DESCRIPTOR_SIZE as u16).to_le_bytes());
        request[8..12].copy_from_slice(&3u32.to_le_bytes());
        request[12..16].copy_from_slice(&1u32.to_le_bytes());
        request[COMPACT_HEADER_SIZE] = ArrowType::Null as u8;

        let mut output = vec![0u8; 8192];
        assert_eq!(processor.compact(&request, &mut output), ResultCode::Ok);
        let (code, arrow_offset, arrow_len, rows, duplicates) = read_result_header(&output);
        assert_eq!(
            (code, arrow_offset, rows, duplicates),
            (ResultCode::Ok as u32, RESULT_HEADER_SIZE as u32, 3, 0)
        );

        let start = arrow_offset as usize;
        let end = start + arrow_len as usize;
        let mut reader = StreamReader::try_new(Cursor::new(&output[start..end]), None).unwrap();
        let batch = reader.next().unwrap().unwrap();
        let nulls = batch
            .column(0)
            .as_any()
            .downcast_ref::<NullArray>()
            .unwrap();
        assert_eq!(nulls.len(), 3);
        assert_eq!(nulls.logical_null_count(), 3);
        assert!(reader.next().is_none());
    }

    // test "ep_create_log_entry with schema" (consumer variant)
    #[test]
    fn create_log_entry_consumer_variant_json() {
        let schema = schema_with_names(&base_fields(), b"id\0type\0timestamp\0value\0");
        let mut ep = EventProcessor::new(
            EpWiring::consumer_variant(),
            100,
            CollisionPolicy::Latest,
            schema,
        )
        .unwrap();
        let input = br#"[{"id":"test","type":"click","timestamp":1705315800000000}]"#;
        let mut output = vec![0u8; 64 * 1024];
        assert_eq!(
            ep.create_log_entry(input, InputFormat::Json, &mut output),
            ResultCode::Ok
        );
        let (code, arrow_offset, arrow_len, processed, dupes) = read_result_header(&output);
        assert_eq!(code, 0);
        assert_eq!(arrow_offset, 32);
        assert!(arrow_len > 0);
        assert_eq!(processed, 1);
        assert_eq!(dupes, 0);
    }

    // test "ep_create_log_entry with schema" (columine base-path variant)
    #[test]
    fn create_log_entry_columine_base_path() {
        let schema = schema_with_names(&base_fields(), b"id\0type\0timestamp\0value\0");
        assert!(schema.is_base_event_log);
        let mut ep =
            EventProcessor::new(EpWiring::columine(), 100, CollisionPolicy::Latest, schema)
                .unwrap();
        assert!(ep.use_base_scanners);
        let input =
            br#"[{"id":"a-1","type":"click","timestamp":100,"value":{"x":1}},{"id":"a-2","type":"view","timestamp":200}]"#;
        let mut output = vec![0u8; 64 * 1024];
        assert_eq!(
            ep.create_log_entry(input, InputFormat::Json, &mut output),
            ResultCode::Ok
        );
        let (code, _, arrow_len, processed, dupes) = read_result_header(&output);
        assert_eq!(code, 0);
        assert!(arrow_len > 0);
        assert_eq!(processed, 2);
        assert_eq!(dupes, 0);
        // No dedup wired: a log cannot be opened, and no slot answers.
        assert_eq!(ep.open_log(b"log", 10), Err(ResultCode::InvalidInput));
        assert_eq!(ep.stats(0), 0);
    }

    /// Base and extraction paths emit byte-identical IPC for the same
    /// four-column content. Their representation differences are compensated:
    /// the base JSON scanner converts numeric timestamps ms→µs (×1000), while
    /// extraction stores them raw; the base path stores raw JSON value bytes,
    /// while extraction stores typed MessagePack. The shared test case
    /// pre-scales the extraction timestamp.
    #[test]
    fn base_and_dynamic_pipelines_agree() {
        let names = b"id\0type\0timestamp\0value\0";
        let input = br#"[{"id":"a-1","type":"click","timestamp":100}]"#;
        let dyn_input = br#"[{"id":"a-1","type":"click","timestamp":100000}]"#;

        let mut base_ep = EventProcessor::new(
            EpWiring::columine(),
            10,
            CollisionPolicy::Latest,
            schema_with_names(&base_fields(), names),
        )
        .unwrap();
        let mut base_out = vec![0u8; 64 * 1024];
        assert_eq!(
            base_ep.create_log_entry(input, InputFormat::Json, &mut base_out),
            ResultCode::Ok
        );

        let mut dyn_ep = EventProcessor::new(
            EpWiring::consumer_variant(),
            10,
            CollisionPolicy::Latest,
            schema_with_names(&base_fields(), names),
        )
        .unwrap();
        let mut dyn_out = vec![0u8; 64 * 1024];
        assert_eq!(
            dyn_ep.create_log_entry(dyn_input, InputFormat::Json, &mut dyn_out),
            ResultCode::Ok
        );

        let (_, base_off, base_len, ..) = read_result_header(&base_out);
        let (_, dyn_off, dyn_len, ..) = read_result_header(&dyn_out);
        assert_eq!(base_len, dyn_len);
        assert_eq!(
            base_out[base_off as usize..(base_off + base_len) as usize],
            dyn_out[dyn_off as usize..(dyn_off + dyn_len) as usize]
        );
    }

    // test "parse and extraction errors map to explicit result codes"
    #[test]
    fn error_result_codes() {
        let schema = schema_with_names(&base_fields(), b"id\0type\0timestamp\0value\0");
        let mut ep = EventProcessor::new(
            EpWiring::consumer_variant(),
            10,
            CollisionPolicy::Latest,
            schema,
        )
        .unwrap();
        let mut output = vec![0u8; 64 * 1024];
        assert_eq!(
            ep.create_log_entry(b"not json", InputFormat::Json, &mut output),
            ResultCode::ParseError
        );
        let (code, ..) = read_result_header(&output);
        assert_eq!(code, ResultCode::ParseError as u32);
        // Diagnostic bytes occupy the reserved region for consumer wiring.
        assert_eq!(output[20], DIAGNOSTIC_ABI_VERSION);
        assert_eq!(output[21], diagnostic_stage::JSON);

        assert_eq!(
            ep.create_log_entry(b"[]", InputFormat::ArrowPassthrough, &mut output),
            ResultCode::InvalidFormat
        );
    }

    /// The consumer wiring judges a batch against the destination log's own
    /// seen-set — a batch staged for one log never blocks another — drops
    /// discarded duplicates from the batch, counts them in the header, and
    /// the committed set survives a checkpoint/restore round trip into a
    /// log opened with its own ceiling.
    #[test]
    fn dedup_and_checkpoint_through_ep() {
        let schema = schema_with_names(&base_fields(), b"id\0type\0timestamp\0value\0");
        let mut ep = EventProcessor::new(
            EpWiring::consumer_variant(),
            100,
            CollisionPolicy::Discard,
            schema,
        )
        .unwrap();
        let input = br#"[{"id":"dup","type":"a","timestamp":1},{"id":"dup","type":"a","timestamp":2},{"id":"uniq","type":"a","timestamp":3}]"#;
        let mut output = vec![0u8; 64 * 1024];
        // With no log named, the batch is validated and encoded, nothing
        // judged: the admission boundary's call.
        assert_eq!(
            ep.create_log_entry(input, InputFormat::Json, &mut output),
            ResultCode::Ok
        );
        let header = read_result_header(&output);
        assert_eq!((header.3, header.4), (3, 0));
        let log = ep.open_log(b"log-a", 100).unwrap();
        assert_eq!(ep.open_log(b"log-a", 5).unwrap(), log, "a key opens once");
        let other = ep.open_log(b"log-b", 100).unwrap();
        assert_ne!(log, other);
        assert_eq!(
            ep.create_log_entry_for(Some(log), input, InputFormat::Json, &mut output),
            ResultCode::Ok
        );
        let (_, arrow_offset, arrow_len, processed, dupes) = read_result_header(&output);
        assert_eq!(processed, 2);
        assert_eq!(dupes, 1);
        // The discarded row is not in the batch: only two ids reach the body.
        let body = &output[arrow_offset as usize..(arrow_offset + arrow_len) as usize];
        assert_eq!(body.windows(3).filter(|w| *w == b"dup").count(), 1);
        assert!(body.windows(4).any(|w| w == b"uniq"));
        let first = read_batch_id(&output);
        assert_ne!(first, 0, "a judged entry names its batch");
        // Stats and the checkpoint see only committed batches.
        assert_eq!(ep.stats(log), 0);
        assert_eq!(ep.checkpoint_len(log), dedup::checkpoint::HEADER_SIZE);
        // A second writer's batch on the same log is in flight beside the
        // first: it sees the staged ids as duplicates and gets its own id.
        assert_eq!(
            ep.create_log_entry_for(Some(log), input, InputFormat::Json, &mut output),
            ResultCode::Ok,
        );
        let second = read_batch_id(&output);
        assert!(second != 0 && second != first);
        assert_eq!(
            read_result_header(&output).4,
            3,
            "every id is staged by the first batch"
        );
        assert_eq!(ep.abandon_batch(log, second), ResultCode::Ok);
        assert_eq!(ep.abandon_batch(log, second), ResultCode::UnknownBatch);
        // Another log's batch is its own.
        assert_eq!(
            ep.create_log_entry_for(Some(other), input, InputFormat::Json, &mut output),
            ResultCode::Ok
        );
        let others = read_batch_id(&output);
        assert_eq!(ep.abandon_batch(other, others), ResultCode::Ok);
        assert_eq!(ep.commit_batch(log, first, 7), ResultCode::Ok);
        assert_eq!(ep.commit_batch(log, first, 7), ResultCode::UnknownBatch);
        assert_eq!(ep.stats(log), 3 | (1 << 32));
        assert_eq!(
            ep.admitted_tx(log, b"dup", IdNamespace::Bytes),
            AdmittedTx::At(7)
        );
        assert_eq!(
            ep.admitted_tx(other, b"dup", IdNamespace::Bytes),
            AdmittedTx::Absent
        );
        assert_eq!(
            ep.admitted_tx(log, b"nope", IdNamespace::Bytes),
            AdmittedTx::Absent
        );

        let mut checkpoint_buf = vec![0u8; 8192];
        let size = ep.checkpoint(log, &mut checkpoint_buf);
        assert_eq!(size, ep.checkpoint_len(log));

        let schema2 = schema_with_names(&base_fields(), b"id\0type\0timestamp\0value\0");
        let mut restored = EventProcessor::new(
            EpWiring::consumer_variant(),
            100,
            CollisionPolicy::Discard,
            schema2,
        )
        .unwrap();
        let rlog = restored.open_log(b"log-a", 100).unwrap();
        assert_eq!(
            restored.restore(rlog, &checkpoint_buf[..size]),
            ResultCode::Ok
        );
        // The restored set still knows both ids, at the tx that admitted them.
        assert_eq!(
            restored.admitted_tx(rlog, b"dup", IdNamespace::Bytes),
            AdmittedTx::At(7)
        );
        assert_eq!(
            restored.admitted_tx(rlog, b"uniq", IdNamespace::Bytes),
            AdmittedTx::At(7)
        );
        assert_eq!(restored.stats(rlog), 3 | (1 << 32));
        // Cutting below the horizon evicts them; the ids are new again.
        assert_eq!(restored.cut_below(rlog, 8), 2);
        assert_eq!(
            restored.admitted_tx(rlog, b"dup", IdNamespace::Bytes),
            AdmittedTx::Absent
        );
        // Closing the log frees its slot for the next key.
        assert_eq!(restored.close_log(b"log-a"), ResultCode::Ok);
        assert_eq!(restored.close_log(b"log-a"), ResultCode::InvalidInput);
        assert_eq!(restored.open_log(b"log-c", 100).unwrap(), rlog);

        // A ceiling the checkpoint does not fit under refuses the restore.
        let schema3 = schema_with_names(&base_fields(), b"id\0type\0timestamp\0value\0");
        let mut tight = EventProcessor::new(
            EpWiring::consumer_variant(),
            100,
            CollisionPolicy::Discard,
            schema3,
        )
        .unwrap();
        let tlog = tight.open_log(b"log-a", 1).unwrap();
        assert_eq!(
            tight.restore(tlog, &checkpoint_buf[..size]),
            ResultCode::AdmissionRefused
        );
    }

    /// An id past the 64-byte bound refuses the whole batch with the row
    /// and cause in the diagnostic bytes, and the set is untouched.
    #[test]
    fn admission_refusal_names_the_row_and_leaves_the_set_untouched() {
        let schema = schema_with_names(&base_fields(), b"id\0type\0timestamp\0value\0");
        let mut ep = EventProcessor::new(
            EpWiring::consumer_variant(),
            100,
            CollisionPolicy::Discard,
            schema,
        )
        .unwrap();
        let long = "x".repeat(MAX_ID_BYTES + 1);
        let input = format!(
            r#"[{{"id":"fine","type":"a","timestamp":1}},{{"id":"{long}","type":"a","timestamp":2}}]"#
        );
        let mut output = vec![0u8; 64 * 1024];
        let log = ep.open_log(b"log", 100).unwrap();
        assert_eq!(
            ep.create_log_entry_for(Some(log), input.as_bytes(), InputFormat::Json, &mut output),
            ResultCode::AdmissionRefused
        );
        assert_eq!(output[21], diagnostic_stage::DEDUP);
        assert_eq!(output[22], dedup_detail::ID_TOO_LONG);
        assert_eq!(u16::from_le_bytes([output[28], output[29]]), 1);
        let seen = ep.log(log).unwrap();
        assert!(seen.is_empty());
        assert_eq!(seen.open_batches(), 0);
        // A slot that was never opened is refused by name, not judged.
        assert_eq!(
            ep.create_log_entry_for(Some(99), input.as_bytes(), InputFormat::Json, &mut output),
            ResultCode::InvalidInput
        );

        // The ceiling refuses at admission, the same way.
        let small = ep.open_log(b"small", 1).unwrap();
        let two = br#"[{"id":"a","type":"a","timestamp":1},{"id":"b","type":"a","timestamp":2}]"#;
        assert_eq!(
            ep.create_log_entry_for(Some(small), two, InputFormat::Json, &mut output),
            ResultCode::AdmissionRefused
        );
        assert_eq!(output[22], dedup_detail::CEILING_REACHED);
        assert!(ep.log(small).unwrap().is_empty());
    }

    /// A signal type declared in the ordinal namespace judges its ids as
    /// ordinals; every other type keeps the byte namespace.
    #[test]
    fn ordinal_namespace_is_selected_by_the_type_column() {
        let schema = schema_with_names(&base_fields(), b"id\0type\0timestamp\0value\0");
        let mut ep = EventProcessor::new(
            EpWiring::consumer_variant(),
            100,
            CollisionPolicy::Discard,
            schema,
        )
        .unwrap();
        assert_eq!(ep.declare_ordinal_id_types(b"tick\0"), ResultCode::Ok);
        assert_eq!(
            ep.declare_ordinal_id_types(b"tock\0"),
            ResultCode::InvalidInput,
            "declared once per processor"
        );
        let log = ep.open_log(b"log", 100).unwrap();
        let input = br#"[{"id":"7","type":"tick","timestamp":1},{"id":"7","type":"other","timestamp":2},{"id":"7","type":"tick","timestamp":3}]"#;
        let mut output = vec![0u8; 64 * 1024];
        assert_eq!(
            ep.create_log_entry_for(Some(log), input, InputFormat::Json, &mut output),
            ResultCode::Ok
        );
        let (_, _, _, processed, dupes) = read_result_header(&output);
        assert_eq!((processed, dupes), (2, 1));
        assert_eq!(
            ep.commit_batch(log, read_batch_id(&output), 1),
            ResultCode::Ok
        );
        assert_eq!(
            ep.admitted_tx(log, b"7", IdNamespace::Ordinal),
            AdmittedTx::Member
        );
        assert_eq!(
            ep.admitted_tx(log, b"7", IdNamespace::Bytes),
            AdmittedTx::At(1)
        );

        let bad = br#"[{"id":"07","type":"tick","timestamp":1}]"#;
        assert_eq!(
            ep.create_log_entry_for(Some(log), bad, InputFormat::Json, &mut output),
            ResultCode::AdmissionRefused
        );
        assert_eq!(output[22], dedup_detail::NOT_AN_ORDINAL);
    }

    /// Result header layout pinned byte-for-byte (ResultHeader is 32 bytes;
    /// ResultDiagnostic 12 bytes at the reserved offset).
    #[test]
    fn result_header_layout_pinned() {
        let mut out = [0u8; RESULT_HEADER_SIZE];
        write_result_header(&mut out, ResultCode::EncodeError, 32, 77, 5, 2);
        assert_eq!(u32::from_le_bytes(out[0..4].try_into().unwrap()), 3);
        assert_eq!(u32::from_le_bytes(out[4..8].try_into().unwrap()), 32);
        assert_eq!(u32::from_le_bytes(out[8..12].try_into().unwrap()), 77);
        assert_eq!(u32::from_le_bytes(out[12..16].try_into().unwrap()), 5);
        assert_eq!(u32::from_le_bytes(out[16..20].try_into().unwrap()), 2);
        assert_eq!(&out[20..32], &[0u8; 12]);

        let diagnostic = ResultDiagnostic {
            stage: diagnostic_stage::MSGPACK,
            detail: diagnostic_detail::BUFFER_OVERFLOW,
            expected_type: 3,
            actual_type: 4,
            field_index: 7,
            row_index: 9,
        };
        write_result_header_with_diagnostic(&mut out, ResultCode::OutOfMemory, &diagnostic);
        // Diagnostic detail byte 4 is buffer_overflow in the ABI order decoded
        // by the TypeScript diagnostic vocabulary.
        assert_eq!(
            out[20..32],
            [1, 3, 4, 3, 4, 0, 7, 0, 9, 0, 0, 0],
            "version|stage|detail|expected|actual|res0|field u16|row u16|res1 u16"
        );
    }

    /// msgpack workspace growth: columine wiring retries and succeeds where
    /// consumer wiring surfaces the overflow — the drift axis pinned.
    #[test]
    fn msgpack_growth_is_wiring_dependent() {
        // The undeclared carrier schema forces the msgpack workspace into use with
        // an undeclared field large enough to overflow the initial 64K...
        // growing 64K deliberately is slow; instead shrink the buffers to
        // make the axis observable cheaply.
        let fields = vec![
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new(ArrowType::Binary, true),
        ];
        let field_names = format!("id\0{}\0", columine_parsing::UNDECLARED_COLUMN_NAME);
        let schema = schema_with_names(&fields, field_names.as_bytes());
        assert!(!schema.is_base_event_log);

        let mut consumer_ep = EventProcessor::new(
            EpWiring::consumer_variant(),
            10,
            CollisionPolicy::Latest,
            schema.clone(),
        )
        .unwrap();
        consumer_ep.work_buffer = vec![0; 8];
        let mut col_ep =
            EventProcessor::new(EpWiring::columine(), 10, CollisionPolicy::Latest, schema).unwrap();
        col_ep.work_buffer = vec![0; 8];

        // Msgpack map {id:"x", big:"yyyyyyyyyyyyyyyy"} — undeclared `big`
        // routes through the msgpack workspace.
        let mut input = vec![0x82];
        input.push(0xa2);
        input.extend(b"id");
        input.push(0xa1);
        input.extend(b"x");
        input.push(0xa3);
        input.extend(b"big");
        input.push(0xb0);
        input.extend([b'y'; 16]);

        let mut output = vec![0u8; 64 * 1024];
        assert_eq!(
            consumer_ep.create_log_entry(&input, InputFormat::MsgpackStream, &mut output),
            ResultCode::ParseError,
            "consumer wiring: no msgpack growth, overflow surfaces"
        );
        assert_eq!(
            col_ep.create_log_entry(&input, InputFormat::MsgpackStream, &mut output),
            ResultCode::Ok,
            "columine wiring: workspace grows and the batch succeeds"
        );
    }

    #[test]
    fn semantic_validation_refuses_before_extraction_or_dedup() {
        let base = DynamicSchemaConfig::from_physical_fields_with_names(
            &[
                SignalSchemaField::new(ArrowType::Utf8, false),
                SignalSchemaField::new(ArrowType::Utf8, false),
                SignalSchemaField::new(ArrowType::Int64, false),
                SignalSchemaField::new(ArrowType::Binary, true),
            ],
            b"id\0type\0timestamp\0value\0",
        )
        .unwrap();
        let schema = DynamicSchemaConfig {
            semantic_schema: Some(
                br#"{"hi":{"kind":"object","fields":{"greeting":{"kind":"string"}}}}"#.to_vec(),
            ),
            ..base
        };
        let mut ep = EventProcessor::new(
            EpWiring::consumer_variant(),
            8,
            CollisionPolicy::Latest,
            schema,
        )
        .unwrap();
        let input = br#"[{"id":"greeting:42","type":"hi","timestamp":1,"value":{"greeting":42}}]"#;
        let mut output = vec![0xa5; 4096];
        assert_eq!(
            ep.create_log_entry(input, InputFormat::Json, &mut output),
            ResultCode::ParseError
        );
        let diagnostic = ep.payload_validation_diagnostic().unwrap();
        assert_eq!(diagnostic.path, "value.greeting");
        assert_eq!(diagnostic.expected, "string");
        assert_eq!(diagnostic.observed, "number");
        assert_eq!(diagnostic.event_index, 0);
        assert_eq!(diagnostic.signal_type, "hi");
        assert_eq!(read_result_header(&output).0, ResultCode::ParseError as u32);
        assert!(output[32..].iter().all(|byte| *byte == 0xa5));
    }
}
