//! Public event-processor parse plane: parse → columns → Arrow IPC.
//!
//! The crate owns schema/config retention, semantic validation, JSON and
//! MessagePack extraction (with the base four-column scanner path for
//! four-field schemas and workspace growth), DynamicColumns, and the Arrow
//! IPC encoding — everything schema-shaped and format-generic.
//!
//! Per-log event admission lives in a private consumer crate: the seen-sets,
//! destination logs, id namespaces, judge/commit/
//! abandon window, and redelivery horizon. That crate composes this parse
//! plane through the borrowed [`ValidatedInput`]/[`ParsedBatch`] API — the
//! precheck completes once, the extracted columns are judged in place, and
//! the encoded batch is emitted once — without a second parse, cloned
//! columns, or intermediate IPC.
//!
//! The `ep_*` wasm exports of `columine-ep-wasm` are thin bindings around
//! this library core.

pub mod compact;

pub use compact::{
    COMPACT_ABI_VERSION, COMPACT_BATCH_MAGIC, COMPACT_DESCRIPTOR_SIZE, COMPACT_DIAGNOSTIC_STAGE,
    COMPACT_HEADER_SIZE, CompactBatchView, CompactValidationError, compact_detail,
};

use columine_arrow::{
    DynamicColumns, DynamicSchemaConfig, IpcError, MAX_VALUE_BYTES, MIN_ARROW_OUTPUT_CAPACITY,
    MetadataLimits, MetadataStorage, required_arrow_ipc_len, write_arrow_ipc_from_borrowed_columns,
    write_arrow_ipc_from_dynamic_columns,
};
use columine_parsing::{
    ExtractionConfig, json_extractor, json_scanner, msgpack_extractor, msgpack_scanner,
    validate::{
        BatchValidationError, SemanticSchemaSet, parse_schema_envelope, validate_json_batch,
        validate_msgpack_batch,
    },
};

/// This crate's npm artifact version.
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
pub use columine_parsing::json_extractor::{NO_FIELD, diagnostic_detail, diagnostic_stage};

/// How the parse plane is wired (`ParseOptions`). Every published
/// configuration is one of these values; per-log admission composes the
/// plane with `base_path: false` and diagnostics on, while columine's own
/// artifact keeps the base scanner path, MessagePack workspace growth, and
/// plain headers.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ParseOptions {
    /// Base 4-column scanner path for `field_count == 4` schemas whose
    /// schema IS the base event log.
    pub base_path: bool,
    /// Grow the msgpack workspace and retry on overflow.
    pub msgpack_growth: bool,
    /// Write diagnostic bytes into the result header's reserved bytes.
    pub diagnostics: bool,
}

/// Why `validate` or `parse` refused an input. `code` is the header/status
/// value; `diagnostic` is the per-field extraction or semantic-validation
/// diagnostic for the header's reserved bytes, when the failure has one.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessError {
    pub code: ResultCode,
    pub diagnostic: Option<ResultDiagnostic>,
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

/// The public event-processor parse plane.
pub struct EventProcessor {
    options: ParseOptions,
    schema_config: DynamicSchemaConfig,
    extraction_config: Option<ExtractionConfig>,
    semantic_schemas: Option<SemanticSchemaSet>,
    last_validation_diagnostic: Option<PayloadValidationDiagnostic>,
    dynamic_columns: DynamicColumns,
    /// True when the schema IS the base event log and the options keep the
    /// scanner path, so the four-column scanners can write by index.
    use_base_scanners: bool,
    record_batch_metadata: MetadataStorage,
    /// Reusable MessagePack workspace for declared Binary values and
    /// internal batches.
    work_buffer: Vec<u8>,
    /// Independent workspace for an in-progress undeclared carrier map.
    fallback_work_buffer: Vec<u8>,
}

impl EventProcessor {
    /// Init with schema + field names (the primary path; names enable JSON
    /// key matching). `column_capacity` sizes the batch columns; the wasm
    /// exports build every handle at [`WASM_EVENT_CAPACITY`]. `options`
    /// selects the base scanner path, MessagePack workspace growth, and
    /// header diagnostics.
    pub fn new(
        options: ParseOptions,
        column_capacity: u32,
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
            use_base_scanners: options.base_path && schema_config.is_base_event_log,
            dynamic_columns: DynamicColumns::new(&schema_config.field_metadata, column_capacity),
            record_batch_metadata,
            extraction_config,
            semantic_schemas,
            last_validation_diagnostic: None,
            schema_config,
            options,
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

    /// The retained schema configuration.
    pub fn schema_config(&self) -> &DynamicSchemaConfig {
        &self.schema_config
    }

    /// Run the semantic precheck on `input` and, on success, hand back the
    /// proof that it completed: a [`ValidatedInput`] that can be extracted
    /// exactly once. The processor retains its one column plane; nothing is
    /// cloned or re-validated downstream.
    ///
    /// [`ProcessError`] on a semantic refusal carries the header's
    /// diagnostic bytes; the structured path/message form is available
    /// through [`EventProcessor::payload_validation_diagnostic`].
    pub fn validate<'a>(
        &'a mut self,
        input: &'a [u8],
        format: InputFormat,
    ) -> Result<ValidatedInput<'a>, ProcessError> {
        self.last_validation_diagnostic = None;
        if let Some((event_index, event_type, violation)) = self.validate_input(input, format) {
            self.last_validation_diagnostic = Some(PayloadValidationDiagnostic {
                stage: diagnostic_stage::VALIDATION,
                detail: diagnostic_detail::PAYLOAD_VIOLATION,
                event_index: u32::try_from(event_index).unwrap_or(u32::MAX),
                signal_type: event_type,
                path: violation.path,
                expected: violation.expected,
                observed: violation.observed,
            });
            return Err(ProcessError {
                code: ResultCode::ParseError,
                diagnostic: Some(ResultDiagnostic {
                    stage: diagnostic_stage::VALIDATION,
                    detail: diagnostic_detail::PAYLOAD_VIOLATION,
                    ..ResultDiagnostic::default()
                }),
            });
        }
        Ok(ValidatedInput {
            processor: self,
            input,
            format,
        })
    }

    /// The convenience composition: semantic validation, then extraction
    /// into the retained column plane.
    pub fn parse<'a>(
        &'a mut self,
        input: &'a [u8],
        format: InputFormat,
    ) -> Result<ParsedBatch<'a>, ProcessError> {
        self.validate(input, format)?.parse()
    }

    /// Process one input batch into `output` with no seen-set — validation,
    /// extraction and encoding only, the admission boundary's call:
    /// `[ResultHeader 32B][Arrow IPC stream]`. Returns the header's code.
    pub fn create_log_entry(
        &mut self,
        input: &[u8],
        format: InputFormat,
        output: &mut [u8],
    ) -> ResultCode {
        if output.len() < RESULT_HEADER_SIZE {
            return ResultCode::OutOfMemory;
        }
        match self.parse(input, format) {
            Ok(batch) => batch.encode(output),
            Err(error) => {
                match error.diagnostic {
                    Some(diagnostic) if self.options.diagnostics => {
                        write_result_header_with_diagnostic(output, error.code as u32, &diagnostic);
                    }
                    _ => write_result_header(output, error.code as u32, 0, 0, 0),
                }
                error.code
            }
        }
    }
}

/// Proof that the semantic precheck on one input completed. Consumed by
/// [`ValidatedInput::parse`], which extracts into the processor's retained
/// column plane exactly once.
pub struct ValidatedInput<'a> {
    processor: &'a mut EventProcessor,
    input: &'a [u8],
    format: InputFormat,
}

impl<'a> ValidatedInput<'a> {
    /// Extract the validated input into the retained column plane. The
    /// scanners write the four base event-log columns by index when the
    /// options keep the base path and the schema IS that event log.
    pub fn parse(self) -> Result<ParsedBatch<'a>, ProcessError> {
        let ValidatedInput {
            processor,
            input,
            format,
        } = self;
        if processor.use_base_scanners {
            return processor.parse_base(input, format);
        }
        processor.parse_dynamic(input, format)
    }
}

/// The parsed batch: borrowed views of the processor's one column plane,
/// schema configuration and mutable Arrow IPC metadata. Consuming
/// [`ParsedBatch::encode`] releases those borrows.
pub struct ParsedBatch<'a> {
    columns: &'a mut DynamicColumns,
    schema_config: &'a DynamicSchemaConfig,
    record_batch_metadata: &'a mut MetadataStorage,
}

impl ParsedBatch<'_> {
    /// The extracted columns, for direct admission judgment (the per-log
    /// owner filters rows through its own keep mask before encoding).
    pub fn columns_mut(&mut self) -> &mut DynamicColumns {
        self.columns
    }

    /// Rows currently in the columns.
    pub fn row_count(&self) -> u32 {
        self.columns.count
    }

    /// Encode the columns into `[ResultHeader][Arrow IPC]`, writing a
    /// success header with zeroed bytes 16..32. `Err` means the encoded
    /// batch did not fit — nothing reached the caller.
    pub fn encode(self, output: &mut [u8]) -> ResultCode {
        let rows = self.columns.count;
        match write_arrow_ipc_from_dynamic_columns(
            self.columns,
            self.schema_config,
            &mut output[RESULT_HEADER_SIZE..],
            self.record_batch_metadata,
        ) {
            Ok(len) => {
                let written = u32::try_from(len)
                    .unwrap_or_else(|_| columine_types::die!("dynamic IPC length exceeds u32"));
                write_result_header(
                    output,
                    ResultCode::Ok as u32,
                    RESULT_HEADER_SIZE as u32,
                    written,
                    rows,
                );
                ResultCode::Ok
            }
            Err(
                IpcError::BufferTooSmall { .. } | IpcError::InvalidColumn | IpcError::SizeOverflow,
            ) => {
                write_result_header(output, ResultCode::EncodeError as u32, 0, 0, 0);
                ResultCode::EncodeError
            }
        }
    }
}
impl EventProcessor {
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
                    ResultCode::Ok as u32,
                    RESULT_HEADER_SIZE as u32,
                    written,
                    view.row_count(),
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

    /// BASE PATH: the base event-log scanners, which write the four
    /// `BASE_EVENT_LOG_FIELDS` columns by index. Selected only when the
    /// options keep the base path and the schema IS that event log, and it
    /// shares the one column store and the one IPC writer with the
    /// extraction path.
    fn parse_base(
        &mut self,
        input: &[u8],
        format: InputFormat,
    ) -> Result<ParsedBatch<'_>, ProcessError> {
        let cols = &mut self.dynamic_columns;
        cols.reset();

        let parse_result = match format {
            InputFormat::Json => {
                json_scanner::parse_json_events(input, cols).map_err(|_| ProcessError {
                    code: ResultCode::ParseError,
                    diagnostic: None,
                })
            }
            InputFormat::Msgpack => {
                msgpack_scanner::parse_msgpack_events(input, cols).map_err(|_| ProcessError {
                    code: ResultCode::ParseError,
                    diagnostic: None,
                })
            }
            InputFormat::MsgpackStream => msgpack_scanner::parse_msgpack_stream(input, cols)
                .map_err(|_| ProcessError {
                    code: ResultCode::ParseError,
                    diagnostic: None,
                }),
            // Arrow passthrough is unsupported on the base path.
            InputFormat::ArrowPassthrough => Err(ProcessError {
                code: ResultCode::InvalidFormat,
                diagnostic: None,
            }),
        };
        parse_result?;

        // No dedup in columine — all events are processed.
        Ok(ParsedBatch {
            columns: &mut self.dynamic_columns,
            schema_config: &self.schema_config,
            record_batch_metadata: &mut self.record_batch_metadata,
        })
    }

    /// EXTRACTION PATH: extractors into `DynamicColumns`.
    fn parse_dynamic(
        &mut self,
        input: &[u8],
        format: InputFormat,
    ) -> Result<ParsedBatch<'_>, ProcessError> {
        if format == InputFormat::ArrowPassthrough {
            return Err(ProcessError {
                code: ResultCode::InvalidFormat,
                diagnostic: None,
            });
        }
        let Some(config) = self.extraction_config.take() else {
            // No field names: JSON keys cannot be matched to columns.
            return Err(ProcessError {
                code: ResultCode::ParseError,
                diagnostic: None,
            });
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
                // A batch past the column plane is a size refusal, not
                // malformed input: it names itself, and `row_index`
                // carries the plane's capacity — the batch size a
                // caller can retry with — as the JSON path's does.
                let (detail, row_index) = match err {
                    json_extractor::ExtractionError::OutOfMemory => {
                        (diagnostic_detail::OUT_OF_MEMORY, 0)
                    }
                    json_extractor::ExtractionError::BufferOverflow => {
                        (diagnostic_detail::BUFFER_OVERFLOW, 0)
                    }
                    json_extractor::ExtractionError::TooManyEvents => (
                        diagnostic_detail::TOO_MANY_EVENTS,
                        u16::try_from(self.dynamic_columns.capacity).unwrap_or(u16::MAX),
                    ),
                    _ => (diagnostic_detail::INVALID_JSON, 0),
                };
                ResultDiagnostic {
                    stage: diagnostic_stage::MSGPACK,
                    detail,
                    expected_type: 0,
                    actual_type: 0,
                    field_index: NO_FIELD,
                    row_index,
                }
            };
            return Err(ProcessError {
                code,
                diagnostic: Some(diagnostic),
            });
        }

        Ok(ParsedBatch {
            columns: &mut self.dynamic_columns,
            schema_config: &self.schema_config,
            record_batch_metadata: &mut self.record_batch_metadata,
        })
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
                    let may_grow = workspace_overflow && (fallback || self.options.msgpack_growth);
                    if !may_grow || self.grow_work_buffer(fallback).is_err() {
                        return Err(json_extractor::ExtractionError::BufferOverflow);
                    }
                }
                other => return other,
            }
        }
    }
}
/// Write the 32-byte result header (`writeResultHeader`). The code is a raw
/// u32: this is the byte primitive every header writer casts into, so a
/// status vocabulary extension (admission codes 8 and 9) composes on
/// the same bytes. The success posture is the zeroed tail: bytes 16..32
/// carry nothing — the admission layer decorates them itself.
pub fn write_result_header(
    output: &mut [u8],
    code: u32,
    arrow_offset: u32,
    arrow_len: u32,
    rows: u32,
) {
    output[0..4].copy_from_slice(&code.to_le_bytes());
    output[4..8].copy_from_slice(&arrow_offset.to_le_bytes());
    output[8..12].copy_from_slice(&arrow_len.to_le_bytes());
    output[12..16].copy_from_slice(&rows.to_le_bytes());
    output[16..32].fill(0);
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
    write_result_header(output, code as u32, arrow_offset, arrow_len, rows);
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
/// (`writeResultHeaderWithDiagnostic`).
pub fn write_result_header_with_diagnostic(
    output: &mut [u8],
    code: u32,
    diagnostic: &ResultDiagnostic,
) {
    output[0..4].copy_from_slice(&code.to_le_bytes());
    output[4..20].fill(0);
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

    /// columine's own artifact wiring: base scanner path, MessagePack
    /// workspace growth, plain headers.
    fn columine_options() -> ParseOptions {
        ParseOptions {
            base_path: true,
            msgpack_growth: true,
            diagnostics: false,
        }
    }

    /// The admission composition's wiring: extraction-only, no growth
    /// retry, diagnostics in the header.
    fn admission_options() -> ParseOptions {
        ParseOptions {
            base_path: false,
            msgpack_growth: false,
            diagnostics: true,
        }
    }

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
        let schema =
            schema_with_names(&[SignalSchemaField::new(ArrowType::Null, true)], b"value\0");
        let mut processor = EventProcessor::new(columine_options(), 1, schema).unwrap();

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

    #[test]
    fn create_log_entry_json() {
        let schema = schema_with_names(&base_fields(), b"id\0type\0timestamp\0value\0");
        let mut ep = EventProcessor::new(admission_options(), 100, schema).unwrap();
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

    #[test]
    fn create_log_entry_base_path() {
        let schema = schema_with_names(&base_fields(), b"id\0type\0timestamp\0value\0");
        assert!(schema.is_base_event_log);
        let mut ep = EventProcessor::new(columine_options(), 100, schema).unwrap();
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
            columine_options(),
            10,
            schema_with_names(&base_fields(), names),
        )
        .unwrap();
        let mut base_out = vec![0u8; 64 * 1024];
        assert_eq!(
            base_ep.create_log_entry(input, InputFormat::Json, &mut base_out),
            ResultCode::Ok
        );

        let mut dyn_ep = EventProcessor::new(
            admission_options(),
            10,
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
            &base_out[base_off as usize..(base_off + base_len) as usize],
            &dyn_out[dyn_off as usize..(dyn_off + dyn_len) as usize],
            "base and extraction paths emit byte-identical IPC"
        );
    }

    #[test]
    fn error_result_codes() {
        let schema = schema_with_names(&base_fields(), b"id\0type\0timestamp\0value\0");
        let mut ep = EventProcessor::new(admission_options(), 10, schema).unwrap();
        let mut output = vec![0u8; 64 * 1024];
        assert_eq!(
            ep.create_log_entry(b"not json", InputFormat::Json, &mut output),
            ResultCode::ParseError
        );
        let (code, ..) = read_result_header(&output);
        assert_eq!(code, ResultCode::ParseError as u32);
        // Diagnostic bytes occupy the reserved region when diagnostics are on.
        assert_eq!(output[20], DIAGNOSTIC_ABI_VERSION);
        assert_eq!(output[21], diagnostic_stage::JSON);

        assert_eq!(
            ep.create_log_entry(b"[]", InputFormat::ArrowPassthrough, &mut output),
            ResultCode::InvalidFormat
        );
    }

    /// Result header layout pinned byte-for-byte (ResultHeader is 32 bytes;
    /// ResultDiagnostic 12 bytes at the reserved offset).
    #[test]
    fn result_header_layout_pinned() {
        let mut out = [0u8; RESULT_HEADER_SIZE];
        write_result_header(&mut out, ResultCode::EncodeError as u32, 32, 77, 5);
        assert_eq!(u32::from_le_bytes(out[0..4].try_into().unwrap()), 3);
        assert_eq!(u32::from_le_bytes(out[4..8].try_into().unwrap()), 32);
        assert_eq!(u32::from_le_bytes(out[8..12].try_into().unwrap()), 77);
        assert_eq!(u32::from_le_bytes(out[12..16].try_into().unwrap()), 5);
        assert_eq!(&out[16..32], &[0u8; 16], "success zeroes the whole tail");
        assert_eq!(&out[20..32], &[0u8; 12]);

        let diagnostic = ResultDiagnostic {
            stage: diagnostic_stage::MSGPACK,
            detail: diagnostic_detail::BUFFER_OVERFLOW,
            expected_type: 3,
            actual_type: 4,
            field_index: 7,
            row_index: 9,
        };
        write_result_header_with_diagnostic(&mut out, ResultCode::OutOfMemory as u32, &diagnostic);
        // Diagnostic detail byte 4 is buffer_overflow in the ABI order decoded
        // by the TypeScript diagnostic vocabulary.
        assert_eq!(
            out[20..32],
            [1, 3, 4, 3, 4, 0, 7, 0, 9, 0, 0, 0],
            "version|stage|detail|expected|actual|res0|field u16|row u16|res1 u16"
        );
    }

    /// msgpack workspace growth: columine's options retry and succeed where
    /// the admission options surface the overflow — the drift axis pinned.
    #[test]
    fn msgpack_growth_is_options_dependent() {
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

        let mut consumer_ep = EventProcessor::new(admission_options(), 10, schema.clone()).unwrap();
        consumer_ep.work_buffer = vec![0; 8];
        let mut col_ep = EventProcessor::new(columine_options(), 10, schema).unwrap();
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
            "admission options: no msgpack growth, overflow surfaces"
        );
        assert_eq!(
            col_ep.create_log_entry(&input, InputFormat::MsgpackStream, &mut output),
            ResultCode::Ok,
            "columine options: workspace grows and the batch succeeds"
        );
    }

    /// A msgpack stream one event past the column plane is refused as
    /// TOO_MANY_EVENTS naming the plane's capacity, never as INVALID_JSON:
    /// the input is well-formed, the batch is too big.
    #[test]
    fn msgpack_batch_past_the_column_plane_names_too_many_events_and_the_capacity() {
        let fields = vec![
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new(ArrowType::Binary, true),
        ];
        let field_names = format!("id\0{}\0", columine_parsing::UNDECLARED_COLUMN_NAME);
        let schema = schema_with_names(&fields, field_names.as_bytes());
        const CAPACITY: u32 = 2;
        let mut ep = EventProcessor::new(admission_options(), CAPACITY, schema).unwrap();

        // Three maps {id:"<n>"} back to back: one past the plane.
        let mut input = Vec::new();
        for id in [b"a", b"b", b"c"] {
            input.push(0x81);
            input.push(0xa2);
            input.extend(b"id");
            input.push(0xa1);
            input.extend(id);
        }
        let mut output = vec![0u8; 64 * 1024];
        assert_eq!(
            ep.create_log_entry(&input, InputFormat::MsgpackStream, &mut output),
            ResultCode::ParseError
        );
        assert_eq!(output[20], DIAGNOSTIC_ABI_VERSION);
        assert_eq!(output[21], diagnostic_stage::MSGPACK);
        assert_eq!(output[22], diagnostic_detail::TOO_MANY_EVENTS);
        assert_eq!(
            u16::from_le_bytes([output[26], output[27]]),
            NO_FIELD,
            "a size refusal names no field"
        );
        assert_eq!(
            u32::from(u16::from_le_bytes([output[28], output[29]])),
            CAPACITY,
            "row_index carries the column plane's capacity"
        );

        // Exactly the plane is legal.
        let exact = &input[..input.len() / 3 * 2];
        assert_eq!(
            ep.create_log_entry(exact, InputFormat::MsgpackStream, &mut output),
            ResultCode::Ok
        );
    }

    #[test]
    fn semantic_validation_refuses_before_extraction() {
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
        let mut ep = EventProcessor::new(admission_options(), 8, schema).unwrap();
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
