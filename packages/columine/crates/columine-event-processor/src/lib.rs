//! Unified event processor: parse → columns → (dedup) → Arrow IPC.
//!
//! Ports BOTH drifted `event_processor.zig` variants as one core
//! parameterized the way axe-runtime's `addEpModules` parameterizes modules
//! (`EpWiring`):
//!
//! - **AxE artifact** (`axe-runtime/src/event_processor.zig`, VERSION 2):
//!   dynamic-only extraction, dedup (bloom + checkpoint), diagnostic bytes
//!   in the result header, JSON fallback-workspace growth, NO msgpack
//!   workspace growth.
//! - **Columine npm artifact** (`columine/src/event_processor.zig`,
//!   VERSION 1): no dedup, base 4-column path for `field_count == 4`
//!   schemas (scanners + `EventColumns`), workspace growth on BOTH json and
//!   msgpack extraction.
//!
//! The wasm `ep_*` export surface (handle table, host buffers, ep_alloc)
//! is the bindings slice; this crate is the library core those thin
//! wrappers call.

pub mod bloom;
pub mod checkpoint;

pub use bloom::{BloomFilter, CollisionPolicy, DedupState};

use columine_arrow::{
    DynamicColumns, DynamicSchemaConfig, EventColumns, IpcError, MAX_VALUE_BYTES, MetadataLimits,
    MetadataStorage, write_arrow_ipc_from_columns_with_schema,
    write_arrow_ipc_from_dynamic_columns,
};
use columine_parsing::{
    ExtractionConfig, build_extraction_config, json_extractor, json_scanner, msgpack_extractor,
    msgpack_scanner,
};

/// Module version: 2 is the AxE artifact, 1 the columine npm artifact.
pub const AXE_VERSION: u32 = 2;
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
}

/// Result header size (`ResultHeader`, 32 bytes: code u32 | arrow_ipc_offset
/// u32 | arrow_ipc_len u32 | events_processed u32 | duplicates_filtered u32 |
/// reserved [12]u8). Written as explicit LE bytes; layout pinned by test.
pub const RESULT_HEADER_SIZE: usize = 32;

pub const DIAGNOSTIC_ABI_VERSION: u8 = 1;

/// Extraction diagnostic packed into the header's reserved bytes
/// (AxE variant; `ResultDiagnostic`, 12 bytes: version u8 | stage u8 |
/// detail u8 | expected_type u8 | actual_type u8 | reserved0 u8 |
/// field_index u16 | row_index u16 | reserved1 u16).
///
/// JSON extraction threads the per-field `ExtractionDiagnostic` from the
/// failure site; the msgpack extractor does not carry one, so its
/// stage/detail derive from the error at this level (as in the Zig EP).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ResultDiagnostic {
    pub stage: u8,
    pub detail: u8,
    pub expected_type: u8,
    pub actual_type: u8,
    pub field_index: u16,
    pub row_index: u16,
}

/// Diagnostic byte vocabularies and NO_FIELD live with the extractor that
/// populates them — one source of truth for the ABI order (a local copy here
/// once drifted two detail values AND two stage values from the Zig enums).
pub use columine_parsing::json_extractor::{NO_FIELD, diagnostic_detail, diagnostic_stage};

/// Parse-path wiring: the axis on which the two Zig artifacts differ
/// (`addEpModules` equivalent).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EpWiring {
    /// Dedup by event id (AxE artifact wires it; columine does not).
    pub dedup: bool,
    /// Base 4-column scanner path for `field_count == 4` schemas
    /// (columine keeps it; the AxE artifact deleted it).
    pub base_path: bool,
    /// Grow the msgpack workspace and retry on overflow (columine yes,
    /// AxE no — its msgpack path errors without retry).
    pub msgpack_growth: bool,
    /// Write diagnostic bytes into the result header (AxE yes).
    pub diagnostics: bool,
}

impl EpWiring {
    pub fn axe() -> Self {
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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EpInitError {
    Metadata(columine_arrow::MetadataError),
    Config(columine_parsing::ConfigError),
}

/// The unified event processor core.
pub struct EventProcessor {
    pub wiring: EpWiring,
    pub schema_config: DynamicSchemaConfig,
    extraction_config: Option<ExtractionConfig>,
    dynamic_columns: DynamicColumns,
    event_columns: Option<EventColumns>,
    record_batch_metadata: MetadataStorage,
    pub dedup_state: Option<DedupState>,
    /// Reusable MessagePack workspace for declared Binary values and
    /// internal batches.
    work_buffer: Vec<u8>,
    /// Independent workspace for an in-progress value.$extra map.
    fallback_work_buffer: Vec<u8>,
}

impl EventProcessor {
    /// Init with schema + field names (the primary path; names enable JSON
    /// key matching). `capacity` sizes both the batch columns and, when
    /// dedup is wired, the bloom filter (as in the Zig where the same
    /// `capacity` feeds both).
    pub fn new(
        wiring: EpWiring,
        capacity: u32,
        policy: CollisionPolicy,
        schema_config: DynamicSchemaConfig,
    ) -> Result<Self, EpInitError> {
        Self::with_column_capacity(wiring, capacity, capacity, policy, schema_config)
    }

    /// Init with a batch-column capacity distinct from the dedup capacity.
    /// The wasm artifact caps columns at `WASM_EVENT_CAPACITY` (256) to fit
    /// linear memory while the bloom filter still sizes from the caller's
    /// full `capacity` (event_processor.zig `initCommon`: `event_cap =
    /// @min(capacity, WASM_EVENT_CAPACITY)` on wasm32, full elsewhere).
    pub fn with_column_capacity(
        wiring: EpWiring,
        capacity: u32,
        column_capacity: u32,
        policy: CollisionPolicy,
        schema_config: DynamicSchemaConfig,
    ) -> Result<Self, EpInitError> {
        let record_batch_metadata =
            MetadataStorage::for_fields(&schema_config.field_metadata, MetadataLimits::default())
                .map_err(EpInitError::Metadata)?;

        // Empty-names init keeps an empty extraction config: JSON extraction
        // then fails (keys cannot be matched to columns), exactly like the
        // Zig no-names path.
        let extraction_config = if schema_config.field_names.is_empty() {
            None
        } else {
            let names: Vec<&str> = schema_config
                .field_names
                .iter()
                .map(String::as_str)
                .collect();
            Some(
                build_extraction_config(&schema_config.field_metadata, &names)
                    .map_err(EpInitError::Config)?,
            )
        };

        let use_base = wiring.base_path && !schema_config.has_extraction_fields;
        Ok(Self {
            dynamic_columns: DynamicColumns::new(&schema_config.field_metadata, column_capacity),
            event_columns: use_base.then(|| EventColumns::new(column_capacity)),
            record_batch_metadata,
            dedup_state: wiring.dedup.then(|| DedupState::new(capacity, policy)),
            extraction_config,
            schema_config,
            wiring,
            work_buffer: vec![0; INITIAL_WORK_BUFFER_SIZE],
            fallback_work_buffer: vec![0; INITIAL_FALLBACK_WORK_BUFFER_SIZE],
        })
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

    /// Process one input batch into `output`:
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
        let arrow_offset = RESULT_HEADER_SIZE as u32;

        if self.event_columns.is_some() {
            return self.create_log_entry_base(input, format, output, arrow_offset);
        }
        self.create_log_entry_dynamic(input, format, output, arrow_offset)
    }

    /// BASE PATH (columine npm variant): scanners into `EventColumns`.
    fn create_log_entry_base(
        &mut self,
        input: &[u8],
        format: InputFormat,
        output: &mut [u8],
        arrow_offset: u32,
    ) -> ResultCode {
        let cols = self
            .event_columns
            .as_mut()
            .unwrap_or_else(|| columine_types::die!("base path without event columns"));
        cols.reset();

        let parse_result = match format {
            InputFormat::Json => {
                json_scanner::parse_json_events(input, cols).map_err(|_| ResultCode::ParseError)
            }
            InputFormat::Msgpack => msgpack_scanner::parse_msgpack_events(input, cols)
                .map_err(|_| ResultCode::ParseError),
            InputFormat::MsgpackStream => msgpack_scanner::parse_msgpack_stream(input, cols)
                .map_err(|_| ResultCode::ParseError),
            // ZIG-PARITY: columine's base path treats ARROW_PASSTHROUGH as
            // OK-with-empty-columns (`.ARROW_PASSTHROUGH => .OK`) while its
            // extraction path and the AxE artifact refuse INVALID_FORMAT;
            // intended fix: refuse on both paths.
            InputFormat::ArrowPassthrough => Ok(()),
        };
        if parse_result.is_err() {
            write_result_header(output, ResultCode::ParseError, 0, 0, 0, 0);
            return ResultCode::ParseError;
        }

        // No dedup in columine — all events are processed.
        let processed = cols.count;
        match write_arrow_ipc_from_columns_with_schema(
            cols,
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
            Err(IpcError::BufferTooSmall) => {
                write_result_header(output, ResultCode::EncodeError, 0, 0, 0, 0);
                ResultCode::EncodeError
            }
        }
    }

    /// EXTRACTION PATH: extractors into `DynamicColumns`, optional dedup,
    /// dynamic writer.
    fn create_log_entry_dynamic(
        &mut self,
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
                // JSON extraction carries the per-field diagnostic from the
                // failure site; the msgpack extractor does not thread one, so
                // its stage/detail derive from the error (as in the Zig EP).
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

        // Dedup: read event ids from column 0 (AxE artifact).
        let mut processed = 0u32;
        let mut duplicates = 0u32;
        if let Some(dedup) = self.dedup_state.as_mut() {
            let col0 = &self.dynamic_columns.columns[0];
            for row in 0..self.dynamic_columns.count {
                let event_id = col0
                    .read_variable(row)
                    .unwrap_or_else(|| columine_types::die!("id column is not variable-width"));
                if dedup.should_process(event_id) {
                    processed += 1;
                } else {
                    duplicates += 1;
                }
            }
        } else {
            processed = self.dynamic_columns.count;
        }

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
                ResultCode::Ok
            }
            Err(IpcError::BufferTooSmall) => {
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
                    // Zig retries ONLY a workspace overflow (diagnostic says
                    // msgpack/buffer_overflow); a column-limit overflow
                    // surfaces immediately (`extractJsonEventsWithWorkspaceGrowth`).
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

    /// Checkpoint the dedup state into `output`; 0 = error (no dedup wired
    /// or buffer too small), matching `ep_checkpoint`'s sentinel.
    pub fn checkpoint(&self, output: &mut [u8]) -> usize {
        let Some(state) = self.dedup_state.as_ref() else {
            return 0;
        };
        checkpoint::serialize(state, output).unwrap_or(0)
    }

    /// Restore dedup state from checkpoint bytes (`ep_restore`).
    pub fn restore(&mut self, input: &[u8]) -> ResultCode {
        match checkpoint::deserialize(input) {
            Ok(state) => {
                self.dedup_state = Some(state);
                ResultCode::Ok
            }
            Err(_) => ResultCode::ParseError,
        }
    }

    /// Packed dedup stats (`ep_get_stats`):
    /// `total_events as u32 | duplicates as u32 << 32` (Zig truncates).
    pub fn stats(&self) -> u64 {
        let Some(state) = self.dedup_state.as_ref() else {
            return 0;
        };
        let total = state.total_events as u32;
        let dupes = state.duplicates_detected as u32;
        u64::from(total) | (u64::from(dupes) << 32)
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

/// Write the header with the diagnostic packed into the reserved bytes
/// (`writeResultHeaderWithDiagnostic`, AxE artifact).
pub fn write_result_header_with_diagnostic(
    output: &mut [u8],
    code: ResultCode,
    diagnostic: &ResultDiagnostic,
) {
    write_result_header(output, code, 0, 0, 0, 0);
    // ResultDiagnostic layout at reserved (offset 20).
    output[20] = DIAGNOSTIC_ABI_VERSION;
    output[21] = diagnostic.stage;
    output[22] = diagnostic.detail;
    output[23] = diagnostic.expected_type;
    output[24] = diagnostic.actual_type;
    output[25] = 0; // reserved0
    output[26..28].copy_from_slice(&diagnostic.field_index.to_le_bytes());
    output[28..30].copy_from_slice(&diagnostic.row_index.to_le_bytes());
    output[30..32].fill(0); // reserved1
}

/// Read back the header fields (test/consumer view).
pub fn read_result_header(output: &[u8]) -> (u32, u32, u32, u32, u32) {
    let f = |at: usize| u32::from_le_bytes(output[at..at + 4].try_into().unwrap_or([0; 4]));
    (f(0), f(4), f(8), f(12), f(16))
}

#[cfg(test)]
mod tests {
    use super::*;
    use columine_arrow::{ArrowType, SignalSchemaField};

    fn base_fields() -> Vec<SignalSchemaField> {
        vec![
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new(ArrowType::Int64, false),
            SignalSchemaField::new(ArrowType::Binary, true),
        ]
    }

    fn schema_with_names(fields: &[SignalSchemaField], names: &[u8]) -> DynamicSchemaConfig {
        let schema_bytes = [
            0xFF, 0xFF, 0xFF, 0xFF, 0x08, 0x00, 0x00, 0x00, 0, 0, 0, 0, 0, 0, 0, 0,
        ];
        DynamicSchemaConfig::with_field_names(&schema_bytes, fields, names)
    }

    // test "ep_create_log_entry with schema" (axe-runtime variant)
    #[test]
    fn create_log_entry_axe_json() {
        let schema = schema_with_names(&base_fields(), b"id\0type\0timestamp\0value\0");
        let mut ep =
            EventProcessor::new(EpWiring::axe(), 100, CollisionPolicy::Latest, schema).unwrap();
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
        assert!(!schema.has_extraction_fields);
        let mut ep =
            EventProcessor::new(EpWiring::columine(), 100, CollisionPolicy::Latest, schema)
                .unwrap();
        assert!(ep.event_columns.is_some());
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
        // No dedup wired.
        assert_eq!(ep.stats(), 0);
    }

    /// Base and extraction paths emit byte-identical IPC for the same
    /// 4-column content (extends the columine-arrow writer-unification pin
    /// through the full EP pipeline). Two deliberate per-path differences
    /// are compensated for, both faithful to Zig: the base json_scanner
    /// converts numeric timestamps ms→µs (×1000) while the extractor stores
    /// them raw, and the base path stores raw JSON value bytes where the
    /// extractor stores typed msgpack — so the shared case is a 3-field
    /// event with the timestamp pre-scaled on the extraction side.
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
            EpWiring::axe(),
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
        let mut ep =
            EventProcessor::new(EpWiring::axe(), 10, CollisionPolicy::Latest, schema).unwrap();
        let mut output = vec![0u8; 64 * 1024];
        assert_eq!(
            ep.create_log_entry(b"not json", InputFormat::Json, &mut output),
            ResultCode::ParseError
        );
        let (code, ..) = read_result_header(&output);
        assert_eq!(code, ResultCode::ParseError as u32);
        // Diagnostic bytes rode the reserved region (AxE wiring).
        assert_eq!(output[20], DIAGNOSTIC_ABI_VERSION);
        assert_eq!(output[21], diagnostic_stage::JSON);

        assert_eq!(
            ep.create_log_entry(b"[]", InputFormat::ArrowPassthrough, &mut output),
            ResultCode::InvalidFormat
        );
    }

    /// AxE dedup counts duplicates in the header and survives a
    /// checkpoint/restore round trip.
    #[test]
    fn dedup_and_checkpoint_through_ep() {
        let schema = schema_with_names(&base_fields(), b"id\0type\0timestamp\0value\0");
        let mut ep =
            EventProcessor::new(EpWiring::axe(), 100, CollisionPolicy::Discard, schema).unwrap();
        let input = br#"[{"id":"dup","type":"a","timestamp":1},{"id":"dup","type":"a","timestamp":2},{"id":"uniq","type":"a","timestamp":3}]"#;
        let mut output = vec![0u8; 64 * 1024];
        assert_eq!(
            ep.create_log_entry(input, InputFormat::Json, &mut output),
            ResultCode::Ok
        );
        let (_, _, _, processed, dupes) = read_result_header(&output);
        assert_eq!(processed, 2);
        assert_eq!(dupes, 1);
        assert_eq!(ep.stats(), 3 | (1 << 32));

        let mut checkpoint_buf = vec![0u8; 8192];
        let size = ep.checkpoint(&mut checkpoint_buf);
        assert!(size > 0);

        let schema2 = schema_with_names(&base_fields(), b"id\0type\0timestamp\0value\0");
        let mut restored =
            EventProcessor::new(EpWiring::axe(), 100, CollisionPolicy::Discard, schema2).unwrap();
        assert_eq!(restored.restore(&checkpoint_buf[..size]), ResultCode::Ok);
        // The restored filter still knows both ids.
        let state = restored.dedup_state.as_ref().unwrap();
        assert!(state.bloom.maybe_contains(b"dup"));
        assert!(state.bloom.maybe_contains(b"uniq"));
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
        // detail byte 4 = buffer_overflow in the Zig DiagnosticDetail order
        // (decoded by lib.ts DIAGNOSTIC_DETAILS — the ABI this test pins).
        assert_eq!(
            out[20..32],
            [1, 3, 4, 3, 4, 0, 7, 0, 9, 0, 0, 0],
            "version|stage|detail|expected|actual|res0|field u16|row u16|res1 u16"
        );
    }

    /// msgpack workspace growth: columine wiring retries and succeeds where
    /// AxE wiring surfaces the overflow — the drift axis pinned.
    #[test]
    fn msgpack_growth_is_wiring_dependent() {
        // A value.$extra schema forces the msgpack workspace into use with
        // an undeclared field large enough to overflow the initial 64K...
        // growing 64K deliberately is slow; instead shrink the buffers to
        // make the axis observable cheaply.
        let fields = vec![
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new(ArrowType::Binary, true),
        ];
        let schema = schema_with_names(&fields, b"id\0value.$extra\0");
        assert!(schema.has_extraction_fields);

        let mut axe_ep =
            EventProcessor::new(EpWiring::axe(), 10, CollisionPolicy::Latest, schema.clone())
                .unwrap();
        axe_ep.work_buffer = vec![0; 8];
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
            axe_ep.create_log_entry(&input, InputFormat::MsgpackStream, &mut output),
            ResultCode::ParseError,
            "AxE wiring: no msgpack growth, overflow surfaces"
        );
        assert_eq!(
            col_ep.create_log_entry(&input, InputFormat::MsgpackStream, &mut output),
            ResultCode::Ok,
            "columine wiring: workspace grows and the batch succeeds"
        );
    }
}
