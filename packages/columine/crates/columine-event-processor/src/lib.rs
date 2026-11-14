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
//! Value-object payloads are judged by a [`VoAdmission`] capability the
//! caller injects through [`EventProcessor::new_with_vo_admission`]. The
//! capability is prepared for every declared value-object schema before any
//! processor state exists, and [`EventProcessor::new`] uses the explicit
//! [`UnavailableVoAdmission`] refusal, so a value-object schema never yields
//! a processor that would accept frames unchecked.
//!
//! An `axId` payload field is judged by an [`AddressGrammar`], which the host
//! owns and names as the processor's second type parameter: a caller picks it
//! with `EventProcessor::<A, G>::new_with_vo_admission`. The default,
//! [`NoAddressGrammar`], refuses every address, so a processor built by
//! [`EventProcessor::new`] never admits one unchecked.
//!
//! The `ep_*` wasm exports of `columine-ep-wasm` are thin bindings around
//! this library core.

pub mod compact;

pub use compact::{
    COMPACT_ABI_VERSION, COMPACT_BATCH_MAGIC, COMPACT_DESCRIPTOR_SIZE, COMPACT_DIAGNOSTIC_STAGE,
    COMPACT_HEADER_SIZE, CompactBatchView, CompactValidationError, compact_detail,
};

use columine_arrow::{
    DictionaryValues, DynamicColumns, DynamicSchemaConfig, IpcError, MAX_VALUE_BYTES,
    MIN_ARROW_OUTPUT_CAPACITY, MetadataStorage, StreamDictionaryDelta, required_arrow_ipc_len,
    write_arrow_ipc_from_borrowed_columns, write_arrow_ipc_from_dynamic_columns,
};
use columine_parsing::identity::{FieldIdentity, IdentityError};
use columine_parsing::{
    CellLayout, CellWriter, ExtractionConfig, FrameProjections, json_extractor, json_scanner,
    msgpack_extractor, msgpack_scanner,
    validate::{
        AddressGrammar, BatchValidationError, NoAddressGrammar, PayloadViolation, PrepareRefusal,
        PreparedSchemas, UnavailableVoAdmission, VoAdmission, VoFrameProof, VoPreparationError,
        VoPresence, VoRefusal, parse_schema_envelope,
    },
};
/// The stream dictionary's seam (specs/columine/10-physical-vocabulary.md
/// §5): a parse call takes its owner's image, a compact call its length.
pub use columine_types::category::{CategoryImage, CategoryRefusal, ListImage, NoCategories};
/// The value-cell seam (specs/columine/10-physical-vocabulary.md §7): a
/// processor built by [`EventProcessor::new_with_cells`] hands its embedder
/// each value cell.
pub use columine_types::cells::{
    CellRefusal, CellTarget, NoValueCells, RecordRow, TEXT_HASH_BYTES, TextPlace, ValueCells,
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
    /// The caller's output buffer cannot hold the result header or the
    /// encoded batch; the same call succeeds with a larger buffer.
    OutputTooSmall = 4,
    InvalidFormat = 5,
    InvalidInput = 6,
    SchemaMismatch = 7,
    /// The memory to process the batch could not be had: nothing was
    /// produced, and the same call may succeed once the host has more room.
    MemoryExhausted = 8,
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
/// multiplies it: 32 utf8 columns create cleanly at 61440 and are refused as
/// `CreateFailure::MemoryExhausted` at 61932, where the plane crosses the
/// artifact's `--max-memory=256MiB`.
///
/// Rows above this capacity are not lost: the compact path encodes a
/// caller-supplied batch of up to `MAX_EVENTS_PER_BATCH` rows without touching
/// the parse plane at all, so paying for 65536 parse rows per handle buys
/// nothing.
pub const WASM_EVENT_CAPACITY: u32 = 256;

/// The most values the stream dictionary of one wasm handle holds: a value
/// past it refuses its batch with `ParseError`. Together with
/// [`WASM_CATEGORY_VALUE_BYTES`] it bounds the memory one handle's image takes.
pub const WASM_CATEGORY_LIMIT: u32 = 65_536;

/// The longest value, in bytes, the stream dictionary of one wasm handle
/// takes: a longer one refuses its batch with `ParseError`.
pub const WASM_CATEGORY_VALUE_BYTES: usize = 1_024;

/// Why an `ep_create_*` call produced no handle (u32 values are ABI).
///
/// Handles occupy 1..=255, so any other return value is a failure and these
/// name which one.
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
    /// An eight-byte field descriptor names no plane, dictionary slot or
    /// index plane, sets its reserved byte, or carries a parameter its plane
    /// does not take.
    SchemaFieldMetadata = 0x8000_0005,
    /// The schema message and the physical metadata table disagree on width.
    SchemaFieldCount = 0x8000_0006,
    /// A physical tag disagrees with that field's logical Arrow type.
    SchemaTypeMismatch = 0x8000_0007,
    /// A field's nullability disagrees, or a Null field is non-nullable.
    SchemaNullability = 0x8000_0008,
    /// The field-name blob is malformed or not one name per field.
    SchemaFieldNames = 0x8000_0009,
    /// The extraction configuration or the semantic schema refused the schema.
    Init = 0x8000_000a,
    /// All 255 handle slots are in use.
    HandlesExhausted = 0x8000_000b,
    /// The memory to build the handle could not be had; nothing was created.
    MemoryExhausted = 0x8000_000c,
    /// The Schema message is longer than `columine_arrow::MAX_SCHEMA_MESSAGE_BYTES`.
    SchemaTooLarge = 0x8000_000d,
    /// The schema's dictionaries and its fields disagree: a malformed
    /// dictionary table, a dictionary no field names, a field naming one the
    /// table lacks, an index plane too narrow for its dictionary, or an enum
    /// member whose variants its dictionary does not hold.
    SchemaDictionary = 0x8000_000e,
    /// A field's `sb.fields` identity metadata is malformed, gives one column
    /// two type digests, or names one declared field on two columns.
    SchemaFieldIdentity = 0x8000_000f,
}

impl CreateFailure {
    /// Every handle-creation failure, ascending by code. The codes are one
    /// contiguous block, which `creation_failure_codes_are_one_contiguous_block`
    /// (`tests/host_abi.rs`) checks, so a variant missing from this list shows up
    /// as a short block there. The generated host table is rendered from this list.
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
        Self::MemoryExhausted,
        Self::SchemaTooLarge,
        Self::SchemaDictionary,
        Self::SchemaFieldIdentity,
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
            E::MessageTooLarge => Self::SchemaTooLarge,
            E::OutOfMemory => Self::MemoryExhausted,
            E::InvalidDictionaryTable | E::DictionaryMismatch { .. } => Self::SchemaDictionary,
        }
    }
}

impl From<EpInitError> for CreateFailure {
    fn from(error: EpInitError) -> Self {
        match error {
            EpInitError::MemoryExhausted
            | EpInitError::Config(columine_parsing::ConfigError::OutOfMemory)
            | EpInitError::SemanticSchema(
                columine_parsing::validate::SchemaParseError::OutOfMemory,
            )
            | EpInitError::ValueCells(CellRefusal::OutOfMemory)
            | EpInitError::FieldIdentity(IdentityError::OutOfMemory) => Self::MemoryExhausted,
            EpInitError::Config(
                columine_parsing::ConfigError::MissingDictionary
                | columine_parsing::ConfigError::EnumDictionaryMismatch,
            ) => Self::SchemaDictionary,
            EpInitError::FieldIdentity(
                IdentityError::Malformed { .. }
                | IdentityError::TwoTypes { .. }
                | IdentityError::Repeated { .. },
            ) => Self::SchemaFieldIdentity,
            EpInitError::Config(_)
            | EpInitError::SemanticSchema(_)
            | EpInitError::VoAdmission(_)
            | EpInitError::ValueCells(CellRefusal::Unsupported | CellRefusal::Refused { .. }) => {
                Self::Init
            }
        }
    }
}

impl From<columine_arrow::PlaneOutOfMemory> for EpInitError {
    fn from(_: columine_arrow::PlaneOutOfMemory) -> Self {
        Self::MemoryExhausted
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

const INITIAL_WORK_BUFFER_SIZE: usize = 256;
const MAX_WORK_BUFFER_SIZE: usize = MAX_VALUE_BYTES as usize;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EpInitError {
    Config(columine_parsing::ConfigError),
    SemanticSchema(columine_parsing::validate::SchemaParseError),
    /// The value-object capability refused to prepare a declared schema.
    VoAdmission(VoPreparationError),
    /// A field's `sb.fields` identity metadata refused the schema
    /// (specs/columine/10-physical-vocabulary.md §6).
    FieldIdentity(IdentityError),
    /// The value cell writer refused a target of the schema at creation
    /// (specs/columine/10-physical-vocabulary.md §7).
    ValueCells(CellRefusal),
    /// The processor's storage could not be backed; nothing was built.
    MemoryExhausted,
}

/// Structured diagnostic for a semantic payload refusal. The result header
/// carries the existing stage/detail lane; this owned value preserves the
/// path/message payload for native callers and ABI adapters.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PayloadValidationDiagnostic {
    pub stage: u8,
    pub detail: u8,
    /// The refused event, or the refused row of a compact batch.
    pub event_index: u32,
    /// The event's signal type, when it named one; a compact row carries
    /// none.
    pub signal_type: Option<String>,
    pub path: String,
    pub expected: String,
    pub observed: String,
    /// The value-object admission cause, when the capability or its proof
    /// refused the payload.
    pub vo: Option<VoRefusal>,
}

impl PayloadValidationDiagnostic {
    fn new(event_index: usize, signal_type: Option<String>, violation: PayloadViolation) -> Self {
        Self {
            stage: diagnostic_stage::VALIDATION,
            detail: diagnostic_detail::PAYLOAD_VIOLATION,
            event_index: u32::try_from(event_index).unwrap_or(u32::MAX),
            signal_type,
            path: violation.path,
            expected: violation.expected,
            observed: violation.observed,
            vo: violation.vo,
        }
    }
}

/// The public event-processor parse plane, generic over its value-object
/// admission capability and the [`AddressGrammar`] its `axId` nodes are judged
/// by, and over the writer of its value cells: an embedder's [`ValueCells`],
/// or with [`NoValueCells`] columine's native MessagePack cells.
///
/// The prepared envelope is read-only after construction and the plane is
/// the only mutable state, so a validated input can borrow the envelope's
/// admission proofs while it extracts into the plane.
pub struct EventProcessor<
    A = UnavailableVoAdmission,
    G: AddressGrammar = NoAddressGrammar,
    C: ValueCells = NoValueCells,
> {
    semantic: Option<PreparedSchemas<A, G>>,
    last_validation_diagnostic: Option<PayloadValidationDiagnostic>,
    /// Each field's declared identities, decoded once at creation.
    field_identities: Vec<Vec<FieldIdentity>>,
    plane: Plane,
    /// The embedder's value cell writer; `None` is columine's native path.
    cells: Option<C>,
}

/// The processor's mutable parse state: one column plane, its schema and
/// extraction configuration, retained Arrow metadata, and workspaces.
struct Plane {
    options: ParseOptions,
    schema_config: DynamicSchemaConfig,
    extraction_config: Option<ExtractionConfig>,
    dynamic_columns: DynamicColumns,
    /// True when the schema IS the base event log, the options keep the
    /// scanner path, and no value object is declared, so the four-column
    /// scanners can write by index.
    use_base_scanners: bool,
    record_batch_metadata: MetadataStorage,
    /// Reusable MessagePack workspace for declared Binary values and
    /// internal batches.
    work_buffer: Vec<u8>,
    /// Independent workspace for an in-progress undeclared carrier map.
    fallback_work_buffer: Vec<u8>,
    /// The buffer ranges of one caller-built batch, backed at creation for
    /// the schema's buffer count, which bounds the ranges a batch can name.
    compact_ranges: Vec<(usize, usize)>,
    /// The bytes the embedder writes for one cell, retained across batches.
    cell_scratch: Vec<u8>,
}

impl EventProcessor {
    /// Init with schema + field names (the primary path; names enable JSON
    /// key matching) and no value-object capability: a semantic schema that
    /// declares a value object is refused with [`EpInitError::VoAdmission`].
    /// `column_capacity` sizes the batch columns; the wasm exports build every
    /// handle at [`WASM_EVENT_CAPACITY`]. `options` selects the base scanner
    /// path, MessagePack workspace growth, and header diagnostics.
    /// It has no address grammar either ([`NoAddressGrammar`]): an `axId` field
    /// is accepted at init and refuses every address at validation.
    pub fn new(
        options: ParseOptions,
        column_capacity: u32,
        schema_config: DynamicSchemaConfig,
    ) -> Result<Self, EpInitError> {
        Self::new_with_vo_admission(
            options,
            column_capacity,
            schema_config,
            UnavailableVoAdmission,
        )
    }
}

impl<A: VoAdmission, G: AddressGrammar> EventProcessor<A, G> {
    /// [`EventProcessor::new`] with a value-object capability. The semantic
    /// envelope is decoded once and `admission` is prepared once for every
    /// distinct value-object schema it declares, before any metadata,
    /// extraction configuration, or column plane is allocated; a refusal
    /// leaves no processor behind.
    /// Every `axId` node of the envelope is judged by the grammar `G`, which the
    /// caller names with `EventProcessor::<A, G>::new_with_vo_admission`.
    pub fn new_with_vo_admission(
        options: ParseOptions,
        column_capacity: u32,
        schema_config: DynamicSchemaConfig,
        admission: A,
    ) -> Result<Self, EpInitError> {
        Self::create(options, column_capacity, schema_config, admission, None)
    }
}

impl<A: VoAdmission, G: AddressGrammar, C: ValueCells> EventProcessor<A, G, C> {
    /// [`EventProcessor::new_with_vo_admission`] whose value cells
    /// (specs/columine/10-physical-vocabulary.md §7) are written by the
    /// embedder's `cells`: each Binary field whose `sb.cell` names
    /// `cells.encoding()`, the declared record, the carrier and each text.
    /// `cells` is asked once per target before the processor exists; a
    /// target it refuses refuses creation with [`EpInitError::ValueCells`].
    pub fn new_with_cells(
        options: ParseOptions,
        column_capacity: u32,
        schema_config: DynamicSchemaConfig,
        admission: A,
        cells: C,
    ) -> Result<Self, EpInitError> {
        Self::create(
            options,
            column_capacity,
            schema_config,
            admission,
            Some(cells),
        )
    }

    /// One creation for both writers: `None` is columine's native path,
    /// whose targets are put to [`NoValueCells`] so its refusals are the
    /// seam's own.
    fn create(
        options: ParseOptions,
        column_capacity: u32,
        schema_config: DynamicSchemaConfig,
        admission: A,
        mut cells: Option<C>,
    ) -> Result<Self, EpInitError> {
        let semantic = match schema_config.semantic_schema.as_deref() {
            None => None,
            Some(envelope) => {
                let schemas =
                    parse_schema_envelope(envelope).map_err(EpInitError::SemanticSchema)?;
                Some(
                    PreparedSchemas::<A, G>::prepare(schemas, admission).map_err(|refusal| {
                        match refusal {
                            PrepareRefusal::Refused(error) => EpInitError::VoAdmission(error),
                            PrepareRefusal::OutOfMemory => EpInitError::MemoryExhausted,
                        }
                    })?,
                )
            }
        };

        // Identities are decoded before any storage is backed, so a refused
        // schema leaves nothing behind.
        let field_identities =
            columine_parsing::identity::decode_field_identities(&schema_config.identity_metadata)
                .map_err(EpInitError::FieldIdentity)?;

        let record_batch_metadata = MetadataStorage::for_fields(&schema_config.field_metadata)
            .map_err(|columine_arrow::MetadataOutOfMemory| EpInitError::MemoryExhausted)?;

        // A caller-built batch names at most one range per buffer of its
        // schema, so the range table never grows past this.
        let mut compact_ranges = Vec::new();
        compact_ranges
            .try_reserve_exact(
                usize::try_from(schema_config.compute_buffer_count())
                    .map_err(|_| EpInitError::MemoryExhausted)?,
            )
            .map_err(|_| EpInitError::MemoryExhausted)?;

        // Empty-names init keeps an empty extraction config: JSON extraction
        // cannot match keys to columns without field names.
        let extraction_config = if schema_config.field_names.is_empty() {
            None
        } else {
            let mut names: Vec<&str> = Vec::new();
            names
                .try_reserve_exact(schema_config.field_names.len())
                .map_err(|_| EpInitError::MemoryExhausted)?;
            names.extend(schema_config.field_names.iter().map(String::as_str));
            Some(
                columine_parsing::build_extraction_config_with_semantic_schemas(
                    &schema_config.field_metadata,
                    &names,
                    &schema_config.dictionaries,
                    semantic.as_ref().map(PreparedSchemas::schemas),
                    schema_config.payload_discriminator.as_ref(),
                    CellLayout {
                        cell_metadata: &schema_config.cell_metadata,
                        record_members: &schema_config.record_members,
                        text_columns: &schema_config.text_columns,
                        dynamic_members: &schema_config.dynamic_members,
                        encoding: cells.as_ref().map(ValueCells::encoding),
                    },
                )
                .map_err(EpInitError::Config)?,
            )
        };
        if let Some(config) = extraction_config.as_ref() {
            match cells.as_mut() {
                Some(cells) => config.prepare_cells(cells),
                None => config.prepare_cells(&mut NoValueCells),
            }
            .map_err(EpInitError::ValueCells)?;
        }

        // An embedder writes its value cells only on the extraction path.
        // The base scanners store each event's raw `value` span, which is not
        // the frame a value-object admission judged, so a processor that
        // declares value objects always takes the extraction path.
        let use_base_scanners = options.base_path
            && schema_config.is_base_event_log
            && semantic
                .as_ref()
                .is_none_or(|prepared| prepared.presence() == VoPresence::Absent)
            && cells.is_none();
        let dynamic_columns = DynamicColumns::new(&schema_config.field_metadata, column_capacity)?;
        Ok(Self {
            semantic,
            last_validation_diagnostic: None,
            field_identities,
            cells,
            plane: Plane {
                use_base_scanners,
                dynamic_columns,
                record_batch_metadata,
                extraction_config,
                schema_config,
                options,
                work_buffer: Vec::new(),
                fallback_work_buffer: Vec::new(),
                compact_ranges,
                cell_scratch: Vec::new(),
            },
        })
    }

    /// Replace the retained column plane with one sized for `rows`, under
    /// the same clamp as construction. The prepared capability, schema
    /// metadata, extraction configuration, and work buffers are kept. A plane
    /// that cannot be backed is refused and the current one is kept.
    pub fn grow_plane(&mut self, rows: u32) -> Result<(), columine_arrow::PlaneOutOfMemory> {
        let plane = &mut self.plane;
        plane.dynamic_columns = DynamicColumns::new(&plane.schema_config.field_metadata, rows)?;
        Ok(())
    }

    /// Last semantic validation refusal, if the most recent append was refused
    /// by the schema-layer judgment.
    pub fn payload_validation_diagnostic(&self) -> Option<&PayloadValidationDiagnostic> {
        self.last_validation_diagnostic.as_ref()
    }

    /// The retained schema configuration.
    pub fn schema_config(&self) -> &DynamicSchemaConfig {
        &self.plane.schema_config
    }

    /// Each field's declared identities, decoded from its `sb.fields`
    /// metadata at creation (specs/columine/10-physical-vocabulary.md §6):
    /// one list per field, empty for a field that carries none.
    pub fn field_identities(&self) -> &[Vec<FieldIdentity>] {
        &self.field_identities
    }

    /// Run the semantic precheck on `input` and, on success, hand back the
    /// proof that it completed: a [`ValidatedInput`] that can be extracted
    /// exactly once. The processor retains its one column plane; nothing is
    /// cloned or re-validated downstream.
    ///
    /// Every event must name a signal type the envelope declares. With a
    /// value-object node in the envelope, input the judgment cannot read is
    /// also refused here rather than left to extraction. Arrow passthrough
    /// never reaches the column plane on any profile.
    ///
    /// [`ProcessError`] on a semantic refusal carries the header's
    /// diagnostic bytes; the structured path/message form is available
    /// through [`EventProcessor::payload_validation_diagnostic`].
    pub fn validate<'a>(
        &'a mut self,
        input: &'a [u8],
        format: InputFormat,
    ) -> Result<ValidatedInput<'a, A>, ProcessError> {
        let EventProcessor {
            semantic,
            last_validation_diagnostic,
            plane,
            field_identities: _,
            cells,
        } = self;
        // The envelope stays shared for `'a`: the projected frames borrow it.
        let semantic: &'a Option<PreparedSchemas<A, G>> = semantic;
        *last_validation_diagnostic = None;
        let mut projected = Vec::new();
        if let Some(prepared) = semantic {
            // A passthrough batch is never judged, so with value objects armed it
            // could carry a frame nothing admitted: refuse it before any work.
            if format == InputFormat::ArrowPassthrough && prepared.presence() == VoPresence::Present
            {
                return Err(ProcessError {
                    code: ResultCode::InvalidFormat,
                    diagnostic: None,
                });
            }
            let result = match format {
                InputFormat::Json => prepared.validate_json_batch(input).map(|()| Vec::new()),
                InputFormat::Msgpack => prepared.validate_msgpack_batch(input, false),
                InputFormat::MsgpackStream => prepared.validate_msgpack_batch(input, true),
                InputFormat::ArrowPassthrough => Ok(Vec::new()),
            };
            match result {
                Ok(frames) => projected = frames,
                // Ordinary profiles leave unreadable input to extraction,
                // which names the malformed token.
                Err(BatchValidationError::InvalidInput) => {
                    if prepared.presence() == VoPresence::Present {
                        return Err(ProcessError {
                            code: ResultCode::ParseError,
                            diagnostic: Some(ResultDiagnostic {
                                stage: diagnostic_stage::VALIDATION,
                                detail: diagnostic_detail::INVALID_JSON,
                                field_index: NO_FIELD,
                                ..ResultDiagnostic::default()
                            }),
                        });
                    }
                }
                Err(BatchValidationError::OutOfMemory) => {
                    return Err(ProcessError {
                        code: ResultCode::MemoryExhausted,
                        diagnostic: Some(ResultDiagnostic {
                            stage: diagnostic_stage::VALIDATION,
                            detail: diagnostic_detail::MEMORY_EXHAUSTED,
                            field_index: NO_FIELD,
                            ..ResultDiagnostic::default()
                        }),
                    });
                }
                Err(BatchValidationError::Violation {
                    event_index,
                    event_type,
                    violation,
                }) => {
                    *last_validation_diagnostic = Some(PayloadValidationDiagnostic::new(
                        event_index,
                        event_type,
                        violation.into_inner(),
                    ));
                    return Err(ProcessError {
                        code: ResultCode::ParseError,
                        diagnostic: Some(ResultDiagnostic {
                            stage: diagnostic_stage::VALIDATION,
                            detail: diagnostic_detail::PAYLOAD_VIOLATION,
                            ..ResultDiagnostic::default()
                        }),
                    });
                }
            }
        }
        Ok(ValidatedInput {
            plane,
            input,
            format,
            projected,
            cells: cells.as_mut().map(|cells| cells as &mut dyn ValueCells),
        })
    }

    /// The convenience composition: semantic validation, then extraction
    /// into the retained column plane, with no stream dictionary: a value of
    /// a stream-dictionary column refuses the batch.
    pub fn parse<'a>(
        &'a mut self,
        input: &'a [u8],
        format: InputFormat,
    ) -> Result<ParsedBatch<'a>, ProcessError> {
        self.validate(input, format)?.parse()
    }

    /// [`EventProcessor::parse`] against the stream dictionary's `image`
    /// (specs/columine/10-physical-vocabulary.md §5): each value of a
    /// stream-dictionary column stores the ordinal `image` answers, and the
    /// batch encodes the values it added.
    ///
    /// Undoing is the image owner's. Columine never removes a value: on any
    /// `Err`, and when the owner drops the batch or its rows, the owner may
    /// truncate `image` to the length it held before this call (the
    /// batch's [`ParsedBatch::category_mark`]). A retry of the same input
    /// then answers the same ordinals.
    pub fn parse_with<'a>(
        &'a mut self,
        input: &'a [u8],
        format: InputFormat,
        image: &'a mut dyn CategoryImage,
    ) -> Result<ParsedBatch<'a>, ProcessError> {
        self.validate(input, format)?.parse_with(image)
    }

    /// Process one input batch into `output` with no seen-set — validation,
    /// extraction and encoding only, the admission boundary's call:
    /// `[ResultHeader 32B][Arrow IPC stream]`. Returns the header's code.
    /// No stream dictionary: a value of a stream-dictionary column refuses
    /// the batch.
    pub fn create_log_entry(
        &mut self,
        input: &[u8],
        format: InputFormat,
        output: &mut [u8],
        batch_metadata: &[(&str, &str)],
    ) -> ResultCode {
        self.log_entry(input, format, output, batch_metadata, None)
    }

    /// [`EventProcessor::create_log_entry`] against the stream dictionary's
    /// `image`, under [`EventProcessor::parse_with`]'s undo contract: on any
    /// code but `Ok` the owner may truncate `image` to the length it held
    /// before this call.
    pub fn create_log_entry_with(
        &mut self,
        input: &[u8],
        format: InputFormat,
        output: &mut [u8],
        batch_metadata: &[(&str, &str)],
        image: &mut dyn CategoryImage,
    ) -> ResultCode {
        self.log_entry(input, format, output, batch_metadata, Some(image))
    }

    fn log_entry(
        &mut self,
        input: &[u8],
        format: InputFormat,
        output: &mut [u8],
        batch_metadata: &[(&str, &str)],
        image: Option<&mut dyn CategoryImage>,
    ) -> ResultCode {
        if output.len() < RESULT_HEADER_SIZE {
            return ResultCode::OutputTooSmall;
        }
        let diagnostics = self.plane.options.diagnostics;
        let parsed = match self.validate(input, format) {
            Ok(validated) => match image {
                Some(image) => validated.parse_with(image),
                None => validated.parse(),
            },
            Err(error) => Err(error),
        };
        match parsed {
            Ok(batch) => batch.encode(output, batch_metadata),
            Err(error) => {
                match error.diagnostic {
                    Some(diagnostic) if diagnostics => {
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
/// column plane exactly once. It carries the admission proofs of every
/// value-object frame certified as a projection into the reader's layout,
/// so extraction writes their certified bytes without admitting twice.
pub struct ValidatedInput<'a, A: VoAdmission + 'a = UnavailableVoAdmission> {
    plane: &'a mut Plane,
    input: &'a [u8],
    format: InputFormat,
    projected: Vec<A::Frame<'a>>,
    /// The embedder's value cell writer; `None` writes columine's native
    /// cells.
    cells: Option<&'a mut dyn ValueCells>,
}

impl<'a, A: VoAdmission + 'a> ValidatedInput<'a, A> {
    /// Extract the validated input into the retained column plane. The
    /// scanners write the four base event-log columns by index when the
    /// options keep the base path and the schema IS that event log. Each
    /// projected frame's certified bytes are copied into its column, and the
    /// proofs are released once the extraction returns. There is no stream
    /// dictionary: a value of a stream-dictionary column refuses the batch.
    pub fn parse(self) -> Result<ParsedBatch<'a>, ProcessError> {
        self.extract(None)
    }

    /// [`ValidatedInput::parse`] against the stream dictionary's `image`,
    /// under [`EventProcessor::parse_with`]'s undo contract.
    pub fn parse_with(
        self,
        image: &'a mut dyn CategoryImage,
    ) -> Result<ParsedBatch<'a>, ProcessError> {
        self.extract(Some(image))
    }

    fn extract(
        self,
        mut image: Option<&'a mut dyn CategoryImage>,
    ) -> Result<ParsedBatch<'a>, ProcessError> {
        let ValidatedInput {
            plane,
            input,
            format,
            projected,
            cells,
        } = self;
        let frames: Vec<(&[u8], &[u8])> = projected
            .iter()
            .map(|proof| (proof.lent(), proof.bytes()))
            .collect();
        let mark = image.as_deref().map_or(0, |image| image.len());
        let mut batch = if plane.use_base_scanners {
            // The base scanners store each event's raw value bytes, so a
            // projected frame inside them has no cell of its own.
            if !frames.is_empty() {
                return Err(unwritten_projection());
            }
            plane.parse_base(input, format)?
        } else {
            let mut no_categories = NoCategories;
            let lane: &mut dyn CategoryImage = match image.as_deref_mut() {
                Some(image) => image,
                None => &mut no_categories,
            };
            let mut writer = match cells {
                Some(cells) => CellWriter::Embedder(cells),
                None => CellWriter::Native,
            };
            plane.parse_dynamic(
                input,
                format,
                FrameProjections::new(&frames),
                lane,
                &mut writer,
            )?
        };
        batch.image = image;
        batch.mark = mark;
        Ok(batch)
    }
}

/// A projected value-object frame found no cell where its certified bytes
/// could be written; the batch is refused rather than storing the writer's
/// layout.
fn unwritten_projection() -> ProcessError {
    ProcessError {
        code: ResultCode::ParseError,
        diagnostic: Some(ResultDiagnostic {
            stage: diagnostic_stage::VALIDATION,
            detail: diagnostic_detail::PAYLOAD_VIOLATION,
            field_index: NO_FIELD,
            ..ResultDiagnostic::default()
        }),
    }
}

/// The parsed batch: borrowed views of the processor's one column plane,
/// schema configuration and mutable Arrow IPC metadata, and the stream
/// dictionary's image it was parsed against. Consuming
/// [`ParsedBatch::encode`] releases those borrows.
pub struct ParsedBatch<'a> {
    columns: &'a mut DynamicColumns,
    schema_config: &'a DynamicSchemaConfig,
    record_batch_metadata: &'a mut MetadataStorage,
    image: Option<&'a mut dyn CategoryImage>,
    /// The image's length when extraction began: the values from here on
    /// are the ones this batch added.
    mark: u32,
}

/// The values a batch added to the stream dictionary: `image` from ordinal
/// `mark` on, read in place for the dictionary message.
struct AddedValues<'a> {
    image: &'a dyn CategoryImage,
    mark: u32,
}

impl DictionaryValues for AddedValues<'_> {
    fn len(&self) -> u32 {
        self.image.len().saturating_sub(self.mark)
    }

    fn value(&self, index: u32) -> Option<&[u8]> {
        self.image
            .value(self.mark.checked_add(index)?)
            .map(str::as_bytes)
    }
}

/// The values `image` holds from `mark` on, `None` when it holds none: a
/// batch that added no value writes no dictionary message.
fn added_values(image: &dyn CategoryImage, mark: u32) -> Option<AddedValues<'_>> {
    (image.len() > mark).then_some(AddedValues { image, mark })
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

    /// The stream dictionary image's length when this batch's extraction
    /// began, 0 with no image: the length its owner truncates to in order to
    /// undo the batch.
    pub fn category_mark(&self) -> u32 {
        self.mark
    }

    /// Encode the columns into `[ResultHeader][Arrow IPC]`, writing a
    /// success header with zeroed bytes 16..32. When the batch added values
    /// to the stream dictionary, one DictionaryBatch of exactly those values
    /// precedes the record batch, a delta when the image held values before
    /// them (specs/columine/10-physical-vocabulary.md §5).
    ///
    /// On `OutputTooSmall`, a complete result header reports offset 32 and
    /// the IPC writer's required capacity in `arrow_ipc_len`, with zero rows.
    /// That capacity includes `MIN_ARROW_OUTPUT_CAPACITY`; it need not equal
    /// the stream length reported after a successful retry. Retry the same
    /// input with `RESULT_HEADER_SIZE + arrow_ipc_len` output bytes.
    /// No IPC bytes are written on this refusal. If even the header does not
    /// fit, the method returns `OutputTooSmall` without changing the buffer.
    /// Invalid columns and unrepresentable sizes remain `EncodeError`.
    pub fn encode(self, output: &mut [u8], batch_metadata: &[(&str, &str)]) -> ResultCode {
        if output.len() < RESULT_HEADER_SIZE {
            return ResultCode::OutputTooSmall;
        }
        let rows = self.columns.count;
        let mark = self.mark;
        let added = self
            .image
            .as_deref()
            .and_then(|image| added_values(image, mark));
        let stream_dictionary = added.as_ref().map(|values| StreamDictionaryDelta {
            values,
            is_delta: mark > 0,
        });
        match write_arrow_ipc_from_dynamic_columns(
            self.columns,
            self.schema_config,
            &mut output[RESULT_HEADER_SIZE..],
            self.record_batch_metadata,
            batch_metadata,
            stream_dictionary,
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
            Err(IpcError::BufferTooSmall { required })
                if output.len() - RESULT_HEADER_SIZE < required =>
            {
                let Ok(required) = u32::try_from(required) else {
                    write_result_header(output, ResultCode::EncodeError as u32, 0, 0, 0);
                    return ResultCode::EncodeError;
                };
                write_result_header(
                    output,
                    ResultCode::OutputTooSmall as u32,
                    RESULT_HEADER_SIZE as u32,
                    required,
                    0,
                );
                ResultCode::OutputTooSmall
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
impl<A: VoAdmission, G: AddressGrammar, C: ValueCells> EventProcessor<A, G, C> {
    /// Encode one validated CPB1 column batch into `[ResultHeader][Arrow IPC]`
    /// with no stream dictionary: [`EventProcessor::compact_with`] over an
    /// empty image.
    pub fn compact(
        &mut self,
        batch: &[u8],
        output: &mut [u8],
        batch_metadata: &[(&str, &str)],
    ) -> ResultCode {
        self.compact_with(batch, output, batch_metadata, &NoCategories, 0)
    }

    /// Encode one validated CPB1 column batch into `[ResultHeader][Arrow IPC]`.
    ///
    /// Validation and exact-size preflight finish before any Arrow output byte
    /// is mutated. On `EncodeError` caused by insufficient capacity, the
    /// header reports offset 32 and the exact required Arrow IPC length so the
    /// caller can retry with `32 + max(4096, arrow_len)` bytes.
    ///
    /// An encoded column carries its indexes (specs/columine/10-physical-vocabulary.md
    /// §8): each valid index of a schema-dictionary column names a value of
    /// its dictionary, and each of a stream-dictionary column an ordinal
    /// `image` holds, or the batch is `InvalidInput` naming the field and
    /// row. `mark` is the image's length before the values this batch
    /// brought; those values are written as one DictionaryBatch ahead of the
    /// record batch, a delta when `mark` is above zero. A `mark` past the
    /// image is `InvalidInput`.
    ///
    /// A compact batch is caller-built columns: the payload is already
    /// columns, so the semantic judgment has nothing to read. No production
    /// caller arms a semantic envelope on a compact handle, and a processor
    /// whose envelope holds value-object nodes refuses compact batches as
    /// `InvalidFormat` rather than writing value objects it never admitted.
    pub fn compact_with(
        &mut self,
        batch: &[u8],
        output: &mut [u8],
        batch_metadata: &[(&str, &str)],
        image: &dyn CategoryImage,
        mark: u32,
    ) -> ResultCode {
        if output.len() < RESULT_HEADER_SIZE {
            return ResultCode::OutputTooSmall;
        }
        if self
            .semantic
            .as_ref()
            .is_some_and(|prepared| prepared.presence() == VoPresence::Present)
        {
            let diagnostic = ResultDiagnostic {
                stage: diagnostic_stage::VALIDATION,
                detail: diagnostic_detail::PAYLOAD_VIOLATION,
                field_index: NO_FIELD,
                ..ResultDiagnostic::default()
            };
            write_compact_result_header(output, ResultCode::InvalidFormat, 0, 0, 0, &diagnostic);
            return ResultCode::InvalidFormat;
        }
        if mark > image.len() {
            let error = CompactValidationError::mark_past_image();
            write_compact_result_header(output, error.code, 0, 0, 0, &error.diagnostic);
            return error.code;
        }
        let plane = &mut self.plane;
        let view =
            match CompactBatchView::parse(batch, &plane.schema_config, &mut plane.compact_ranges)
                .and_then(|view| {
                    view.check_dictionary_indexes(&plane.schema_config, image)
                        .map(|()| view)
                }) {
                Ok(view) => view,
                Err(error) => {
                    write_compact_result_header(output, error.code, 0, 0, 0, &error.diagnostic);
                    return error.code;
                }
            };
        let added = added_values(image, mark);
        let stream_dictionary = added.as_ref().map(|values| StreamDictionaryDelta {
            values,
            is_delta: mark > 0,
        });

        let required_ipc = match required_arrow_ipc_len(
            &plane.schema_config,
            |index| view.column(index, &plane.schema_config),
            batch_metadata,
            stream_dictionary,
        ) {
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
            return ResultCode::OutputTooSmall;
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
            &plane.schema_config,
            &mut output[RESULT_HEADER_SIZE..],
            &mut plane.record_batch_metadata,
            |index| view.column(index, &plane.schema_config),
            |index| view.null_count(index),
            batch_metadata,
            stream_dictionary,
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
}

impl Plane {
    /// Grow a workspace geometrically toward `MAX_WORK_BUFFER_SIZE`
    /// (`ensureWorkBufferCapacity`). Err means already at the cap.
    /// Double one workspace up to its ceiling: `BufferOverflow` at the
    /// ceiling, `MemoryExhausted` when the larger workspace cannot be backed
    /// (the current one is kept either way).
    fn grow_work_buffer(&mut self, fallback: bool) -> Result<(), json_extractor::ExtractionError> {
        let buffer = if fallback {
            &mut self.fallback_work_buffer
        } else {
            &mut self.work_buffer
        };
        if buffer.len() >= MAX_WORK_BUFFER_SIZE {
            return Err(json_extractor::ExtractionError::BufferOverflow);
        }
        let target = (buffer.len() * 2).min(MAX_WORK_BUFFER_SIZE);
        zero_extend(buffer, target)
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
                json_scanner::parse_json_events(input, cols).map_err(base_scan_error)
            }
            InputFormat::Msgpack => msgpack_scanner::parse_msgpack_events(input, cols)
                .map_err(|error| base_scan_error(error.into())),
            InputFormat::MsgpackStream => msgpack_scanner::parse_msgpack_stream(input, cols)
                .map_err(|error| base_scan_error(error.into())),
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
            // The base event log has no stream-dictionary column.
            image: None,
            mark: 0,
        })
    }

    /// EXTRACTION PATH: extractors into `DynamicColumns`.
    fn parse_dynamic(
        &mut self,
        input: &[u8],
        format: InputFormat,
        projections: FrameProjections<'_>,
        image: &mut dyn CategoryImage,
        cells: &mut CellWriter<'_>,
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
        let extract_result = self.extract_with_growth(
            input,
            format,
            &config,
            projections,
            &mut extraction_diagnostic,
            image,
            cells,
        );
        self.extraction_config = Some(config);

        if let Err(err) = extract_result {
            if err == json_extractor::ExtractionError::UnwrittenProjection {
                return Err(unwritten_projection());
            }
            let code = match err {
                json_extractor::ExtractionError::WorkBufferTooSmall => ResultCode::OutputTooSmall,
                json_extractor::ExtractionError::MemoryExhausted => ResultCode::MemoryExhausted,
                json_extractor::ExtractionError::InvalidJson
                | json_extractor::ExtractionError::InvalidFieldType
                | json_extractor::ExtractionError::BufferOverflow
                | json_extractor::ExtractionError::MsgpackError
                | json_extractor::ExtractionError::TooManyEvents
                | json_extractor::ExtractionError::UnwrittenProjection
                | json_extractor::ExtractionError::Category(_)
                | json_extractor::ExtractionError::Cell(_) => ResultCode::ParseError,
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
                    json_extractor::ExtractionError::WorkBufferTooSmall => {
                        (diagnostic_detail::WORK_BUFFER_TOO_SMALL, 0)
                    }
                    json_extractor::ExtractionError::MemoryExhausted => {
                        (diagnostic_detail::MEMORY_EXHAUSTED, 0)
                    }
                    json_extractor::ExtractionError::BufferOverflow => {
                        (diagnostic_detail::BUFFER_OVERFLOW, 0)
                    }
                    json_extractor::ExtractionError::TooManyEvents => (
                        diagnostic_detail::TOO_MANY_EVENTS,
                        u16::try_from(self.dynamic_columns.capacity).unwrap_or(u16::MAX),
                    ),
                    json_extractor::ExtractionError::Category(_) => {
                        (diagnostic_detail::DICTIONARY_VALUE, 0)
                    }
                    json_extractor::ExtractionError::Cell(_) => {
                        (diagnostic_detail::CELL_REFUSED, 0)
                    }
                    json_extractor::ExtractionError::InvalidJson
                    | json_extractor::ExtractionError::InvalidFieldType
                    | json_extractor::ExtractionError::MsgpackError
                    | json_extractor::ExtractionError::UnwrittenProjection => {
                        (diagnostic_detail::INVALID_JSON, 0)
                    }
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
            image: None,
            mark: 0,
        })
    }

    /// Extraction with workspace growth-and-retry
    /// (`extractJsonEventsWithWorkspaceGrowth` and columine's msgpack
    /// counterpart): on BufferOverflow the (fallback) workspace doubles and
    /// the batch re-extracts from a reset column set; at the cap the error
    /// surfaces.
    ///
    /// `image` keeps the values a discarded attempt added. That is correct:
    /// the retry meets the same texts in the same order, so each answers the
    /// ordinal the discarded attempt gave it, and the batch's added values
    /// are the same either way.
    #[allow(
        clippy::too_many_arguments,
        reason = "one batch: input and format, configuration, projections, diagnostic, the stream image and the value cell writer"
    )]
    fn extract_with_growth(
        &mut self,
        input: &[u8],
        format: InputFormat,
        config: &ExtractionConfig,
        projections: FrameProjections<'_>,
        diagnostic: &mut json_extractor::ExtractionDiagnostic,
        image: &mut dyn CategoryImage,
        cells: &mut CellWriter<'_>,
    ) -> Result<usize, json_extractor::ExtractionError> {
        // A processor that never sees a format retains no scratch allocation
        // for it. The first extraction starts small; overflow proves growth.
        let buffer = if format == InputFormat::Json {
            &mut self.fallback_work_buffer
        } else {
            &mut self.work_buffer
        };
        if buffer.is_empty()
            && let Err(error) = zero_extend(buffer, INITIAL_WORK_BUFFER_SIZE)
        {
            diagnostic.workspace_exhausted();
            return Err(error);
        }
        loop {
            self.dynamic_columns.reset();
            let result = match format {
                InputFormat::Json => json_extractor::extract_json_events(
                    input,
                    config,
                    &mut self.dynamic_columns,
                    &mut self.fallback_work_buffer,
                    diagnostic,
                    &mut *image,
                    &mut *cells,
                    &mut self.cell_scratch,
                ),
                InputFormat::Msgpack | InputFormat::MsgpackStream => {
                    msgpack_extractor::extract_msgpack_events_with_projections(
                        input,
                        config,
                        &mut self.dynamic_columns,
                        &mut self.work_buffer,
                        format == InputFormat::MsgpackStream,
                        projections,
                        &mut *image,
                        &mut *cells,
                        &mut self.cell_scratch,
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
                    if !may_grow {
                        return Err(json_extractor::ExtractionError::BufferOverflow);
                    }
                    if let Err(error) = self.grow_work_buffer(fallback) {
                        if error == json_extractor::ExtractionError::MemoryExhausted {
                            diagnostic.workspace_exhausted();
                        }
                        return Err(error);
                    }
                }
                other => return other,
            }
        }
    }
}
/// Grow `buffer` to `len` zeroed bytes, backed fallibly.
fn zero_extend(buffer: &mut Vec<u8>, len: usize) -> Result<(), json_extractor::ExtractionError> {
    buffer
        .try_reserve_exact(len.saturating_sub(buffer.len()))
        .map_err(|_| json_extractor::ExtractionError::MemoryExhausted)?;
    buffer.resize(len, 0);
    Ok(())
}

/// A base scanner refusal: a column that could not grow is the memory
/// refusal, every other failure a parse error.
fn base_scan_error(error: columine_arrow::ParseError) -> ProcessError {
    ProcessError {
        code: if error == columine_arrow::ParseError::OutOfMemory {
            ResultCode::MemoryExhausted
        } else {
            ResultCode::ParseError
        },
        diagnostic: None,
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

    /// The event log's `timestamp` column: nanoseconds since the epoch, UTC.
    fn timestamp_field() -> SignalSchemaField {
        SignalSchemaField::new_timestamp(arrow_schema::TimeUnit::Nanosecond, false)
    }

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
            timestamp_field(),
            SignalSchemaField::new(ArrowType::Binary, true),
        ]
    }

    fn schema_with_names(fields: &[SignalSchemaField], names: &[u8]) -> DynamicSchemaConfig {
        DynamicSchemaConfig::from_physical_fields_with_names(fields, names).unwrap()
    }

    fn assert_batch_metadata(output: &[u8], expected: &str) {
        let (_, offset, length, _, _) = read_result_header(output);
        let stream = &output[offset as usize..(offset + length) as usize];
        let schema_length = u32::from_le_bytes(stream[4..8].try_into().unwrap()) as usize;
        let schema = arrow_ipc::root_as_message(&stream[8..8 + schema_length]).unwrap();
        assert!(
            schema.custom_metadata().is_none(),
            "schema is entry-independent"
        );
        let batch_start = 8 + schema_length;
        let batch_length =
            u32::from_le_bytes(stream[batch_start + 4..batch_start + 8].try_into().unwrap())
                as usize;
        let batch =
            arrow_ipc::root_as_message(&stream[batch_start + 8..batch_start + 8 + batch_length])
                .unwrap();
        let metadata = batch
            .custom_metadata()
            .expect("per-batch metadata forwarded");
        assert_eq!(metadata.len(), 1);
        assert_eq!(metadata.get(0).key(), Some("source"));
        assert_eq!(metadata.get(0).value(), Some(expected));
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
        assert_eq!(
            processor.compact(&request, &mut output, &[("source", "compact-α")]),
            ResultCode::Ok
        );
        assert_batch_metadata(&output, "compact-α");
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
            ep.create_log_entry(
                input,
                InputFormat::Json,
                &mut output,
                &[("source", "parsed-α")]
            ),
            ResultCode::Ok
        );
        let (code, arrow_offset, arrow_len, processed, dupes) = read_result_header(&output);
        assert_eq!(code, 0);
        assert_eq!(arrow_offset, 32);
        assert!(arrow_len > 0);
        assert_eq!(processed, 1);
        assert_eq!(dupes, 0);
        assert_batch_metadata(&output, "parsed-α");
    }

    #[test]
    fn create_log_entry_base_path() {
        let schema = schema_with_names(&base_fields(), b"id\0type\0timestamp\0value\0");
        assert!(schema.is_base_event_log);
        let mut ep = EventProcessor::new(columine_options(), 100, schema).unwrap();
        assert!(ep.plane.use_base_scanners);
        let input =
            br#"[{"id":"a-1","type":"click","timestamp":100,"value":{"x":1}},{"id":"a-2","type":"view","timestamp":200}]"#;
        let mut output = vec![0u8; 64 * 1024];
        assert_eq!(
            ep.create_log_entry(input, InputFormat::Json, &mut output, &[]),
            ResultCode::Ok
        );
        let (code, _, arrow_len, processed, dupes) = read_result_header(&output);
        assert_eq!(code, 0);
        assert!(arrow_len > 0);
        assert_eq!(processed, 2);
        assert_eq!(dupes, 0);
    }

    /// Base and extraction paths emit byte-identical IPC for the same
    /// four-column content. Both store a numeric timestamp as the
    /// nanoseconds it is; the base path stores raw JSON value bytes, while
    /// extraction stores typed MessagePack, so the shared case has no value.
    #[test]
    fn base_and_dynamic_pipelines_agree() {
        let names = b"id\0type\0timestamp\0value\0";
        let input = br#"[{"id":"a-1","type":"click","timestamp":100}]"#;

        let mut base_ep = EventProcessor::new(
            columine_options(),
            10,
            schema_with_names(&base_fields(), names),
        )
        .unwrap();
        let mut base_out = vec![0u8; 64 * 1024];
        assert_eq!(
            base_ep.create_log_entry(input, InputFormat::Json, &mut base_out, &[]),
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
            dyn_ep.create_log_entry(input, InputFormat::Json, &mut dyn_out, &[]),
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
            ep.create_log_entry(b"not json", InputFormat::Json, &mut output, &[]),
            ResultCode::ParseError
        );
        let (code, ..) = read_result_header(&output);
        assert_eq!(code, ResultCode::ParseError as u32);
        // Diagnostic bytes occupy the reserved region when diagnostics are on.
        assert_eq!(output[20], DIAGNOSTIC_ABI_VERSION);
        assert_eq!(output[21], diagnostic_stage::JSON);

        assert_eq!(
            ep.create_log_entry(b"[]", InputFormat::ArrowPassthrough, &mut output, &[]),
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
        write_result_header_with_diagnostic(
            &mut out,
            ResultCode::OutputTooSmall as u32,
            &diagnostic,
        );
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
        // An undeclared carrier forces the workspace into use. A small
        // reservation makes the growth-policy boundary observable cheaply.
        let fields = vec![
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new(ArrowType::Binary, true),
        ];
        let field_names = format!("id\0{}\0", columine_parsing::UNDECLARED_COLUMN_NAME);
        let schema = schema_with_names(&fields, field_names.as_bytes());
        assert!(!schema.is_base_event_log);

        let mut consumer_ep = EventProcessor::new(admission_options(), 10, schema.clone()).unwrap();
        consumer_ep.plane.work_buffer = vec![0; 8];
        let mut col_ep = EventProcessor::new(columine_options(), 10, schema).unwrap();
        col_ep.plane.work_buffer = vec![0; 8];

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
            consumer_ep.create_log_entry(&input, InputFormat::MsgpackStream, &mut output, &[]),
            ResultCode::ParseError,
            "admission options: no msgpack growth, overflow surfaces"
        );
        assert_eq!(
            col_ep.create_log_entry(&input, InputFormat::MsgpackStream, &mut output, &[]),
            ResultCode::Ok,
            "columine options: workspace grows and the batch succeeds"
        );
    }

    #[test]
    fn lazy_workspaces_match_presized_carrier_extraction_across_growth() {
        let fields = [
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new(ArrowType::Binary, true),
        ];
        let names = format!("id\0{}\0", columine_parsing::UNDECLARED_COLUMN_NAME);
        let schema = schema_with_names(&fields, names.as_bytes());
        let options = ParseOptions {
            base_path: false,
            msgpack_growth: true,
            diagnostics: true,
        };
        let mut lazy = EventProcessor::new(options, 2, schema.clone()).unwrap();
        let mut presized = EventProcessor::new(options, 2, schema).unwrap();
        presized.plane.work_buffer.resize(64 * 1024, 0);
        presized.plane.fallback_work_buffer.resize(16 * 1024, 0);
        let mut actual = vec![0; 256 * 1024];
        let mut expected = vec![0; 256 * 1024];

        for length in [0, 255, 256, 257, 4096, 65_537, 1] {
            let value = "x".repeat(length);
            let json = format!(r#"[{{"id":"a","extra":"{value}"}}]"#);
            let mut msgpack = vec![0x82, 0xa2, b'i', b'd', 0xa1, b'a', 0xa5];
            msgpack.extend(b"extra");
            msgpack.push(0xdb);
            msgpack.extend(u32::try_from(length).unwrap().to_be_bytes());
            msgpack.extend(value.as_bytes());
            for (input, format) in [
                (json.as_bytes(), InputFormat::Json),
                (msgpack.as_slice(), InputFormat::MsgpackStream),
            ] {
                assert_eq!(
                    lazy.create_log_entry(input, format, &mut actual, &[]),
                    ResultCode::Ok
                );
                assert_eq!(
                    presized.create_log_entry(input, format, &mut expected, &[]),
                    ResultCode::Ok
                );
                let (_, offset, length, rows, _) = read_result_header(&actual);
                assert_eq!(rows, 1);
                let end = (offset + length) as usize;
                assert_eq!(&actual[..end], &expected[..end]);
            }
        }
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
            ep.create_log_entry(&input, InputFormat::MsgpackStream, &mut output, &[]),
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
            ep.create_log_entry(exact, InputFormat::MsgpackStream, &mut output, &[]),
            ResultCode::Ok
        );
    }

    #[test]
    fn semantic_validation_refuses_before_extraction() {
        let base = DynamicSchemaConfig::from_physical_fields_with_names(
            &[
                SignalSchemaField::new(ArrowType::Utf8, false),
                SignalSchemaField::new(ArrowType::Utf8, false),
                timestamp_field(),
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
            ep.create_log_entry(input, InputFormat::Json, &mut output, &[]),
            ResultCode::ParseError
        );
        let diagnostic = ep.payload_validation_diagnostic().unwrap();
        assert_eq!(diagnostic.path, "value.greeting");
        assert_eq!(diagnostic.expected, "string");
        assert_eq!(diagnostic.observed, "number");
        assert_eq!(diagnostic.event_index, 0);
        assert_eq!(diagnostic.signal_type.as_deref(), Some("hi"));
        assert_eq!(read_result_header(&output).0, ResultCode::ParseError as u32);
        assert!(output[32..].iter().all(|byte| *byte == 0xa5));
    }

    use columine_parsing::validate::{SchemaParseError, VoAdmissionError, VoMetadataField};

    const LINE_VO: &str = r#"{"kind":"vo","root":"orders.Line","frame":"0a1b","rows":["00ff"]}"#;

    fn semantic_schema(envelope: &str) -> DynamicSchemaConfig {
        DynamicSchemaConfig {
            semantic_schema: Some(envelope.as_bytes().to_vec()),
            ..schema_with_names(&base_fields(), b"id\0type\0timestamp\0value\0")
        }
    }

    /// The same value-object metadata declared twice, under two event types.
    fn line_envelope() -> String {
        format!(
            r#"{{"line":{{"kind":"object","fields":{{"frame":{LINE_VO}}}}},"lines":{{"kind":"array","item":{LINE_VO}}}}}"#
        )
    }

    #[test]
    fn vo_schema_without_a_capability_refuses_at_init() {
        let refused = Some(EpInitError::VoAdmission(VoPreparationError {
            root: "orders.Line".into(),
            error: VoAdmissionError::Unavailable,
        }));
        assert_eq!(
            EventProcessor::new(admission_options(), 8, semantic_schema(&line_envelope())).err(),
            refused
        );
        assert_eq!(
            EventProcessor::<UnavailableVoAdmission, NoAddressGrammar>::new_with_vo_admission(
                admission_options(),
                8,
                semantic_schema(&line_envelope()),
                UnavailableVoAdmission,
            )
            .err(),
            refused
        );
    }

    #[test]
    fn malformed_vo_metadata_refuses_at_init_before_any_capability_runs() {
        let envelope = r#"{"line":{"kind":"vo","root":"orders.Line","frame":"0A","rows":["00"]}}"#;
        let malformed = Some(EpInitError::SemanticSchema(
            SchemaParseError::InvalidVoMetadata(VoMetadataField::Frame),
        ));
        assert_eq!(
            EventProcessor::new(admission_options(), 8, semantic_schema(envelope)).err(),
            malformed
        );
    }

    #[test]
    fn grow_plane_replaces_only_the_column_plane() {
        let envelope = r#"{"hi":{"kind":"object","fields":{"greeting":{"kind":"string"}}}}"#;
        let mut ep =
            EventProcessor::new(admission_options(), 2, semantic_schema(envelope)).unwrap();
        ep.plane.fallback_work_buffer.resize(64, 0);
        ep.grow_plane(16).expect("plane fits");
        assert_eq!(ep.plane.dynamic_columns.capacity, 16);
        assert_eq!(
            ep.plane.fallback_work_buffer.len(),
            64,
            "work buffers are kept"
        );
        let mut output = vec![0u8; 4096];
        assert_eq!(
            ep.create_log_entry(
                br#"[{"id":"a","type":"hi","timestamp":1,"value":{"greeting":42}}]"#,
                InputFormat::Json,
                &mut output,
                &[]
            ),
            ResultCode::ParseError,
            "the prepared envelope still judges"
        );
    }

    #[test]
    fn unknown_event_types_refuse_before_columns_change() {
        let envelope = r#"{"hi":{"kind":"object","fields":{"greeting":{"kind":"string"}}}}"#;
        let mut ep =
            EventProcessor::new(admission_options(), 8, semantic_schema(envelope)).unwrap();
        let mut output = vec![0u8; 64 * 1024];
        assert_eq!(
            ep.create_log_entry(
                br#"[{"id":"a","type":"hi","timestamp":1,"value":{"greeting":"ok"}}]"#,
                InputFormat::Json,
                &mut output,
                &[]
            ),
            ResultCode::Ok
        );
        assert_eq!(ep.plane.dynamic_columns.count, 1);
        for (input, signal_type, observed) in [
            (
                &br#"[{"id":"a","type":"other","timestamp":1,"value":{"greeting":42}}]"#[..],
                Some("other"),
                "undeclared",
            ),
            (&br#"[{"id":"b","timestamp":2}]"#[..], None, "absent"),
        ] {
            assert_eq!(
                ep.create_log_entry(input, InputFormat::Json, &mut output, &[]),
                ResultCode::ParseError
            );
            let diagnostic = ep.payload_validation_diagnostic().unwrap();
            assert_eq!(diagnostic.signal_type.as_deref(), signal_type);
            assert_eq!(
                (diagnostic.path.as_str(), diagnostic.observed.as_str()),
                ("type", observed)
            );
            assert_eq!(
                ep.plane.dynamic_columns.count, 1,
                "the refused batch left no row"
            );
        }
    }

    #[test]
    fn repeated_type_or_value_refuses_before_columns_change() {
        let envelope = r#"{"hi":{"kind":"object","fields":{"greeting":{"kind":"string"}}}}"#;
        let mut ep =
            EventProcessor::new(admission_options(), 8, semantic_schema(envelope)).unwrap();
        let mut output = vec![0u8; 64 * 1024];
        assert_eq!(
            ep.create_log_entry(
                br#"[{"id":"a","type":"hi","timestamp":1,"value":{"greeting":"ok"}}]"#,
                InputFormat::Json,
                &mut output,
                &[]
            ),
            ResultCode::Ok
        );
        assert_eq!(ep.plane.dynamic_columns.count, 1);
        for (input, path) in [
            (
                &br#"[{"id":"b","type":"hi","type":"hi","timestamp":2,"value":{"greeting":"ok"}}]"#[..],
                "type",
            ),
            (
                &br#"[{"id":"b","type":"hi","timestamp":2,"value":{"greeting":"ok"},"value":{"greeting":42}}]"#[..],
                "value",
            ),
        ] {
            assert_eq!(
                ep.create_log_entry(input, InputFormat::Json, &mut output, &[]),
                ResultCode::ParseError
            );
            let diagnostic = ep.payload_validation_diagnostic().unwrap();
            assert_eq!(
                (diagnostic.path.as_str(), diagnostic.observed.as_str()),
                (path, "repeated key")
            );
            assert_eq!(ep.plane.dynamic_columns.count, 1, "the refused batch left no row");
        }
    }

    #[test]
    fn range_and_exact_array_refuse_through_the_real_ingress() {
        let envelope = r#"{"order":{"kind":"object","fields":{"qty":{"kind":"range","min":1,"max":10,"integer":true},"dims":{"kind":"array","item":{"kind":"number"},"length":3}}}}"#;
        let mut ep =
            EventProcessor::new(admission_options(), 8, semantic_schema(envelope)).unwrap();
        let mut output = vec![0u8; 64 * 1024];
        assert_eq!(
            ep.create_log_entry(
                br#"[{"id":"a","type":"order","timestamp":1,"value":{"qty":3,"dims":[1,2.5,3]}}]"#,
                InputFormat::Json,
                &mut output,
                &[]
            ),
            ResultCode::Ok
        );
        assert_eq!(ep.plane.dynamic_columns.count, 1);

        for (input, path, expected, observed) in [
            (
                &br#"[{"id":"a","type":"order","timestamp":1,"value":{"qty":11,"dims":[1,2,3]}}]"#
                    [..],
                "value.qty",
                "safe integer in [1, 10]",
                "number",
            ),
            (
                &br#"[{"id":"a","type":"order","timestamp":1,"value":{"qty":2.5,"dims":[1,2,3]}}]"#
                    [..],
                "value.qty",
                "safe integer in [1, 10]",
                "number",
            ),
            (
                &br#"[{"id":"a","type":"order","timestamp":1,"value":{"qty":3,"dims":[1,2]}}]"#[..],
                "value.dims",
                "array of length 3",
                "array of length 2",
            ),
        ] {
            let mut refused = vec![0xa5; 4096];
            assert_eq!(
                ep.create_log_entry(input, InputFormat::Json, &mut refused, &[]),
                ResultCode::ParseError
            );
            let diagnostic = ep.payload_validation_diagnostic().unwrap();
            assert_eq!(
                (
                    diagnostic.path.as_str(),
                    diagnostic.expected.as_str(),
                    diagnostic.observed.as_str(),
                    diagnostic.vo,
                ),
                (path, expected, observed, None)
            );
            assert!(refused[32..].iter().all(|byte| *byte == 0xa5));
            assert_eq!(
                ep.plane.dynamic_columns.count, 1,
                "the refused batch left no row"
            );
        }
    }

    #[test]
    fn an_optional_typed_member_stores_null_and_absent_as_one_arrow_null() {
        use columine_parsing::{CellExt, ColumnValue};
        let envelope = r#"{"hi":{"kind":"object","fields":{"note":{"kind":"optional","value":{"kind":"string"}}}}}"#;
        let fields = [
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new(ArrowType::Utf8, false),
            timestamp_field(),
            SignalSchemaField::new(ArrowType::Utf8, true),
        ];
        let schema = DynamicSchemaConfig {
            semantic_schema: Some(envelope.as_bytes().to_vec()),
            ..schema_with_names(&fields, b"id\0type\0timestamp\0value.note\0")
        };
        let mut ep = EventProcessor::new(admission_options(), 8, schema).unwrap();
        let mut output = vec![0u8; 64 * 1024];
        assert_eq!(
            ep.create_log_entry(
                br#"[{"id":"a","type":"hi","timestamp":1,"value":{"note":"x"}},{"id":"b","type":"hi","timestamp":2,"value":{"note":null}},{"id":"c","type":"hi","timestamp":3,"value":{}}]"#,
                InputFormat::Json,
                &mut output,
                &[]
            ),
            ResultCode::Ok
        );
        let columns = &ep.plane.dynamic_columns;
        assert_eq!(
            [0, 1, 2].map(|row| columns.cell(3, row)),
            [Some(ColumnValue::Utf8("x".into())), None, None]
        );
    }
    /// The cells of `columns` after one event of `event_type` per payload,
    /// each payload member given as its MessagePack bytes (a member not
    /// listed is absent), required equal through the MessagePack lane and the
    /// JSON lane (`json`, the same events) and returned.
    fn cells_on_both_lanes<const C: usize, const R: usize>(
        envelope: &str,
        fields: &[SignalSchemaField],
        names: &[u8],
        event_type: &str,
        payloads: [&[(&str, &[u8])]; R],
        json: &[u8],
        columns: [usize; C],
    ) -> [[Option<columine_parsing::ColumnValue<'static>>; R]; C] {
        use columine_parsing::CellExt;
        let schema = DynamicSchemaConfig {
            semantic_schema: Some(envelope.as_bytes().to_vec()),
            ..schema_with_names(fields, names)
        };
        let text = |out: &mut Vec<u8>, text: &str| {
            out.push(0xa0 | u8::try_from(text.len()).unwrap());
            out.extend(text.as_bytes());
        };
        let mut msgpack = Vec::new();
        for (row, members) in payloads.iter().enumerate() {
            msgpack.push(0x84);
            text(&mut msgpack, "id");
            text(&mut msgpack, &row.to_string());
            text(&mut msgpack, "type");
            text(&mut msgpack, event_type);
            text(&mut msgpack, "timestamp");
            msgpack.push(u8::try_from(row + 1).unwrap());
            text(&mut msgpack, "value");
            msgpack.push(0x80 | u8::try_from(members.len()).unwrap());
            for (name, value) in members.iter() {
                text(&mut msgpack, name);
                msgpack.extend(*value);
            }
        }
        let cells = |input: &[u8], format| {
            let mut ep = EventProcessor::new(admission_options(), 8, schema.clone()).unwrap();
            let mut output = vec![0u8; 64 * 1024];
            assert_eq!(
                ep.create_log_entry(input, format, &mut output, &[]),
                ResultCode::Ok
            );
            let columns_plane = &ep.plane.dynamic_columns;
            columns.map(|column| std::array::from_fn(|row| columns_plane.cell(column, row)))
        };
        let from_json = cells(json, InputFormat::Json);
        assert_eq!(cells(&msgpack, InputFormat::MsgpackStream), from_json);
        from_json
    }

    /// A dynamic value keeps null as data at every depth, its top level
    /// included: an `optional(unknown)` member sent as null is a present cell
    /// holding MessagePack nil, and only a missing member is the Arrow null.
    /// A structured member keeps null as its one empty state. Both lanes
    /// store the same bytes.
    #[test]
    fn a_dynamic_member_keeps_null_as_data_on_both_lanes() {
        use columine_parsing::ColumnValue;
        let fields = [
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new(ArrowType::Utf8, false),
            timestamp_field(),
            SignalSchemaField::new(ArrowType::Binary, true),
            SignalSchemaField::new(ArrowType::Binary, true),
        ];
        assert_eq!(
            cells_on_both_lanes(
                r#"{"hi":{"kind":"object","fields":{"data":{"kind":"optional","value":{"kind":"unknown"}},"shape":{"kind":"optional","value":{"kind":"array","item":{"kind":"number"}}}}}}"#,
                &fields,
                b"id\0type\0timestamp\0value.data\0value.shape\0",
                "hi",
                [&[("shape", &b"\xc0"[..])], &[("data", &b"\xc0"[..])], &[("data", &b"\xa2hi"[..])]],
                br#"[{"id":"a","type":"hi","timestamp":1,"value":{"shape":null}},{"id":"b","type":"hi","timestamp":2,"value":{"data":null}},{"id":"c","type":"hi","timestamp":3,"value":{"data":"hi"}}]"#,
                [3, 4],
            ),
            [
                [
                    None,
                    Some(ColumnValue::Binary(vec![0xc0].into())),
                    Some(ColumnValue::Binary(vec![0xa2, b'h', b'i'].into())),
                ],
                [None, None, None],
            ]
        );
    }

    /// A payload whose root is a union of object arms: a member an arm
    /// declares dynamic keeps an explicit null as data, the same as under an
    /// object root, and a missing member is the Arrow null.
    #[test]
    fn a_dynamic_member_of_a_union_arm_keeps_null_as_data_on_both_lanes() {
        use columine_parsing::ColumnValue;
        let fields = [
            SignalSchemaField::new(ArrowType::Utf8, false),
            SignalSchemaField::new(ArrowType::Utf8, false),
            timestamp_field(),
            SignalSchemaField::new(ArrowType::Utf8, true),
            SignalSchemaField::new(ArrowType::Utf8, true),
            SignalSchemaField::new(ArrowType::Binary, true),
        ];
        assert_eq!(
            cells_on_both_lanes(
                r#"{"done":{"kind":"union","variants":[{"kind":"object","fields":{"kind":{"kind":"enum","variants":["notify"]},"target":{"kind":"string"}}},{"kind":"object","fields":{"kind":{"kind":"enum","variants":["verdict"]},"returnValue":{"kind":"optional","value":{"kind":"unknown"}}}}]}}"#,
                &fields,
                b"id\0type\0timestamp\0value.kind\0value.target\0value.returnValue\0",
                "done",
                [
                    &[("kind", &b"\xa7verdict"[..])],
                    &[("kind", &b"\xa7verdict"[..]), ("returnValue", &b"\xc0"[..])],
                    &[("kind", &b"\xa7verdict"[..]), ("returnValue", &b"\xa2hi"[..])],
                    &[("kind", &b"\xa6notify"[..]), ("target", &b"\xa1t"[..])],
                ],
                br#"[{"id":"a","type":"done","timestamp":1,"value":{"kind":"verdict"}},{"id":"b","type":"done","timestamp":2,"value":{"kind":"verdict","returnValue":null}},{"id":"c","type":"done","timestamp":3,"value":{"kind":"verdict","returnValue":"hi"}},{"id":"d","type":"done","timestamp":4,"value":{"kind":"notify","target":"t"}}]"#,
                [5],
            ),
            [[
                None,
                Some(ColumnValue::Binary(vec![0xc0].into())),
                Some(ColumnValue::Binary(vec![0xa2, b'h', b'i'].into())),
                None,
            ]]
        );
    }
    /// The one address the test grammar admits: a host's grammar, named by type.
    struct TestGrammar;

    impl AddressGrammar for TestGrammar {
        fn violation(value: &str) -> Option<&'static str> {
            (value != "d~g~t").then_some("not the test address")
        }
    }

    #[test]
    fn an_ax_id_is_judged_by_the_grammar_the_processor_is_typed_with() {
        let envelope = r#"{"owned":{"kind":"object","fields":{"owner":{"kind":"axId"}}}}"#;
        let mut ep = EventProcessor::<UnavailableVoAdmission, TestGrammar>::new_with_vo_admission(
            admission_options(),
            8,
            semantic_schema(envelope),
            UnavailableVoAdmission,
        )
        .unwrap();
        let mut output = vec![0u8; 64 * 1024];
        assert_eq!(
            ep.create_log_entry(
                br#"[{"id":"a","type":"owned","timestamp":1,"value":{"owner":"d~g~t"}}]"#,
                InputFormat::Json,
                &mut output,
                &[]
            ),
            ResultCode::Ok
        );
        assert_eq!(ep.plane.dynamic_columns.count, 1);

        let mut refused = vec![0xa5; 4096];
        assert_eq!(
            ep.create_log_entry(
                br#"[{"id":"b","type":"owned","timestamp":2,"value":{"owner":"other"}}]"#,
                InputFormat::Json,
                &mut refused,
                &[]
            ),
            ResultCode::ParseError
        );
        let diagnostic = ep.payload_validation_diagnostic().unwrap();
        assert_eq!(
            (
                diagnostic.path.as_str(),
                diagnostic.expected.as_str(),
                diagnostic.observed.as_str(),
                diagnostic.vo,
            ),
            ("value.owner", "axId: not the test address", "string", None)
        );
        assert!(refused[32..].iter().all(|byte| *byte == 0xa5));
        assert_eq!(
            ep.plane.dynamic_columns.count, 1,
            "the refused batch left no row"
        );
    }
}
