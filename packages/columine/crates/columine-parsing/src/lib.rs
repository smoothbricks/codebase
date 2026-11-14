//! JSON and MessagePack parsing into Arrow-compatible columnar storage.
//!
//! The parsers share scanners and typed extraction diagnostics; column storage
//! and schema metadata live in [`columine-arrow`]. [`ColumnValue`] remains the
//! extractors' typed-token carrier and the tests' readable cell view.

use std::borrow::Cow;
use std::collections::HashMap;

#[cfg(test)]
mod cross_format;
#[cfg(test)]
#[path = "../../columine-vm/tests/common/memory.rs"]
mod memory;

pub mod identity;
pub mod instant;
pub mod json_extractor;
pub mod json_parser;
pub mod json_scanner;
pub mod msgpack_extractor;
pub mod msgpack_scanner;
pub mod scan;
pub mod validate;

pub use columine_arrow::schema::{DiscriminatedMember, PayloadDiscriminator};
pub use columine_arrow::{
    ArrowType, BASE_EVENT_LOG_FIELDS, BASE_EVENT_LOG_NAMES, ColumnStorage, DynamicColumns,
    MAX_EVENTS_PER_BATCH, MAX_VALUE_BYTES, ParseError, PlaneKind, SignalSchemaField,
};
use columine_arrow::{
    DictionaryEncoding, DictionarySlot, DynamicMember, RecordMember, SchemaDictionary, TextColumn,
};
use columine_types::category::CategoryImage;
use columine_types::cells::{
    CellRefusal, CellTarget, NoValueCells, RecordRow, TEXT_HASH_BYTES, TextPlace, ValueCells,
};
use columine_types::value::{ValueKind, ValueView};

/// Column indices of the base event log, matching
/// [`BASE_EVENT_LOG_FIELDS`] by position.
pub mod base_column {
    pub const ID: u32 = 0;
    pub const TYPE: u32 = 1;
    pub const TIMESTAMP: u32 = 2;
    pub const VALUE: u32 = 3;
}

/// Commit one base event into the four base-event-log columns.
///
/// Both scanners end here, so the column order and the null-`value` rule live
/// in one place rather than being restated per format.
pub(crate) fn commit_base_event(
    columns: &mut DynamicColumns,
    id: &[u8],
    event_type: &[u8],
    timestamp_nanos: i64,
    value: Option<&[u8]>,
) -> Result<(), ParseError> {
    if !columns.begin_row() {
        return Err(ParseError::TooManyEvents);
    }
    let result = (|| {
        columns.append_variable(base_column::ID, id)?;
        columns.append_variable(base_column::TYPE, event_type)?;
        columns.append_int(base_column::TIMESTAMP, timestamp_nanos)?;
        match value {
            Some(bytes) => columns.append_variable(base_column::VALUE, bytes),
            None => columns.append_null(base_column::VALUE),
        }
    })();
    if result.is_err() {
        // A half-written row must not become visible.
        columns.abandon_row();
        return result;
    }
    columns.end_row();
    Ok(())
}

/// Base event-log columns for scanner tests: the one column store, configured
/// with the base schema the scanners write by index.
#[cfg(test)]
pub(crate) fn base_columns(capacity: u32) -> DynamicColumns {
    DynamicColumns::new(&BASE_EVENT_LOG_FIELDS, capacity).expect("column plane fits")
}

/// A base event as a row view over the base event-log columns (scanner tests
/// and row-oriented consumers; the columnar buffers are the storage of record).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ParsedEvent {
    pub id: String,
    pub event_type: String,
    pub timestamp_nanos: i64,
    pub value: Option<Vec<u8>>,
}

/// Row view over the real columnar storage.
pub fn parsed_event(columns: &DynamicColumns, row: u32) -> Option<ParsedEvent> {
    if row >= columns.count {
        return None;
    }
    let text = |column: u32| -> Option<String> {
        Some(String::from_utf8_lossy(columns.get_column(column)?.read_variable(row)?).into_owned())
    };
    Some(ParsedEvent {
        id: text(base_column::ID)?,
        event_type: text(base_column::TYPE)?,
        timestamp_nanos: columns.get_column(base_column::TIMESTAMP)?.read_int(row)?,
        value: (!columns.is_null(base_column::VALUE, row))
            .then(|| {
                columns
                    .get_column(base_column::VALUE)
                    .and_then(|column| column.read_variable(row))
                    .map(<[u8]>::to_vec)
            })
            .flatten(),
    })
}

/// Typed value carrier between the token stream and the typed appends.
///
/// One variant per plane KIND, not per plane: the column knows its own width,
/// so a producer says "this is a signed integer" and the plane decides whether
/// that integer fits. Twenty-three planes therefore need seven carriers.
///
/// Text and bytes borrow the input wherever it already holds them, so a
/// value reaches its column with one copy, the column's own; only a JSON
/// string with escapes or a hex-decoded width carries owned bytes.
#[derive(Clone, Debug, PartialEq)]
pub enum ColumnValue<'v> {
    /// Text for the Utf8 and LargeUtf8 planes.
    Utf8(Cow<'v, str>),
    /// Opaque bytes for the Binary and LargeBinary planes.
    Binary(Cow<'v, [u8]>),
    /// Signed integer for the Int8/16/32/64 and IntervalYearMonth planes,
    /// which is also every temporal plane.
    Int(i64),
    /// Unsigned integer for the UInt8/16/32/64 planes.
    UInt(u64),
    /// Float for the Float16/32/64 planes.
    Float(f64),
    Bool(bool),
    /// Exactly-width bytes for the decimal, wide-interval and fixed-size
    /// binary planes, which the plane does not interpret.
    FixedBytes(Cow<'v, [u8]>),
}

/// Dispatch an extracted value to the matching typed append operation.
///
/// [`ColumnValue`] materialization mirrors the parser's owned tokens; it is
/// useful for tests and keeps storage concerns out of the token stream.
pub(crate) fn append_cell(
    columns: &mut DynamicColumns,
    column: usize,
    value: Option<ColumnValue<'_>>,
) -> Result<(), ParseError> {
    let column = column as u32;
    match value {
        None => columns.append_null(column),
        Some(ColumnValue::Utf8(text)) => columns.append_variable(column, text.as_bytes()),
        Some(ColumnValue::Binary(bytes)) => columns.append_variable(column, &bytes),
        Some(ColumnValue::Int(value)) => columns.append_int(column, value),
        Some(ColumnValue::UInt(value)) => columns.append_uint(column, value),
        Some(ColumnValue::Float(value)) => columns.append_float(column, value),
        Some(ColumnValue::Bool(value)) => columns.append_bool(column, value),
        Some(ColumnValue::FixedBytes(bytes)) => columns.append_fixed_bytes(column, &bytes),
    }
}

/// Read one cell back out of the real columnar storage as a typed value
/// (test/differential view; production consumers read the Arrow buffers).
pub fn read_cell(
    columns: &DynamicColumns,
    column: usize,
    row: usize,
) -> Option<ColumnValue<'static>> {
    let (col_idx, row_idx) = (column as u32, row as u32);
    if columns.is_null(col_idx, row_idx) {
        return None;
    }
    let storage = columns.get_column(col_idx)?;
    Some(match storage.kind {
        // Every row of the Null plane is null, so `is_null` already returned;
        // reaching here would mean a validity bit was set on a plane that has
        // no value to be valid.
        PlaneKind::Empty => return None,
        PlaneKind::Bool => ColumnValue::Bool(storage.read_bool(row_idx)?),
        PlaneKind::SignedInt { .. } => ColumnValue::Int(storage.read_int(row_idx)?),
        PlaneKind::UnsignedInt { .. } => ColumnValue::UInt(storage.read_uint(row_idx)?),
        PlaneKind::Float { .. } => ColumnValue::Float(storage.read_float(row_idx)?),
        PlaneKind::FixedBytes { .. } => {
            ColumnValue::FixedBytes(Cow::Owned(storage.read_fixed_bytes(row_idx)?.to_vec()))
        }
        PlaneKind::Text { .. } => ColumnValue::Utf8(Cow::Owned(
            String::from_utf8_lossy(storage.read_variable(row_idx)?).into_owned(),
        )),
        PlaneKind::Bytes { .. } => {
            ColumnValue::Binary(Cow::Owned(storage.read_variable(row_idx)?.to_vec()))
        }
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct FieldLookup {
    pub column: usize,
    /// The whole field descriptor, not just its tag: the extractors coerce by
    /// plane kind, and one plane's kind depends on its `type_param`.
    pub field: SignalSchemaField,
}
/// Schema-name lookup for extraction: O(1) name → column/type, plus the
/// [`UNDECLARED_COLUMN_NAME`] carrier column when declared.
#[derive(Debug)]
pub struct ExtractionConfig {
    pub(crate) field_entries: Vec<(usize, SignalSchemaField)>,
    pub(crate) field_map: HashMap<String, FieldLookup>,
    /// Every `value.<member>` column keyed by `<member>`, so resolving a bare
    /// member name against its payload column builds no string.
    pub(crate) payload_map: HashMap<String, FieldLookup>,
    pub(crate) fallback_column: Option<usize>,
    pub(crate) presence_entries: Vec<(usize, usize)>,
    /// Relative payload paths whose object schemas opt into capture.
    pub(crate) open_paths: Vec<String>,
    pub(crate) semantic_schemas: Option<crate::validate::SemanticSchemaSet>,
    /// Whether any column names a `value.<field>` payload path — the arming
    /// condition for the nested-envelope descent in the extractors.
    pub(crate) has_value_fields: bool,
    /// The envelope key whose string value discriminates payload members;
    /// `None` leaves every nested member resolving by its plain name.
    pub(crate) discriminator_key: Option<String>,
    /// `(discriminant, payload member)` → its discriminated column, from the
    /// caller's [`PayloadDiscriminator`]. A member whose physical type differs
    /// between discriminants lives in a column of its own on the flat lane,
    /// and an admitted nested payload must land in that same column or the
    /// member is silently lost to the carrier.
    pub(crate) variant_map: MemberTable<FieldLookup>,
    /// The values of every dictionary of the creation call's table, in table
    /// order; a stream dictionary's values are empty, they are its owner's.
    pub(crate) dictionaries: Vec<Vec<Option<String>>>,
    /// Each column's position in [`Self::dictionaries`] when the column is
    /// encoded against a schema dictionary, by column: a lane stores a text's
    /// index among that dictionary's values.
    pub(crate) column_dictionaries: Vec<Option<usize>>,
    /// `(event type, payload member)` for every dynamic member no column
    /// places: those an event's schema declares dynamic, under an object root
    /// or any arm of a union root, and those the schema's owner names. An
    /// explicit null in such a member's place in the declared record is data,
    /// never the empty state. A dynamic member a column places is not here:
    /// it marks its column in [`Self::dynamic_columns`].
    pub(crate) dynamic_members: MemberTable<()>,
    /// Whether each column's cell is a dynamic value, by column: an explicit
    /// null in it is data whichever member or spelling reached it.
    pub(crate) dynamic_columns: Vec<bool>,
    /// Who writes each column's cells, by column ([`CellRole`]).
    pub(crate) cell_roles: Vec<CellRole>,
    /// The [`DECLARED_RECORD_COLUMN_NAME`] column, when the schema has one.
    pub(crate) record_column: Option<usize>,
    /// `(discriminant, payload member)` → the routing row of a declared
    /// member that lives in the row's declared record.
    pub(crate) record_routes: MemberTable<RecordRoute>,
}

/// Values keyed by `(discriminant or event type, payload member)`, held sorted so a lookup
/// by borrowed text builds no key.
#[derive(Clone, Debug)]
pub(crate) struct MemberTable<T> {
    entries: Vec<(String, String, T)>,
}

impl<T> Default for MemberTable<T> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
        }
    }
}

impl<T> MemberTable<T> {
    fn position(&self, discriminant: &str, member: &str) -> Result<usize, usize> {
        self.entries.binary_search_by(|(at, named, _)| {
            (at.as_str(), named.as_str()).cmp(&(discriminant, member))
        })
    }

    /// Bind `(discriminant, member)` to `value`; a later binding of the same
    /// pair replaces the earlier one. Growing the table is fallible.
    fn bind(&mut self, discriminant: &str, member: &str, value: T) -> Result<(), ConfigError> {
        match self.position(discriminant, member) {
            Ok(index) => self.entries[index].2 = value,
            Err(index) => {
                self.entries
                    .try_reserve(1)
                    .map_err(|_| ConfigError::OutOfMemory)?;
                self.entries
                    .insert(index, (owned(discriminant)?, owned(member)?, value));
            }
        }
        Ok(())
    }

    pub(crate) fn get(&self, discriminant: &str, member: &str) -> Option<&T> {
        self.position(discriminant, member)
            .ok()
            .map(|index| &self.entries[index].2)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Every bound value, in key order.
    pub(crate) fn values(&self) -> impl Iterator<Item = &T> {
        self.entries.iter().map(|(_, _, value)| value)
    }

    /// Keep the bindings `keep` answers true for.
    fn retain(&mut self, mut keep: impl FnMut(&str, &str) -> bool) {
        self.entries
            .retain(|(discriminant, member, _)| keep(discriminant, member));
    }
}

/// A nested payload member as its event's own schema declares it.
#[derive(Clone, Copy, Debug)]
pub(crate) enum MemberSchema<'a> {
    /// No semantic envelope, no known signal type, or a payload that is not
    /// an object schema: the member resolves by column name alone.
    Unjudged,
    /// The event's object schema declares the member.
    Declared(&'a crate::validate::SemanticSchema),
    /// The event's open object schema does not declare the member: it is
    /// captured into the carrier, never written to another type's column.
    UndeclaredOpen,
}

/// What an explicit null in a member's cell is, from the member's declared
/// semantic schema ([`ExtractionConfig::null_cell`]). A typed or structured
/// member keeps null as its one empty state; a dynamic value keeps null as
/// data at every depth, its top level included.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NullCell {
    /// The cell is the Arrow null, the same as a missing member's.
    Empty,
    /// The cell is present and holds the MessagePack nil a Binary plane
    /// writes for a null inside any other dynamic value.
    Data,
}

/// Certified value-object frames, each paired with the batch slice it was
/// admitted from: `(lent, certified)`. An admission that projected a frame
/// into the reader's layout hands the pair here, and the extractor writes the
/// certified bytes wherever it would have written the lent slice. The pairs
/// borrow the admission's proofs, so the certified bytes are copied once,
/// into the column, and nowhere else.
#[derive(Clone, Copy, Debug, Default)]
pub struct FrameProjections<'p> {
    frames: &'p [(&'p [u8], &'p [u8])],
}

impl<'p> FrameProjections<'p> {
    pub fn new(frames: &'p [(&'p [u8], &'p [u8])]) -> Self {
        Self { frames }
    }

    /// The certified bytes replacing `lent`, matched by identity (address
    /// and length), never by content.
    pub(crate) fn certified(&self, lent: &[u8]) -> Option<&'p [u8]> {
        self.frames
            .iter()
            .find(|(frame, _)| core::ptr::eq(*frame, lent))
            .map(|(_, certified)| *certified)
    }

    pub(crate) fn len(&self) -> usize {
        self.frames.len()
    }
}

impl ExtractionConfig {
    /// What an explicit null in `member`'s place in the declared record is
    /// for an event of `event_type`: data when the member is dynamic, else
    /// the empty state. A member a column places is not judged here: its
    /// column decides ([`Self::column_null`]). An event of no known type
    /// judges no member, so its nulls are empty.
    pub(crate) fn null_cell(&self, event_type: Option<&str>, member: &str) -> NullCell {
        match event_type {
            Some(event_type)
                if !self.dynamic_members.is_empty()
                    && self.dynamic_members.get(event_type, member).is_some() =>
            {
                NullCell::Data
            }
            _ => NullCell::Empty,
        }
    }

    /// What an explicit null under the top-level `key` of an event of
    /// `event_type` is in the declared record. The flat spelling
    /// `value.<member>` is that payload member, whose null is the one the
    /// nested spelling gives it; any other top-level key is a system member,
    /// whose null is empty.
    pub(crate) fn top_level_null_cell(&self, event_type: Option<&str>, key: &str) -> NullCell {
        key.strip_prefix("value.")
            .map_or(NullCell::Empty, |member| self.null_cell(event_type, member))
    }

    /// What an explicit null in `column`'s cell is: data when the column's
    /// cell is a dynamic value, else the empty state. A column's cell is one
    /// form for every row (specs/columine/10-physical-vocabulary.md §7).
    pub(crate) fn column_null(&self, column: usize) -> NullCell {
        if self
            .dynamic_columns
            .get(column)
            .is_some_and(|dynamic| *dynamic)
        {
            NullCell::Data
        } else {
            NullCell::Empty
        }
    }

    /// The column the lanes write `member` of an event of `event_type` to:
    /// its discriminated column, else its `value.<member>` column, when a
    /// member writes that column.
    fn placed_column(&self, event_type: &str, member: &str) -> Option<usize> {
        self.variant_map
            .get(event_type, member)
            .or_else(|| self.payload_map.get(member))
            .map(|lookup| lookup.column)
            .filter(|column| self.member_write(*column).is_some())
    }

    /// Whether a lane reads an event's `type`: its schema routes and judges
    /// the event's members, a named dynamic member is keyed by it, and a
    /// discriminator spelled `type` selects columns and routing rows by it. A
    /// configuration with none of these never decodes it.
    pub(crate) fn reads_event_type(&self) -> bool {
        self.semantic_schemas.is_some()
            || !self.dynamic_members.is_empty()
            || self.discriminates_by_type()
    }

    /// Whether the discriminator is the event's `type`
    /// (specs/columine/10-physical-vocabulary.md §7: "A routing row's
    /// discriminant is the event's `type`").
    fn discriminates_by_type(&self) -> bool {
        self.discriminator_key.as_deref() == Some("type")
    }

    /// The discriminant of an event whose discriminator key spelled
    /// `discriminant` and whose signal type is `event_type`: a discriminator
    /// spelled `type` is the signal type itself, wherever the lane learned it,
    /// a look-ahead past the payload included.
    pub(crate) fn event_discriminant<'a>(
        &self,
        discriminant: Option<&'a str>,
        event_type: Option<&'a str>,
    ) -> Option<&'a str> {
        if self.discriminates_by_type() {
            event_type
        } else {
            discriminant
        }
    }
    pub(crate) fn is_open_payload_path(&self, path: &str) -> bool {
        self.open_paths.iter().any(|open| open == path)
    }

    /// How one nested payload member of an event of `event_type` is judged by
    /// that event's OWN schema. Columns are shared across signal types, so a
    /// member resolves against the schema of the type that carries it, never
    /// against whichever signal type happens to declare the same name.
    pub(crate) fn payload_member_schema(
        &self,
        event_type: Option<&str>,
        member: &str,
    ) -> MemberSchema<'_> {
        let schema = self
            .semantic_schemas
            .as_ref()
            .zip(event_type)
            .and_then(|(schemas, event_type)| schemas.get(event_type));
        let Some(crate::validate::SemanticSchema::Object { fields, open }) = schema else {
            return MemberSchema::Unjudged;
        };
        match fields.iter().find(|(field, _)| field == member) {
            Some((_, schema)) => MemberSchema::Declared(schema),
            None if *open => MemberSchema::UndeclaredOpen,
            None => MemberSchema::Unjudged,
        }
    }

    pub(crate) fn lookup_field(&self, name: &str) -> Option<&FieldLookup> {
        self.field_map
            .get(name)
            .or_else(|| self.payload_map.get(name))
    }

    /// Whether `name` is the envelope member carrying the discriminant.
    pub(crate) fn is_discriminator_key(&self, name: &[u8]) -> bool {
        self.discriminator_key.as_deref().map(str::as_bytes) == Some(name)
    }

    /// Resolve one nested-payload member: its discriminated column for this
    /// discriminant first, then the plain `value.<member>` column.
    pub(crate) fn lookup_payload_member(
        &self,
        discriminant: Option<&str>,
        member: &str,
        column_name: &str,
    ) -> Option<&FieldLookup> {
        if let Some(discriminant) = discriminant
            && !self.variant_map.is_empty()
            && let Some(lookup) = self.variant_map.get(discriminant, member)
        {
            return Some(lookup);
        }
        self.lookup_field(column_name)
    }

    /// The values of the schema dictionary `column` is encoded against;
    /// `None` for a plain or stream-dictionary column.
    pub(crate) fn schema_dictionary(&self, column: usize) -> Option<&[Option<String>]> {
        let position = self.column_dictionaries.get(column).copied().flatten()?;
        self.dictionaries.get(position).map(Vec::as_slice)
    }

    /// How a member resolved to `column` is written, or `None` for a column
    /// only the lane itself writes: the carrier, the declared record and a
    /// text's inline column (specs/columine/10-physical-vocabulary.md §7). A
    /// member named after one of them is not that cell.
    pub(crate) fn member_write(&self, column: usize) -> Option<MemberWrite> {
        if Some(column) == self.fallback_column || Some(column) == self.record_column {
            return None;
        }
        match self
            .cell_roles
            .get(column)
            .copied()
            .unwrap_or(CellRole::Lane)
        {
            CellRole::Lane => Some(MemberWrite::Lane),
            CellRole::Seam(cell) => Some(MemberWrite::Seam(cell)),
            CellRole::TextInline => None,
        }
    }

    /// Who writes the carrier of undeclared members.
    pub(crate) fn carrier(&self) -> Carrier {
        let Some(column) = self.fallback_column else {
            return Carrier::Absent;
        };
        match self.cell_roles.get(column).copied() {
            Some(CellRole::Seam(SeamCell::Embedder)) => Carrier::Embedder(column),
            Some(CellRole::Seam(SeamCell::Raw)) => Carrier::Raw,
            Some(
                CellRole::Lane | CellRole::TextInline | CellRole::Seam(SeamCell::TextHash { .. }),
            )
            | None => Carrier::Native(column),
        }
    }

    /// The carrier column the lane writes as MessagePack itself.
    pub(crate) fn native_carrier(&self) -> Option<usize> {
        match self.carrier() {
            Carrier::Native(column) => Some(column),
            Carrier::Absent | Carrier::Embedder(_) | Carrier::Raw => None,
        }
    }

    /// The record column, when the embedder writes it.
    fn embedder_record(&self) -> Option<usize> {
        self.record_column.filter(|column| {
            self.cell_roles.get(*column) == Some(&CellRole::Seam(SeamCell::Embedder))
        })
    }

    /// The routing row of `member` under `discriminant`, when the member
    /// lives in the declared record.
    pub(crate) fn record_route(
        &self,
        discriminant: Option<&str>,
        member: &str,
    ) -> Option<RecordRow<'_>> {
        let route = self.record_routes.get(discriminant?, member)?;
        Some(RecordRow {
            path: &route.path,
            descriptor_position: route.descriptor_position,
        })
    }

    /// Ask `cells` once for every value cell this configuration hands it
    /// (specs/columine/10-physical-vocabulary.md §7): each embedder-written
    /// member column, the carrier, each text and each record routing row.
    pub fn prepare_cells(&self, cells: &mut dyn ValueCells) -> Result<(), CellRefusal> {
        for (column, role) in self.cell_roles.iter().enumerate() {
            match *role {
                CellRole::Seam(SeamCell::Embedder) if Some(column) == self.fallback_column => {
                    cells.prepare(CellTarget::Carrier {
                        column: seam_column(column),
                    })?;
                }
                CellRole::Seam(SeamCell::Embedder) if Some(column) == self.record_column => {}
                CellRole::Seam(SeamCell::Embedder) => cells.prepare(CellTarget::Member {
                    column: seam_column(column),
                })?,
                CellRole::Seam(SeamCell::TextHash { inline }) => {
                    cells.prepare(CellTarget::Text {
                        hash_column: seam_column(column),
                        inline_column: inline.map(seam_column),
                    })?
                }
                CellRole::Lane | CellRole::TextInline | CellRole::Seam(SeamCell::Raw) => {}
            }
        }
        for route in self.record_routes.values() {
            cells.prepare(CellTarget::Record {
                row: RecordRow {
                    path: &route.path,
                    descriptor_position: route.descriptor_position,
                },
            })?;
        }
        Ok(())
    }

    /// Placement is total (specs/columine/10-physical-vocabulary.md §7):
    /// where the lanes descend the payload, every top-level member an event's
    /// object schema declares resolves, as the lanes resolve it, to a column
    /// a member writes or to a record routing row. A whole-payload `value`
    /// column places every member, so a schema without `value.<member>`
    /// columns has nothing to place.
    fn check_placement(&self) -> Result<(), ConfigError> {
        let Some(schemas) = self
            .semantic_schemas
            .as_ref()
            .filter(|_| self.has_value_fields)
        else {
            return Ok(());
        };
        for (event_type, schema) in schemas {
            let crate::validate::SemanticSchema::Object { fields, .. } = schema else {
                continue;
            };
            for (member, _) in fields {
                let placed = self.placed_column(event_type, member).is_some()
                    || self.record_routes.get(event_type, member).is_some();
                if !placed {
                    return Err(ConfigError::UnplacedMember);
                }
            }
        }
        Ok(())
    }
}

/// Who computes the value cells of a parse (specs/columine/10-physical-vocabulary.md
/// §7): columine's native MessagePack writer, or an embedder's
/// [`ValueCells`]. It matches the [`CellLayout`] the configuration was built
/// with; a cell the configuration hands an embedder that a `Native` writer
/// cannot write refuses as [`CellRefusal::Unsupported`].
pub enum CellWriter<'a> {
    Native,
    Embedder(&'a mut dyn ValueCells),
}

impl CellWriter<'_> {
    /// The embedder, or `Unsupported` for the native writer.
    fn embedder(&mut self) -> Result<&mut dyn ValueCells, json_extractor::ExtractionError> {
        match self {
            Self::Native => Err(json_extractor::ExtractionError::Cell(
                CellRefusal::Unsupported,
            )),
            Self::Embedder(cells) => Ok(&mut **cells),
        }
    }
}

/// The creation facts that decide who writes each value cell
/// (specs/columine/10-physical-vocabulary.md §7): each field's `sb.cell`
/// metadata, the record routing rows, the text rows, and the encoding of the
/// embedder's writer. `encoding: None` is the native path; the default is
/// the native path over a schema with no cell metadata.
#[derive(Clone, Copy, Debug, Default)]
pub struct CellLayout<'a> {
    pub cell_metadata: &'a [Option<String>],
    pub record_members: &'a [RecordMember],
    pub text_columns: &'a [TextColumn],
    /// Members whose explicit null is data, named by the schema's owner.
    pub dynamic_members: &'a [DynamicMember],
    /// Binary columns whose cell is a dynamic value, named by the schema's
    /// owner.
    pub dynamic_columns: &'a [usize],
    pub encoding: Option<&'a str>,
}

/// Who writes one column's cells, decided once at configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CellRole {
    /// The lane's own coercion: every typed plane, and with no embedder a
    /// Binary plane as columine's MessagePack.
    Lane,
    /// A cell the value-cell seam rules.
    Seam(SeamCell),
    /// A text's inline Utf8 column, written only beside its hash.
    TextInline,
}

/// A column whose cells the value-cell seam rules.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SeamCell {
    /// A Binary plane without `sb.cell` beside an embedder: raw bytes only.
    Raw,
    /// A Binary plane whose `sb.cell` names the embedder's encoding.
    Embedder,
    /// A text's content-hash column, and its inline column when it has one.
    TextHash { inline: Option<usize> },
}

/// How a member resolved to a column is written.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MemberWrite {
    Lane,
    Seam(SeamCell),
}

/// Who writes the carrier of undeclared members.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Carrier {
    /// The schema has no carrier column.
    Absent,
    /// The lane writes it as columine's MessagePack.
    Native(usize),
    /// The embedder writes it.
    Embedder(usize),
    /// A Binary column without `sb.cell` beside an embedder: it takes no
    /// captured member.
    Raw,
}

/// A declared member's place in the row's declared record.
#[derive(Clone, Debug)]
pub(crate) struct RecordRoute {
    path: Vec<u16>,
    descriptor_position: u32,
}

/// A column index as the value-cell seam spells it. Every column index is
/// below the schema's `u32` field count.
fn seam_column(column: usize) -> u32 {
    u32::try_from(column).unwrap_or(u32::MAX)
}

/// Start one row's embedder-written record and carrier.
pub(crate) fn begin_cells(
    config: &ExtractionConfig,
    cells: &mut CellWriter<'_>,
) -> Result<(), json_extractor::ExtractionError> {
    let record = config.embedder_record().is_some();
    let carrier = matches!(config.carrier(), Carrier::Embedder(_));
    if record || carrier {
        let writer = cells.embedder()?;
        if record {
            writer.begin_record();
        }
        if carrier {
            writer.begin_undeclared();
        }
    }
    Ok(())
}

/// Store one row's embedder-written record and carrier; a writer that put
/// nothing leaves its cell null.
pub(crate) fn finish_cells(
    config: &ExtractionConfig,
    columns: &mut DynamicColumns,
    cells: &mut CellWriter<'_>,
    scratch: &mut Vec<u8>,
) -> Result<(), json_extractor::ExtractionError> {
    if let Some(column) = config.embedder_record() {
        scratch.clear();
        if cells
            .embedder()?
            .finish_record(scratch)
            .map_err(json_extractor::ExtractionError::Cell)?
        {
            json_extractor::append(
                columns,
                column,
                Some(ColumnValue::Binary(Cow::Borrowed(scratch))),
            )?;
        }
    }
    if let Carrier::Embedder(column) = config.carrier() {
        scratch.clear();
        if cells
            .embedder()?
            .finish_undeclared(scratch)
            .map_err(json_extractor::ExtractionError::Cell)?
        {
            json_extractor::append(
                columns,
                column,
                Some(ColumnValue::Binary(Cow::Borrowed(scratch))),
            )?;
        }
    }
    Ok(())
}

/// A structured member's own embedder-written cell. An explicit null that
/// is the member's empty state stays the null cell. Below an open object of
/// the member's declared `schema`, each undeclared key goes to the carrier
/// under its payload path, by the rule the native writer filters with.
#[allow(
    clippy::too_many_arguments,
    reason = "one member cell: its column, its value and null rule, its open-path capture, the writer and its scratch"
)]
pub(crate) fn member_cell(
    config: &ExtractionConfig,
    columns: &mut DynamicColumns,
    column: usize,
    value: &dyn ValueView,
    null: NullCell,
    capture: Option<(&crate::validate::SemanticSchema, &str)>,
    cells: &mut CellWriter<'_>,
    scratch: &mut Vec<u8>,
) -> Result<(), json_extractor::ExtractionError> {
    if value.kind() == ValueKind::Null && null == NullCell::Empty {
        return Ok(());
    }
    scratch.clear();
    cells
        .embedder()?
        .member(seam_column(column), value, scratch)
        .map_err(json_extractor::ExtractionError::Cell)?;
    json_extractor::append(
        columns,
        column,
        Some(ColumnValue::Binary(Cow::Borrowed(scratch))),
    )?;
    if let Some((schema, name)) = capture {
        let mut path = String::new();
        crate::validate::push_path_segment(&mut path, name)
            .map_err(|_| json_extractor::ExtractionError::MemoryExhausted)?;
        undeclared_below(config, schema, value, &mut path, cells)?;
    }
    Ok(())
}

/// Hand every undeclared key below the open objects of `schema` to the
/// carrier, under `path` extended by each key.
fn undeclared_below(
    config: &ExtractionConfig,
    schema: &crate::validate::SemanticSchema,
    value: &dyn ValueView,
    path: &mut String,
    cells: &mut CellWriter<'_>,
) -> Result<(), json_extractor::ExtractionError> {
    use crate::validate::SemanticSchema;
    match schema {
        SemanticSchema::Optional(inner) => undeclared_below(config, inner, value, path, cells),
        SemanticSchema::Object { fields, open } => {
            let mut outcome = Ok(());
            if value.kind() == ValueKind::Object {
                value.visit_object(&mut |name, child| {
                    if outcome.is_ok() {
                        outcome =
                            undeclared_member(config, fields, *open, name, child, path, cells);
                    }
                });
            }
            outcome
        }
        SemanticSchema::Unknown
        | SemanticSchema::String { .. }
        | SemanticSchema::Key { .. }
        | SemanticSchema::AxId
        | SemanticSchema::Number
        | SemanticSchema::Boolean
        | SemanticSchema::Binary
        | SemanticSchema::BigInt(_)
        | SemanticSchema::I32
        | SemanticSchema::U32
        | SemanticSchema::Range(_)
        | SemanticSchema::Enum(_)
        | SemanticSchema::Array { .. }
        | SemanticSchema::Union(_)
        | SemanticSchema::Map { .. }
        | SemanticSchema::Record(_)
        | SemanticSchema::Vo(_) => Ok(()),
    }
}

/// One key of an object judged by `fields`: a declared key is walked by its
/// own schema, an undeclared one under an open object is captured, and an
/// undeclared one under a closed object stays in the member.
#[allow(
    clippy::too_many_arguments,
    reason = "one key of the walk: the object's schema, the key and value, the path and the writer"
)]
fn undeclared_member(
    config: &ExtractionConfig,
    fields: &[(String, crate::validate::SemanticSchema)],
    open: bool,
    name: &str,
    value: &dyn ValueView,
    path: &mut String,
    cells: &mut CellWriter<'_>,
) -> Result<(), json_extractor::ExtractionError> {
    let known = fields.iter().find(|(field, _)| field == name);
    if known.is_none() && !open {
        return Ok(());
    }
    let mark = crate::validate::push_path_segment(path, name)
        .map_err(|_| json_extractor::ExtractionError::MemoryExhausted)?;
    let result = match known {
        Some((_, schema)) => undeclared_below(config, schema, value, path, cells),
        None => undeclared_cell(config, path, value, cells),
    };
    path.truncate(mark);
    result
}

/// One undeclared member, under its payload path, into an embedder-written
/// carrier. A schema with no carrier drops it, as the native writer does; a
/// carrier the embedder does not write takes no structured value.
pub(crate) fn undeclared_cell(
    config: &ExtractionConfig,
    path: &str,
    value: &dyn ValueView,
    cells: &mut CellWriter<'_>,
) -> Result<(), json_extractor::ExtractionError> {
    match config.carrier() {
        Carrier::Embedder(_) => cells
            .embedder()?
            .put_undeclared(path, value)
            .map_err(json_extractor::ExtractionError::Cell),
        Carrier::Absent => Ok(()),
        Carrier::Raw | Carrier::Native(_) => Err(json_extractor::ExtractionError::InvalidFieldType),
    }
}

/// One declared member without a column, into the row's declared record.
/// An explicit null that is the member's empty state is not put.
pub(crate) fn record_cell(
    row: RecordRow<'_>,
    value: &dyn ValueView,
    null: NullCell,
    cells: &mut CellWriter<'_>,
) -> Result<(), json_extractor::ExtractionError> {
    if value.kind() == ValueKind::Null && null == NullCell::Empty {
        return Ok(());
    }
    cells
        .embedder()?
        .put(&row, value)
        .map_err(json_extractor::ExtractionError::Cell)
}

/// A text that arrived as its text: its hash column stores the hash the
/// embedder computes, and its inline column the text when the embedder keeps
/// it inline. A text answered inline with no inline column is refused.
pub(crate) fn text_cell(
    columns: &mut DynamicColumns,
    hash_column: usize,
    inline: Option<usize>,
    text: &str,
    cells: &mut CellWriter<'_>,
) -> Result<(), json_extractor::ExtractionError> {
    let mut hash = [0; TEXT_HASH_BYTES];
    let place = cells
        .embedder()?
        .text(seam_column(hash_column), text, &mut hash)
        .map_err(json_extractor::ExtractionError::Cell)?;
    json_extractor::append(
        columns,
        hash_column,
        Some(ColumnValue::FixedBytes(Cow::Borrowed(&hash))),
    )?;
    match (place, inline) {
        (TextPlace::Inline, Some(inline)) => {
            json_extractor::append(
                columns,
                inline,
                Some(ColumnValue::Utf8(Cow::Borrowed(text))),
            )?;
            if let Some(seen) = columns.columns_seen.get_mut(inline) {
                *seen = true;
            }
            Ok(())
        }
        (TextPlace::Inline, None) => Err(json_extractor::ExtractionError::InvalidFieldType),
        (TextPlace::HashOnly, _) => Ok(()),
    }
}

/// A text that arrived as its content hash: the embedder answers whether it
/// holds the content, and the hash column stores the hash.
pub(crate) fn text_hash_cell(
    columns: &mut DynamicColumns,
    hash_column: usize,
    hash: &[u8],
    cells: &mut CellWriter<'_>,
) -> Result<(), json_extractor::ExtractionError> {
    let hash: &[u8; TEXT_HASH_BYTES] = hash
        .try_into()
        .map_err(|_| json_extractor::ExtractionError::InvalidFieldType)?;
    cells
        .embedder()?
        .text_by_hash(seam_column(hash_column), hash)
        .map_err(json_extractor::ExtractionError::Cell)?;
    json_extractor::append(
        columns,
        hash_column,
        Some(ColumnValue::FixedBytes(Cow::Borrowed(hash))),
    )
}

/// The index a dictionary-encoded column stores for `text`
/// (specs/columine/10-physical-vocabulary.md §4, §5): its position among a
/// schema dictionary's `values`, or the ordinal `image` answers for the
/// stream dictionary. A text the schema dictionary does not hold is
/// `InvalidFieldType`; a text the image refuses is `Category`.
pub(crate) fn dictionary_index(
    slot: DictionarySlot,
    values: Option<&[Option<String>]>,
    text: &str,
    image: &mut dyn CategoryImage,
) -> Result<u64, json_extractor::ExtractionError> {
    match slot {
        DictionarySlot::Stream => image
            .ordinal(text)
            .map(u64::from)
            .map_err(json_extractor::ExtractionError::Category),
        DictionarySlot::Schema(_) => values
            .and_then(|values| {
                values
                    .iter()
                    .position(|value| value.as_deref() == Some(text))
            })
            .and_then(|index| u64::try_from(index).ok())
            .ok_or(json_extractor::ExtractionError::InvalidFieldType),
    }
}

/// The integer an eight-byte signed column takes from a text: a decimal
/// `i64`, and on a Timestamp column also an RFC 3339 instant converted to
/// the column's unit. Both lanes coerce a text through this one rule.
pub(crate) fn signed_text(field: SignalSchemaField, text: &str) -> Option<i64> {
    if let Ok(value) = text.parse::<i64>() {
        return Some(value);
    }
    if field.arrow_type != ArrowType::Timestamp {
        return None;
    }
    instant::parse_instant(text, columine_arrow::time_unit(field.type_param)?).ok()
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ConfigError {
    FieldNameCountMismatch,
    DuplicateFieldName,
    InvalidPresenceField,
    /// A [`PayloadDiscriminator`] member names a column the schema lacks.
    InvalidDiscriminatedColumn,
    /// The configuration's storage could not be backed.
    OutOfMemory,
    /// A column is encoded against a schema dictionary the creation call's
    /// table does not hold.
    MissingDictionary,
    /// An enum member's column is not a schema-dictionary column whose
    /// non-null values are the enum's variants in order.
    EnumDictionaryMismatch,
    /// A Binary field's `sb.cell` names an encoding this processor does not
    /// write.
    CellEncodingMismatch,
    /// A member an event's object schema declares resolves to no column and
    /// no record routing row.
    UnplacedMember,
    /// Record routing rows without a discriminator key, or without a
    /// [`DECLARED_RECORD_COLUMN_NAME`] Binary column the processor writes.
    RecordColumnMissing,
    /// A text row whose hash field is not `FixedSizeBinary(32)` or whose
    /// inline field is not Utf8.
    InvalidTextColumn,
    /// A dynamic column the schema lacks, or one that is not Binary.
    InvalidDynamicColumn,
}

/// Back `additional` more elements of `buf`, or refuse as out of memory.
fn room<T>(buf: &mut Vec<T>, additional: usize) -> Result<(), ConfigError> {
    buf.try_reserve_exact(additional)
        .map_err(|_| ConfigError::OutOfMemory)
}

/// An owned copy of `text` in exactly backed storage.
fn owned(text: &str) -> Result<String, ConfigError> {
    let mut out = String::new();
    out.try_reserve_exact(text.len())
        .map_err(|_| ConfigError::OutOfMemory)?;
    out.push_str(text);
    Ok(out)
}

/// An owned copy of a dictionary's values in exactly backed storage.
fn owned_values(values: &[Option<String>]) -> Result<Vec<Option<String>>, ConfigError> {
    let mut out = Vec::new();
    room(&mut out, values.len())?;
    for value in values {
        out.push(value.as_deref().map(owned).transpose()?);
    }
    Ok(out)
}

const VALUE_PRESENCE_PREFIX: &str = "event_value_present.";

/// The sole Arrow/config/spec spelling for the bounded undeclared-value carrier.
pub const UNDECLARED_COLUMN_NAME: &str = "value.$undeclared";

/// The Arrow/config spelling of the row's declared record: the declared
/// members that have no column of their own
/// (specs/columine/10-physical-vocabulary.md §7).
pub const DECLARED_RECORD_COLUMN_NAME: &str = "value.$declared";

/// Each column's [`CellRole`] under `layout`
/// (specs/columine/10-physical-vocabulary.md §7, §8).
fn cell_roles(
    fields: &[SignalSchemaField],
    layout: &CellLayout<'_>,
) -> Result<Vec<CellRole>, ConfigError> {
    let native = NoValueCells.encoding();
    let mut roles = Vec::new();
    room(&mut roles, fields.len())?;
    for (column, field) in fields.iter().enumerate() {
        let named = layout.cell_metadata.get(column).and_then(Option::as_deref);
        roles.push(match (field.plane_kind(), named, layout.encoding) {
            (PlaneKind::Bytes { .. }, None, None) => CellRole::Lane,
            (PlaneKind::Bytes { .. }, None, Some(_)) => CellRole::Seam(SeamCell::Raw),
            (PlaneKind::Bytes { .. }, Some(named), None) if named == native => CellRole::Lane,
            (PlaneKind::Bytes { .. }, Some(named), Some(encoding)) if named == encoding => {
                CellRole::Seam(SeamCell::Embedder)
            }
            (PlaneKind::Bytes { .. }, Some(_), _) => return Err(ConfigError::CellEncodingMismatch),
            (
                PlaneKind::Empty
                | PlaneKind::Bool
                | PlaneKind::SignedInt { .. }
                | PlaneKind::UnsignedInt { .. }
                | PlaneKind::Float { .. }
                | PlaneKind::FixedBytes { .. }
                | PlaneKind::Text { .. },
                _,
                _,
            ) => CellRole::Lane,
        });
    }
    for text in layout.text_columns {
        let hash = fields.get(text.hash).is_some_and(|field| {
            field.dictionary.is_none()
                && field.arrow_type == ArrowType::FixedSizeBinary
                && usize::from(field.type_param) == TEXT_HASH_BYTES
        });
        let inline = text.inline.is_none_or(|inline| {
            fields.get(inline).is_some_and(|field| {
                field.dictionary.is_none() && field.arrow_type == ArrowType::Utf8
            })
        });
        if !hash || !inline {
            return Err(ConfigError::InvalidTextColumn);
        }
        if let Some(role) = roles.get_mut(text.hash) {
            *role = CellRole::Seam(SeamCell::TextHash {
                inline: text.inline,
            });
        }
        if let Some(role) = text.inline.and_then(|inline| roles.get_mut(inline)) {
            *role = CellRole::TextInline;
        }
    }
    Ok(roles)
}

fn decode_presence_source(name: &str) -> Result<Option<String>, ConfigError> {
    let Some(encoded) = name.strip_prefix(VALUE_PRESENCE_PREFIX) else {
        return Ok(None);
    };
    let bytes = encoded.as_bytes();
    // Decoding never lengthens: every byte or `%XX` triple yields one byte.
    let mut decoded = Vec::new();
    room(&mut decoded, bytes.len())?;
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            let Some(hex) = bytes.get(index + 1..index + 3) else {
                return Err(ConfigError::InvalidPresenceField);
            };
            let hex = std::str::from_utf8(hex).map_err(|_| ConfigError::InvalidPresenceField)?;
            decoded
                .push(u8::from_str_radix(hex, 16).map_err(|_| ConfigError::InvalidPresenceField)?);
            index += 3;
        } else {
            decoded.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(decoded)
        .map(Some)
        .map_err(|_| ConfigError::InvalidPresenceField)
}

pub fn build_extraction_config(
    fields: &[SignalSchemaField],
    names: &[&str],
) -> Result<ExtractionConfig, ConfigError> {
    build_extraction_config_with_semantic_schemas(
        fields,
        names,
        &[],
        None,
        None,
        CellLayout::default(),
    )
}

pub fn build_extraction_config_with_semantic_schemas(
    fields: &[SignalSchemaField],
    names: &[&str],
    dictionaries: &[SchemaDictionary],
    semantic_schemas: Option<&crate::validate::SemanticSchemaSet>,
    discriminator: Option<&PayloadDiscriminator>,
    cells: CellLayout<'_>,
) -> Result<ExtractionConfig, ConfigError> {
    if fields.len() != names.len() {
        return Err(ConfigError::FieldNameCountMismatch);
    }
    // Each field adds at most one entry to each table, so every table is
    // backed for all fields before the first is read.
    let mut field_entries = Vec::new();
    room(&mut field_entries, fields.len())?;
    let mut field_map = HashMap::new();
    field_map
        .try_reserve(fields.len())
        .map_err(|_| ConfigError::OutOfMemory)?;
    let mut unresolved_presence = Vec::new();
    room(&mut unresolved_presence, fields.len())?;
    let mut open_paths = Vec::new();
    if let Some(semantic_schemas) = semantic_schemas {
        let mut path = String::new();
        for schema in semantic_schemas.values() {
            crate::validate::collect_open_paths(schema, &mut path, &mut open_paths)?;
        }
        open_paths.sort_unstable();
        open_paths.dedup();
    }
    // Each column encoded against a schema dictionary names that dictionary's
    // position in the copied table, so a lane resolves a text without a
    // search by slot.
    let mut schema_dictionaries = Vec::new();
    room(&mut schema_dictionaries, dictionaries.len())?;
    for dictionary in dictionaries {
        schema_dictionaries.push(owned_values(&dictionary.values)?);
    }
    let mut column_dictionaries = Vec::new();
    room(&mut column_dictionaries, fields.len())?;
    for field in fields {
        column_dictionaries.push(match field.dictionary {
            Some(DictionaryEncoding {
                slot: slot @ DictionarySlot::Schema(_),
                ..
            }) => Some(
                dictionaries
                    .iter()
                    .position(|dictionary| dictionary.slot == slot)
                    .ok_or(ConfigError::MissingDictionary)?,
            ),
            Some(DictionaryEncoding {
                slot: DictionarySlot::Stream,
                ..
            })
            | None => None,
        });
    }
    let mut fallback_column = None;
    let mut record_column = None;
    for (column, (field, name)) in fields.iter().zip(names).enumerate() {
        if let Some(source_name) = decode_presence_source(name)? {
            if field.arrow_type != ArrowType::Bool {
                return Err(ConfigError::InvalidPresenceField);
            }
            unresolved_presence.push((column, source_name));
            field_entries.push((column, *field));
            continue;
        }
        if field_map
            .insert(
                owned(name)?,
                FieldLookup {
                    column,
                    field: *field,
                },
            )
            .is_some()
        {
            return Err(ConfigError::DuplicateFieldName);
        }
        if *name == UNDECLARED_COLUMN_NAME {
            fallback_column = Some(column);
        }

        if *name == DECLARED_RECORD_COLUMN_NAME {
            record_column = Some(column);
        }
        field_entries.push((column, *field));
    }
    let mut presence_entries = Vec::new();
    room(&mut presence_entries, unresolved_presence.len())?;
    for (presence_column, source_name) in unresolved_presence {
        let source = field_map
            .get(&source_name)
            .ok_or(ConfigError::InvalidPresenceField)?;
        presence_entries.push((presence_column, source.column));
    }
    // The nested-envelope descent arms only for schemas that declare payload
    // columns: a pure system schema has no `value.<field>` names, and its
    // `value` member (an op result's unwrapped scalar, say) must keep its
    // undeclared handling instead of being walked as a payload object.
    let mut payload_map = HashMap::new();
    payload_map
        .try_reserve(field_map.len())
        .map_err(|_| ConfigError::OutOfMemory)?;
    for (name, lookup) in &field_map {
        if let Some(member) = name.strip_prefix("value.") {
            payload_map.insert(owned(member)?, *lookup);
        }
    }
    let has_value_fields = !payload_map.is_empty();
    let mut variant_map = MemberTable::default();
    let mut dynamic_members = MemberTable::default();
    if let Some(semantic_schemas) = semantic_schemas {
        for (event_type, schema) in semantic_schemas {
            crate::validate::collect_dynamic_members(schema, &mut |member| {
                dynamic_members.bind(event_type, member, ())
            })?;
        }
    }
    for named in cells.dynamic_members {
        dynamic_members.bind(&named.event_type, &named.member, ())?;
    }
    // A dynamic value's cell is Binary.
    let mut dynamic_columns = Vec::new();
    room(&mut dynamic_columns, fields.len())?;
    dynamic_columns.resize(fields.len(), false);
    for column in cells.dynamic_columns {
        let binary = fields
            .get(*column)
            .is_some_and(|field| matches!(field.plane_kind(), PlaneKind::Bytes { .. }));
        if !binary {
            return Err(ConfigError::InvalidDynamicColumn);
        }
        dynamic_columns[*column] = true;
    }
    if let Some(discriminator) = discriminator {
        for member in &discriminator.members {
            let field = fields
                .get(member.column)
                .ok_or(ConfigError::InvalidDiscriminatedColumn)?;
            variant_map.bind(
                &member.discriminant,
                &member.member,
                FieldLookup {
                    column: member.column,
                    field: *field,
                },
            )?;
        }
    }
    // An enum member's column is a schema-dictionary column whose values are
    // the enum's variants: the wire spells a variant as its text and the
    // column stores the text's index. The column is resolved as the lanes
    // resolve it, its discriminated column first.
    if let Some(semantic_schemas) = semantic_schemas {
        for (event_type, schema) in semantic_schemas {
            crate::validate::collect_enum_members(schema, |member, variants| {
                let Some(lookup) = variant_map
                    .get(event_type, member)
                    .or_else(|| payload_map.get(member))
                else {
                    return Ok(());
                };
                let values = column_dictionaries
                    .get(lookup.column)
                    .copied()
                    .flatten()
                    .and_then(|position| schema_dictionaries.get(position));
                match values {
                    Some(values) if values.iter().flatten().eq(variants) => Ok(()),
                    Some(_) | None => Err(ConfigError::EnumDictionaryMismatch),
                }
            })?;
        }
    }
    let cell_roles = cell_roles(fields, &cells)?;
    // Record rows need the discriminant that selects them and a record
    // column this processor writes.
    if !cells.record_members.is_empty() {
        let writes_record = record_column.is_some_and(|column| {
            fields
                .get(column)
                .is_some_and(|field| matches!(field.plane_kind(), PlaneKind::Bytes { .. }))
                && matches!(
                    cell_roles.get(column),
                    Some(CellRole::Lane | CellRole::Seam(SeamCell::Embedder))
                )
        });
        if discriminator.is_none() || !writes_record {
            return Err(ConfigError::RecordColumnMissing);
        }
    }
    let mut record_routes = MemberTable::default();
    for row in cells.record_members {
        let mut path = Vec::new();
        room(&mut path, row.path.len())?;
        path.extend_from_slice(&row.path);
        record_routes.bind(
            &row.discriminant,
            &row.member,
            RecordRoute {
                path,
                descriptor_position: row.descriptor_position,
            },
        )?;
    }
    let mut config = ExtractionConfig {
        field_entries,
        field_map,
        payload_map,
        fallback_column,
        presence_entries,
        open_paths,
        semantic_schemas: semantic_schemas
            .map(crate::validate::SemanticSchemaSet::try_clone)
            .transpose()
            .map_err(|_| ConfigError::OutOfMemory)?,
        has_value_fields,
        discriminator_key: discriminator
            .map(|discriminator| owned(&discriminator.key))
            .transpose()?,
        variant_map,
        dictionaries: schema_dictionaries,
        column_dictionaries,
        dynamic_members,
        dynamic_columns,
        cell_roles,
        record_column,
        record_routes,
    };
    // One authority per member (specs/columine/10-physical-vocabulary.md §7):
    // a dynamic member a column places makes that column dynamic, and only a
    // member no column places stays keyed by `(event type, member)`, for the
    // declared record.
    let mut dynamic_members = core::mem::take(&mut config.dynamic_members);
    let mut dynamic_columns = core::mem::take(&mut config.dynamic_columns);
    dynamic_members.retain(
        |event_type, member| match config.placed_column(event_type, member) {
            Some(column) => {
                dynamic_columns[column] = true;
                false
            }
            None => true,
        },
    );
    config.dynamic_members = dynamic_members;
    config.dynamic_columns = dynamic_columns;
    config.check_placement()?;
    Ok(config)
}

/// Spell the `value.<member>` column name into `name`, reusing its storage;
/// growing it is fallible.
pub(crate) fn spell_payload_column(
    name: &mut String,
    member: &str,
) -> Result<(), json_extractor::ExtractionError> {
    name.clear();
    name.try_reserve("value.".len() + member.len())
        .map_err(|_| json_extractor::ExtractionError::MemoryExhausted)?;
    name.push_str("value.");
    name.push_str(member);
    Ok(())
}

/// Release an extraction configuration.
pub fn free_extraction_config(config: ExtractionConfig) {
    drop(config);
}

/// Method-style cell/count views for tests and differential assertions.
pub trait CellExt {
    fn cell(&self, column: usize, row: usize) -> Option<ColumnValue<'static>>;
    fn count(&self) -> usize;
}

impl CellExt for DynamicColumns {
    fn cell(&self, column: usize, row: usize) -> Option<ColumnValue<'static>> {
        read_cell(self, column, row)
    }
    fn count(&self) -> usize {
        self.count as usize
    }
}

#[cfg(test)]
mod properties {
    use super::*;
    use proptest::prelude::*;

    /// Arbitrary well-formed MessagePack value bytes (leaves + shallow
    /// containers) for skip/position properties.
    fn msgpack_value() -> impl Strategy<Value = Vec<u8>> {
        let leaf = prop_oneof![
            (0_u8..=0x7f).prop_map(|byte| vec![byte]),
            any::<i64>().prop_map(|value| {
                let mut out = vec![0xd3];
                out.extend(value.to_be_bytes());
                out
            }),
            any::<f64>().prop_map(|value| {
                let mut out = vec![0xcb];
                out.extend(value.to_bits().to_be_bytes());
                out
            }),
            "[a-z]{0,20}".prop_map(|text| {
                let mut out = vec![0xa0 | u8::try_from(text.len()).unwrap()];
                out.extend(text.as_bytes());
                out
            }),
            Just(vec![0xc0]),
            Just(vec![0xc2]),
            Just(vec![0xc3]),
        ];
        leaf.prop_recursive(3, 24, 4, |inner| {
            prop_oneof![
                prop::collection::vec(inner.clone(), 0..4).prop_map(|items| {
                    let mut out = vec![0x90 | u8::try_from(items.len()).unwrap()];
                    for item in items {
                        out.extend(item);
                    }
                    out
                }),
                prop::collection::vec(("[a-z]{1,8}", inner), 0..4).prop_map(|pairs| {
                    let mut out = vec![0x80 | u8::try_from(pairs.len()).unwrap()];
                    for (key, value) in pairs {
                        out.push(0xa0 | u8::try_from(key.len()).unwrap());
                        out.extend(key.as_bytes());
                        out.extend(value);
                    }
                    out
                }),
            ]
        })
    }

    proptest! {
        /// Scanner output and the streaming parser agree on generated event
        /// documents. The generator limits strings to JSON-safe ASCII because
        /// escaping itself is covered by the lexer unit tests.
        #[test]
        fn json_scanner_and_parser_agree(
            id in "[a-z0-9]{1,12}",
            event_type in "[a-z]{1,12}",
            timestamp in 0_i64..4_000_000_000_000_i64,
            value in -10_000_i64..10_000_i64,
        ) {
            let document = format!(r#"[{{"id":"{id}","type":"{event_type}","timestamp":{timestamp},"value":{value}}}]"#);
            let mut parser = json_parser::JsonParser::new(document.as_bytes());
            parser.expect_array_begin().unwrap();
            parser.expect_object_begin().unwrap();
            assert_eq!(parser.expect_field_name().unwrap(), "id");
            assert!(parser.expect_string().unwrap().is(&id));

            let mut columns = base_columns(1);
            json_scanner::parse_json_events(document.as_bytes(), &mut columns).unwrap();
            let event = parsed_event(&columns, 0).unwrap();
            assert_eq!(event.id, id);
            assert_eq!(event.event_type, event_type);
            assert_eq!(event.timestamp_nanos, timestamp);
            assert_eq!(event.value.as_deref(), Some(value.to_string().as_bytes()));
        }

        /// Hand-written MessagePack maps retain the scanner/extractor typed
        /// values without depending on a general MessagePack crate.
        #[test]
        fn msgpack_scan_and_extract_round_trip(
            id in "[a-z0-9]{1,12}",
            event_type in "[a-z]{1,12}",
            timestamp in 0_i64..4_000_000_000_000_i64,
            quantity in 0_i64..100_i64,
        ) {
            let mut input = vec![0x84];
            write_fixstr(&mut input, "id"); write_fixstr(&mut input, &id);
            write_fixstr(&mut input, "type"); write_fixstr(&mut input, &event_type);
            write_fixstr(&mut input, "timestamp"); input.push(0xd3); input.extend(timestamp.to_be_bytes());
            write_fixstr(&mut input, "quantity"); input.push(quantity as u8);

            let mut base = base_columns(1);
            msgpack_scanner::parse_msgpack_stream(&input, &mut base).unwrap();
            assert_eq!(parsed_event(&base, 0).unwrap().id, id);

            let fields = [
                SignalSchemaField::new(ArrowType::Utf8, false),
                SignalSchemaField::new(ArrowType::Utf8, false),
                SignalSchemaField::new(ArrowType::Int64, false),
                SignalSchemaField::new(ArrowType::Int64, true),
            ];
            let config = build_extraction_config(&fields, &["id", "type", "timestamp", "quantity"]).unwrap();
            // An exactly-capacity batch is legal; this input has spare
            // capacity and therefore must be accepted.
            let mut typed = DynamicColumns::new(&fields, 2).expect("column plane fits");
            let mut work = [0_u8; 128];
            assert_eq!(msgpack_extractor::extract_msgpack_events(&input, &config, &mut typed, &mut work, true, &mut columine_types::category::NoCategories).unwrap(), 1);
            assert_eq!(read_cell(&typed, 0, 0), Some(ColumnValue::Utf8(id.into())));
            assert_eq!(read_cell(&typed, 1, 0), Some(ColumnValue::Utf8(event_type.into())));
            assert_eq!(read_cell(&typed, 2, 0), Some(ColumnValue::Int(timestamp)));
            assert_eq!(read_cell(&typed, 3, 0), Some(ColumnValue::Int(quantity)));
        }

        /// skip_value consumes exactly one well-formed value: the reader's
        /// final position equals the value length regardless of trailing
        /// bytes. This pins the zero-copy `value` capture in the scanners.
        #[test]
        fn msgpack_skip_value_consumes_exactly_one_value(
            value in msgpack_value(),
            trailer in prop::collection::vec(any::<u8>(), 0..8),
        ) {
            let mut input = value.clone();
            input.extend(&trailer);
            let mut reader = msgpack_scanner::Reader::new(&input);
            reader.skip_value().unwrap();
            prop_assert_eq!(reader.position(), value.len());
        }

        /// JSON extractor differential: declared fields land typed, the
        /// undeclared field lands as typed msgpack in the undeclared carrier, byte-exact.
        #[test]
        fn json_extractor_routes_declared_and_undeclared_fields(
            id in "[a-z0-9]{1,12}",
            quantity in -1_000_i64..1_000,
            extra_key in "[a-z]{1,8}",
            extra_flag in proptest::bool::ANY,
        ) {
            let document = format!(
                r#"[{{"id":"{id}","qty":{quantity},"x{extra_key}":{extra_flag}}}]"#
            );
            let fields = [
                SignalSchemaField::new(ArrowType::Utf8, false),
                SignalSchemaField::new(ArrowType::Int64, false),
                SignalSchemaField::new(ArrowType::Binary, true),
            ];
            let config = build_extraction_config(&fields, &["id", "qty", UNDECLARED_COLUMN_NAME]).unwrap();
            let mut typed = DynamicColumns::new(&fields, 2).expect("column plane fits");
            let mut work = [0_u8; 256];
            assert_eq!(
                json_extractor::extract_json_events(document.as_bytes(), &config, &mut typed, &mut work, &mut json_extractor::ExtractionDiagnostic::default(), &mut columine_types::category::NoCategories, &mut CellWriter::Native, &mut Vec::new()).unwrap(),
                1
            );
            prop_assert_eq!(read_cell(&typed, 0, 0), Some(ColumnValue::Utf8(id.into())));
            prop_assert_eq!(read_cell(&typed, 1, 0), Some(ColumnValue::Int(quantity)));
            let mut expected = vec![0xdf, 0, 0, 0, 1];
            expected.push(0xa0 | u8::try_from(extra_key.len() + 1).unwrap());
            expected.push(b'x');
            expected.extend(extra_key.as_bytes());
            expected.push(if extra_flag { 0xc3 } else { 0xc2 });
            prop_assert_eq!(read_cell(&typed, 2, 0), Some(ColumnValue::Binary(expected.into())));
        }
    }

    fn write_fixstr(output: &mut Vec<u8>, value: &str) {
        output.push(0xa0 | u8::try_from(value.len()).unwrap());
        output.extend_from_slice(value.as_bytes());
    }
}
