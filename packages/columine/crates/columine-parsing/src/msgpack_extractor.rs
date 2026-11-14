//! MessagePack event extraction into typed columns.

use std::borrow::Cow;

use crate::msgpack_scanner::{BIGINT_EXTENSION, Reader, bigint_ext_i64, bigint_ext_u64};
use crate::validate::{
    MsgpackValue, SemanticSchema, ValueParseError, encode_segment_into, encoded_segment_len,
    parse_msgpack_value, push_path_segment,
};
use crate::{
    Carrier, CellWriter, ColumnValue, DynamicColumns, ExtractionConfig, FrameProjections,
    MemberSchema, MemberWrite, NullCell, ParseError, PlaneKind, SeamCell, SignalSchemaField,
    json_extractor::ExtractionError,
};
use columine_types::category::CategoryImage;

/// Extracts either a concatenated MessagePack map stream or an array of maps.
pub fn extract_msgpack_events(
    input: &[u8],
    config: &ExtractionConfig,
    columns: &mut DynamicColumns,
    work_buffer: &mut [u8],
    stream: bool,
    image: &mut dyn CategoryImage,
) -> Result<usize, ExtractionError> {
    extract_msgpack_events_with_projections(
        input,
        config,
        columns,
        work_buffer,
        stream,
        FrameProjections::default(),
        image,
        &mut CellWriter::Native,
        &mut Vec::new(),
    )
}

/// [`extract_msgpack_events`] writing each projected value-object frame's
/// certified bytes in place of the batch slice it was admitted from. A
/// projection that finds no cell to replace — a frame stored inside a raw
/// Binary document, say — refuses the batch rather than storing the writer's
/// layout.
#[allow(
    clippy::too_many_arguments,
    reason = "one batch: input, configuration, columns, workspace, framing, projections, the stream image and the value cell writer with its scratch"
)]
pub fn extract_msgpack_events_with_projections(
    input: &[u8],
    config: &ExtractionConfig,
    columns: &mut DynamicColumns,
    work_buffer: &mut [u8],
    stream: bool,
    projections: FrameProjections<'_>,
    image: &mut dyn CategoryImage,
    cells: &mut CellWriter<'_>,
    scratch: &mut Vec<u8>,
) -> Result<usize, ExtractionError> {
    if input.is_empty() {
        return Ok(0);
    }
    let mut frames = FrameWrites {
        projections,
        written: 0,
    };
    let mut reader = Reader::new(input);
    let mut count = 0;
    // An exactly-capacity stream is legal; reject only when more events
    // follow a full batch.
    if stream {
        while !reader.at_end() {
            extract_msgpack_event(
                &mut reader,
                config,
                columns,
                work_buffer,
                &mut frames,
                image,
                cells,
                scratch,
            )?;
            count += 1;
            if count >= columns.capacity as usize && !reader.at_end() {
                return Err(ExtractionError::TooManyEvents);
            }
        }
    } else {
        let size = reader
            .read_array_header()
            .ok_or(ExtractionError::InvalidJson)?;
        // The header declares the batch size up front: refuse only a batch
        // genuinely larger than capacity; exactly-capacity is legal.
        if size as usize > columns.capacity as usize {
            return Err(ExtractionError::TooManyEvents);
        }
        for _ in 0..size {
            extract_msgpack_event(
                &mut reader,
                config,
                columns,
                work_buffer,
                &mut frames,
                image,
                cells,
                scratch,
            )?;
            count += 1;
        }
    }
    if frames.written != frames.projections.len() {
        return Err(ExtractionError::UnwrittenProjection);
    }
    Ok(count)
}

/// The projections of one extraction attempt and how many were written.
struct FrameWrites<'p> {
    projections: FrameProjections<'p>,
    written: usize,
}

#[allow(
    clippy::too_many_arguments,
    reason = "one batch: input, configuration, columns, workspace, framing, projections, the stream image and the value cell writer with its scratch"
)]
fn extract_msgpack_event(
    reader: &mut Reader<'_>,
    config: &ExtractionConfig,
    columns: &mut DynamicColumns,
    work_buffer: &mut [u8],
    frames: &mut FrameWrites<'_>,
    image: &mut dyn CategoryImage,
    cells: &mut CellWriter<'_>,
    scratch: &mut Vec<u8>,
) -> Result<(), ExtractionError> {
    // Parse the map header before opening a row, so an invalid header never
    // leaves a partially initialized row.
    let fields = reader
        .read_map_header()
        .ok_or(ExtractionError::InvalidJson)?;
    if !columns.begin_row() {
        return Err(ExtractionError::TooManyEvents);
    }
    let result = extract_msgpack_fields(
        reader,
        fields,
        config,
        columns,
        work_buffer,
        frames,
        image,
        cells,
        scratch,
    );
    if result.is_err() {
        // Abandon the row on error; failed extraction must not commit it.
        columns.abandon_row();
    }
    result
}

#[allow(
    clippy::too_many_arguments,
    reason = "one batch: input, configuration, columns, workspace, framing, projections, the stream image and the value cell writer with its scratch"
)]
fn extract_msgpack_fields(
    reader: &mut Reader<'_>,
    fields: u32,
    config: &ExtractionConfig,
    columns: &mut DynamicColumns,
    work_buffer: &mut [u8],
    frames: &mut FrameWrites<'_>,
    image: &mut dyn CategoryImage,
    cells: &mut CellWriter<'_>,
    scratch: &mut Vec<u8>,
) -> Result<(), ExtractionError> {
    // Presence is schema-sized state owned by DynamicColumns and allocated
    // once with the column storage. Resetting it per row avoids both the old
    // 64-field ceiling and any per-event allocation.
    columns.columns_seen.fill(false);
    crate::begin_cells(config, cells)?;
    // A fallback column with an unusably small work buffer (< 5 bytes, where
    // even the MessagePack header cannot fit) is a configuration error. Refuse
    // rather than silently dropping undeclared fields.
    if config.native_carrier().is_some() && work_buffer.len() < 5 {
        return Err(ExtractionError::WorkBufferTooSmall);
    }
    let extra_active = config.native_carrier().is_some();
    let mut state = ExtraState {
        count: 0,
        end: 0,
        active: extra_active,
    };
    let mut saw_nested_value = false;
    let mut saw_flat_value_key = false;
    let mut column_name = String::new();
    // The discriminant, once its key has been read: discriminated columns and
    // enum ordinals resolve against it. Canonical encoders sort keys, so a
    // discriminator key that sorts before `value` is always seen first; a
    // producer that spells `value` earlier resolves undiscriminated, unless
    // the discriminator is `type`, which is looked ahead for.
    let mut discriminant: Option<&str> = None;
    // The signal type, which selects the schema nested payload members are
    // routed by. A producer that spells `value` before `type` is looked ahead
    // for it, so routing never depends on key order.
    let mut event_type: Option<&str> = None;
    for field_index in 0..fields {
        let key_start = reader.position();
        let key = reader.read_string().ok_or(ExtractionError::InvalidJson)?;
        let key_end = reader.position();
        if config.is_discriminator_key(key) {
            let mut probe = Reader::new(&reader.input()[reader.position()..]);
            discriminant = probe
                .read_string()
                .and_then(|bytes| std::str::from_utf8(bytes).ok());
        }
        if key == b"type" {
            let mut probe = Reader::new(&reader.input()[reader.position()..]);
            event_type = probe
                .read_string()
                .and_then(|bytes| std::str::from_utf8(bytes).ok());
        }
        if saw_nested_value && key.starts_with(b"value.")
            || saw_flat_value_key && key == b"value" && config.has_value_fields
        {
            // One spelling per event: the validator judges the nested form,
            // and a second spelling in the same event would let the judged
            // bytes and the stored bytes diverge.
            return Err(ExtractionError::InvalidJson);
        }
        // A flat `value.<member>` key's null is its member's, and a dynamic
        // member is keyed by the signal type: the first flat key looks ahead
        // for a `type` spelled after it, as the nested descent does.
        if !saw_flat_value_key
            && key.starts_with(b"value.")
            && event_type.is_none()
            && config.reads_event_type()
        {
            event_type = lookahead_event_type(reader, fields - field_index - 1);
        }
        saw_flat_value_key |= key.starts_with(b"value.");
        // The nested-envelope descent, the msgpack twin of the JSON
        // extractor's: payload members resolve their `value.<member>`
        // columns; a non-map `value` and a pure system schema keep the plain
        // member handling.
        if key == b"value"
            && config.has_value_fields
            && matches!(
                reader.input().get(reader.position()),
                Some(0x80..=0x8f | 0xde | 0xdf)
            )
        {
            saw_nested_value = true;
            if event_type.is_none() && config.reads_event_type() {
                event_type = lookahead_event_type(reader, fields - field_index - 1);
            }
            let members = reader
                .read_map_header()
                .ok_or(ExtractionError::InvalidJson)?;
            for _ in 0..members {
                let member_start = reader.position();
                let member = reader.read_string().ok_or(ExtractionError::InvalidJson)?;
                let member_end = reader.position();
                let (lookup_name, member_name, null) = match std::str::from_utf8(member) {
                    Ok(name) => {
                        crate::spell_payload_column(&mut column_name, name)?;
                        (
                            Some(column_name.as_str()),
                            Some(name),
                            config.null_cell(event_type, name),
                        )
                    }
                    // A non-UTF-8 member is undeclared by construction.
                    Err(_) => (None, None, NullCell::Empty),
                };
                extract_msgpack_member(
                    reader,
                    config,
                    columns,
                    work_buffer,
                    &mut state,
                    frames,
                    config.event_discriminant(discriminant, event_type),
                    event_type,
                    null,
                    lookup_name,
                    member_name,
                    member,
                    member_start,
                    member_end,
                    image,
                    cells,
                    scratch,
                )?;
            }
            continue;
        }
        let lookup_name = std::str::from_utf8(key).ok();
        let null = lookup_name.map_or(NullCell::Empty, |key| {
            config.top_level_null_cell(event_type, key)
        });
        // A flat `value.<member>` key is that payload member: it takes the
        // member's discriminated column and routing row, as the nested
        // descent gives them. Any other top-level key is a system member.
        let flat_member = lookup_name.and_then(|key| key.strip_prefix("value."));
        extract_msgpack_member(
            reader,
            config,
            columns,
            work_buffer,
            &mut state,
            frames,
            flat_member.and(config.event_discriminant(discriminant, event_type)),
            None,
            null,
            lookup_name,
            flat_member.or(lookup_name),
            key,
            key_start,
            key_end,
            image,
            cells,
            scratch,
        )?;
    }
    let extra_count = state.count;
    let extra_end = state.end;
    for (presence_column, source_column) in &config.presence_entries {
        let present = columns.columns_seen[*source_column];
        append(columns, *presence_column, Some(ColumnValue::Bool(present)))?;
        columns.columns_seen[*presence_column] = true;
    }
    for (column, _) in &config.field_entries {
        if !columns.columns_seen[*column] {
            append(columns, *column, None)?;
        }
    }
    if let Some(column) = config.native_carrier() {
        if extra_count == 0 {
            append(columns, column, None)?;
        } else {
            work_buffer[0] = 0xdf;
            work_buffer[1..5].copy_from_slice(&extra_count.to_be_bytes());
            append(
                columns,
                column,
                Some(ColumnValue::Binary(Cow::Borrowed(
                    &work_buffer[..extra_end],
                ))),
            )?;
        }
    }
    crate::finish_cells(config, columns, cells, scratch)?;
    columns.end_row();
    Ok(())
}

/// The string `type` member among the `remaining` keys after the nested
/// `value` map the reader is positioned at, read from a probe so the real
/// reader does not move. Only producers that spell `value` before `type` pay
/// for it.
fn lookahead_event_type<'a>(reader: &Reader<'a>, remaining: u32) -> Option<&'a str> {
    let mut probe = Reader::new(&reader.input()[reader.position()..]);
    probe.skip_value()?;
    for _ in 0..remaining {
        let key = probe.read_string()?;
        if key == b"type" {
            return std::str::from_utf8(probe.read_string()?).ok();
        }
        probe.skip_value()?;
    }
    None
}

/// Byte length of the MessagePack string header for a payload of `len` bytes.
const fn msgpack_str_header_len(len: usize) -> usize {
    match len {
        0..=31 => 1,
        32..=255 => 2,
        256..=65535 => 3,
        _ => 5,
    }
}

/// Write a MessagePack string header for `len` bytes at `at`, returning the
/// position past it. The caller has already bounds-checked header + payload.
fn write_msgpack_str_header(buffer: &mut [u8], at: usize, len: usize) -> usize {
    match len {
        0..=31 => {
            buffer[at] = 0xa0 | len as u8;
            at + 1
        }
        32..=255 => {
            buffer[at] = 0xd9;
            buffer[at + 1] = len as u8;
            at + 2
        }
        256..=65535 => {
            buffer[at] = 0xda;
            buffer[at + 1..at + 3].copy_from_slice(&(len as u16).to_be_bytes());
            at + 3
        }
        _ => {
            buffer[at] = 0xdb;
            buffer[at + 1..at + 5].copy_from_slice(&(len as u32).to_be_bytes());
            at + 5
        }
    }
}

/// Bounded carrier accumulation state threaded through one row's members.
struct ExtraState {
    count: u32,
    end: usize,
    active: bool,
}

/// Handle one named member of the event or its nested payload, the msgpack
/// twin of the JSON extractor's `extract_named_member`: a declared column
/// extracts typed, a member the layout routes to the declared record is put
/// there, an unknown member captures raw key/value slices into the bounded
/// carrier (path-keyed, `%`/`.` re-encoded via `encode_path_segment`),
/// otherwise the value is skipped. A member named after the carrier column
/// routes to capture, never to the carrier's own cell.
/// `record_null` is what an explicit null is in the declared record, as the
/// caller's spelling of the member resolves it; a column's null is the
/// column's own. `member` is the payload member whose discriminated column
/// and routing row place it; `key` is the path the carrier keys it by.
#[allow(
    clippy::too_many_arguments,
    reason = "one seam, two call shapes: top-level and payload descent"
)]
fn extract_msgpack_member(
    reader: &mut Reader<'_>,
    config: &ExtractionConfig,
    columns: &mut DynamicColumns,
    work_buffer: &mut [u8],
    state: &mut ExtraState,
    frames: &mut FrameWrites<'_>,
    discriminant: Option<&str>,
    event_type: Option<&str>,
    record_null: NullCell,
    lookup_name: Option<&str>,
    member: Option<&str>,
    key: &[u8],
    key_start: usize,
    key_end: usize,
    image: &mut dyn CategoryImage,
    cells: &mut CellWriter<'_>,
    scratch: &mut Vec<u8>,
) -> Result<(), ExtractionError> {
    // A non-UTF-8 key is undeclared by construction: under an open payload
    // object it is captured like any other undeclared member.
    let path = std::str::from_utf8(key).ok();
    let member_schema = config.payload_member_schema(event_type, member.unwrap_or_default());
    // An open schema's undeclared member is the carrier's: it is never
    // written to another type's column, nor placed by a routing row.
    let placed = match member_schema {
        MemberSchema::UndeclaredOpen => false,
        MemberSchema::Unjudged | MemberSchema::Declared(_) => true,
    };
    let lookup = if placed {
        lookup_name.and_then(|name| {
            config.lookup_payload_member(discriminant, member.unwrap_or(name), name)
        })
    } else {
        None
    }
    .and_then(|lookup| {
        config
            .member_write(lookup.column)
            .map(|write| (lookup, write))
    });
    if let Some((lookup, write)) = lookup {
        // A column's cell is one form for every row: the column decides its
        // null, whichever member or spelling reached it.
        let null = config.column_null(lookup.column);
        match write {
            MemberWrite::Lane => {
                let dictionary = config.schema_dictionary(lookup.column);
                extract_typed_value(
                    reader,
                    lookup.field,
                    columns,
                    lookup.column,
                    dictionary,
                    null,
                    frames,
                    image,
                )?;
            }
            MemberWrite::Seam(cell) => {
                let capture = match member_schema {
                    MemberSchema::Declared(schema) => path
                        .filter(|name| config.is_open_payload_path(name))
                        .map(|name| (schema, name)),
                    MemberSchema::Unjudged | MemberSchema::UndeclaredOpen => None,
                };
                seam_member(
                    reader,
                    config,
                    columns,
                    lookup.column,
                    cell,
                    capture,
                    null,
                    frames,
                    cells,
                    scratch,
                )?;
            }
        }
        columns.columns_seen[lookup.column] = true;
        return Ok(());
    }
    // A payload member without a column that the layout routes lives in the
    // row's declared record, under its routing row, whether or not a semantic
    // schema judged it.
    if placed
        && let Some(member) = member
        && let Some(row) = config.record_route(discriminant, member)
    {
        let value = msgpack_value(reader)?;
        return crate::record_cell(row, &value, record_null, cells);
    }
    match config.carrier() {
        Carrier::Embedder(_) => {
            // The embedder's carrier is keyed by text paths: a key that is
            // not UTF-8 has none.
            let name = path.ok_or(ExtractionError::InvalidFieldType)?;
            let value = msgpack_value(reader)?;
            let mut path = String::new();
            push_path_segment(&mut path, name).map_err(|_| ExtractionError::MemoryExhausted)?;
            return crate::undeclared_cell(config, &path, &value, cells);
        }
        Carrier::Raw => return Err(ExtractionError::InvalidFieldType),
        Carrier::Absent | Carrier::Native(_) => {}
    }
    if !state.active {
        reader.skip_value().ok_or(ExtractionError::InvalidJson)?;
        return Ok(());
    }
    let value_start = reader.position();
    reader.skip_value().ok_or(ExtractionError::InvalidJson)?;
    let value_end = reader.position();
    if state.count == 0 {
        state.end = 5;
    }
    let raw_value = &reader.input()[value_start..value_end];
    let raw_key = &reader.input()[key_start..key_end];
    // The carrier is path-keyed: a literal `%` or `.` in a segment escapes as
    // %25/%2E (validate::encode_path_segment is the one spelling of that
    // rule) so a captured key can never be misread as nesting. Escaping
    // changes the string's length header, so an affected key re-encodes;
    // every clean key copies its raw already-encoded slice, and a non-UTF-8
    // key is opaque bytes with nothing to escape.
    let escaped = std::str::from_utf8(key)
        .ok()
        .filter(|name| name.contains(['%', '.']))
        .map(|name| (name, encoded_segment_len(name)));
    let key_len = match escaped {
        Some((_, len)) => msgpack_str_header_len(len) + len,
        None => raw_key.len(),
    };
    let end = state
        .end
        .checked_add(key_len + raw_value.len())
        .ok_or(ExtractionError::BufferOverflow)?;
    if end > work_buffer.len() {
        return Err(ExtractionError::BufferOverflow);
    }
    let value_at = match escaped {
        Some((name, len)) => {
            let mut at = write_msgpack_str_header(work_buffer, state.end, len);
            encode_segment_into(name, |chunk| {
                work_buffer[at..at + chunk.len()].copy_from_slice(chunk.as_bytes());
                at += chunk.len();
            });
            at
        }
        None => {
            work_buffer[state.end..state.end + raw_key.len()].copy_from_slice(raw_key);
            state.end + raw_key.len()
        }
    };
    work_buffer[value_at..end].copy_from_slice(raw_value);
    state.end = end;
    state.count += 1;
    Ok(())
}

/// Coerce one MessagePack value into one column.
///
/// The Arrow-plane coercion table is one contract, shared in spirit with
/// `json_extractor::extract_typed_value`, and keyed on plane KIND so that
/// twenty-three planes reduce to eight coercions. MessagePack carries markers
/// that prove an integer is an integer, so nothing here truncates a float. A
/// dictionary-encoded column is decided by its dictionary before its plane: a
/// text stores its index, and anything else but nil is refused.
#[allow(
    clippy::too_many_arguments,
    reason = "the typed fill of one member: its plane, its schema dictionary, the stream image, its null and the frame projections"
)]
fn extract_typed_value(
    reader: &mut Reader<'_>,
    field: SignalSchemaField,
    columns: &mut DynamicColumns,
    column: usize,
    dictionary: Option<&[Option<String>]>,
    null: NullCell,
    frames: &mut FrameWrites<'_>,
    image: &mut dyn CategoryImage,
) -> Result<(), ExtractionError> {
    let first = *reader
        .input()
        .get(reader.position())
        .ok_or(ExtractionError::InvalidJson)?;
    let kind = field.plane_kind();
    if let Some(encoding) = field.dictionary {
        let value = if first == 0xc0 {
            reader.skip_value();
            None
        } else if Reader::is_string(first) {
            // A text must be UTF-8 before the dictionary or the image sees it.
            let text = std::str::from_utf8(
                reader
                    .read_string()
                    .ok_or(ExtractionError::InvalidFieldType)?,
            )
            .map_err(|_| ExtractionError::InvalidFieldType)?;
            let index = crate::dictionary_index(encoding.slot, dictionary, text, image)?;
            if !kind.holds_uint(index) {
                return Err(ExtractionError::InvalidFieldType);
            }
            Some(ColumnValue::UInt(index))
        } else {
            return Err(ExtractionError::InvalidFieldType);
        };
        return append(columns, column, value);
    }
    let value = match kind {
        PlaneKind::Text { .. } => {
            if first == 0xc0 {
                reader.skip_value();
                None
            } else {
                Some(ColumnValue::Utf8(Cow::Borrowed(
                    std::str::from_utf8(
                        reader
                            .read_string()
                            .ok_or(ExtractionError::InvalidFieldType)?,
                    )
                    .map_err(|_| ExtractionError::InvalidFieldType)?,
                )))
            }
        }
        // Integer planes take integers only, range-checked to the plane's
        // width. The eight-byte planes additionally take a decimal string,
        // because a 64-bit value does not survive a JSON number and the
        // cross-format contract keeps the two extractors interchangeable; a
        // Timestamp column also takes an RFC 3339 instant, converted
        // exactly to its unit.
        PlaneKind::SignedInt { width } => {
            if first == 0xc0 {
                reader.skip_value();
                None
            } else if Reader::is_integer(first) {
                let wide = reader
                    .read_integer()
                    .ok_or(ExtractionError::InvalidFieldType)?;
                if !kind.holds_int(wide) {
                    return Err(ExtractionError::InvalidFieldType);
                }
                Some(ColumnValue::Int(wide))
            } else if Reader::is_float(first) {
                // An integral float is the standard-MessagePack spelling of an
                // integer above u32 from a JavaScript number (the encoder
                // reserves the 64-bit markers for BigInt), and the JSON twin
                // accepts the same digits as a number token. A fractional or
                // out-of-range float is still not an integer.
                let wide = integral_float(reader)?;
                if !kind.holds_int(wide) {
                    return Err(ExtractionError::InvalidFieldType);
                }
                Some(ColumnValue::Int(wide))
            } else if width == 8 && Reader::is_ext(first) {
                // The canonical bigint extension, the bigint spelling on both
                // wires; a magnitude past i64 does not fit the plane.
                let (kind, payload) = reader.read_ext().ok_or(ExtractionError::InvalidFieldType)?;
                if kind != BIGINT_EXTENSION {
                    return Err(ExtractionError::InvalidFieldType);
                }
                Some(ColumnValue::Int(
                    bigint_ext_i64(payload).ok_or(ExtractionError::InvalidFieldType)?,
                ))
            } else if width == 8 && Reader::is_string(first) {
                let text = std::str::from_utf8(
                    reader
                        .read_string()
                        .ok_or(ExtractionError::InvalidFieldType)?,
                )
                .map_err(|_| ExtractionError::InvalidFieldType)?;
                Some(ColumnValue::Int(
                    crate::signed_text(field, text).ok_or(ExtractionError::InvalidFieldType)?,
                ))
            } else {
                return Err(ExtractionError::InvalidFieldType);
            }
        }
        PlaneKind::UnsignedInt { width } => {
            if first == 0xc0 {
                reader.skip_value();
                None
            } else if Reader::is_integer(first) {
                // `read_unsigned_integer` and not `read_integer`: a
                // MessagePack `uint64` above `i64::MAX` is exactly the value
                // the unsigned planes exist to carry.
                let wide = reader
                    .read_unsigned_integer()
                    .ok_or(ExtractionError::InvalidFieldType)?;
                if !kind.holds_uint(wide) {
                    return Err(ExtractionError::InvalidFieldType);
                }
                Some(ColumnValue::UInt(wide))
            } else if Reader::is_float(first) {
                // Same integral-float spelling as the signed plane, above.
                let wide = u64::try_from(integral_float(reader)?)
                    .map_err(|_| ExtractionError::InvalidFieldType)?;
                if !kind.holds_uint(wide) {
                    return Err(ExtractionError::InvalidFieldType);
                }
                Some(ColumnValue::UInt(wide))
            } else if width == 8 && Reader::is_ext(first) {
                let (kind, payload) = reader.read_ext().ok_or(ExtractionError::InvalidFieldType)?;
                if kind != BIGINT_EXTENSION {
                    return Err(ExtractionError::InvalidFieldType);
                }
                Some(ColumnValue::UInt(
                    bigint_ext_u64(payload).ok_or(ExtractionError::InvalidFieldType)?,
                ))
            } else if width == 8 && Reader::is_string(first) {
                let text = std::str::from_utf8(
                    reader
                        .read_string()
                        .ok_or(ExtractionError::InvalidFieldType)?,
                )
                .map_err(|_| ExtractionError::InvalidFieldType)?;
                Some(ColumnValue::UInt(
                    text.parse()
                        .map_err(|_| ExtractionError::InvalidFieldType)?,
                ))
            } else {
                return Err(ExtractionError::InvalidFieldType);
            }
        }
        PlaneKind::Float { .. } => {
            if first == 0xc0 {
                reader.skip_value();
                None
            } else if Reader::is_float(first) {
                Some(ColumnValue::Float(
                    reader
                        .read_float()
                        .ok_or(ExtractionError::InvalidFieldType)?,
                ))
            } else if Reader::is_integer(first) {
                Some(ColumnValue::Float(
                    reader
                        .read_integer()
                        .ok_or(ExtractionError::InvalidFieldType)? as f64,
                ))
            } else {
                return Err(ExtractionError::InvalidFieldType);
            }
        }
        PlaneKind::Bool => match first {
            0xc0 => {
                reader.skip_value();
                None
            }
            0xc2 => {
                reader.skip_value();
                Some(ColumnValue::Bool(false))
            }
            0xc3 => {
                reader.skip_value();
                Some(ColumnValue::Bool(true))
            }
            _ => return Err(ExtractionError::InvalidFieldType),
        },
        // MessagePack has a real byte string, so an exactly-width plane takes
        // a `bin` of exactly that many bytes — no hex detour, unlike JSON.
        PlaneKind::FixedBytes { width } => {
            if first == 0xc0 {
                reader.skip_value();
                None
            } else if matches!(first, 0xc4..=0xc6) {
                let payload = reader.read_bin().ok_or(ExtractionError::InvalidJson)?;
                if payload.len() != width as usize {
                    return Err(ExtractionError::InvalidFieldType);
                }
                Some(ColumnValue::FixedBytes(Cow::Borrowed(payload)))
            } else {
                return Err(ExtractionError::InvalidFieldType);
            }
        }
        PlaneKind::Bytes { .. } => {
            if first == 0xc0 && null == NullCell::Empty {
                reader.skip_value();
                None
            } else if matches!(first, 0xc4..=0xc6) {
                // Binary values are stored as the payload of their canonical
                // MessagePack bin representation. Keeping the payload avoids a
                // second decode after Arrow materialization and preserves raw
                // bytes. A value-object frame the admission projected into the
                // reader's layout is stored as its certified bytes instead.
                let payload = certified_bin(reader, frames)?;
                Some(ColumnValue::Binary(Cow::Borrowed(payload)))
            } else {
                // External standard MessagePack may provide a structured value
                // directly. Preserve its exact document bytes for normal
                // Binary materialization.
                let start = reader.position();
                reader.skip_value().ok_or(ExtractionError::InvalidJson)?;
                Some(ColumnValue::Binary(Cow::Borrowed(
                    &reader.input()[start..reader.position()],
                )))
            }
        }
        PlaneKind::Empty => {
            reader.skip_value().ok_or(ExtractionError::InvalidJson)?;
            None
        }
    };
    append(columns, column, value)
}
fn append(
    columns: &mut DynamicColumns,
    column: usize,
    value: Option<ColumnValue<'_>>,
) -> Result<(), ExtractionError> {
    crate::append_cell(columns, column, value).map_err(|error| match error {
        ParseError::BufferOverflow => ExtractionError::BufferOverflow,
        ParseError::OutOfMemory => ExtractionError::MemoryExhausted,
        _ => ExtractionError::InvalidFieldType,
    })
}

/// Read a float marker that spells an integer: finite, integral, and inside
/// `i64`. Anything else is not an integer, whatever plane asked.
fn integral_float(reader: &mut Reader<'_>) -> Result<i64, ExtractionError> {
    let value = reader
        .read_float()
        .ok_or(ExtractionError::InvalidFieldType)?;
    // 2^63 is exactly representable and is the first f64 i64 cannot hold.
    const LIMIT: f64 = 9_223_372_036_854_775_808.0;
    if !value.is_finite() || value.fract() != 0.0 || value >= LIMIT || value < -LIMIT {
        return Err(ExtractionError::InvalidFieldType);
    }
    #[allow(
        clippy::cast_possible_truncation,
        reason = "integral and range-checked against i64 one line above"
    )]
    Ok(value as i64)
}

/// A `bin` payload, or the certified bytes of the value-object frame the
/// admission projected from exactly that payload.
fn certified_bin<'s, 'i: 's, 'p: 's>(
    reader: &mut Reader<'i>,
    frames: &mut FrameWrites<'p>,
) -> Result<&'s [u8], ExtractionError> {
    let payload = reader.read_bin().ok_or(ExtractionError::InvalidJson)?;
    Ok(match frames.projections.certified(payload) {
        Some(certified) => {
            frames.written += 1;
            certified
        }
        None => payload,
    })
}

/// One member whose column the value-cell seam rules
/// (specs/columine/10-physical-vocabulary.md §7, §8): the embedder writes a
/// structured member's cell; a text stores the hash the embedder computes,
/// from its text or from a 32-byte `bin` hash; and a raw Binary column takes
/// a `bin` payload or a projected frame's certified bytes, nothing structured.
#[allow(
    clippy::too_many_arguments,
    reason = "one seam cell: its column and kind, its open-path capture and null rule, the frame projections, the writer and its scratch"
)]
fn seam_member(
    reader: &mut Reader<'_>,
    config: &ExtractionConfig,
    columns: &mut DynamicColumns,
    column: usize,
    cell: SeamCell,
    capture: Option<(&SemanticSchema, &str)>,
    null: NullCell,
    frames: &mut FrameWrites<'_>,
    cells: &mut CellWriter<'_>,
    scratch: &mut Vec<u8>,
) -> Result<(), ExtractionError> {
    let first = *reader
        .input()
        .get(reader.position())
        .ok_or(ExtractionError::InvalidJson)?;
    match cell {
        SeamCell::Embedder => {
            let value = msgpack_value(reader)?;
            crate::member_cell(
                config, columns, column, &value, null, capture, cells, scratch,
            )
        }
        SeamCell::Raw => {
            let value = if first == 0xc0 {
                reader.skip_value();
                None
            } else if matches!(first, 0xc4..=0xc6) {
                Some(ColumnValue::Binary(Cow::Borrowed(certified_bin(
                    reader, frames,
                )?)))
            } else {
                return Err(ExtractionError::InvalidFieldType);
            };
            append(columns, column, value)
        }
        SeamCell::TextHash { inline } => {
            if first == 0xc0 {
                reader.skip_value();
                Ok(())
            } else if matches!(first, 0xc4..=0xc6) {
                let hash = reader.read_bin().ok_or(ExtractionError::InvalidJson)?;
                crate::text_hash_cell(columns, column, hash, cells)
            } else if Reader::is_string(first) {
                let text = std::str::from_utf8(
                    reader
                        .read_string()
                        .ok_or(ExtractionError::InvalidFieldType)?,
                )
                .map_err(|_| ExtractionError::InvalidFieldType)?;
                crate::text_cell(columns, column, inline, text, cells)
            } else {
                Err(ExtractionError::InvalidFieldType)
            }
        }
    }
}

/// The next MessagePack value, whole, as the borrowed view a value cell
/// writer reads.
fn msgpack_value<'a>(reader: &mut Reader<'a>) -> Result<MsgpackValue<'a>, ExtractionError> {
    parse_msgpack_value(reader, 0).map_err(|error| match error {
        ValueParseError::OutOfMemory => ExtractionError::MemoryExhausted,
        ValueParseError::InvalidJson
        | ValueParseError::InvalidMsgpack
        | ValueParseError::InvalidNumber
        | ValueParseError::TooDeep => ExtractionError::InvalidJson,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ArrowType, CellExt, SignalSchemaField, build_extraction_config};
    use columine_arrow::{DictionarySlot, IndexWidth, SchemaDictionary, TimeUnit};
    use columine_types::category::{CategoryRefusal, ListImage, NoCategories};

    /// [`extract_msgpack_events`] for a schema with no stream-dictionary column.
    fn extract(
        input: &[u8],
        config: &ExtractionConfig,
        columns: &mut DynamicColumns,
        work: &mut [u8],
        stream: bool,
    ) -> Result<usize, ExtractionError> {
        extract_msgpack_events(input, config, columns, work, stream, &mut NoCategories)
    }

    fn encoded(slot: DictionarySlot) -> SignalSchemaField {
        SignalSchemaField::new_dictionary(slot, IndexWidth::U8, true)
    }

    /// `{key: <value>}` with `value` already encoded.
    fn one_member(key: &str, value: &[u8]) -> Vec<u8> {
        let mut event = vec![0x81];
        str_(&mut event, key);
        event.extend(value);
        event
    }

    /// A MessagePack str of `text` (under 32 bytes).
    fn text(text: &str) -> Vec<u8> {
        let mut out = Vec::new();
        str_(&mut out, text);
        out
    }
    fn field(arrow_type: ArrowType) -> SignalSchemaField {
        SignalSchemaField::new(arrow_type, true)
    }
    fn str_(out: &mut Vec<u8>, value: &str) {
        out.push(0xa0 | value.len() as u8);
        out.extend(value.as_bytes())
    }
    fn config(fields: &[SignalSchemaField], names: &[&str]) -> ExtractionConfig {
        build_extraction_config(fields, names).unwrap()
    }

    /// A projected frame's certified bytes replace exactly the batch slice it
    /// was admitted from — matched by identity, not by content — and a
    /// projection with no cell to replace refuses the batch.
    #[test]
    fn projected_frames_write_certified_bytes_or_refuse() {
        let fields = [field(ArrowType::Binary), field(ArrowType::Binary)];
        let config_two = config(&fields, &["frame", "copy"]);
        // {"frame": <bin8 01 02>, "copy": <bin8 01 02>}: equal bytes, two slices.
        let mut input = vec![0x82];
        str_(&mut input, "frame");
        input.extend([0xc4, 2, 1, 2]);
        str_(&mut input, "copy");
        input.extend([0xc4, 2, 1, 2]);
        let frame_at = 1 + 6 + 2;
        let lent = &input[frame_at..frame_at + 2];
        let certified = [9_u8, 9, 9];
        let pairs = [(lent, &certified[..])];
        let mut columns = DynamicColumns::new(&fields, 1).expect("column plane fits");
        let mut work = [0; 64];
        assert_eq!(
            extract_msgpack_events_with_projections(
                &input,
                &config_two,
                &mut columns,
                &mut work,
                true,
                FrameProjections::new(&pairs),
                &mut NoCategories,
                &mut crate::CellWriter::Native,
                &mut Vec::new(),
            ),
            Ok(1)
        );
        assert_eq!(
            columns.cell(0, 0),
            Some(ColumnValue::Binary(std::borrow::Cow::Owned(vec![9, 9, 9])))
        );
        assert_eq!(
            columns.cell(1, 0),
            Some(ColumnValue::Binary(std::borrow::Cow::Owned(vec![1, 2]))),
            "an equal slice elsewhere is not the lent frame"
        );

        // A frame inside a map stored as one raw Binary document has no cell.
        let mut nested = vec![0x81];
        str_(&mut nested, "frame");
        nested.push(0x81);
        str_(&mut nested, "inner");
        nested.extend([0xc4, 2, 1, 2]);
        let inner_at = nested.len() - 2;
        let pairs = [(&nested[inner_at..], &certified[..])];
        let one = [field(ArrowType::Binary)];
        let config_one = config(&one, &["frame"]);
        let mut columns = DynamicColumns::new(&one, 1).expect("column plane fits");
        assert_eq!(
            extract_msgpack_events_with_projections(
                &nested,
                &config_one,
                &mut columns,
                &mut work,
                true,
                FrameProjections::new(&pairs),
                &mut NoCategories,
                &mut crate::CellWriter::Native,
                &mut Vec::new(),
            ),
            Err(ExtractionError::UnwrittenProjection)
        );
    }

    /// The msgpack twin of the JSON routing rule: an undeclared member of an
    /// open payload is captured, never written to the column another signal
    /// type declares under that name, in either key order.
    #[test]
    fn undeclared_open_member_captures_instead_of_another_types_column() {
        let fields = [
            field(ArrowType::Utf8),
            field(ArrowType::Utf8),
            field(ArrowType::Binary),
        ];
        let schemas = crate::validate::parse_schema_envelope(
            br#"{"a":{"kind":"object","fields":{},"open":true},"b":{"kind":"object","fields":{"note":{"kind":"string"}}}}"#,
        )
        .unwrap();
        let config = crate::build_extraction_config_with_semantic_schemas(
            &fields,
            &["type", "value.note", crate::UNDECLARED_COLUMN_NAME],
            &[],
            Some(&schemas),
            None,
            crate::CellLayout::default(),
        )
        .unwrap();
        let event = |event_type: &str, type_first: bool| {
            let mut payload = vec![0x81];
            str_(&mut payload, "note");
            str_(&mut payload, "x");
            let mut input = vec![0x82];
            let mut type_pair = Vec::new();
            str_(&mut type_pair, "type");
            str_(&mut type_pair, event_type);
            let mut value_pair = Vec::new();
            str_(&mut value_pair, "value");
            value_pair.extend(&payload);
            if type_first {
                input.extend(type_pair);
                input.extend(value_pair);
            } else {
                input.extend(value_pair);
                input.extend(type_pair);
            }
            input
        };
        let captured = ColumnValue::Binary(std::borrow::Cow::Owned(vec![
            0xdf, 0, 0, 0, 1, 0xa4, b'n', b'o', b't', b'e', 0xa1, b'x',
        ]));
        for type_first in [true, false] {
            let mut columns = DynamicColumns::new(&fields, 1).expect("column plane fits");
            let mut work = [0; 64];
            extract(
                &event("a", type_first),
                &config,
                &mut columns,
                &mut work,
                true,
            )
            .unwrap();
            assert_eq!(columns.cell(1, 0), None, "b's column stays empty");
            assert_eq!(columns.cell(2, 0), Some(captured.clone()));
        }
        let mut columns = DynamicColumns::new(&fields, 1).expect("column plane fits");
        let mut work = [0; 64];
        extract(&event("b", true), &config, &mut columns, &mut work, true).unwrap();
        assert_eq!(columns.cell(1, 0), Some(ColumnValue::Utf8("x".into())));
        assert_eq!(columns.cell(2, 0), None);
    }

    /// A dynamic member's explicit nil is a present null in either payload
    /// spelling: the flat `value.<member>` key stores the cell the nested
    /// `value` map stores, whichever side of it `type` is spelled on, and an
    /// absent member stays empty in both.
    #[test]
    fn a_dynamic_members_nil_is_data_in_flat_and_nested_spellings() {
        let fields = [field(ArrowType::Utf8), field(ArrowType::Binary)];
        let schemas = crate::validate::parse_schema_envelope(
            br#"{"hi":{"kind":"object","fields":{"data":{"kind":"unknown"}}}}"#,
        )
        .unwrap();
        let config = crate::build_extraction_config_with_semantic_schemas(
            &fields,
            &["type", "value.data"],
            &[],
            Some(&schemas),
            None,
            crate::CellLayout::default(),
        )
        .unwrap();
        let event = |pairs: &[(&str, &[u8])]| {
            let mut out = vec![0x80 | pairs.len() as u8];
            for (key, value) in pairs {
                str_(&mut out, key);
                out.extend(*value);
            }
            out
        };
        let hi = text("hi");
        let nil_map = one_member("data", &[0xc0]);
        let rows = [
            event(&[("type", &hi), ("value", &nil_map)]),
            event(&[("type", &hi), ("value.data", &[0xc0])]),
            event(&[("value.data", &[0xc0]), ("type", &hi)]),
            event(&[("type", &hi), ("value", &[0x80])]),
            event(&[("type", &hi)]),
        ];
        let mut columns = DynamicColumns::new(&fields, 5).expect("column plane fits");
        let mut work = [0; 64];
        assert_eq!(
            extract(&rows.concat(), &config, &mut columns, &mut work, true),
            Ok(rows.len())
        );
        let present_null = Some(ColumnValue::Binary(std::borrow::Cow::Owned(vec![0xc0])));
        for row in 0..3 {
            assert_eq!(
                columns.cell(1, row),
                present_null,
                "row {row} keeps its nil"
            );
        }
        for row in 3..5 {
            assert_eq!(
                columns.cell(1, row),
                None,
                "row {row}'s absent member is empty"
            );
        }
    }

    /// A JavaScript number above u32 arrives as float64 under standard
    /// MessagePack (the 64-bit integer markers are reserved for BigInt), and
    /// every nanosecond timestamp is such a number.
    #[test]
    fn an_integral_float_lands_in_an_integer_plane_and_a_fraction_does_not() {
        let fields = [field(ArrowType::Utf8), field(ArrowType::Int64)];
        let mut input = vec![0x82];
        str_(&mut input, "id");
        str_(&mut input, "ev-1");
        str_(&mut input, "timestamp");
        input.push(0xcb);
        input.extend(4_294_967_296.0_f64.to_be_bytes());
        let mut columns = DynamicColumns::new(&fields, 2).expect("column plane fits");
        let mut work = [0; 64];
        let config = config(&fields, &["id", "timestamp"]);
        assert_eq!(
            extract(&input, &config, &mut columns, &mut work, true),
            Ok(1)
        );
        assert_eq!(columns.cell(1, 0), Some(ColumnValue::Int(4_294_967_296)));

        let mut fraction = vec![0x82];
        str_(&mut fraction, "id");
        str_(&mut fraction, "ev-2");
        str_(&mut fraction, "timestamp");
        fraction.push(0xcb);
        fraction.extend(1.5_f64.to_be_bytes());
        let mut columns = DynamicColumns::new(&fields, 2).expect("column plane fits");
        assert_eq!(
            extract(&fraction, &config, &mut columns, &mut work, true),
            Err(ExtractionError::InvalidFieldType)
        );
    }

    /// An admitted nested payload spells an enum as its string and a
    /// discriminated member under its plain name; both must land in the
    /// column the flat lane writes, selected by the discriminant.
    #[test]
    fn nested_payload_resolves_discriminated_columns_and_enum_indexes() {
        use crate::validate::parse_schema_envelope;
        let fields = [
            field(ArrowType::Utf8),
            field(ArrowType::Utf8),
            encoded(DictionarySlot::Schema(0)),
            encoded(DictionarySlot::Schema(1)),
            field(ArrowType::Utf8),
        ];
        let dictionaries = [
            SchemaDictionary {
                slot: DictionarySlot::Schema(0),
                arrow_ids: vec![0],
                values: vec![Some("achieved".into()), Some("stuck".into())],
            },
            SchemaDictionary {
                slot: DictionarySlot::Schema(1),
                arrow_ids: vec![1],
                values: vec![Some("human".into())],
            },
        ];
        let names = [
            "id",
            "type",
            "value.outcome",
            "authorRole@close",
            "authorRole@open",
        ];
        let envelope = parse_schema_envelope(
            br#"{"close":{"kind":"object","fields":{"outcome":{"kind":"enum","variants":["achieved","stuck"]},"authorRole":{"kind":"enum","variants":["human"]}}},"open":{"kind":"object","fields":{"authorRole":{"kind":"optional","value":{"kind":"string"}}}}}"#,
        )
        .unwrap();
        let discriminator = crate::PayloadDiscriminator {
            key: "type".into(),
            members: vec![
                crate::DiscriminatedMember {
                    discriminant: "close".into(),
                    member: "authorRole".into(),
                    column: 3,
                },
                crate::DiscriminatedMember {
                    discriminant: "open".into(),
                    member: "authorRole".into(),
                    column: 4,
                },
            ],
        };
        let config = crate::build_extraction_config_with_semantic_schemas(
            &fields,
            &names,
            &dictionaries,
            Some(&envelope),
            Some(&discriminator),
            crate::CellLayout::default(),
        )
        .unwrap();
        // {"id":"e","type":"close","value":{"authorRole":"human","outcome":"stuck"}}
        let mut input = vec![0x83];
        str_(&mut input, "id");
        str_(&mut input, "e");
        str_(&mut input, "type");
        str_(&mut input, "close");
        str_(&mut input, "value");
        input.push(0x82);
        str_(&mut input, "authorRole");
        str_(&mut input, "human");
        str_(&mut input, "outcome");
        str_(&mut input, "stuck");
        let mut columns = DynamicColumns::new(&fields, 2).expect("column plane fits");
        let mut work = [0; 256];
        assert_eq!(
            extract(&input, &config, &mut columns, &mut work, true),
            Ok(1)
        );
        assert_eq!(
            columns.cell(2, 0),
            Some(ColumnValue::UInt(1)),
            "enum string became its dictionary index"
        );
        assert_eq!(
            columns.cell(3, 0),
            Some(ColumnValue::UInt(0)),
            "close's authorRole landed in close's column"
        );
        assert_eq!(
            columns.cell(4, 0),
            None,
            "open's column stays empty for a close event"
        );

        // The same member for the other type resolves to the other column, as text.
        let mut other = vec![0x83];
        str_(&mut other, "id");
        str_(&mut other, "e2");
        str_(&mut other, "type");
        str_(&mut other, "open");
        str_(&mut other, "value");
        other.push(0x81);
        str_(&mut other, "authorRole");
        str_(&mut other, "agent");
        let mut columns = DynamicColumns::new(&fields, 2).expect("column plane fits");
        assert_eq!(
            extract(&other, &config, &mut columns, &mut work, true),
            Ok(1)
        );
        assert_eq!(columns.cell(4, 0), Some(ColumnValue::Utf8("agent".into())));
        assert_eq!(columns.cell(3, 0), None);

        // A string outside the enum's dictionary is refused.
        let mut bad = vec![0x83];
        str_(&mut bad, "id");
        str_(&mut bad, "e3");
        str_(&mut bad, "type");
        str_(&mut bad, "close");
        str_(&mut bad, "value");
        bad.push(0x81);
        str_(&mut bad, "outcome");
        str_(&mut bad, "abandoned");
        let mut columns = DynamicColumns::new(&fields, 2).expect("column plane fits");
        assert_eq!(
            extract(&bad, &config, &mut columns, &mut work, true),
            Err(ExtractionError::InvalidFieldType)
        );
    }

    /// A bigint arrives as the canonical bigint extension on the eight-byte
    /// planes; the 64-bit integer markers remain plain integers.
    #[test]
    fn a_bigint_extension_lands_in_an_int64_plane() {
        let fields = [field(ArrowType::Utf8), field(ArrowType::Int64)];
        let config = config(&fields, &["id", "value.amount"]);
        let mut row = vec![0x82];
        str_(&mut row, "id");
        str_(&mut row, "e");
        str_(&mut row, "value.amount");
        // ext 8, length 2, type 20, sign 1, magnitude 7 → -7
        row.extend([0xc7, 2, 20, 1, 7]);
        let mut columns = DynamicColumns::new(&fields, 2).expect("column plane fits");
        let mut work = [0; 64];
        assert_eq!(extract(&row, &config, &mut columns, &mut work, true), Ok(1));
        assert_eq!(columns.cell(1, 0), Some(ColumnValue::Int(-7)));

        let mut wide = vec![0x82];
        str_(&mut wide, "id");
        str_(&mut wide, "e2");
        str_(&mut wide, "value.amount");
        wide.extend([0xc7, 10, 20, 0]);
        wide.extend([1; 9]);
        let mut columns = DynamicColumns::new(&fields, 2).expect("column plane fits");
        assert_eq!(
            extract(&wide, &config, &mut columns, &mut work, true),
            Err(ExtractionError::InvalidFieldType)
        );
    }
    #[test]
    fn extract_msgpack_events_stream_format_with_typed_extraction() {
        let fields = [
            field(ArrowType::Utf8),
            field(ArrowType::Utf8),
            field(ArrowType::Int32),
            field(ArrowType::Int32),
        ];
        let mut input = vec![0x84];
        for (key, value) in [
            ("id", Some("ev-1")),
            ("type", Some("order")),
            ("timestamp", None),
            ("value.qty", None),
        ] {
            str_(&mut input, key);
            if let Some(v) = value {
                str_(&mut input, v)
            } else if key == "timestamp" {
                input.push(0xd3);
                input.extend(1000_i64.to_be_bytes())
            } else {
                input.push(5)
            }
        }
        let mut columns = DynamicColumns::new(&fields, 10).expect("column plane fits");
        let mut work = [0; 1024];
        assert_eq!(
            extract(
                &input,
                &config(&fields, &["id", "type", "timestamp", "value.qty"]),
                &mut columns,
                &mut work,
                true
            )
            .unwrap(),
            1
        );
        assert_eq!(columns.cell(0, 0), Some(ColumnValue::Utf8("ev-1".into())));
        assert_eq!(columns.cell(1, 0), Some(ColumnValue::Utf8("order".into())));
    }
    #[test]
    fn populates_presence_columns_for_explicit_null_false_and_absent_fields() {
        let fields = [
            field(ArrowType::Utf8),
            field(ArrowType::Utf8),
            field(ArrowType::Bool),
            field(ArrowType::Bool),
            field(ArrowType::Bool),
        ];
        let mut input = vec![0x83];
        str_(&mut input, "id");
        str_(&mut input, "first");
        str_(&mut input, "value.note");
        input.push(0xc0);
        str_(&mut input, "$value$schema.type.flag");
        input.push(0xc2);
        input.push(0x81);
        str_(&mut input, "id");
        str_(&mut input, "second");
        let mut columns = DynamicColumns::new(&fields, 2).expect("column plane fits");
        let mut work = [0; 64];
        let config = config(
            &fields,
            &[
                "id",
                "value.note",
                "event_value_present.value%2Enote",
                "$value$schema.type.flag",
                "event_value_present.%24value%24schema%2Etype%2Eflag",
            ],
        );

        assert_eq!(
            extract(&input, &config, &mut columns, &mut work, true),
            Ok(2)
        );
        assert_eq!(columns.cell(1, 0), None);
        assert_eq!(columns.cell(2, 0), Some(ColumnValue::Bool(true)));
        assert_eq!(columns.cell(2, 1), Some(ColumnValue::Bool(false)));
        assert_eq!(columns.cell(3, 0), Some(ColumnValue::Bool(false)));
        assert_eq!(columns.cell(4, 0), Some(ColumnValue::Bool(true)));
        assert_eq!(columns.cell(4, 1), Some(ColumnValue::Bool(false)));
    }
    #[test]
    fn extract_msgpack_events_accepts_exactly_capacity_batch() {
        // A stream ending exactly at capacity is legal; one event past
        // capacity is rejected.
        let fields = [field(ArrowType::Utf8)];
        let mut input = vec![0x81];
        str_(&mut input, "id");
        str_(&mut input, "x");
        let mut columns = DynamicColumns::new(&fields, 1).expect("column plane fits");
        let mut work = [0; 64];
        assert_eq!(
            extract(
                &input,
                &config(&fields, &["id"]),
                &mut columns,
                &mut work,
                true
            ),
            Ok(1)
        );
        let mut two = input.clone();
        two.extend_from_slice(&input);
        let mut columns2 = DynamicColumns::new(&fields, 1).expect("column plane fits");
        assert_eq!(
            extract(
                &two,
                &config(&fields, &["id"]),
                &mut columns2,
                &mut work,
                true
            ),
            Err(ExtractionError::TooManyEvents)
        );
    }
    #[test]
    fn extract_msgpack_events_supports_schema_width_boundaries() {
        for width in [64, 65, 66, 96] {
            let fields = vec![field(ArrowType::Utf8); width];
            let names = (0..width)
                .map(|index| format!("f{index}"))
                .collect::<Vec<_>>();
            let name_refs = names.iter().map(String::as_str).collect::<Vec<_>>();
            let mut input = vec![0x81];
            str_(&mut input, &names[width - 1]);
            str_(&mut input, "present");
            let mut columns = DynamicColumns::new(&fields, 1).expect("column plane fits");
            let mut work = [0; 64];

            assert_eq!(
                extract(
                    &input,
                    &config(&fields, &name_refs),
                    &mut columns,
                    &mut work,
                    true
                ),
                Ok(1),
                "schema width {width}"
            );
            assert_eq!(
                columns.cell(0, 0),
                None,
                "missing first field at width {width}"
            );
            assert_eq!(
                columns.cell(width - 1, 0),
                Some(ColumnValue::Utf8("present".into())),
                "present last field at width {width}"
            );
        }
    }

    #[test]
    fn extract_msgpack_events_resets_presence_between_rows() {
        let fields = [field(ArrowType::Utf8), field(ArrowType::Utf8)];
        let config = config(&fields, &["id", "value.note"]);
        let mut input = vec![0x82];
        str_(&mut input, "id");
        str_(&mut input, "first");
        str_(&mut input, "value.note");
        str_(&mut input, "present");
        input.push(0x81);
        str_(&mut input, "id");
        str_(&mut input, "second");
        let mut columns = DynamicColumns::new(&fields, 2).expect("column plane fits");
        let mut work = [0; 64];

        assert_eq!(
            extract(&input, &config, &mut columns, &mut work, true),
            Ok(2)
        );
        assert_eq!(
            columns.cell(1, 0),
            Some(ColumnValue::Utf8("present".into()))
        );
        assert_eq!(columns.cell(1, 1), None);
    }

    #[test]
    fn extract_msgpack_events_preserves_last_duplicate_field() {
        let fields = [field(ArrowType::Utf8)];
        let config = config(&fields, &["id"]);
        let mut input = vec![0x82];
        str_(&mut input, "id");
        str_(&mut input, "first");
        str_(&mut input, "id");
        str_(&mut input, "last");
        let mut columns = DynamicColumns::new(&fields, 1).expect("column plane fits");
        let mut work = [0; 64];

        assert_eq!(
            extract(&input, &config, &mut columns, &mut work, true),
            Ok(1)
        );
        assert_eq!(columns.cell(0, 0), Some(ColumnValue::Utf8("last".into())));
    }

    #[test]
    fn extract_msgpack_events_tiny_work_buffer_refuses() {
        // A configured fallback with a work buffer smaller than five bytes is
        // a configuration error: refuse loudly rather than silently dropping
        // undeclared fields.
        let fields = [field(ArrowType::Utf8), field(ArrowType::Binary)];
        let mut input = vec![0x82];
        str_(&mut input, "id");
        str_(&mut input, "x");
        str_(&mut input, "undeclared");
        input.push(7);
        let mut columns = DynamicColumns::new(&fields, 10).expect("column plane fits");
        let mut work = [0; 4];
        assert_eq!(
            extract(
                &input,
                &config(&fields, &["id", crate::UNDECLARED_COLUMN_NAME]),
                &mut columns,
                &mut work,
                true
            ),
            Err(ExtractionError::WorkBufferTooSmall)
        );
    }
    #[test]
    fn extract_msgpack_events_extra_copies_raw_key_value_bytes() {
        let fields = [field(ArrowType::Utf8), field(ArrowType::Binary)];
        let mut input = vec![0x82];
        str_(&mut input, "id");
        str_(&mut input, "x");
        str_(&mut input, "qty");
        input.push(42);
        let mut columns = DynamicColumns::new(&fields, 10).expect("column plane fits");
        let mut work = [0; 128];
        extract(
            &input,
            &config(&fields, &["id", crate::UNDECLARED_COLUMN_NAME]),
            &mut columns,
            &mut work,
            true,
        )
        .unwrap();
        assert_eq!(
            columns.cell(1, 0),
            Some(ColumnValue::Binary(std::borrow::Cow::Owned(vec![
                0xdf, 0, 0, 0, 1, 0xa3, b'q', b't', b'y', 42
            ])))
        );
    }
    #[test]
    fn extract_msgpack_events_escapes_dotted_extra_key() {
        // A literal dot in a captured key re-encodes as %2E so the path-keyed
        // carrier can never misread it as nesting; the re-encoded string gets
        // a fresh MessagePack header because escaping changed its length.
        let fields = [field(ArrowType::Utf8), field(ArrowType::Binary)];
        let mut input = vec![0x82];
        str_(&mut input, "id");
        str_(&mut input, "x");
        str_(&mut input, "a.b");
        input.push(7);
        let mut columns = DynamicColumns::new(&fields, 10).expect("column plane fits");
        let mut work = [0; 128];
        extract(
            &input,
            &config(&fields, &["id", crate::UNDECLARED_COLUMN_NAME]),
            &mut columns,
            &mut work,
            true,
        )
        .unwrap();
        assert_eq!(
            columns.cell(1, 0),
            Some(ColumnValue::Binary(std::borrow::Cow::Owned(vec![
                0xdf, 0, 0, 0, 1, 0xa5, b'a', b'%', b'2', b'E', b'b', 7
            ])))
        );
    }
    #[test]
    fn extract_msgpack_events_descends_a_nested_value_envelope() {
        // The ingest wire shape, msgpack twin of the JSON descent gate.
        let fields = [field(ArrowType::Utf8), field(ArrowType::Utf8)];
        let mut input = vec![0x82];
        str_(&mut input, "id");
        str_(&mut input, "s1");
        str_(&mut input, "value");
        input.push(0x81);
        str_(&mut input, "note");
        str_(&mut input, "hello");
        let mut columns = DynamicColumns::new(&fields, 10).expect("column plane fits");
        let mut work = [0; 128];
        assert_eq!(
            extract(
                &input,
                &config(&fields, &["id", "value.note"]),
                &mut columns,
                &mut work,
                true
            )
            .unwrap(),
            1
        );
        assert_eq!(columns.columns[0].read_variable(0), Some(b"s1".as_slice()));
        assert_eq!(
            columns.columns[1].read_variable(0),
            Some(b"hello".as_slice())
        );
    }
    #[test]
    fn extract_msgpack_events_captures_nested_unknowns_payload_relative() {
        let fields = [field(ArrowType::Utf8), field(ArrowType::Binary)];
        let mut input = vec![0x82];
        str_(&mut input, "id");
        str_(&mut input, "s1");
        str_(&mut input, "value");
        input.push(0x81);
        str_(&mut input, "bogus");
        input.push(7);
        let mut columns = DynamicColumns::new(&fields, 10).expect("column plane fits");
        let mut work = [0; 128];
        assert_eq!(
            extract(
                &input,
                &config(&fields, &["id", crate::UNDECLARED_COLUMN_NAME]),
                &mut columns,
                &mut work,
                true
            )
            .unwrap(),
            1
        );
        assert_eq!(
            columns.cell(1, 0),
            Some(ColumnValue::Binary(std::borrow::Cow::Owned(vec![
                0xdf, 0, 0, 0, 1, 0xa5, b'b', b'o', b'g', b'u', b's', 7
            ])))
        );
    }
    #[test]
    fn extract_msgpack_events_refuses_a_mixed_payload_spelling() {
        // Nested `value` object plus a flat `value.note` key in one event.
        let fields = [field(ArrowType::Utf8), field(ArrowType::Utf8)];
        let mut input = vec![0x82];
        str_(&mut input, "value");
        input.push(0x81);
        str_(&mut input, "note");
        str_(&mut input, "a");
        str_(&mut input, "value.note");
        str_(&mut input, "b");
        let mut columns = DynamicColumns::new(&fields, 10).expect("column plane fits");
        let mut work = [0; 128];
        assert!(
            extract(
                &input,
                &config(&fields, &["id", "value.note"]),
                &mut columns,
                &mut work,
                true
            )
            .is_err()
        );
    }
    #[test]
    fn extract_msgpack_events_non_utf8_key_goes_to_extra() {
        // A non-UTF-8 key is undeclared, not a parse error.
        let fields = [field(ArrowType::Utf8), field(ArrowType::Binary)];
        let mut input = vec![0x82];
        str_(&mut input, "id");
        str_(&mut input, "x");
        input.push(0xa2);
        input.extend([0xff, 0xfe]);
        input.push(1);
        let mut columns = DynamicColumns::new(&fields, 10).expect("column plane fits");
        let mut work = [0; 64];
        assert_eq!(
            extract(
                &input,
                &config(&fields, &["id", crate::UNDECLARED_COLUMN_NAME]),
                &mut columns,
                &mut work,
                true
            )
            .unwrap(),
            1
        );
        assert!(!columns.is_null(1, 0));
    }
    #[test]
    fn extract_msgpack_events_empty_stream() {
        let fields = [field(ArrowType::Utf8)];
        let mut columns = DynamicColumns::new(&fields, 10).expect("column plane fits");
        let mut work = [0; 64];
        assert_eq!(
            extract(
                &[],
                &config(&fields, &["id"]),
                &mut columns,
                &mut work,
                true
            )
            .unwrap(),
            0
        );
    }
    #[test]
    fn extract_msgpack_events_empty_array() {
        let fields = [field(ArrowType::Utf8)];
        let mut columns = DynamicColumns::new(&fields, 10).expect("column plane fits");
        let mut work = [0; 64];
        assert_eq!(
            extract(
                &[0x90],
                &config(&fields, &["id"]),
                &mut columns,
                &mut work,
                false
            )
            .unwrap(),
            0
        );
    }

    //#region smoo/columine!n/vocab-temporal-instant
    /// 2024-01-15T10:30:00Z in seconds since the epoch.
    const BASE_SECONDS: i64 = 19_737 * 86_400 + 37_800;

    /// The one cell of an \`at\` column of \`field\`, from encoded \`value\`.
    fn temporal(
        field: SignalSchemaField,
        value: &[u8],
    ) -> Result<Option<ColumnValue<'static>>, ExtractionError> {
        let fields = [field];
        let mut columns = DynamicColumns::new(&fields, 1).expect("column plane fits");
        extract(
            &one_member("at", value),
            &config(&fields, &["at"]),
            &mut columns,
            &mut [0; 64],
            true,
        )?;
        Ok(columns.cell(0, 0))
    }

    #[test]
    fn an_instant_is_stored_in_the_timestamp_unit() {
        let instant = text("2024-01-15T10:30:00.123Z");
        assert_eq!(
            temporal(
                SignalSchemaField::new_timestamp(TimeUnit::Nanosecond, true),
                &instant
            ),
            Ok(Some(ColumnValue::Int(
                BASE_SECONDS * 1_000_000_000 + 123_000_000
            )))
        );
        assert_eq!(
            temporal(
                SignalSchemaField::new_timestamp(TimeUnit::Millisecond, true),
                &instant
            ),
            Ok(Some(ColumnValue::Int(BASE_SECONDS * 1_000 + 123)))
        );
        assert_eq!(
            temporal(
                SignalSchemaField::new_timestamp(TimeUnit::Second, true),
                &text("2024-01-15T10:30:00Z")
            ),
            Ok(Some(ColumnValue::Int(BASE_SECONDS)))
        );
        let mut integer = vec![0xd3];
        integer.extend(1_705_314_600_i64.to_be_bytes());
        assert_eq!(
            temporal(
                SignalSchemaField::new_timestamp(TimeUnit::Second, true),
                &integer
            ),
            Ok(Some(ColumnValue::Int(1_705_314_600)))
        );
    }

    #[test]
    fn an_instant_finer_than_the_timestamp_unit_is_refused() {
        assert_eq!(
            temporal(
                SignalSchemaField::new_timestamp(TimeUnit::Millisecond, true),
                &text("2024-01-15T10:30:00.123456789Z")
            ),
            Err(ExtractionError::InvalidFieldType)
        );
    }

    #[test]
    fn an_int64_column_refuses_an_instant() {
        assert_eq!(
            temporal(field(ArrowType::Int64), &text("2024-01-15T10:30:00Z")),
            Err(ExtractionError::InvalidFieldType)
        );
    }
    //#endregion smoo/columine!n/vocab-temporal-instant

    //#region smoo/columine!n/vocab-temporal-planes
    #[test]
    fn a_duration_column_takes_an_integer_and_refuses_an_instant() {
        let duration = SignalSchemaField::new_duration(TimeUnit::Microsecond, true);
        assert_eq!(
            temporal(duration, &[0xd1, 0xfa, 0x24]),
            Ok(Some(ColumnValue::Int(-1_500)))
        );
        assert_eq!(
            temporal(duration, &text("2024-01-15T10:30:00Z")),
            Err(ExtractionError::InvalidFieldType)
        );
    }
    //#endregion smoo/columine!n/vocab-temporal-planes

    //#region smoo/columine!n/vocab-enum-dictionary
    #[test]
    fn an_enum_text_stores_its_index_and_an_unknown_one_is_refused() {
        let fields = [encoded(DictionarySlot::Schema(0))];
        let dictionaries = [SchemaDictionary {
            slot: DictionarySlot::Schema(0),
            arrow_ids: vec![0],
            values: vec![Some("achieved".into()), None, Some("stuck".into())],
        }];
        let config = crate::build_extraction_config_with_semantic_schemas(
            &fields,
            &["outcome"],
            &dictionaries,
            None,
            None,
            crate::CellLayout::default(),
        )
        .unwrap();
        let mut input = one_member("outcome", &text("stuck"));
        input.extend(one_member("outcome", &[0xc0]));
        let mut columns = DynamicColumns::new(&fields, 4).expect("column plane fits");
        assert_eq!(
            extract(&input, &config, &mut columns, &mut [0; 64], true),
            Ok(2)
        );
        assert_eq!(columns.cell(0, 0), Some(ColumnValue::UInt(2)));
        assert_eq!(columns.cell(0, 1), None);
        for refused in [
            text("abandoned"),
            vec![0x01],
            // A str that is not UTF-8 never reaches the dictionary.
            vec![0xa2, 0xff, 0xfe],
        ] {
            let mut columns = DynamicColumns::new(&fields, 1).expect("column plane fits");
            assert_eq!(
                extract(
                    &one_member("outcome", &refused),
                    &config,
                    &mut columns,
                    &mut [0; 64],
                    true
                ),
                Err(ExtractionError::InvalidFieldType),
                "{refused:?}"
            );
        }
    }
    //#endregion smoo/columine!n/vocab-enum-dictionary

    //#region smoo/columine!n/vocab-stream-dictionary
    #[test]
    fn a_stream_dictionary_column_stores_the_image_ordinals() {
        let fields = [encoded(DictionarySlot::Stream)];
        let config = config(&fields, &["state"]);
        let mut input = Vec::new();
        for state in ["open", "closed", "open"] {
            input.extend(one_member("state", &text(state)));
        }
        let mut columns = DynamicColumns::new(&fields, 4).expect("column plane fits");
        let mut image = ListImage::new(8, 32);
        assert_eq!(
            extract_msgpack_events(
                &input,
                &config,
                &mut columns,
                &mut [0; 64],
                true,
                &mut image
            ),
            Ok(3)
        );
        assert_eq!(columns.cell(0, 0), Some(ColumnValue::UInt(0)));
        assert_eq!(columns.cell(0, 1), Some(ColumnValue::UInt(1)));
        assert_eq!(columns.cell(0, 2), Some(ColumnValue::UInt(0)));
        assert_eq!(image.len(), 2);
        assert_eq!(image.value(1), Some("closed"));
    }
    //#endregion smoo/columine!n/vocab-stream-dictionary

    //#region smoo/columine!n/vocab-image-limit
    #[test]
    fn a_value_past_the_image_limit_is_a_category_refusal() {
        let fields = [encoded(DictionarySlot::Stream)];
        let config = config(&fields, &["state"]);
        let mut input = one_member("state", &text("open"));
        input.extend(one_member("state", &text("closed")));
        let mut columns = DynamicColumns::new(&fields, 4).expect("column plane fits");
        assert_eq!(
            extract_msgpack_events(
                &input,
                &config,
                &mut columns,
                &mut [0; 64],
                true,
                &mut ListImage::new(1, 32),
            ),
            Err(ExtractionError::Category(CategoryRefusal::Full {
                limit: 1
            }))
        );
        assert_eq!(columns.count(), 1);
    }
    //#endregion smoo/columine!n/vocab-image-limit
}

#[cfg(test)]
mod bin_unwrap_pin {
    //! f33e06007: a declared-Binary value carried as a standard msgpack bin
    //! (0xc4/c5/c6) stores the bin PAYLOAD, not the wrapped token — wrapping
    //! would double-encode internal typed persistence and corrupt raw bytes.
    use crate::msgpack_scanner::Reader;

    #[test]
    fn read_bin_unwraps_all_three_headers() {
        let mut input = vec![0xc4, 3, 1, 2, 3];
        assert_eq!(Reader::new(&input).read_bin(), Some(&[1u8, 2, 3][..]));
        input = vec![0xc5, 0, 3, 4, 5, 6];
        assert_eq!(Reader::new(&input).read_bin(), Some(&[4u8, 5, 6][..]));
        input = vec![0xc6, 0, 0, 0, 2, 7, 8];
        assert_eq!(Reader::new(&input).read_bin(), Some(&[7u8, 8][..]));
        assert_eq!(Reader::new(&[0xa1, b'x']).read_bin(), None);
        assert_eq!(Reader::new(&[0xc4, 5, 1]).read_bin(), None);
    }
}
