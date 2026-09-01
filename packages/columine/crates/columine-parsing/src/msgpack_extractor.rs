//! MessagePack event extraction into typed columns.

use crate::msgpack_scanner::Reader;
use crate::validate::encode_path_segment;
use crate::{
    ColumnValue, DynamicColumns, ExtractionConfig, ParseError, PlaneKind, SignalSchemaField,
    json_extractor::ExtractionError, json_scanner::parse_iso8601_to_micros,
};

/// Extracts either a concatenated MessagePack map stream or an array of maps.
pub fn extract_msgpack_events(
    input: &[u8],
    config: &ExtractionConfig,
    columns: &mut DynamicColumns,
    work_buffer: &mut [u8],
    stream: bool,
) -> Result<usize, ExtractionError> {
    if input.is_empty() {
        return Ok(0);
    }
    let mut reader = Reader::new(input);
    let mut count = 0;
    // An exactly-capacity stream is legal; reject only when more events
    // follow a full batch.
    if stream {
        while !reader.at_end() {
            extract_msgpack_event(&mut reader, config, columns, work_buffer)?;
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
            extract_msgpack_event(&mut reader, config, columns, work_buffer)?;
            count += 1;
        }
    }
    Ok(count)
}

fn extract_msgpack_event(
    reader: &mut Reader<'_>,
    config: &ExtractionConfig,
    columns: &mut DynamicColumns,
    work_buffer: &mut [u8],
) -> Result<(), ExtractionError> {
    // Parse the map header before opening a row, so an invalid header never
    // leaves a partially initialized row.
    let fields = reader
        .read_map_header()
        .ok_or(ExtractionError::InvalidJson)?;
    if !columns.begin_row() {
        return Err(ExtractionError::TooManyEvents);
    }
    let result = extract_msgpack_fields(reader, fields, config, columns, work_buffer);
    if result.is_err() {
        // Abandon the row on error; failed extraction must not commit it.
        columns.abandon_row();
    }
    result
}

fn extract_msgpack_fields(
    reader: &mut Reader<'_>,
    fields: u32,
    config: &ExtractionConfig,
    columns: &mut DynamicColumns,
    work_buffer: &mut [u8],
) -> Result<(), ExtractionError> {
    // Presence is schema-sized state owned by DynamicColumns and allocated
    // once with the column storage. Resetting it per row avoids both the old
    // 64-field ceiling and any per-event allocation.
    columns.columns_seen.fill(false);
    // A fallback column with an unusably small work buffer (< 5 bytes, where
    // even the MessagePack header cannot fit) is a configuration error. Refuse
    // rather than silently dropping undeclared fields.
    if config.fallback_column.is_some() && work_buffer.len() < 5 {
        return Err(ExtractionError::OutOfMemory);
    }
    let extra_active = config.fallback_column.is_some();
    let mut state = ExtraState {
        count: 0,
        end: 0,
        active: extra_active,
    };
    let mut saw_nested_value = false;
    let mut saw_flat_value_key = false;
    let mut column_name = String::new();
    for _ in 0..fields {
        let key_start = reader.position();
        let key = reader.read_string().ok_or(ExtractionError::InvalidJson)?;
        let key_end = reader.position();
        if saw_nested_value && key.starts_with(b"value.")
            || saw_flat_value_key && key == b"value" && config.has_value_fields
        {
            // One spelling per event: the validator judges the nested form,
            // and a second spelling in the same event would let the judged
            // bytes and the stored bytes diverge.
            return Err(ExtractionError::InvalidJson);
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
            let members = reader
                .read_map_header()
                .ok_or(ExtractionError::InvalidJson)?;
            for _ in 0..members {
                let member_start = reader.position();
                let member = reader.read_string().ok_or(ExtractionError::InvalidJson)?;
                let member_end = reader.position();
                let lookup_name = match std::str::from_utf8(member) {
                    Ok(name) => {
                        column_name.clear();
                        column_name.push_str("value.");
                        column_name.push_str(name);
                        Some(column_name.as_str())
                    }
                    // A non-UTF-8 member is undeclared by construction.
                    Err(_) => None,
                };
                extract_msgpack_member(
                    reader,
                    config,
                    columns,
                    work_buffer,
                    &mut state,
                    lookup_name,
                    member,
                    member_start,
                    member_end,
                )?;
            }
            continue;
        }
        let lookup_name = std::str::from_utf8(key).ok();
        extract_msgpack_member(
            reader,
            config,
            columns,
            work_buffer,
            &mut state,
            lookup_name,
            key,
            key_start,
            key_end,
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
    if let Some(column) = config.fallback_column {
        if extra_count == 0 {
            append(columns, column, None)?;
        } else {
            work_buffer[0] = 0xdf;
            work_buffer[1..5].copy_from_slice(&extra_count.to_be_bytes());
            append(
                columns,
                column,
                Some(ColumnValue::Binary(work_buffer[..extra_end].to_vec())),
            )?;
        }
    }
    columns.end_row();
    Ok(())
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

/// Write one MessagePack-encoded string at `at`, returning the position past
/// it. The caller has already bounds-checked header + payload.
fn write_msgpack_str(buffer: &mut [u8], at: usize, value: &str) -> usize {
    let bytes = value.as_bytes();
    let mut cursor = at;
    match bytes.len() {
        len @ 0..=31 => {
            buffer[cursor] = 0xa0 | len as u8;
            cursor += 1;
        }
        len @ 32..=255 => {
            buffer[cursor] = 0xd9;
            buffer[cursor + 1] = len as u8;
            cursor += 2;
        }
        len @ 256..=65535 => {
            buffer[cursor] = 0xda;
            buffer[cursor + 1..cursor + 3].copy_from_slice(&(len as u16).to_be_bytes());
            cursor += 3;
        }
        len => {
            buffer[cursor] = 0xdb;
            buffer[cursor + 1..cursor + 5].copy_from_slice(&(len as u32).to_be_bytes());
            cursor += 5;
        }
    }
    buffer[cursor..cursor + bytes.len()].copy_from_slice(bytes);
    cursor + bytes.len()
}

/// Bounded carrier accumulation state threaded through one row's members.
struct ExtraState {
    count: u32,
    end: usize,
    active: bool,
}

/// Handle one named member of the event or its nested payload, the msgpack
/// twin of the JSON extractor's `extract_named_member`: a declared column
/// extracts typed, an unknown member captures raw key/value slices into the
/// bounded carrier (path-keyed, `%`/`.` re-encoded via `encode_path_segment`),
/// otherwise the value is skipped. A member named after the carrier column
/// routes to capture, never to the carrier's own cell.
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
    lookup_name: Option<&str>,
    key: &[u8],
    key_start: usize,
    key_end: usize,
) -> Result<(), ExtractionError> {
    let lookup = lookup_name
        .and_then(|name| config.lookup_field(name))
        .filter(|lookup| Some(lookup.column) != config.fallback_column);
    if let Some(lookup) = lookup {
        extract_typed_value(reader, lookup.field, columns, lookup.column)?;
        columns.columns_seen[lookup.column] = true;
        return Ok(());
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
        .map(encode_path_segment);
    let key_len = match &escaped {
        Some(name) => msgpack_str_header_len(name.len()) + name.len(),
        None => raw_key.len(),
    };
    let end = state
        .end
        .checked_add(key_len + raw_value.len())
        .ok_or(ExtractionError::BufferOverflow)?;
    if end > work_buffer.len() {
        return Err(ExtractionError::BufferOverflow);
    }
    let value_at = match &escaped {
        Some(name) => write_msgpack_str(work_buffer, state.end, name),
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
/// that prove an integer is an integer, so nothing here truncates a float.
fn extract_typed_value(
    reader: &mut Reader<'_>,
    field: SignalSchemaField,
    columns: &mut DynamicColumns,
    column: usize,
) -> Result<(), ExtractionError> {
    let first = *reader
        .input()
        .get(reader.position())
        .ok_or(ExtractionError::InvalidJson)?;
    let kind = field.plane_kind();
    let value = match kind {
        PlaneKind::Text { .. } => {
            if first == 0xc0 {
                reader.skip_value();
                None
            } else {
                Some(ColumnValue::Utf8(
                    std::str::from_utf8(
                        reader
                            .read_string()
                            .ok_or(ExtractionError::InvalidFieldType)?,
                    )
                    .map_err(|_| ExtractionError::InvalidFieldType)?
                    .to_owned(),
                ))
            }
        }
        // Integer planes take integers only, range-checked to the plane's
        // width. The eight-byte signed plane additionally takes
        // bigint-as-string and ISO-8601 instants, because a 64-bit value does
        // not survive a JSON number and the cross-format contract keeps the
        // two extractors interchangeable; the eight-byte unsigned plane takes
        // the string form for the same reason.
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
            } else if width == 8 && Reader::is_string(first) {
                let text = std::str::from_utf8(
                    reader
                        .read_string()
                        .ok_or(ExtractionError::InvalidFieldType)?,
                )
                .map_err(|_| ExtractionError::InvalidFieldType)?;
                let micros = match text.parse::<i64>() {
                    Ok(value) => value,
                    Err(_) => parse_iso8601_to_micros(text)
                        .map_err(|_| ExtractionError::InvalidFieldType)?,
                };
                Some(ColumnValue::Int(micros))
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
                Some(ColumnValue::FixedBytes(payload.to_vec()))
            } else {
                return Err(ExtractionError::InvalidFieldType);
            }
        }
        PlaneKind::Bytes { .. } => {
            if first == 0xc0 {
                reader.skip_value();
                None
            } else if matches!(first, 0xc4..=0xc6) {
                // Binary values are stored as the payload of their canonical
                // MessagePack bin representation. Keeping the payload avoids a
                // second decode after Arrow materialization and preserves raw
                // bytes.
                let payload = reader.read_bin().ok_or(ExtractionError::InvalidJson)?;
                Some(ColumnValue::Binary(payload.to_vec()))
            } else {
                // External standard MessagePack may provide a structured value
                // directly. Preserve its exact document bytes for normal
                // Binary materialization.
                let start = reader.position();
                reader.skip_value().ok_or(ExtractionError::InvalidJson)?;
                Some(ColumnValue::Binary(
                    reader.input()[start..reader.position()].to_vec(),
                ))
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
    value: Option<ColumnValue>,
) -> Result<(), ExtractionError> {
    crate::append_cell(columns, column, value).map_err(|error| match error {
        ParseError::BufferOverflow => ExtractionError::BufferOverflow,
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ArrowType, CellExt, SignalSchemaField, build_extraction_config};
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

    /// A JavaScript number above u32 arrives as float64 under standard
    /// MessagePack (the 64-bit integer markers are reserved for BigInt), and
    /// every microsecond timestamp is such a number.
    #[test]
    fn an_integral_float_lands_in_an_integer_plane_and_a_fraction_does_not() {
        let fields = [field(ArrowType::Utf8), field(ArrowType::Int64)];
        let mut input = vec![0x82];
        str_(&mut input, "id");
        str_(&mut input, "ev-1");
        str_(&mut input, "timestamp");
        input.push(0xcb);
        input.extend(4_294_967_296.0_f64.to_be_bytes());
        let mut columns = DynamicColumns::new(&fields, 2);
        let mut work = [0; 64];
        let config = config(&fields, &["id", "timestamp"]);
        assert_eq!(
            extract_msgpack_events(&input, &config, &mut columns, &mut work, true),
            Ok(1)
        );
        assert_eq!(columns.cell(1, 0), Some(ColumnValue::Int(4_294_967_296)));

        let mut fraction = vec![0x82];
        str_(&mut fraction, "id");
        str_(&mut fraction, "ev-2");
        str_(&mut fraction, "timestamp");
        fraction.push(0xcb);
        fraction.extend(1.5_f64.to_be_bytes());
        let mut columns = DynamicColumns::new(&fields, 2);
        assert_eq!(
            extract_msgpack_events(&fraction, &config, &mut columns, &mut work, true),
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
        let mut columns = DynamicColumns::new(&fields, 10);
        let mut work = [0; 1024];
        assert_eq!(
            extract_msgpack_events(
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
        let mut columns = DynamicColumns::new(&fields, 2);
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
            extract_msgpack_events(&input, &config, &mut columns, &mut work, true),
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
        let mut columns = DynamicColumns::new(&fields, 1);
        let mut work = [0; 64];
        assert_eq!(
            extract_msgpack_events(
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
        let mut columns2 = DynamicColumns::new(&fields, 1);
        assert_eq!(
            extract_msgpack_events(
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
            let mut columns = DynamicColumns::new(&fields, 1);
            let mut work = [0; 64];

            assert_eq!(
                extract_msgpack_events(
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
        let mut columns = DynamicColumns::new(&fields, 2);
        let mut work = [0; 64];

        assert_eq!(
            extract_msgpack_events(&input, &config, &mut columns, &mut work, true),
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
        let mut columns = DynamicColumns::new(&fields, 1);
        let mut work = [0; 64];

        assert_eq!(
            extract_msgpack_events(&input, &config, &mut columns, &mut work, true),
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
        let mut columns = DynamicColumns::new(&fields, 10);
        let mut work = [0; 4];
        assert_eq!(
            extract_msgpack_events(
                &input,
                &config(&fields, &["id", crate::UNDECLARED_COLUMN_NAME]),
                &mut columns,
                &mut work,
                true
            ),
            Err(ExtractionError::OutOfMemory)
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
        let mut columns = DynamicColumns::new(&fields, 10);
        let mut work = [0; 128];
        extract_msgpack_events(
            &input,
            &config(&fields, &["id", crate::UNDECLARED_COLUMN_NAME]),
            &mut columns,
            &mut work,
            true,
        )
        .unwrap();
        assert_eq!(
            columns.cell(1, 0),
            Some(ColumnValue::Binary(vec![
                0xdf, 0, 0, 0, 1, 0xa3, b'q', b't', b'y', 42
            ]))
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
        let mut columns = DynamicColumns::new(&fields, 10);
        let mut work = [0; 128];
        extract_msgpack_events(
            &input,
            &config(&fields, &["id", crate::UNDECLARED_COLUMN_NAME]),
            &mut columns,
            &mut work,
            true,
        )
        .unwrap();
        assert_eq!(
            columns.cell(1, 0),
            Some(ColumnValue::Binary(vec![
                0xdf, 0, 0, 0, 1, 0xa5, b'a', b'%', b'2', b'E', b'b', 7
            ]))
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
        let mut columns = DynamicColumns::new(&fields, 10);
        let mut work = [0; 128];
        assert_eq!(
            extract_msgpack_events(
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
        let mut columns = DynamicColumns::new(&fields, 10);
        let mut work = [0; 128];
        assert_eq!(
            extract_msgpack_events(
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
            Some(ColumnValue::Binary(vec![
                0xdf, 0, 0, 0, 1, 0xa5, b'b', b'o', b'g', b'u', b's', 7
            ]))
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
        let mut columns = DynamicColumns::new(&fields, 10);
        let mut work = [0; 128];
        assert!(
            extract_msgpack_events(
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
        let mut columns = DynamicColumns::new(&fields, 10);
        let mut work = [0; 64];
        assert_eq!(
            extract_msgpack_events(
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
        let mut columns = DynamicColumns::new(&fields, 10);
        let mut work = [0; 64];
        assert_eq!(
            extract_msgpack_events(
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
        let mut columns = DynamicColumns::new(&fields, 10);
        let mut work = [0; 64];
        assert_eq!(
            extract_msgpack_events(
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
