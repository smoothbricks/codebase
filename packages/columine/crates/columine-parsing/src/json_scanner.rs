//! Replaces `packages/columine/src/parsing/json_scanner.zig`.

use crate::{
    EventColumns, ParseError,
    json_parser::{JsonParser, ParserError, Token},
};

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JsonScannerError {
    InvalidJson,
    MissingField,
    InvalidFieldType,
    TooManyEvents,
    BufferOverflow,
}

impl From<JsonScannerError> for ParseError {
    fn from(value: JsonScannerError) -> Self {
        match value {
            JsonScannerError::InvalidJson => Self::InvalidJson,
            JsonScannerError::MissingField => Self::MissingField,
            JsonScannerError::InvalidFieldType => Self::InvalidFieldType,
            JsonScannerError::TooManyEvents => Self::TooManyEvents,
            JsonScannerError::BufferOverflow => Self::BufferOverflow,
        }
    }
}

/// Parses a JSON event array into the Stage-4B event-column boundary.
pub fn parse_json_events(input: &[u8], output: &mut EventColumns) -> Result<(), JsonScannerError> {
    let mut parser = JsonParser::new(input);
    parser.expect_array_begin().map_err(json_error)?;
    while !parser.is_array_end() {
        parse_event_object(&mut parser, output)?;
    }
    match parser.next_token().map_err(json_error)? {
        Token::ArrayEnd => Ok(()),
        _ => Err(JsonScannerError::InvalidJson),
    }
}

fn parse_event_object(
    parser: &mut JsonParser<'_>,
    output: &mut EventColumns,
) -> Result<(), JsonScannerError> {
    parser.expect_object_begin().map_err(json_error)?;
    let mut id = None;
    let mut event_type = None;
    let mut timestamp_micros = None;
    let mut value = None;
    while !parser.is_object_end() {
        let field = parser.expect_field_name().map_err(json_error)?;
        match field.as_str() {
            "id" => {
                id = Some(
                    parser
                        .expect_string()
                        .map_err(|_| JsonScannerError::InvalidFieldType)?,
                )
            }
            "type" => {
                event_type = Some(
                    parser
                        .expect_string()
                        .map_err(|_| JsonScannerError::InvalidFieldType)?,
                )
            }
            "timestamp" => timestamp_micros = Some(parse_timestamp_token(parser)?),
            "value" => {
                let token = parser.next_spanned().map_err(json_error)?;
                let start = token.start;
                let end = parser.skip_value_from(token).map_err(json_error)?;
                value = Some(parser.input()[start..end].to_vec());
            }
            _ => parser.skip_value().map_err(json_error)?,
        }
    }
    match parser.next_token().map_err(json_error)? {
        Token::ObjectEnd => {}
        _ => return Err(JsonScannerError::InvalidJson),
    }
    output
        .add_event(
            id.ok_or(JsonScannerError::MissingField)?.as_bytes(),
            event_type.ok_or(JsonScannerError::MissingField)?.as_bytes(),
            timestamp_micros.ok_or(JsonScannerError::MissingField)?,
            value.as_deref(),
        )
        .map_err(|error| match error {
            ParseError::TooManyEvents => JsonScannerError::TooManyEvents,
            ParseError::BufferOverflow => JsonScannerError::BufferOverflow,
            _ => JsonScannerError::InvalidJson,
        })
}

fn parse_timestamp_token(parser: &mut JsonParser<'_>) -> Result<i64, JsonScannerError> {
    match parser.next_token().map_err(json_error)? {
        Token::String(value) => {
            parse_iso8601_to_micros(&value).map_err(|_| JsonScannerError::InvalidFieldType)
        }
        Token::Number(value) => value
            .parse::<i64>()
            .map_err(|_| JsonScannerError::InvalidFieldType)
            .and_then(|milliseconds| {
                milliseconds
                    .checked_mul(1_000)
                    .ok_or(JsonScannerError::InvalidFieldType)
            }),
        _ => Err(JsonScannerError::InvalidFieldType),
    }
}
fn json_error(_: ParserError) -> JsonScannerError {
    JsonScannerError::InvalidJson
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TimestampError {
    InvalidFormat,
}

/// Scalar ISO-8601 conversion matching Zig's UTC, millisecond-only contract.
pub fn parse_iso8601_to_micros(value: &str) -> Result<i64, TimestampError> {
    let bytes = value.as_bytes();
    if bytes.len() < 20 || bytes.last() != Some(&b'Z') {
        return Err(TimestampError::InvalidFormat);
    }
    let year = zig_int(&bytes[0..4])? as i32;
    if bytes.get(4) != Some(&b'-') {
        return Err(TimestampError::InvalidFormat);
    }
    let month = zig_int(&bytes[5..7])? as u32;
    if bytes.get(7) != Some(&b'-') {
        return Err(TimestampError::InvalidFormat);
    }
    let day = zig_int(&bytes[8..10])? as u32;
    if bytes.get(10) != Some(&b'T') {
        return Err(TimestampError::InvalidFormat);
    }
    let hour = zig_int(&bytes[11..13])? as u32;
    if bytes.get(13) != Some(&b':') {
        return Err(TimestampError::InvalidFormat);
    }
    let minute = zig_int(&bytes[14..16])? as u32;
    if bytes.get(16) != Some(&b':') {
        return Err(TimestampError::InvalidFormat);
    }
    let second = zig_int(&bytes[17..19])? as u32;
    if !(1970..=2099).contains(&year)
        || !(1..=12).contains(&month)
        || !(1..=31).contains(&day)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return Err(TimestampError::InvalidFormat);
    }
    // Zig parses the fraction with `parseInt(...) catch 0`: a garbage
    // fraction yields 0 milliseconds, it never rejects the timestamp.
    let millis = if bytes.len() > 20 && bytes[19] == b'.' {
        let fraction = &bytes[20..bytes.len() - 1];
        if fraction.len() >= 3 {
            zig_int(&fraction[..3]).unwrap_or(0)
        } else if fraction.is_empty() {
            0
        } else {
            // Zig right-pads to three digits ("1" -> "100") before parsing.
            let mut padded = [b'0'; 3];
            padded[..fraction.len()].copy_from_slice(fraction);
            zig_int(&padded).unwrap_or(0)
        }
    } else {
        0
    };
    let seconds = epoch_days(year, month, day) * 86_400
        + i64::from(hour) * 3_600
        + i64::from(minute) * 60
        + i64::from(second);
    Ok(seconds * 1_000_000 + millis * 1_000)
}
/// `std.fmt.parseInt` semantics for the slice widths used here: an optional
/// leading '+' is accepted ("2024-+1-15T..." parses month 1 in Zig). The Zig
/// year field is parseInt(i32) and also accepts '-', but any sign-bearing
/// 4-char year falls outside the 1970..=2099 range check, so rejecting '-'
/// here produces the identical InvalidFormat outcome.
fn zig_int(bytes: &[u8]) -> Result<i64, TimestampError> {
    let digits = match bytes.first() {
        Some(b'+') => &bytes[1..],
        _ => bytes,
    };
    if digits.is_empty() || !digits.iter().all(u8::is_ascii_digit) {
        return Err(TimestampError::InvalidFormat);
    }
    digits.iter().try_fold(0_i64, |total, digit| {
        total
            .checked_mul(10)
            .and_then(|n| n.checked_add(i64::from(*digit - b'0')))
            .ok_or(TimestampError::InvalidFormat)
    })
}
fn epoch_days(year: i32, month: u32, day: u32) -> i64 {
    let mut y = i64::from(year);
    let mut m = i64::from(month);
    if m <= 2 {
        y -= 1;
        m += 12;
    }
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (m - 3) + 2) / 5 + i64::from(day) - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;
    fn columns() -> EventColumns {
        EventColumns::new(10)
    }
    #[test]
    fn parse_iso8601_to_micros_full_format_with_millis() {
        assert_eq!(
            parse_iso8601_to_micros("2024-01-15T10:30:00.123Z").unwrap(),
            19_737 * 86_400 * 1_000_000 + 37_800 * 1_000_000 + 123_000
        );
    }
    #[test]
    fn parse_iso8601_to_micros_no_milliseconds() {
        assert_eq!(
            parse_iso8601_to_micros("2024-01-15T10:30:00Z").unwrap(),
            19_737 * 86_400 * 1_000_000 + 37_800 * 1_000_000
        );
    }
    #[test]
    fn parse_iso8601_to_micros_epoch() {
        assert_eq!(parse_iso8601_to_micros("1970-01-01T00:00:00Z").unwrap(), 0);
    }
    #[test]
    fn parse_iso8601_to_micros_rejects_no_z_suffix() {
        assert_eq!(
            parse_iso8601_to_micros("2024-01-15T10:30:00"),
            Err(TimestampError::InvalidFormat)
        );
    }
    #[test]
    fn parse_iso8601_to_micros_rejects_invalid_year() {
        assert_eq!(
            parse_iso8601_to_micros("1900-01-15T10:30:00Z"),
            Err(TimestampError::InvalidFormat)
        );
    }
    #[test]
    fn parse_iso8601_to_micros_single_digit_millis() {
        assert_eq!(
            parse_iso8601_to_micros("2024-01-15T10:30:00.1Z").unwrap() % 1_000_000,
            100_000
        );
    }
    #[test]
    fn parse_iso8601_to_micros_two_digit_millis() {
        assert_eq!(
            parse_iso8601_to_micros("2024-01-15T10:30:00.12Z").unwrap() % 1_000_000,
            120_000
        );
    }
    #[test]
    fn parse_iso8601_to_micros_garbage_fraction_yields_zero_millis() {
        // Zig: `parseInt(frac) catch 0` — a non-numeric fraction is 0 ms,
        // never a rejection.
        let base = parse_iso8601_to_micros("2024-01-15T10:30:00Z").unwrap();
        assert_eq!(
            parse_iso8601_to_micros("2024-01-15T10:30:00.abcZ").unwrap(),
            base
        );
        assert_eq!(
            parse_iso8601_to_micros("2024-01-15T10:30:00.1x3Z").unwrap(),
            base
        );
        // Extra fraction digits beyond three are ignored (first three win).
        assert_eq!(
            parse_iso8601_to_micros("2024-01-15T10:30:00.123456Z").unwrap(),
            base + 123_000
        );
    }
    #[test]
    fn parse_json_events_single_event_with_iso_timestamp() {
        let mut c = columns();
        parse_json_events(br#"[{"id":"abc-123","type":"orderPlaced","timestamp":"2024-01-15T10:30:00.000Z","value":{"qty":5}}]"#, &mut c).unwrap();
        assert_eq!(c.count, 1);
        assert_eq!(crate::parsed_event(&c, 0).unwrap().id, "abc-123");
        assert_eq!(
            crate::parsed_event(&c, 0).unwrap().event_type,
            "orderPlaced"
        );
        assert!(crate::parsed_event(&c, 0).unwrap().value.is_some());
    }
    #[test]
    fn parse_json_events_numeric_timestamp() {
        let mut c = columns();
        parse_json_events(
            br#"[{"id":"id-1","type":"test","timestamp":1705315800000}]"#,
            &mut c,
        )
        .unwrap();
        assert_eq!(
            crate::parsed_event(&c, 0).unwrap().timestamp_micros,
            1_705_315_800_000_000
        );
    }
    #[test]
    fn parse_json_events_event_without_value() {
        let mut c = columns();
        parse_json_events(
            br#"[{"id":"id-1","type":"test","timestamp":"1970-01-01T00:00:00Z"}]"#,
            &mut c,
        )
        .unwrap();
        assert!(crate::parsed_event(&c, 0).unwrap().value.is_none());
    }
    #[test]
    fn parse_json_events_multiple_events() {
        let mut c = columns();
        parse_json_events(br#"[{"id":"id-1","type":"a","timestamp":"1970-01-01T00:00:00Z","value":1},{"id":"id-2","type":"b","timestamp":"1970-01-01T00:00:01Z"},{"id":"id-3","type":"c","timestamp":"1970-01-01T00:00:02Z","value":"str"}]"#, &mut c).unwrap();
        assert_eq!(c.count, 3);
        assert!(crate::parsed_event(&c, 0).unwrap().value.is_some());
        assert!(crate::parsed_event(&c, 1).unwrap().value.is_none());
        assert!(crate::parsed_event(&c, 2).unwrap().value.is_some());
    }
    #[test]
    fn parse_json_events_invalid_json_returns_error() {
        assert_eq!(
            parse_json_events(b"{not valid json", &mut columns()),
            Err(JsonScannerError::InvalidJson)
        );
    }
    #[test]
    fn parse_json_events_missing_required_field_returns_error() {
        assert_eq!(
            parse_json_events(
                br#"[{"id":"id-1","timestamp":"1970-01-01T00:00:00Z"}]"#,
                &mut columns()
            ),
            Err(JsonScannerError::MissingField)
        );
    }
    #[test]
    fn parse_json_events_nested_value_preserved_as_raw_json() {
        let mut c = columns();
        parse_json_events(br#"[{"id":"id-1","type":"test","timestamp":"1970-01-01T00:00:00Z","value":{"nested":{"deep":true}}}]"#, &mut c).unwrap();
        assert_eq!(
            crate::parsed_event(&c, 0).unwrap().value.as_deref(),
            Some(br#"{"nested":{"deep":true}}"#.as_slice())
        );
    }
    #[test]
    fn parse_json_events_empty_array() {
        let mut c = columns();
        parse_json_events(b"[]", &mut c).unwrap();
        assert_eq!(c.count, 0);
    }
}
