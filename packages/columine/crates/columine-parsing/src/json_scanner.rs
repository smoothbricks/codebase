//! Streaming JSON scanner for the base event log.

use crate::instant::parse_instant;
use crate::{
    DynamicColumns, ParseError,
    json_parser::{JsonParser, ParserError, Token},
};
use columine_arrow::TimeUnit;

/// Parses a JSON event array into the base event-log columns
/// (`columine_arrow::BASE_EVENT_LOG_FIELDS`: id, type, timestamp, value).
pub fn parse_json_events(input: &[u8], output: &mut DynamicColumns) -> Result<(), ParseError> {
    let mut parser = JsonParser::new(input);
    // Decoded id and type of an event that spells them with escapes; plain
    // strings are stored straight from the input.
    let mut scratch = (Vec::new(), Vec::new());
    parser.expect_array_begin().map_err(json_error)?;
    while !parser.is_array_end() {
        parse_event_object(&mut parser, output, &mut scratch)?;
    }
    match parser.next_token().map_err(json_error)? {
        Token::ArrayEnd => Ok(()),
        _ => Err(ParseError::InvalidJson),
    }
}

fn parse_event_object(
    parser: &mut JsonParser<'_>,
    output: &mut DynamicColumns,
    (id_scratch, type_scratch): &mut (Vec<u8>, Vec<u8>),
) -> Result<(), ParseError> {
    parser.expect_object_begin().map_err(json_error)?;
    let mut id = None;
    let mut event_type = None;
    let mut timestamp_nanos = None;
    let mut value = None;
    while !parser.is_object_end() {
        let field = parser.expect_field_name().map_err(json_error)?;
        match field {
            _ if field.is("id") => {
                id = Some(
                    parser
                        .expect_string()
                        .map_err(|_| ParseError::InvalidFieldType)?,
                )
            }
            _ if field.is("type") => {
                event_type = Some(
                    parser
                        .expect_string()
                        .map_err(|_| ParseError::InvalidFieldType)?,
                )
            }
            // The id scratch is free until the event commits.
            _ if field.is("timestamp") => {
                timestamp_nanos = Some(parse_timestamp_token(parser, id_scratch)?)
            }
            _ if field.is("value") => {
                let token = parser.next_spanned().map_err(json_error)?;
                let start = token.start;
                let end = parser.skip_value_from(token).map_err(json_error)?;
                value = Some(&parser.input()[start..end]);
            }
            _ => parser.skip_value().map_err(json_error)?,
        }
    }
    match parser.next_token().map_err(json_error)? {
        Token::ObjectEnd => {}
        _ => return Err(ParseError::InvalidJson),
    }
    let id = id
        .ok_or(ParseError::MissingField)?
        .text(id_scratch)
        .map_err(json_error)?;
    let event_type = event_type
        .ok_or(ParseError::MissingField)?
        .text(type_scratch)
        .map_err(json_error)?;
    crate::commit_base_event(
        output,
        id.as_bytes(),
        event_type.as_bytes(),
        timestamp_nanos.ok_or(ParseError::MissingField)?,
        value,
    )
}

/// The base log's timestamp in nanoseconds: a number is nanoseconds, a
/// string an RFC 3339 instant.
fn parse_timestamp_token(
    parser: &mut JsonParser<'_>,
    scratch: &mut Vec<u8>,
) -> Result<i64, ParseError> {
    match parser.next_token().map_err(json_error)? {
        Token::String(value) => parse_instant(
            value.text(scratch).map_err(json_error)?,
            TimeUnit::Nanosecond,
        )
        .map_err(|_| ParseError::InvalidFieldType),
        Token::Number(value) => value
            .parse::<i64>()
            .map_err(|_| ParseError::InvalidFieldType),
        _ => Err(ParseError::InvalidFieldType),
    }
}
fn json_error(error: ParserError) -> ParseError {
    match error {
        ParserError::OutOfMemory => ParseError::OutOfMemory,
        ParserError::InvalidJson
        | ParserError::UnexpectedToken
        | ParserError::EndOfInput
        | ParserError::InvalidNumber => ParseError::InvalidJson,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn columns() -> DynamicColumns {
        DynamicColumns::new(&crate::BASE_EVENT_LOG_FIELDS, 10).expect("column plane fits")
    }
    /// 2024-01-15T10:30:00Z in seconds since the epoch.
    const BASE_SECONDS: i64 = 19_737 * 86_400 + 37_800;
    #[test]
    fn parse_json_events_single_event_with_an_instant_timestamp() {
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
    fn a_numeric_base_timestamp_is_nanoseconds() {
        let mut c = columns();
        parse_json_events(
            br#"[{"id":"id-1","type":"test","timestamp":1705315800000000000}]"#,
            &mut c,
        )
        .unwrap();
        assert_eq!(
            crate::parsed_event(&c, 0).unwrap().timestamp_nanos,
            1_705_315_800_000_000_000
        );
    }
    #[test]
    fn an_instant_base_timestamp_is_stored_in_nanoseconds() {
        let mut c = columns();
        parse_json_events(
            br#"[{"id":"id-1","type":"test","timestamp":"2024-01-15T10:30:00.123456789Z"}]"#,
            &mut c,
        )
        .unwrap();
        assert_eq!(
            crate::parsed_event(&c, 0).unwrap().timestamp_nanos,
            BASE_SECONDS * 1_000_000_000 + 123_456_789
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
            Err(ParseError::InvalidJson)
        );
    }
    #[test]
    fn parse_json_events_missing_required_field_returns_error() {
        assert_eq!(
            parse_json_events(
                br#"[{"id":"id-1","timestamp":"1970-01-01T00:00:00Z"}]"#,
                &mut columns()
            ),
            Err(ParseError::MissingField)
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
