//! Replaces `packages/columine/src/parsing/msgpack_scanner.zig`.

use crate::{EventColumns, ParseError};

pub use crate::json_scanner::parse_iso8601_to_micros;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MsgpackScannerError {
    InvalidMsgpack,
    MissingField,
    InvalidFieldType,
    TooManyEvents,
    BufferOverflow,
}
impl From<MsgpackScannerError> for ParseError {
    fn from(value: MsgpackScannerError) -> Self {
        match value {
            MsgpackScannerError::InvalidMsgpack => Self::InvalidMsgpack,
            MsgpackScannerError::MissingField => Self::MissingField,
            MsgpackScannerError::InvalidFieldType => Self::InvalidFieldType,
            MsgpackScannerError::TooManyEvents => Self::TooManyEvents,
            MsgpackScannerError::BufferOverflow => Self::BufferOverflow,
        }
    }
}

pub fn parse_msgpack_events(
    input: &[u8],
    output: &mut EventColumns,
) -> Result<(), MsgpackScannerError> {
    if input.is_empty() {
        return Ok(());
    }
    let mut reader = Reader::new(input);
    let count = reader
        .read_array_header()
        .ok_or(MsgpackScannerError::InvalidMsgpack)?;
    for _ in 0..count {
        parse_event_map(&mut reader, output)?;
    }
    Ok(())
}
pub fn parse_msgpack_stream(
    input: &[u8],
    output: &mut EventColumns,
) -> Result<(), MsgpackScannerError> {
    let mut reader = Reader::new(input);
    while !reader.at_end() {
        parse_event_map(&mut reader, output)?;
    }
    Ok(())
}

fn parse_event_map(
    reader: &mut Reader<'_>,
    output: &mut EventColumns,
) -> Result<(), MsgpackScannerError> {
    let fields = reader
        .read_map_header()
        .ok_or(MsgpackScannerError::InvalidMsgpack)?;
    let mut id = None;
    let mut event_type = None;
    let mut timestamp = None;
    let mut value = None;
    for _ in 0..fields {
        let key = reader
            .read_string()
            .ok_or(MsgpackScannerError::InvalidMsgpack)?;
        match key {
            b"id" => {
                id = Some(
                    std::str::from_utf8(
                        reader
                            .read_string()
                            .ok_or(MsgpackScannerError::InvalidFieldType)?,
                    )
                    .map_err(|_| MsgpackScannerError::InvalidFieldType)?
                    .to_owned(),
                )
            }
            b"type" => {
                event_type = Some(
                    std::str::from_utf8(
                        reader
                            .read_string()
                            .ok_or(MsgpackScannerError::InvalidFieldType)?,
                    )
                    .map_err(|_| MsgpackScannerError::InvalidFieldType)?
                    .to_owned(),
                )
            }
            b"timestamp" => {
                timestamp = Some(
                    reader
                        .read_timestamp()
                        .ok_or(MsgpackScannerError::InvalidFieldType)?,
                )
            }
            b"value" => {
                let start = reader.position();
                reader
                    .skip_value()
                    .ok_or(MsgpackScannerError::InvalidMsgpack)?;
                let raw = &reader.input()[start..reader.position()];
                value = if raw == [0xc0] {
                    None
                } else {
                    Some(raw.to_vec())
                };
            }
            _ => reader
                .skip_value()
                .ok_or(MsgpackScannerError::InvalidMsgpack)?,
        }
    }
    output
        .add_event(
            id.ok_or(MsgpackScannerError::MissingField)?.as_bytes(),
            event_type
                .ok_or(MsgpackScannerError::MissingField)?
                .as_bytes(),
            timestamp.ok_or(MsgpackScannerError::MissingField)?,
            value.as_deref(),
        )
        .map_err(|error| match error {
            ParseError::TooManyEvents => MsgpackScannerError::TooManyEvents,
            ParseError::BufferOverflow => MsgpackScannerError::BufferOverflow,
            _ => MsgpackScannerError::InvalidMsgpack,
        })
}

/// Minimal byte reader for the MessagePack surface used by Columine.
pub(crate) struct Reader<'a> {
    input: &'a [u8],
    pos: usize,
}
impl<'a> Reader<'a> {
    pub(crate) fn new(input: &'a [u8]) -> Self {
        Self { input, pos: 0 }
    }
    pub(crate) fn input(&self) -> &'a [u8] {
        self.input
    }
    pub(crate) fn position(&self) -> usize {
        self.pos
    }
    pub(crate) fn at_end(&self) -> bool {
        self.pos == self.input.len()
    }
    fn take(&mut self) -> Option<u8> {
        let byte = *self.input.get(self.pos)?;
        self.pos += 1;
        Some(byte)
    }
    /// msgpack_extractor.zig `readBinary`: consume a standard bin value
    /// (0xc4/0xc5/0xc6) and return its payload slice.
    pub(crate) fn read_bin(&mut self) -> Option<&'a [u8]> {
        let marker = self.take()?;
        let len = match marker {
            0xc4 => usize::from(self.take()?),
            0xc5 => {
                let hi = self.take()?;
                let lo = self.take()?;
                usize::from(u16::from_be_bytes([hi, lo]))
            }
            0xc6 => {
                let b = self.take_slice(4)?;
                u32::from_be_bytes([b[0], b[1], b[2], b[3]]) as usize
            }
            _ => return None,
        };
        self.take_slice(len)
    }
    fn take_slice(&mut self, length: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(length)?;
        let slice = self.input.get(self.pos..end)?;
        self.pos = end;
        Some(slice)
    }
    pub(crate) fn read_map_header(&mut self) -> Option<u32> {
        let byte = self.take()?;
        match byte {
            0x80..=0x8f => Some(u32::from(byte & 0x0f)),
            0xde => Some(u32::from(u16::from_be_bytes(
                self.take_slice(2)?.try_into().ok()?,
            ))),
            0xdf => Some(u32::from_be_bytes(self.take_slice(4)?.try_into().ok()?)),
            _ => None,
        }
    }
    pub(crate) fn read_array_header(&mut self) -> Option<u32> {
        let byte = self.take()?;
        match byte {
            0x90..=0x9f => Some(u32::from(byte & 0x0f)),
            0xdc => Some(u32::from(u16::from_be_bytes(
                self.take_slice(2)?.try_into().ok()?,
            ))),
            0xdd => Some(u32::from_be_bytes(self.take_slice(4)?.try_into().ok()?)),
            _ => None,
        }
    }
    pub(crate) fn read_string(&mut self) -> Option<&'a [u8]> {
        let byte = self.take()?;
        let length = match byte {
            0xa0..=0xbf => usize::from(byte & 0x1f),
            0xd9 => usize::from(self.take()?),
            0xda => usize::from(u16::from_be_bytes(self.take_slice(2)?.try_into().ok()?)),
            0xdb => {
                usize::try_from(u32::from_be_bytes(self.take_slice(4)?.try_into().ok()?)).ok()?
            }
            _ => return None,
        };
        self.take_slice(length)
    }
    pub(crate) fn read_integer(&mut self) -> Option<i64> {
        let byte = self.take()?;
        match byte {
            0x00..=0x7f => Some(i64::from(byte)),
            0xe0..=0xff => Some(i64::from(i8::from_ne_bytes([byte]))),
            0xcc => Some(i64::from(self.take()?)),
            0xcd => Some(i64::from(u16::from_be_bytes(
                self.take_slice(2)?.try_into().ok()?,
            ))),
            0xce => Some(i64::from(u32::from_be_bytes(
                self.take_slice(4)?.try_into().ok()?,
            ))),
            0xcf => i64::try_from(u64::from_be_bytes(self.take_slice(8)?.try_into().ok()?)).ok(),
            0xd0 => Some(i64::from(i8::from_ne_bytes([self.take()?]))),
            0xd1 => Some(i64::from(i16::from_be_bytes(
                self.take_slice(2)?.try_into().ok()?,
            ))),
            0xd2 => Some(i64::from(i32::from_be_bytes(
                self.take_slice(4)?.try_into().ok()?,
            ))),
            0xd3 => Some(i64::from_be_bytes(self.take_slice(8)?.try_into().ok()?)),
            _ => None,
        }
    }
    pub(crate) fn read_float(&mut self) -> Option<f64> {
        let byte = self.take()?;
        match byte {
            0xca => Some(f64::from(f32::from_bits(u32::from_be_bytes(
                self.take_slice(4)?.try_into().ok()?,
            )))),
            0xcb => Some(f64::from_bits(u64::from_be_bytes(
                self.take_slice(8)?.try_into().ok()?,
            ))),
            _ => None,
        }
    }
    /// Timestamp: integer ms, float ms (some encoders), or ISO-8601 string →
    /// microseconds, dispatched on the marker byte exactly like Zig's
    /// readTimestamp. Zig's `ms * 1000` / `@intFromFloat` overflow paths are
    /// UB in ReleaseSmall; the checked/saturating versions here are the safe
    /// superset of an unreachable-in-practice domain.
    pub(crate) fn read_timestamp(&mut self) -> Option<i64> {
        let byte = *self.input.get(self.pos)?;
        if byte & 0x80 == 0 || byte & 0xe0 == 0xe0 || (0xcc..=0xd3).contains(&byte) {
            return self.read_integer()?.checked_mul(1_000);
        }
        if matches!(byte, 0xca | 0xcb) {
            let milliseconds = self.read_float()? as i64;
            return milliseconds.checked_mul(1_000);
        }
        if byte & 0xe0 == 0xa0 || matches!(byte, 0xd9..=0xdb) {
            let text = std::str::from_utf8(self.read_string()?).ok()?;
            return parse_iso8601_to_micros(text).ok();
        }
        None
    }
    pub(crate) fn skip_value(&mut self) -> Option<()> {
        let byte = *self.input.get(self.pos)?;
        match byte {
            0x00..=0x7f | 0xe0..=0xff | 0xc0 | 0xc2 | 0xc3 => self.pos += 1,
            0x80..=0x8f => {
                self.pos += 1;
                for _ in 0..u32::from(byte & 0x0f) * 2 {
                    self.skip_value()?;
                }
            }
            0x90..=0x9f => {
                self.pos += 1;
                for _ in 0..u32::from(byte & 0x0f) {
                    self.skip_value()?;
                }
            }
            0xa0..=0xbf => {
                self.pos = self.pos.checked_add(1 + usize::from(byte & 0x1f))?;
            }
            0xc4 => {
                self.pos += 1;
                let n = usize::from(self.take()?);
                self.pos = self.pos.checked_add(n)?;
            }
            0xc5 => {
                self.pos += 1;
                let n = usize::from(u16::from_be_bytes(self.take_slice(2)?.try_into().ok()?));
                self.pos = self.pos.checked_add(n)?;
            }
            0xc6 => {
                self.pos += 1;
                let n = usize::try_from(u32::from_be_bytes(self.take_slice(4)?.try_into().ok()?))
                    .ok()?;
                self.pos = self.pos.checked_add(n)?;
            }
            0xc7 => {
                self.pos += 1;
                let n = usize::from(self.take()?);
                self.pos = self.pos.checked_add(n + 1)?;
            }
            0xc8 => {
                self.pos += 1;
                let n = usize::from(u16::from_be_bytes(self.take_slice(2)?.try_into().ok()?));
                self.pos = self.pos.checked_add(n + 1)?;
            }
            0xc9 => {
                self.pos += 1;
                let n = usize::try_from(u32::from_be_bytes(self.take_slice(4)?.try_into().ok()?))
                    .ok()?;
                self.pos = self.pos.checked_add(n + 1)?;
            }
            0xca => self.pos = self.pos.checked_add(5)?,
            0xcb => self.pos = self.pos.checked_add(9)?,
            0xcc | 0xd0 => self.pos = self.pos.checked_add(2)?,
            0xcd | 0xd1 => self.pos = self.pos.checked_add(3)?,
            0xce | 0xd2 => self.pos = self.pos.checked_add(5)?,
            0xcf | 0xd3 => self.pos = self.pos.checked_add(9)?,
            0xd4 => self.pos = self.pos.checked_add(3)?,
            0xd5 => self.pos = self.pos.checked_add(4)?,
            0xd6 => self.pos = self.pos.checked_add(6)?,
            0xd7 => self.pos = self.pos.checked_add(10)?,
            0xd8 => self.pos = self.pos.checked_add(18)?,
            0xd9 => {
                self.pos += 1;
                let n = usize::from(self.take()?);
                self.pos = self.pos.checked_add(n)?;
            }
            0xda => {
                self.pos += 1;
                let n = usize::from(u16::from_be_bytes(self.take_slice(2)?.try_into().ok()?));
                self.pos = self.pos.checked_add(n)?;
            }
            0xdb => {
                self.pos += 1;
                let n = usize::try_from(u32::from_be_bytes(self.take_slice(4)?.try_into().ok()?))
                    .ok()?;
                self.pos = self.pos.checked_add(n)?;
            }
            0xdc => {
                self.pos += 1;
                let n = u32::from(u16::from_be_bytes(self.take_slice(2)?.try_into().ok()?));
                for _ in 0..n {
                    self.skip_value()?;
                }
            }
            0xdd => {
                self.pos += 1;
                let n = u32::from_be_bytes(self.take_slice(4)?.try_into().ok()?);
                for _ in 0..n {
                    self.skip_value()?;
                }
            }
            0xde => {
                self.pos += 1;
                let n = u32::from(u16::from_be_bytes(self.take_slice(2)?.try_into().ok()?));
                for _ in 0..n * 2 {
                    self.skip_value()?;
                }
            }
            0xdf => {
                self.pos += 1;
                let n = u32::from_be_bytes(self.take_slice(4)?.try_into().ok()?);
                for _ in 0..n * 2 {
                    self.skip_value()?;
                }
            }
            _ => return None,
        };
        (self.pos <= self.input.len()).then_some(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn str_(out: &mut Vec<u8>, value: &str) {
        out.push(0xa0 | u8::try_from(value.len()).unwrap());
        out.extend_from_slice(value.as_bytes());
    }
    fn int64(out: &mut Vec<u8>, value: i64) {
        out.push(0xd3);
        out.extend_from_slice(&value.to_be_bytes());
    }
    fn event(id: &str, timestamp: i64, value: Option<&[u8]>) -> Vec<u8> {
        let mut out = vec![if value.is_some() { 0x84 } else { 0x83 }];
        str_(&mut out, "id");
        str_(&mut out, id);
        str_(&mut out, "type");
        str_(&mut out, "orderPlaced");
        str_(&mut out, "timestamp");
        int64(&mut out, timestamp);
        if let Some(value) = value {
            str_(&mut out, "value");
            out.extend_from_slice(value);
        }
        out
    }
    #[test]
    fn parse_msgpack_stream_single_event_with_int_timestamp() {
        let mut c = EventColumns::new(10);
        parse_msgpack_stream(
            &event("abc-123", 1_705_315_800_000, Some(&[0x81, 0xa1, b'x', 1])),
            &mut c,
        )
        .unwrap();
        assert_eq!(c.count, 1);
        assert_eq!(crate::parsed_event(&c, 0).unwrap().id, "abc-123");
        assert_eq!(
            crate::parsed_event(&c, 0).unwrap().timestamp_micros,
            1_705_315_800_000_000
        );
        assert!(crate::parsed_event(&c, 0).unwrap().value.is_some());
    }
    #[test]
    fn parse_msgpack_stream_multiple_events_concatenated() {
        let mut input = event("id-1", 1_000, None);
        input.extend(event("id-2", 2_000, None));
        let mut c = EventColumns::new(10);
        parse_msgpack_stream(&input, &mut c).unwrap();
        assert_eq!(c.count, 2);
        assert_eq!(crate::parsed_event(&c, 1).unwrap().id, "id-2");
    }
    #[test]
    fn parse_msgpack_events_array_format() {
        let first = event("id-1", 1_000, None);
        let second = event("id-2", 2_000, None);
        let mut input = vec![0x92];
        input.extend(first);
        input.extend(second);
        let mut c = EventColumns::new(10);
        parse_msgpack_events(&input, &mut c).unwrap();
        assert_eq!(c.count, 2);
    }
    #[test]
    fn parse_msgpack_stream_float_timestamp() {
        // Zig readTimestamp accepts float32/float64 milliseconds (some
        // encoders emit floats); 1500.7 ms truncates to 1500 ms.
        let mut input = vec![0x83];
        str_(&mut input, "id");
        str_(&mut input, "a");
        str_(&mut input, "type");
        str_(&mut input, "b");
        str_(&mut input, "timestamp");
        input.push(0xcb);
        input.extend(1500.7_f64.to_bits().to_be_bytes());
        let mut c = EventColumns::new(1);
        parse_msgpack_stream(&input, &mut c).unwrap();
        assert_eq!(
            crate::parsed_event(&c, 0).unwrap().timestamp_micros,
            1_500_000
        );
    }
    #[test]
    fn parse_msgpack_stream_string_timestamp_iso_8601() {
        let mut input = vec![0x83];
        str_(&mut input, "id");
        str_(&mut input, "a");
        str_(&mut input, "type");
        str_(&mut input, "b");
        str_(&mut input, "timestamp");
        str_(&mut input, "1970-01-01T00:00:00Z");
        let mut c = EventColumns::new(1);
        parse_msgpack_stream(&input, &mut c).unwrap();
        assert_eq!(crate::parsed_event(&c, 0).unwrap().timestamp_micros, 0);
    }
    #[test]
    fn parse_msgpack_stream_missing_required_field() {
        let mut input = event("id", 1_000, None);
        input[0] = 0x82;
        input.truncate(input.len() - 18);
        let mut c = EventColumns::new(1);
        assert_eq!(
            parse_msgpack_stream(&input, &mut c),
            Err(MsgpackScannerError::MissingField)
        );
    }
    #[test]
    fn parse_msgpack_stream_empty_input() {
        let mut c = EventColumns::new(1);
        parse_msgpack_stream(&[], &mut c).unwrap();
        assert_eq!(c.count, 0);
    }
    #[test]
    fn parse_msgpack_events_empty_array() {
        let mut c = EventColumns::new(1);
        parse_msgpack_events(&[0x90], &mut c).unwrap();
        assert_eq!(c.count, 0);
    }
    #[test]
    fn parse_msgpack_stream_value_preserved_as_raw_msgpack() {
        let value = [0x81, 0xa1, b'x', 1];
        let mut c = EventColumns::new(1);
        parse_msgpack_stream(&event("id", 1, Some(&value)), &mut c).unwrap();
        assert_eq!(
            crate::parsed_event(&c, 0).unwrap().value.as_deref(),
            Some(value.as_slice())
        );
    }
    #[test]
    fn parse_msgpack_stream_nil_value_treated_as_null() {
        let mut c = EventColumns::new(1);
        parse_msgpack_stream(&event("id", 1, Some(&[0xc0])), &mut c).unwrap();
        assert!(crate::parsed_event(&c, 0).unwrap().value.is_none());
    }
    #[test]
    fn parse_msgpack_stream_event_without_value_field() {
        let mut c = EventColumns::new(1);
        parse_msgpack_stream(&event("id", 1, None), &mut c).unwrap();
        assert!(crate::parsed_event(&c, 0).unwrap().value.is_none());
    }
    #[test]
    fn parse_msgpack_stream_invalid_input_not_a_map() {
        let mut c = EventColumns::new(1);
        assert_eq!(
            parse_msgpack_stream(&[0x01], &mut c),
            Err(MsgpackScannerError::InvalidMsgpack)
        );
    }
}
