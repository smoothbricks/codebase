//! Replaces `packages/columine/src/parsing/json_parser.zig`.
//!
//! This is deliberately a scalar streaming lexer: a hand-rolled simd128
//! variant of the `scan` kernels measured ~23% slower than this shape at
//! `opt-level = 3` (see `scan.rs`'s module doc for the numbers and the
//! re-measure condition).

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ParserError {
    InvalidJson,
    UnexpectedToken,
    EndOfInput,
    InvalidNumber,
}

#[derive(Clone, Debug, PartialEq)]
pub enum Token {
    ObjectBegin,
    ObjectEnd,
    ArrayBegin,
    ArrayEnd,
    String(String),
    Number(String),
    True,
    False,
    Null,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct SpannedToken {
    pub token: Token,
    pub start: usize,
    pub end: usize,
}

pub const BACKEND_NAME: &str = "scalar";

pub fn backend_name() -> &'static str {
    BACKEND_NAME
}
pub fn target_arch() -> &'static str {
    std::env::consts::ARCH
}
pub fn target_is_wasm() -> bool {
    cfg!(target_arch = "wasm32") || cfg!(target_arch = "wasm64")
}

/// Structural context, mirroring the validation `std.json.Scanner` performs.
/// The Zig port's first cut accepted `[1 2]`, `{"a" 1}`, `[,]`, `[01]`, and
/// trailing commas that the Zig backends reject; this state machine restores
/// the JSON grammar the Zig scanner enforces.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Frame {
    /// Inside `[...]`; the payload tracks what may come next.
    Array(ArrayPhase),
    /// Inside `{...}`; the payload tracks what may come next.
    Object(ObjectPhase),
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ArrayPhase {
    ValueOrEnd,
    CommaOrEnd,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ObjectPhase {
    KeyOrEnd,
    Colon,
    CommaOrEnd,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Top {
    NotStarted,
    Done,
}

pub struct JsonParser<'a> {
    input: &'a [u8],
    cursor: usize,
    stack: Vec<Frame>,
    top: Top,
    peeked: Option<SpannedToken>,
}

impl<'a> JsonParser<'a> {
    pub fn init(input: &'a [u8]) -> Self {
        Self::new(input)
    }

    pub fn new(input: &'a [u8]) -> Self {
        Self {
            input,
            cursor: 0,
            stack: Vec::new(),
            top: Top::NotStarted,
            peeked: None,
        }
    }
    pub fn input(&self) -> &'a [u8] {
        self.input
    }
    pub fn cursor(&self) -> usize {
        self.cursor
    }

    fn skip_whitespace(&mut self) {
        // Zig std.json's whitespace set exactly (space/\t/\r/\n, no \x0C).
        self.cursor = crate::scan::skip_whitespace(self.input, self.cursor);
    }

    fn current_byte(&self) -> Result<u8, ParserError> {
        self.input
            .get(self.cursor)
            .copied()
            .ok_or(ParserError::EndOfInput)
    }

    /// Record that a value just completed in the enclosing context. Called
    /// after a scalar and BEFORE pushing a container frame, so popping a
    /// container needs no parent fix-up.
    fn note_value_complete(&mut self) {
        match self.stack.last_mut() {
            None => self.top = Top::Done,
            Some(Frame::Array(phase)) => *phase = ArrayPhase::CommaOrEnd,
            Some(Frame::Object(phase)) => *phase = ObjectPhase::CommaOrEnd,
        }
    }

    /// Lex one value-start token at the cursor and update structural state.
    fn read_value_token(&mut self) -> Result<SpannedToken, ParserError> {
        let start = self.cursor;
        let token = match self.current_byte()? {
            b'{' => {
                self.note_value_complete();
                self.cursor += 1;
                self.stack.push(Frame::Object(ObjectPhase::KeyOrEnd));
                Token::ObjectBegin
            }
            b'[' => {
                self.note_value_complete();
                self.cursor += 1;
                self.stack.push(Frame::Array(ArrayPhase::ValueOrEnd));
                Token::ArrayBegin
            }
            b'"' => {
                let value = self.parse_string()?;
                self.note_value_complete();
                Token::String(value)
            }
            b't' => {
                self.expect_literal(b"true")?;
                self.note_value_complete();
                Token::True
            }
            b'f' => {
                self.expect_literal(b"false")?;
                self.note_value_complete();
                Token::False
            }
            b'n' => {
                self.expect_literal(b"null")?;
                self.note_value_complete();
                Token::Null
            }
            b'-' | b'0'..=b'9' => {
                let value = self.parse_number()?;
                self.note_value_complete();
                Token::Number(value)
            }
            _ => return Err(ParserError::InvalidJson),
        };
        Ok(SpannedToken {
            token,
            start,
            end: self.cursor,
        })
    }

    fn read_key_token(&mut self) -> Result<SpannedToken, ParserError> {
        let start = self.cursor;
        if self.current_byte()? != b'"' {
            return Err(ParserError::InvalidJson);
        }
        let key = self.parse_string()?;
        if let Some(Frame::Object(phase)) = self.stack.last_mut() {
            *phase = ObjectPhase::Colon;
        }
        Ok(SpannedToken {
            token: Token::String(key),
            start,
            end: self.cursor,
        })
    }

    fn advance(&mut self) -> Result<SpannedToken, ParserError> {
        self.skip_whitespace();
        match self.stack.last().copied() {
            None => match self.top {
                Top::NotStarted => self.read_value_token(),
                // After the top-level value: only trailing whitespace is
                // acceptable (std.json: end_of_document, mapped to
                // EndOfInput by the Zig wrapper; anything else is an error).
                Top::Done => {
                    if self.cursor >= self.input.len() {
                        Err(ParserError::EndOfInput)
                    } else {
                        Err(ParserError::InvalidJson)
                    }
                }
            },
            Some(Frame::Array(ArrayPhase::ValueOrEnd)) => {
                if self.current_byte()? == b']' {
                    self.pop_container(Token::ArrayEnd)
                } else {
                    self.read_value_token()
                }
            }
            Some(Frame::Array(ArrayPhase::CommaOrEnd)) => match self.current_byte()? {
                b',' => {
                    self.cursor += 1;
                    self.skip_whitespace();
                    // Trailing commas are rejected, as in std.json.
                    if self.current_byte()? == b']' {
                        return Err(ParserError::InvalidJson);
                    }
                    self.read_value_token()
                }
                b']' => self.pop_container(Token::ArrayEnd),
                _ => Err(ParserError::InvalidJson),
            },
            Some(Frame::Object(ObjectPhase::KeyOrEnd)) => {
                if self.current_byte()? == b'}' {
                    self.pop_container(Token::ObjectEnd)
                } else {
                    self.read_key_token()
                }
            }
            Some(Frame::Object(ObjectPhase::Colon)) => {
                if self.current_byte()? != b':' {
                    return Err(ParserError::InvalidJson);
                }
                self.cursor += 1;
                self.skip_whitespace();
                self.read_value_token()
            }
            Some(Frame::Object(ObjectPhase::CommaOrEnd)) => match self.current_byte()? {
                b',' => {
                    self.cursor += 1;
                    self.skip_whitespace();
                    self.read_key_token()
                }
                b'}' => self.pop_container(Token::ObjectEnd),
                _ => Err(ParserError::InvalidJson),
            },
        }
    }

    fn pop_container(&mut self, token: Token) -> Result<SpannedToken, ParserError> {
        let start = self.cursor;
        self.cursor += 1;
        // The parent phase was already set by note_value_complete() when this
        // container was pushed, so popping needs no parent fix-up.
        self.stack.pop();
        Ok(SpannedToken {
            token,
            start,
            end: self.cursor,
        })
    }

    pub(crate) fn next_spanned(&mut self) -> Result<SpannedToken, ParserError> {
        if let Some(token) = self.peeked.take() {
            return Ok(token);
        }
        self.advance()
    }

    pub fn next_token(&mut self) -> Result<Token, ParserError> {
        Ok(self.next_spanned()?.token)
    }

    fn expect_literal(&mut self, literal: &[u8]) -> Result<(), ParserError> {
        let end = self
            .cursor
            .checked_add(literal.len())
            .ok_or(ParserError::InvalidJson)?;
        if self.input.get(self.cursor..end) != Some(literal) {
            return Err(ParserError::InvalidJson);
        }
        self.cursor = end;
        Ok(())
    }

    fn parse_string(&mut self) -> Result<String, ParserError> {
        debug_assert_eq!(self.input[self.cursor], b'"');
        self.cursor += 1;
        let mut value = Vec::new();
        loop {
            // Bulk-copy the clean run up to the next quote/backslash/control
            // byte, then handle that byte exactly as the per-byte loop did
            // (the bulk extend_from_slice is what makes this loop fast).
            let run_end = crate::scan::find_string_special(self.input, self.cursor);
            value.extend_from_slice(&self.input[self.cursor..run_end]);
            self.cursor = run_end;
            let Some(&byte) = self.input.get(self.cursor) else {
                return Err(ParserError::InvalidJson);
            };
            self.cursor += 1;
            match byte {
                b'"' => return String::from_utf8(value).map_err(|_| ParserError::InvalidJson),
                b'\\' => {
                    let Some(&escape) = self.input.get(self.cursor) else {
                        return Err(ParserError::InvalidJson);
                    };
                    self.cursor += 1;
                    match escape {
                        b'"' | b'\\' | b'/' => value.push(escape),
                        b'b' => value.push(8),
                        b'f' => value.push(12),
                        b'n' => value.push(b'\n'),
                        b'r' => value.push(b'\r'),
                        b't' => value.push(b'\t'),
                        b'u' => self.push_unicode_escape(&mut value)?,
                        _ => return Err(ParserError::InvalidJson),
                    }
                }
                // find_string_special only stops on '"', '\\', or a control
                // byte; anything else here is a classifier bug.
                _ => {
                    debug_assert!(byte < 0x20);
                    return Err(ParserError::InvalidJson);
                }
            }
        }
    }

    fn read_hex4(&mut self) -> Result<u32, ParserError> {
        let end = self.cursor.checked_add(4).ok_or(ParserError::InvalidJson)?;
        let digits = self
            .input
            .get(self.cursor..end)
            .ok_or(ParserError::InvalidJson)?;
        self.cursor = end;
        digits
            .iter()
            .try_fold(0_u32, |acc, byte| hex(*byte).map(|digit| acc * 16 + digit))
    }

    /// `\uXXXX` escapes, including UTF-16 surrogate pairs (`😀`).
    /// Unpaired surrogates are rejected, matching std.json and simdjzon.
    fn push_unicode_escape(&mut self, output: &mut Vec<u8>) -> Result<(), ParserError> {
        let code = self.read_hex4()?;
        let scalar = match code {
            0xD800..=0xDBFF => {
                if self.input.get(self.cursor..self.cursor + 2) != Some(b"\\u") {
                    return Err(ParserError::InvalidJson);
                }
                self.cursor += 2;
                let low = self.read_hex4()?;
                if !(0xDC00..=0xDFFF).contains(&low) {
                    return Err(ParserError::InvalidJson);
                }
                0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00)
            }
            0xDC00..=0xDFFF => return Err(ParserError::InvalidJson),
            _ => code,
        };
        let character = char::from_u32(scalar).ok_or(ParserError::InvalidJson)?;
        let mut encoded = [0; 4];
        output.extend_from_slice(character.encode_utf8(&mut encoded).as_bytes());
        Ok(())
    }

    fn parse_number(&mut self) -> Result<String, ParserError> {
        let start = self.cursor;
        if self.input.get(self.cursor) == Some(&b'-') {
            self.cursor += 1;
        }
        match self.input.get(self.cursor) {
            Some(b'0') => self.cursor += 1,
            Some(b'1'..=b'9') => {
                while matches!(self.input.get(self.cursor), Some(b'0'..=b'9')) {
                    self.cursor += 1;
                }
            }
            _ => return Err(ParserError::InvalidNumber),
        }
        if self.input.get(self.cursor) == Some(&b'.') {
            self.cursor += 1;
            let fraction_start = self.cursor;
            while matches!(self.input.get(self.cursor), Some(b'0'..=b'9')) {
                self.cursor += 1;
            }
            if self.cursor == fraction_start {
                return Err(ParserError::InvalidNumber);
            }
        }
        if matches!(self.input.get(self.cursor), Some(b'e' | b'E')) {
            self.cursor += 1;
            if matches!(self.input.get(self.cursor), Some(b'+' | b'-')) {
                self.cursor += 1;
            }
            let exponent_start = self.cursor;
            while matches!(self.input.get(self.cursor), Some(b'0'..=b'9')) {
                self.cursor += 1;
            }
            if self.cursor == exponent_start {
                return Err(ParserError::InvalidNumber);
            }
        }
        std::str::from_utf8(&self.input[start..self.cursor])
            .map(str::to_owned)
            .map_err(|_| ParserError::InvalidNumber)
    }

    pub fn peek_token(&mut self) -> Result<Token, ParserError> {
        if self.peeked.is_none() {
            self.peeked = Some(self.advance()?);
        }
        Ok(self
            .peeked
            .as_ref()
            .map(|spanned| spanned.token.clone())
            .expect("peeked was just populated"))
    }
    pub fn expect_object_begin(&mut self) -> Result<(), ParserError> {
        expect(self.next_token()?, Token::ObjectBegin)
    }
    pub fn expect_array_begin(&mut self) -> Result<(), ParserError> {
        expect(self.next_token()?, Token::ArrayBegin)
    }
    pub fn expect_field_name(&mut self) -> Result<String, ParserError> {
        match self.next_token()? {
            Token::String(value) => Ok(value),
            Token::ObjectEnd => Err(ParserError::EndOfInput),
            _ => Err(ParserError::UnexpectedToken),
        }
    }
    pub fn expect_string(&mut self) -> Result<String, ParserError> {
        match self.next_token()? {
            Token::String(value) => Ok(value),
            _ => Err(ParserError::UnexpectedToken),
        }
    }
    pub fn expect_int64(&mut self) -> Result<i64, ParserError> {
        match self.next_token()? {
            Token::Number(value) => value.parse().map_err(|_| ParserError::InvalidNumber),
            _ => Err(ParserError::UnexpectedToken),
        }
    }
    pub fn expect_float64(&mut self) -> Result<f64, ParserError> {
        match self.next_token()? {
            Token::Number(value) => value.parse().map_err(|_| ParserError::InvalidNumber),
            _ => Err(ParserError::UnexpectedToken),
        }
    }
    pub fn expect_bool(&mut self) -> Result<bool, ParserError> {
        match self.next_token()? {
            Token::True => Ok(true),
            Token::False => Ok(false),
            _ => Err(ParserError::UnexpectedToken),
        }
    }
    pub fn is_object_end(&mut self) -> bool {
        matches!(self.peek_token(), Ok(Token::ObjectEnd))
    }
    pub fn is_array_end(&mut self) -> bool {
        matches!(self.peek_token(), Ok(Token::ArrayEnd))
    }
    pub fn skip_value(&mut self) -> Result<(), ParserError> {
        let first = self.next_spanned()?;
        self.skip_value_from(first).map(|_| ())
    }
    pub(crate) fn skip_open_container(&mut self, first: Token) -> Result<(), ParserError> {
        let mut depth = match first {
            Token::ObjectBegin | Token::ArrayBegin => 1_u32,
            _ => return Ok(()),
        };
        while depth != 0 {
            match self.next_token()? {
                Token::ObjectBegin | Token::ArrayBegin => depth += 1,
                Token::ObjectEnd | Token::ArrayEnd => depth -= 1,
                _ => {}
            }
        }
        Ok(())
    }
    pub(crate) fn skip_value_from(&mut self, first: SpannedToken) -> Result<usize, ParserError> {
        let mut depth = match first.token {
            Token::ObjectBegin | Token::ArrayBegin => 1_u32,
            _ => return Ok(first.end),
        };
        let mut end = first.end;
        while depth != 0 {
            let token = self.next_spanned()?;
            end = token.end;
            match token.token {
                Token::ObjectBegin | Token::ArrayBegin => depth += 1,
                Token::ObjectEnd | Token::ArrayEnd => depth -= 1,
                _ => {}
            }
        }
        Ok(end)
    }
}

fn hex(byte: u8) -> Result<u32, ParserError> {
    match byte {
        b'0'..=b'9' => Ok(u32::from(byte - b'0')),
        b'a'..=b'f' => Ok(u32::from(byte - b'a' + 10)),
        b'A'..=b'F' => Ok(u32::from(byte - b'A' + 10)),
        _ => Err(ParserError::InvalidJson),
    }
}
fn expect(token: Token, expected: Token) -> Result<(), ParserError> {
    if token == expected {
        Ok(())
    } else {
        Err(ParserError::UnexpectedToken)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn backend_name_reflects_target() {
        assert_eq!(backend_name(), "scalar");
    }
    #[test]
    fn target_info_reports_architecture() {
        assert!(!target_arch().is_empty());
        let _ = target_is_wasm();
    }
    #[test]
    fn json_parser_basic_object_parsing() {
        let mut p = JsonParser::new(br#"{"id":"test-123","count":42}"#);
        p.expect_object_begin().unwrap();
        assert_eq!(p.expect_field_name().unwrap(), "id");
        assert_eq!(p.expect_string().unwrap(), "test-123");
        assert_eq!(p.expect_field_name().unwrap(), "count");
        assert_eq!(p.expect_int64().unwrap(), 42);
        assert_eq!(p.next_token().unwrap(), Token::ObjectEnd);
    }
    #[test]
    fn json_parser_skip_value_for_object() {
        let mut p = JsonParser::new(
            br#"{"simple":"value","complex":{"nested":true,"array":[1,2,3]},"after":"ok"}"#,
        );
        p.expect_object_begin().unwrap();
        p.expect_field_name().unwrap();
        p.expect_string().unwrap();
        assert_eq!(p.expect_field_name().unwrap(), "complex");
        p.skip_value().unwrap();
        assert_eq!(p.expect_field_name().unwrap(), "after");
        assert_eq!(p.expect_string().unwrap(), "ok");
    }
    #[test]
    fn json_parser_array_parsing() {
        let mut p = JsonParser::new(br#"[{"id":"a"},{"id":"b"}]"#);
        p.expect_array_begin().unwrap();
        for expected in ["a", "b"] {
            p.expect_object_begin().unwrap();
            p.expect_field_name().unwrap();
            assert_eq!(p.expect_string().unwrap(), expected);
            assert_eq!(p.next_token().unwrap(), Token::ObjectEnd);
        }
        assert_eq!(p.next_token().unwrap(), Token::ArrayEnd);
    }
    #[test]
    fn json_parser_float_parsing() {
        let mut p = JsonParser::new(br#"{"amount":99.99}"#);
        p.expect_object_begin().unwrap();
        p.expect_field_name().unwrap();
        assert!((p.expect_float64().unwrap() - 99.99).abs() < 0.001);
    }
    #[test]
    fn json_parser_boolean_and_null() {
        let mut p = JsonParser::new(br#"{"active":true,"deleted":false,"data":null}"#);
        p.expect_object_begin().unwrap();
        p.expect_field_name().unwrap();
        assert!(p.expect_bool().unwrap());
        p.expect_field_name().unwrap();
        assert!(!p.expect_bool().unwrap());
        p.expect_field_name().unwrap();
        assert_eq!(p.next_token().unwrap(), Token::Null);
    }
    #[test]
    fn json_parser_is_object_end() {
        let mut p = JsonParser::new(br#"{"a":1}"#);
        p.expect_object_begin().unwrap();
        assert!(!p.is_object_end());
        p.expect_field_name().unwrap();
        p.expect_int64().unwrap();
        assert!(p.is_object_end());
    }

    fn drain(input: &[u8]) -> Result<Vec<Token>, ParserError> {
        let mut p = JsonParser::new(input);
        let mut tokens = Vec::new();
        loop {
            match p.next_token() {
                Ok(token) => tokens.push(token),
                Err(ParserError::EndOfInput) => return Ok(tokens),
                Err(error) => return Err(error),
            }
        }
    }

    /// The Zig backends reject structurally invalid JSON (std.json grammar);
    /// each of these was accepted by the first cut of the scalar lexer.
    #[test]
    fn json_parser_rejects_structurally_invalid_documents() {
        for bad in [
            br#"[1 2]"#.as_slice(),
            br#"{"a" 1}"#,
            br#"{"a":1 "b":2}"#,
            br#"[,]"#,
            br#"[1,]"#,
            br#"{"a":1,}"#,
            br#"["a":1]"#,
            br#"[01]"#,
            br#"[1] x"#,
            br#"{"a",1}"#,
            br#"{1:2}"#,
            br#"[}"#,
        ] {
            assert_eq!(
                drain(bad),
                Err(ParserError::InvalidJson),
                "accepted: {}",
                String::from_utf8_lossy(bad)
            );
        }
    }

    #[test]
    fn json_parser_accepts_whitespace_shaped_documents() {
        assert!(drain(b" [ 1 , 2 ]  ").is_ok());
        assert!(drain(b"{ \"a\" : [ true , null ] }\n").is_ok());
        assert!(drain(b"42").is_ok());
        assert!(drain(b"\t[\r\n1 ]").is_ok());
    }

    /// Form feed is whitespace to `u8::is_ascii_whitespace` but NOT to Zig
    /// std.json (Scanner.zig:1283: space/\t/\r/\n only) — the Zig backends
    /// reject it between tokens.
    #[test]
    fn json_parser_rejects_form_feed_between_tokens() {
        for bad in [
            b"[1,\x0c2]".as_slice(),
            b"\x0c[1]",
            b"[\x0c1]",
            b"{\"a\":\x0c1}",
            b"[1]\x0c",
        ] {
            assert_eq!(
                drain(bad),
                Err(ParserError::InvalidJson),
                "accepted: {}",
                String::from_utf8_lossy(bad)
            );
        }
    }

    #[test]
    fn json_parser_decodes_escapes_and_surrogate_pairs() {
        let mut p = JsonParser::new(r#"["b\nc","😀","😀","A"]"#.as_bytes());
        p.expect_array_begin().unwrap();
        assert_eq!(p.expect_string().unwrap(), "b\nc");
        // Raw UTF-8 passthrough.
        assert_eq!(p.expect_string().unwrap(), "\u{1F600}");
        assert_eq!(p.expect_string().unwrap(), "\u{1F600}");
        assert_eq!(p.expect_string().unwrap(), "A");
        // Escaped UTF-16 surrogate pair decodes to the same scalar.
        let mut escaped = JsonParser::new(b"[\"\\ud83d\\ude00\"]");
        escaped.expect_array_begin().unwrap();
        assert_eq!(escaped.expect_string().unwrap(), "\u{1F600}");
    }

    #[test]
    fn json_parser_rejects_lone_surrogates() {
        for bad in [br#"["\ud83d"]"#.as_slice(), br#"["\ude00"]"#] {
            assert_eq!(drain(bad), Err(ParserError::InvalidJson));
        }
    }
}
