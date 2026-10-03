//! Streaming index of the tarball integrity an npm packument publishes.
//!
//! A packument is passed through to clients byte for byte. While those same chunks are written to
//! the cache, [`NpmIndexScanner`] runs a pushdown JSON recognizer over them and records, for every
//! `versions.*.dist` object, the expectation (`dist.integrity`, `dist.size`) of the tarball path
//! that object names. A later tarball request is verified against that index instead of reparsing
//! the packument.
//!
//! Invariants:
//! - The whole document is checked against the JSON grammar, including UTF-8 validity, escape
//!   syntax, and surrogate pairing; [`NpmIndexScanner::finish`] only succeeds on exactly one
//!   complete value followed by whitespace.
//! - Memory does not grow with string length: only `tarball` (≤ [`MAX_LOCATION_BYTES`]),
//!   `integrity` (≤ [`MAX_INTEGRITY_BYTES`]), and object keys that may be relevant
//!   (≤ [`MAX_KEY_BYTES`]) are retained, in one reused buffer. Version names are hashed, never
//!   retained. Nesting is bounded by [`MAX_DEPTH`].
//! - Keys are compared after escape decoding, exactly as a JSON parser would compare them.
//! - Only entries whose tarball is an absolute URL on the packument's own canonical origin, with
//!   no query, fragment, or userinfo, whose path passes the mirror's npm path validation, and
//!   whose integrity carries a supported digest are indexed. Anything else is omitted, so a
//!   request for it finds no expectation and stays refused.
//! - Index keys are percent-decoded paths, so `/@scope%2fname/-/…` and `/@scope/name/-/…` name
//!   one entry; requests are looked up through the same validation and decoding.
//! - Ambiguity is refused rather than resolved: a duplicate `versions`, `dist`, `tarball`,
//!   `integrity`, or `size` key, a duplicate version name, or two different
//!   expectations for one tarball path fail the scan.

use std::{
    collections::{HashMap, HashSet, hash_map::Entry},
    mem,
};

use base64::{Engine as _, engine::general_purpose::STANDARD};
use cowshed_gateway_types::{CanonicalTarget, decode_percent};
use sha2::{Digest as _, Sha256};
use url::Url;

use crate::{
    cache::{CacheError, ObjectDigest, ObjectExpectation},
    mirror::{MAX_LOCATION_BYTES, validate_mirror_path},
};

/// Published expectation per same-origin tarball path: `Url::path` (never a query), checked by
/// `validate_mirror_path` and percent-decoded.
pub(crate) type NpmTarballIndex = HashMap<String, ObjectExpectation>;

/// Nesting bound; matches the recursion limit `serde_json` applied to packuments before.
const MAX_DEPTH: usize = 128;
/// Longest retained `dist.integrity`; one sha512 SRI token is 95 bytes.
const MAX_INTEGRITY_BYTES: usize = 4 * 1024;
/// Longest retained key; every key the index reads (`versions`, `dist`, `tarball`, `integrity`,
/// `size`) is shorter.
const MAX_KEY_BYTES: usize = 16;

/// Pushdown recognizer over a packument byte stream. Feed it every body chunk with
/// [`push`](Self::push), then call [`finish`](Self::finish) at end of body. Once any push fails the
/// scanner stays failed.
pub(crate) struct NpmIndexScanner {
    target: CanonicalTarget,
    index: NpmTarballIndex,
    /// SHA-256 of every decoded version name, so later duplicates cannot erase integrity.
    version_names: HashSet<[u8; 32]>,
    stack: Vec<Frame>,
    expect: Expect,
    /// Meaning of the value about to start; assigned at each key, consumed at value start.
    slot: Slot,
    lex: Lex,
    string: Str,
    /// Decoded bytes of the current string, up to `string.limit`; reused across strings.
    text: Vec<u8>,
    failed: bool,
}

impl NpmIndexScanner {
    pub(crate) fn new(target: CanonicalTarget) -> Self {
        Self {
            target,
            index: NpmTarballIndex::new(),
            version_names: HashSet::new(),
            stack: Vec::new(),
            expect: Expect::Value,
            slot: Slot::Root,
            lex: Lex::Between,
            string: Str::new(StrKind::IgnoredKey, 0),
            text: Vec::new(),
            failed: false,
        }
    }

    pub(crate) fn push(&mut self, chunk: &[u8]) -> Result<(), CacheError> {
        if self.failed {
            return Err(CacheError::InvalidMetadata);
        }
        let scanned = self.scan(chunk);
        self.failed = scanned.is_err();
        scanned
    }

    pub(crate) fn finish(mut self) -> Result<NpmTarballIndex, CacheError> {
        if self.failed {
            return Err(CacheError::InvalidMetadata);
        }
        if let Lex::Number(number) = self.lex {
            if !number.complete() {
                return Err(CacheError::InvalidMetadata);
            }
            self.end_number(number);
        }
        if matches!(self.lex, Lex::Between) && self.expect == Expect::Done {
            Ok(self.index)
        } else {
            Err(CacheError::InvalidMetadata)
        }
    }

    fn scan(&mut self, mut input: &[u8]) -> Result<(), CacheError> {
        while let Some(&byte) = input.first() {
            match self.lex {
                Lex::String => {
                    let (consumed, closed) = self.string.scan(&mut self.text, input)?;
                    input = &input[consumed..];
                    if closed {
                        self.lex = Lex::Between;
                        self.end_string()?;
                    }
                    continue;
                }
                Lex::Literal(rest) => self.literal(rest, byte)?,
                Lex::Number(mut number) => {
                    if number.accept(byte)? {
                        self.lex = Lex::Number(number);
                    } else {
                        self.end_number(number);
                        self.structural(byte)?;
                    }
                }
                Lex::Between => self.structural(byte)?,
            }
            input = &input[1..];
        }
        Ok(())
    }

    fn structural(&mut self, byte: u8) -> Result<(), CacheError> {
        if matches!(byte, b' ' | b'\t' | b'\n' | b'\r') {
            return Ok(());
        }
        match self.expect {
            Expect::ValueOrClose if byte == b']' => self.close(byte),
            Expect::Value | Expect::ValueOrClose => self.value_start(byte),
            Expect::KeyOrClose if byte == b'}' => self.close(byte),
            Expect::Key | Expect::KeyOrClose if byte == b'"' => {
                self.key_start();
                Ok(())
            }
            Expect::Colon if byte == b':' => {
                self.expect = Expect::Value;
                Ok(())
            }
            Expect::CommaOrClose => match (byte, self.stack.last()) {
                (b',', Some(Frame::Object(_))) => {
                    self.expect = Expect::Key;
                    Ok(())
                }
                (b',', Some(Frame::Array)) => {
                    self.expect = Expect::Value;
                    Ok(())
                }
                (b'}' | b']', _) => self.close(byte),
                _ => Err(CacheError::InvalidMetadata),
            },
            Expect::Key | Expect::KeyOrClose | Expect::Colon | Expect::Done => {
                Err(CacheError::InvalidMetadata)
            }
        }
    }

    fn value_start(&mut self, byte: u8) -> Result<(), CacheError> {
        let slot = mem::replace(&mut self.slot, Slot::Ignore);
        match byte {
            b'{' => {
                let role = match slot {
                    Slot::Root => Role::Root { versions: false },
                    Slot::Versions => Role::Versions,
                    Slot::Version => Role::Version { dist: false },
                    Slot::Dist => Role::Dist(DistFields::default()),
                    Slot::Ignore | Slot::Tarball | Slot::Integrity | Slot::Size => Role::Other,
                };
                self.open(Frame::Object(role))?;
                self.expect = Expect::KeyOrClose;
            }
            b'[' => {
                self.open(Frame::Array)?;
                self.expect = Expect::ValueOrClose;
            }
            b'"' => {
                let limit = match slot {
                    Slot::Tarball => MAX_LOCATION_BYTES,
                    Slot::Integrity => MAX_INTEGRITY_BYTES,
                    _ => 0,
                };
                self.string_start(StrKind::Value(slot), limit);
            }
            b'-' | b'0'..=b'9' => {
                let mut number = Num::new(matches!(slot, Slot::Size));
                number.accept(byte)?;
                self.lex = Lex::Number(number);
            }
            b't' => self.lex = Lex::Literal(b"rue"),
            b'f' => self.lex = Lex::Literal(b"alse"),
            b'n' => self.lex = Lex::Literal(b"ull"),
            _ => return Err(CacheError::InvalidMetadata),
        }
        Ok(())
    }

    fn open(&mut self, frame: Frame) -> Result<(), CacheError> {
        if self.stack.len() >= MAX_DEPTH {
            return Err(CacheError::InvalidMetadata);
        }
        self.stack.push(frame);
        Ok(())
    }

    fn close(&mut self, byte: u8) -> Result<(), CacheError> {
        match (self.stack.pop(), byte) {
            (Some(Frame::Object(Role::Dist(fields))), b'}') => {
                self.index_dist(fields)?;
            }
            (Some(Frame::Object(_)), b'}') | (Some(Frame::Array), b']') => {}
            _ => return Err(CacheError::InvalidMetadata),
        }
        self.value_end();
        Ok(())
    }

    fn literal(&mut self, rest: &'static [u8], byte: u8) -> Result<(), CacheError> {
        match rest.split_first() {
            Some((&expected, tail)) if expected == byte => {
                if tail.is_empty() {
                    self.lex = Lex::Between;
                    self.value_end();
                } else {
                    self.lex = Lex::Literal(tail);
                }
                Ok(())
            }
            _ => Err(CacheError::InvalidMetadata),
        }
    }

    fn end_number(&mut self, number: Num) {
        self.lex = Lex::Between;
        if number.capture
            && let Some(length) = number.integer
        {
            dist_fields(&mut self.stack).size = Field::Value(length);
        }
        self.value_end();
    }

    fn key_start(&mut self) {
        let kind = match self.stack.last() {
            Some(Frame::Object(Role::Versions)) => StrKind::VersionKey(Sha256::new()),
            Some(Frame::Object(Role::Root { .. } | Role::Version { .. } | Role::Dist(_))) => {
                StrKind::Key
            }
            Some(Frame::Object(Role::Other) | Frame::Array) | None => StrKind::IgnoredKey,
        };
        let limit = match kind {
            StrKind::Key => MAX_KEY_BYTES,
            StrKind::IgnoredKey | StrKind::VersionKey(_) | StrKind::Value(_) => 0,
        };
        self.string_start(kind, limit);
    }

    fn string_start(&mut self, kind: StrKind, limit: usize) {
        self.text.clear();
        self.string = Str::new(kind, limit);
        self.lex = Lex::String;
    }

    fn end_string(&mut self) -> Result<(), CacheError> {
        let kind = mem::replace(&mut self.string.kind, StrKind::IgnoredKey);
        let text = (!self.string.overflow).then_some(self.text.as_slice());
        match kind {
            StrKind::IgnoredKey => self.key_end(Slot::Ignore),
            StrKind::Key => {
                let slot = key_slot(self.stack.last_mut(), text)?;
                self.key_end(slot);
            }
            StrKind::VersionKey(hasher) => {
                if !self.version_names.insert(hasher.finalize().into()) {
                    return Err(CacheError::InvalidMetadata);
                }
                self.key_end(Slot::Version);
            }
            StrKind::Value(slot) => {
                let field = match slot {
                    Slot::Tarball => Some(&mut dist_fields(&mut self.stack).tarball),
                    Slot::Integrity => Some(&mut dist_fields(&mut self.stack).integrity),
                    _ => None,
                };
                if let (Some(field), Some(value)) = (
                    field,
                    text.and_then(|bytes| std::str::from_utf8(bytes).ok()),
                ) {
                    *field = Field::Value(value.to_owned());
                }
                self.value_end();
            }
        }
        Ok(())
    }

    fn key_end(&mut self, slot: Slot) {
        self.slot = slot;
        self.expect = Expect::Colon;
    }

    fn value_end(&mut self) {
        self.expect = if self.stack.is_empty() {
            Expect::Done
        } else {
            Expect::CommaOrClose
        };
    }

    fn index_dist(&mut self, fields: DistFields) -> Result<(), CacheError> {
        let (Field::Value(tarball), Field::Value(integrity)) = (fields.tarball, fields.integrity)
        else {
            return Ok(());
        };
        let length = match fields.size {
            Field::Absent => 0,
            Field::Value(length) => length,
            Field::Unusable => return Ok(()),
        };
        let (Some(path), Some(digest)) = (
            same_origin_path(&self.target, &tarball),
            parse_sri(&integrity),
        ) else {
            return Ok(());
        };
        let expectation = ObjectExpectation { length, digest };
        match self.index.entry(path) {
            Entry::Vacant(entry) => {
                entry.insert(expectation);
                Ok(())
            }
            Entry::Occupied(entry) if *entry.get() == expectation => Ok(()),
            Entry::Occupied(_) => Err(CacheError::InvalidMetadata),
        }
    }
}

/// The digest to verify a tarball against from an SRI string: sha512 when present, else sha256.
/// Other algorithms are ignored; a malformed sha256/sha512 token or two different digests of the
/// chosen algorithm make the whole value unusable.
pub(crate) fn parse_sri(value: &str) -> Option<ObjectDigest> {
    let mut sha256 = None;
    let mut sha512 = None;
    for token in value.split_ascii_whitespace() {
        let token = token.split_once('?').map_or(token, |(hash, _)| hash);
        match token.split_once('-') {
            Some(("sha512", encoded)) => agree(&mut sha512, decode_digest(encoded)?)?,
            Some(("sha256", encoded)) => agree(&mut sha256, decode_digest(encoded)?)?,
            _ => {}
        }
    }
    sha512
        .map(ObjectDigest::Sha512)
        .or(sha256.map(ObjectDigest::Sha256))
}

fn decode_digest<const N: usize>(encoded: &str) -> Option<[u8; N]> {
    STANDARD.decode(encoded).ok()?.try_into().ok()
}

fn agree<const N: usize>(slot: &mut Option<[u8; N]>, digest: [u8; N]) -> Option<()> {
    match slot {
        Some(existing) if *existing != digest => None,
        _ => {
            *slot = Some(digest);
            Some(())
        }
    }
}

/// The canonical request path `tarball` is served at, when it is a plain absolute URL on `target`
/// whose path the mirror would accept.
fn same_origin_path(target: &CanonicalTarget, tarball: &str) -> Option<String> {
    let url = Url::parse(tarball).ok()?;
    if url.query().is_some()
        || url.fragment().is_some()
        || !url.username().is_empty()
        || url.password().is_some()
        || CanonicalTarget::from_url(&url).ok()? != *target
    {
        return None;
    }
    validate_mirror_path(url.path()).ok()?;
    decode_percent(url.path(), |_| false).ok()
}

/// What a key means in its object, given the object's role and the decoded key (`None` when the
/// key exceeded [`MAX_KEY_BYTES`] and so names nothing relevant).
fn key_slot(frame: Option<&mut Frame>, name: Option<&[u8]>) -> Result<Slot, CacheError> {
    let Some(Frame::Object(role)) = frame else {
        return Ok(Slot::Ignore);
    };
    match (role, name) {
        (Role::Root { versions }, Some(b"versions")) => {
            claim_flag(versions).map(|()| Slot::Versions)
        }
        (Role::Version { dist, .. }, Some(b"dist")) => claim_flag(dist).map(|()| Slot::Dist),
        (Role::Dist(fields), Some(b"tarball")) => {
            claim(&mut fields.tarball).map(|()| Slot::Tarball)
        }
        (Role::Dist(fields), Some(b"integrity")) => {
            claim(&mut fields.integrity).map(|()| Slot::Integrity)
        }
        (Role::Dist(fields), Some(b"size")) => claim(&mut fields.size).map(|()| Slot::Size),
        _ => Ok(Slot::Ignore),
    }
}

fn claim_flag(seen: &mut bool) -> Result<(), CacheError> {
    if mem::replace(seen, true) {
        Err(CacheError::InvalidMetadata)
    } else {
        Ok(())
    }
}

/// Marks a dist field as present; its value becomes usable only if it completes with the right
/// type.
fn claim<T>(field: &mut Field<T>) -> Result<(), CacheError> {
    match field {
        Field::Absent => {
            *field = Field::Unusable;
            Ok(())
        }
        Field::Unusable | Field::Value(_) => Err(CacheError::InvalidMetadata),
    }
}

/// The open `dist` object a `tarball`/`integrity`/`size` slot was assigned in. Those slots are
/// only produced by keys of a `dist` object and consumed by the value directly following them,
/// before any other frame can open.
fn dist_fields(stack: &mut [Frame]) -> &mut DistFields {
    match stack.last_mut() {
        Some(Frame::Object(Role::Dist(fields))) => fields,
        _ => unreachable!("dist field slot outside a dist object"),
    }
}

enum Frame {
    Array,
    Object(Role),
}

enum Role {
    /// The document's top-level object.
    Root {
        versions: bool,
    },
    /// `versions`: keys are version names.
    Versions,
    /// `versions.<name>`.
    Version {
        dist: bool,
    },
    /// `versions.<name>.dist`.
    Dist(DistFields),
    Other,
}

#[derive(Default)]
struct DistFields {
    tarball: Field<String>,
    integrity: Field<String>,
    size: Field<u64>,
}

#[derive(Default)]
enum Field<T> {
    #[default]
    Absent,
    /// Present, but not a value of the required type (or longer than its retention bound).
    Unusable,
    Value(T),
}

#[derive(Clone, Copy)]
enum Slot {
    Root,
    Ignore,
    Versions,
    Version,
    Dist,
    Tarball,
    Integrity,
    Size,
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Expect {
    Value,
    /// Directly after `[`.
    ValueOrClose,
    Key,
    /// Directly after `{`.
    KeyOrClose,
    Colon,
    CommaOrClose,
    /// The top-level value is complete; only whitespace may follow.
    Done,
}

#[derive(Clone, Copy)]
enum Lex {
    Between,
    /// Inside a string; its state is `NpmIndexScanner::string`.
    String,
    Number(Num),
    /// The remaining bytes of `true`, `false`, or `null`.
    Literal(&'static [u8]),
}

enum StrKind {
    /// A key of an object whose keys are never relevant.
    IgnoredKey,
    /// A key compared against the relevant names.
    Key,
    /// A version name, hashed instead of retained.
    VersionKey(Sha256),
    Value(Slot),
}

struct Str {
    kind: StrKind,
    limit: usize,
    /// The decoded string exceeded `limit`; what was retained is a prefix and means nothing.
    overflow: bool,
    escape: Escape,
    utf8: Utf8,
}

impl Str {
    fn new(kind: StrKind, limit: usize) -> Self {
        Self {
            kind,
            limit,
            overflow: false,
            escape: Escape::None,
            utf8: Utf8::IDLE,
        }
    }

    /// Consumes string content from `input`; returns the bytes consumed and whether the closing
    /// quote was among them.
    fn scan(&mut self, text: &mut Vec<u8>, input: &[u8]) -> Result<(usize, bool), CacheError> {
        let mut index = 0;
        loop {
            if self.escape == Escape::None && self.utf8.pending == 0 {
                // Bulk path: everything up to the next quote, backslash, or control byte, validated
                // as UTF-8 in one pass. A sequence cut off by the run's end falls through to the
                // byte-wise validator, which carries it across chunks.
                let rest = &input[index..];
                let run = rest
                    .iter()
                    .position(|&byte| byte == b'"' || byte == b'\\' || byte < 0x20)
                    .unwrap_or(rest.len());
                let valid = match std::str::from_utf8(&rest[..run]) {
                    Ok(_) => run,
                    Err(error) if error.error_len().is_none() => error.valid_up_to(),
                    Err(_) => return Err(CacheError::InvalidMetadata),
                };
                self.emit(text, &rest[..valid]);
                index += valid;
            }
            let Some(&byte) = input.get(index) else {
                return Ok((index, false));
            };
            index += 1;
            if self.utf8.pending > 0 {
                self.utf8.continuation(byte)?;
                self.emit(text, &[byte]);
                continue;
            }
            self.escape = match self.escape {
                Escape::None => match byte {
                    b'"' => return Ok((index, true)),
                    b'\\' => Escape::Backslash,
                    0x80..=0xFF => {
                        self.utf8.lead(byte)?;
                        self.emit(text, &[byte]);
                        Escape::None
                    }
                    _ => return Err(CacheError::InvalidMetadata),
                },
                Escape::Backslash => {
                    let decoded = match byte {
                        b'"' => b'"',
                        b'\\' => b'\\',
                        b'/' => b'/',
                        b'b' => 0x08,
                        b'f' => 0x0C,
                        b'n' => b'\n',
                        b'r' => b'\r',
                        b't' => b'\t',
                        b'u' => {
                            self.escape = Escape::Unicode {
                                high: None,
                                value: 0,
                                digits: 0,
                            };
                            continue;
                        }
                        _ => return Err(CacheError::InvalidMetadata),
                    };
                    self.emit(text, &[decoded]);
                    Escape::None
                }
                Escape::Unicode {
                    high,
                    value,
                    digits,
                } => {
                    let value = (value << 4) | u16::from(hex(byte)?);
                    if digits < 3 {
                        Escape::Unicode {
                            high,
                            value,
                            digits: digits + 1,
                        }
                    } else {
                        match (high, value) {
                            (None, 0xD800..=0xDBFF) => Escape::LowBackslash(value),
                            (None, 0xDC00..=0xDFFF) => return Err(CacheError::InvalidMetadata),
                            (None, _) => {
                                self.emit_char(text, u32::from(value))?;
                                Escape::None
                            }
                            (Some(high), 0xDC00..=0xDFFF) => {
                                let scalar = 0x10000
                                    + ((u32::from(high) - 0xD800) << 10)
                                    + (u32::from(value) - 0xDC00);
                                self.emit_char(text, scalar)?;
                                Escape::None
                            }
                            (Some(_), _) => return Err(CacheError::InvalidMetadata),
                        }
                    }
                }
                Escape::LowBackslash(high) if byte == b'\\' => Escape::LowU(high),
                Escape::LowU(high) if byte == b'u' => Escape::Unicode {
                    high: Some(high),
                    value: 0,
                    digits: 0,
                },
                Escape::LowBackslash(_) | Escape::LowU(_) => {
                    return Err(CacheError::InvalidMetadata);
                }
            };
        }
    }

    fn emit(&mut self, text: &mut Vec<u8>, bytes: &[u8]) {
        if bytes.is_empty() {
            return;
        }
        match &mut self.kind {
            StrKind::VersionKey(hasher) => hasher.update(bytes),
            _ if self.overflow => {}
            _ if text.len() + bytes.len() <= self.limit => text.extend_from_slice(bytes),
            _ => self.overflow = true,
        }
    }

    fn emit_char(&mut self, text: &mut Vec<u8>, scalar: u32) -> Result<(), CacheError> {
        let decoded = char::from_u32(scalar).ok_or(CacheError::InvalidMetadata)?;
        let mut buffer = [0; 4];
        self.emit(text, decoded.encode_utf8(&mut buffer).as_bytes());
        Ok(())
    }
}

fn hex(byte: u8) -> Result<u8, CacheError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(CacheError::InvalidMetadata),
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
enum Escape {
    None,
    Backslash,
    /// `\u` and `digits` hex digits of `value`; `high` is the preceding high surrogate when this
    /// escape must be its low half.
    Unicode {
        high: Option<u16>,
        value: u16,
        digits: u8,
    },
    /// A high surrogate escape that must be followed by `\u`.
    LowBackslash(u16),
    LowU(u16),
}

/// Incremental UTF-8 validation (RFC 3629): rejects overlongs, surrogates, and values above
/// U+10FFFF, across chunk boundaries.
#[derive(Clone, Copy)]
struct Utf8 {
    pending: u8,
    low: u8,
    high: u8,
}

impl Utf8 {
    const IDLE: Self = Self {
        pending: 0,
        low: 0x80,
        high: 0xBF,
    };

    fn lead(&mut self, byte: u8) -> Result<(), CacheError> {
        let (pending, low, high) = match byte {
            0xC2..=0xDF => (1, 0x80, 0xBF),
            0xE0 => (2, 0xA0, 0xBF),
            0xE1..=0xEC | 0xEE..=0xEF => (2, 0x80, 0xBF),
            0xED => (2, 0x80, 0x9F),
            0xF0 => (3, 0x90, 0xBF),
            0xF1..=0xF3 => (3, 0x80, 0xBF),
            0xF4 => (3, 0x80, 0x8F),
            _ => return Err(CacheError::InvalidMetadata),
        };
        *self = Self { pending, low, high };
        Ok(())
    }

    fn continuation(&mut self, byte: u8) -> Result<(), CacheError> {
        if !(self.low..=self.high).contains(&byte) {
            return Err(CacheError::InvalidMetadata);
        }
        *self = Self {
            pending: self.pending - 1,
            ..Self::IDLE
        };
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum NumPhase {
    Start,
    Minus,
    Zero,
    Int,
    Dot,
    Frac,
    Exp,
    ExpSign,
    ExpDigits,
}

/// A JSON number in progress. `integer` tracks its value while it is a non-negative integer that
/// fits a `u64`; it is read only when the number is a `dist.size`.
#[derive(Clone, Copy)]
struct Num {
    phase: NumPhase,
    capture: bool,
    integer: Option<u64>,
}

impl Num {
    fn new(capture: bool) -> Self {
        Self {
            phase: NumPhase::Start,
            capture,
            integer: Some(0),
        }
    }

    /// `Ok(true)` when `byte` continues the number, `Ok(false)` when it ends a complete number
    /// (and must be read as the next token).
    fn accept(&mut self, byte: u8) -> Result<bool, CacheError> {
        let phase = match (self.phase, byte) {
            (NumPhase::Start, b'-') => NumPhase::Minus,
            (NumPhase::Start | NumPhase::Minus, b'0') => NumPhase::Zero,
            (NumPhase::Start | NumPhase::Minus, b'1'..=b'9') | (NumPhase::Int, b'0'..=b'9') => {
                NumPhase::Int
            }
            (NumPhase::Zero | NumPhase::Int, b'.') => NumPhase::Dot,
            (NumPhase::Dot | NumPhase::Frac, b'0'..=b'9') => NumPhase::Frac,
            (NumPhase::Zero | NumPhase::Int | NumPhase::Frac, b'e' | b'E') => NumPhase::Exp,
            (NumPhase::Exp, b'+' | b'-') => NumPhase::ExpSign,
            (NumPhase::Exp | NumPhase::ExpSign | NumPhase::ExpDigits, b'0'..=b'9') => {
                NumPhase::ExpDigits
            }
            (NumPhase::Zero | NumPhase::Int | NumPhase::Frac | NumPhase::ExpDigits, _) => {
                return Ok(false);
            }
            _ => return Err(CacheError::InvalidMetadata),
        };
        self.integer = match phase {
            NumPhase::Zero | NumPhase::Int => self
                .integer
                .and_then(|value| value.checked_mul(10)?.checked_add(u64::from(byte - b'0'))),
            _ => None,
        };
        self.phase = phase;
        Ok(true)
    }

    fn complete(self) -> bool {
        matches!(
            self.phase,
            NumPhase::Zero | NumPhase::Int | NumPhase::Frac | NumPhase::ExpDigits
        )
    }
}

#[cfg(test)]
mod tests {
    use sha2::Sha512;

    use super::*;

    const REGISTRY: &str = "https://registry.npmjs.org";

    fn target() -> CanonicalTarget {
        CanonicalTarget::from_url(&Url::parse(REGISTRY).unwrap()).unwrap()
    }

    fn sha512(seed: &[u8]) -> [u8; 64] {
        Sha512::digest(seed).into()
    }

    fn sha256(seed: &[u8]) -> [u8; 32] {
        Sha256::digest(seed).into()
    }

    fn sri(algorithm: &str, digest: &[u8]) -> String {
        format!("{algorithm}-{}", STANDARD.encode(digest))
    }

    fn scan_chunks<'a>(
        chunks: impl IntoIterator<Item = &'a [u8]>,
    ) -> Result<NpmTarballIndex, CacheError> {
        let mut scanner = NpmIndexScanner::new(target());
        for chunk in chunks {
            scanner.push(chunk)?;
        }
        scanner.finish()
    }

    fn scan(document: &[u8]) -> Result<NpmTarballIndex, CacheError> {
        scan_chunks([document])
    }

    /// A packument with one version whose `dist` body is `dist`.
    fn one_version(dist: &str) -> String {
        format!(r#"{{"name":"left-pad","versions":{{"1.0.0":{{"dist":{{{dist}}}}}}}}}"#)
    }

    fn tarball_dist(tarball: &str, integrity: &str) -> String {
        format!(r#""tarball":"{tarball}","integrity":"{integrity}""#)
    }

    fn left_pad(version: &str) -> String {
        format!("{REGISTRY}/left-pad/-/left-pad-{version}.tgz")
    }

    /// Escaped keys and URLs, raw multi-byte UTF-8, surrogate-pair escapes, fields in every order,
    /// and relevant-looking keys in irrelevant positions.
    fn rich_document() -> (String, NpmTarballIndex) {
        let first = sri("sha512", &sha512(b"1.0.0"));
        let second = format!(
            "sha256-{} sha512-{}?opt",
            STANDARD.encode(sha256(b"x")),
            STANDARD.encode(sha512(b"1.1.0"))
        );
        let third = sri("sha256", &sha256(b"2.0.0"));
        let document = format!(
            r#"{{
  "_id": "left-pad",
  "description": "pads \"strings\" — café 😀 \ud83d\ude00 \u00e9\\\/\b\f\n\r\t",
  "dist-tags": {{"latest": "2.0.0", "versions": {{"dist": 1}}}},
  "\u0076ersions": {{
    "1.0.0": {{
      "name": "left-pad",
      "dist": {{
        "integrity": "{first}",
        "t\u0061rball": "https:\/\/registry.npmjs.org\/left-pad\/-\/left-pad-1.0.0.tgz",
        "size": 1234
      }}
    }},
    "1.1.0-\ud83d\ude00": {{
      "dist": {{"shasum": "abc", "integrity": "{second}", "tarball": "https://REGISTRY.npmjs.org:443/left-pad/-/left-pad-1.1.0.tgz", "unpackedSize": 9e3}},
      "scripts": {{"dist": {{"tarball": "{REGISTRY}/evil.tgz", "integrity": "{first}"}}}}
    }},
    "2.0.0": {{"deprecated": null, "dist": {{"size": 0, "tarball": "{REGISTRY}/left-pad/-/left-pad-2.0.0.tgz", "integrity": "{third}", "signatures": [{{"sig": true}}, -1.5e-3]}}}}
  }},
  "time": {{"1.0.0": "2016-01-01"}},
  "versions_": false
}}
"#
        );
        let index = NpmTarballIndex::from([
            (
                "/left-pad/-/left-pad-1.0.0.tgz".to_owned(),
                ObjectExpectation {
                    length: 1234,
                    digest: ObjectDigest::Sha512(sha512(b"1.0.0")),
                },
            ),
            (
                "/left-pad/-/left-pad-1.1.0.tgz".to_owned(),
                ObjectExpectation {
                    length: 0,
                    digest: ObjectDigest::Sha512(sha512(b"1.1.0")),
                },
            ),
            (
                "/left-pad/-/left-pad-2.0.0.tgz".to_owned(),
                ObjectExpectation {
                    length: 0,
                    digest: ObjectDigest::Sha256(sha256(b"2.0.0")),
                },
            ),
        ]);
        (document, index)
    }

    #[test]
    fn indexes_published_tarballs_of_a_rich_document() {
        let (document, expected) = rich_document();
        assert_eq!(scan(document.as_bytes()).unwrap(), expected);
    }

    #[test]
    fn every_chunk_boundary_yields_the_same_index() {
        let (document, expected) = rich_document();
        let bytes = document.as_bytes();
        for split in 0..=bytes.len() {
            let (left, right) = bytes.split_at(split);
            assert_eq!(
                scan_chunks([left, right]).unwrap(),
                expected,
                "split at {split}"
            );
        }
        for (first, second) in [(1, 2), (3, 5), (7, 11)] {
            let chunks = bytes.chunks(first).flat_map(|chunk| chunk.chunks(second));
            assert_eq!(scan_chunks(chunks).unwrap(), expected);
        }
        assert_eq!(scan_chunks(bytes.chunks(1)).unwrap(), expected);
    }

    #[test]
    fn non_packument_values_are_valid_but_index_nothing() {
        for document in [
            &b" {} "[..],
            b"[]",
            b"123",
            b"-0.5e+7",
            b"\"versions\"",
            b"true",
            b"null",
            br#"{"versions":null}"#,
            br#"{"versions":[{"dist":{}}]}"#,
            br#"{"versions":{"1.0.0":"dist"}}"#,
            br#"{"versions":{"1.0.0":{"dist":[]}}}"#,
            br#"{"other":{"versions":{"1.0.0":{"dist":{"tarball":"https://registry.npmjs.org/a.tgz","integrity":"sha256-47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="}}}}}"#,
        ] {
            assert!(scan(document).unwrap().is_empty(), "{document:?}");
        }
    }

    #[test]
    fn malformed_or_incomplete_json_is_refused() {
        for document in [
            &b""[..],
            b"   ",
            b"{",
            br#"{"versions":{}"#,
            br#"{"versions":{}} x"#,
            br#"{"versions":{}}{}"#,
            br#"{"a":1}}"#,
            br#"{"a":1,}"#,
            b"[1,]",
            b"[,1]",
            br#"{"a" 1}"#,
            br#"{"a":1 "b":2}"#,
            br#"{"a"}"#,
            br#"{1:2}"#,
            br#"{'a':1}"#,
            br#"{"a":[}"#,
            br#"{"a":]}"#,
            br#"{"a":01}"#,
            br#"{"a":1.}"#,
            br#"{"a":.5}"#,
            br#"{"a":-}"#,
            br#"{"a":1e}"#,
            br#"{"a":1e+}"#,
            br#"{"a":+1}"#,
            b"1.",
            b"-",
            br#"{"a":tru}"#,
            br#"{"a":nul"#,
            b"nulll",
            br#"{"a":True}"#,
            br#""unterminated"#,
            br#"{"a":"\x"}"#,
            br#"{"a":"\u12"}"#,
            br#"{"a":"\u12g4"}"#,
            br#"{"a":"\ud800"}"#,
            br#"{"a":"\ud800x"}"#,
            br#"{"a":"\ud800\n"}"#,
            br#"{"a":"\ud800\u0041"}"#,
            br#"{"a":"\udc00"}"#,
            b"{\"a\":\"\n\"}",
            b"{\"a\":\"\x00\"}",
            b"{\"a\":\"\xc0\x80\"}",
            b"{\"a\":\"\xed\xa0\x80\"}",
            b"{\"a\":\"\xf4\x90\x80\x80\"}",
            b"{\"a\":\"\xf5\"}",
            b"{\"a\":\"\xe2\x82\"}",
            b"{\"a\":\"\x80\"}",
            b"\xef\xbb\xbf{}",
            b"{\"a\":1}\x00",
        ] {
            assert!(
                matches!(scan(document), Err(CacheError::InvalidMetadata)),
                "{:?}",
                String::from_utf8_lossy(document)
            );
        }
    }

    #[test]
    fn a_failed_scan_stays_failed() {
        let mut scanner = NpmIndexScanner::new(target());
        assert!(scanner.push(b"{]").is_err());
        assert!(scanner.push(b"").is_err());
        assert!(scanner.push(b"}").is_err());
        assert!(matches!(scanner.finish(), Err(CacheError::InvalidMetadata)));
    }

    #[test]
    fn nesting_is_bounded() {
        let nested = |depth: usize| format!("{}{}", "[".repeat(depth), "]".repeat(depth));
        assert!(scan(nested(MAX_DEPTH).as_bytes()).is_ok());
        assert!(scan(nested(MAX_DEPTH + 1).as_bytes()).is_err());
    }

    #[test]
    fn duplicate_relevant_keys_are_refused() {
        let integrity = sri("sha512", &sha512(b"a"));
        let dist = tarball_dist(&left_pad("1.0.0"), &integrity);
        let other_dist = tarball_dist(&left_pad("1.0.1"), &integrity);
        for document in [
            r#"{"versions":{},"versions":{}}"#.to_owned(),
            format!(r#"{{"versions":1,"versions":{{"1.0.0":{{"dist":{{{dist}}}}}}}}}"#),
            r#"{"versions":{},"\u0076ersions":{}}"#.to_owned(),
            format!(r#"{{"versions":{{"1.0.0":{{"dist":{{{dist}}},"dist":{{{dist}}}}}}}}}"#),
            format!(r#"{{"versions":{{"1.0.0":{{"dist":null,"dist":{{{dist}}}}}}}}}"#),
            one_version(&format!(r#"{dist},"tarball":"{}""#, left_pad("1.0.0"))),
            one_version(&format!(r#"{dist},"t\u0061rball":7"#)),
            one_version(&format!(r#"{dist},"integrity":"{integrity}""#)),
            one_version(&format!(r#"{dist},"size":1,"size":1"#)),
            format!(
                r#"{{"versions":{{"1.0.0":{{"dist":{{{dist}}}}},"1.0.0":{{"dist":{{{other_dist}}}}}}}}}"#
            ),
            format!(
                r#"{{"versions":{{"1.0.0":{{"dist":{{{dist}}}}},"1.0\u002e0":{{"dist":{{{dist}}}}}}}}}"#
            ),
            format!(r#"{{"versions":{{"1.0.0":{{"dist":{{{dist}}}}},"1.0.0":null}}}}"#),
            format!(r#"{{"versions":{{"1.0.0":{{"dist":{{{dist}}}}},"1.0.0":{{}}}}}}"#),
            format!(r#"{{"versions":{{"1.0.0":{{}},"1.0.0":{{"dist":{{{dist}}}}}}}}}"#),
        ] {
            assert!(
                matches!(scan(document.as_bytes()), Err(CacheError::InvalidMetadata)),
                "{document}"
            );
        }
    }

    #[test]
    fn duplicates_outside_indexed_fields_are_ignored() {
        let integrity = sri("sha512", &sha512(b"a"));
        let dist = tarball_dist(&left_pad("1.0.0"), &integrity);
        for document in [
            format!(r#"{{"name":"a","name":"b","versions":{{"1.0.0":{{"dist":{{{dist}}}}}}}}}"#),
            format!(
                r#"{{"x":{{"versions":1,"versions":2}},"versions":{{"1.0.0":{{"dist":{{{dist}}}}}}}}}"#
            ),
            format!(
                r#"{{"versions":{{"1.0.0":{{"name":"a","name":"a","dist":{{"shasum":"a","shasum":"b",{dist}}}}}}}}}"#
            ),
        ] {
            assert_eq!(scan(document.as_bytes()).unwrap().len(), 1, "{document}");
        }
    }

    #[test]
    fn conflicting_expectations_for_one_tarball_are_refused() {
        let tarball = left_pad("1.0.0");
        let a = sri("sha512", &sha512(b"a"));
        let b = sri("sha512", &sha512(b"b"));
        let packument = |left: &str, right: &str| {
            format!(
                r#"{{"versions":{{"1.0.0":{{"dist":{{{left}}}}},"1.0.1":{{"dist":{{{right}}}}}}}}}"#
            )
        };
        let conflicting = packument(&tarball_dist(&tarball, &a), &tarball_dist(&tarball, &b));
        assert!(scan(conflicting.as_bytes()).is_err());
        let sized = packument(
            &format!(r#"{},"size":1"#, tarball_dist(&tarball, &a)),
            &format!(r#"{},"size":2"#, tarball_dist(&tarball, &a)),
        );
        assert!(scan(sized.as_bytes()).is_err());
        let agreeing = packument(&tarball_dist(&tarball, &a), &tarball_dist(&tarball, &a));
        assert_eq!(scan(agreeing.as_bytes()).unwrap().len(), 1);
    }

    #[test]
    fn only_plain_same_origin_tarball_urls_are_indexed() {
        let integrity = sri("sha512", &sha512(b"a"));
        for tarball in [
            "https://registry.npmjs.org.evil.example/left-pad/-/left-pad-1.0.0.tgz",
            "https://evil.example/left-pad/-/left-pad-1.0.0.tgz",
            "http://registry.npmjs.org/left-pad/-/left-pad-1.0.0.tgz",
            "https://registry.npmjs.org:8443/left-pad/-/left-pad-1.0.0.tgz",
            "https://registry.npmjs.org/left-pad/-/left-pad-1.0.0.tgz?token=1",
            "https://registry.npmjs.org/left-pad/-/left-pad-1.0.0.tgz?",
            "https://registry.npmjs.org/left-pad/-/left-pad-1.0.0.tgz#x",
            "https://user:secret@registry.npmjs.org/left-pad/-/left-pad-1.0.0.tgz",
            "https://user@registry.npmjs.org/left-pad/-/left-pad-1.0.0.tgz",
            "/left-pad/-/left-pad-1.0.0.tgz",
            "/left-pad/-/left-pad-1.0.0.tgz?integrity=x",
            "left-pad-1.0.0.tgz",
            "",
        ] {
            let document = one_version(&tarball_dist(tarball, &integrity));
            assert!(scan(document.as_bytes()).unwrap().is_empty(), "{tarball}");
        }
        let document = one_version(&tarball_dist(&left_pad("1.0.0"), &integrity));
        assert!(
            scan(document.as_bytes())
                .unwrap()
                .contains_key("/left-pad/-/left-pad-1.0.0.tgz")
        );
    }

    #[test]
    fn tarball_paths_are_validated_and_percent_decoded() {
        let a = sri("sha512", &sha512(b"a"));
        let b = sri("sha512", &sha512(b"b"));
        let encoded = format!("{REGISTRY}/@scope%2fpkg/-/pkg-1.0.0.tgz");
        let plain = format!("{REGISTRY}/@scope/pkg/-/pkg-1.0.0.tgz");
        let packument = |left: &str, right: &str| {
            format!(
                r#"{{"versions":{{"1.0.0":{{"dist":{{{left}}}}},"1.0.1":{{"dist":{{{right}}}}}}}}}"#
            )
        };
        let decoded = "/@scope/pkg/-/pkg-1.0.0.tgz";
        for tarball in [
            &encoded,
            &plain,
            &format!("{REGISTRY}/%40scope%2Fpkg/-/pkg-1.0.0.tgz"),
        ] {
            let index = scan(one_version(&tarball_dist(tarball, &a)).as_bytes()).unwrap();
            assert!(index.len() == 1 && index.contains_key(decoded), "{tarball}");
        }
        let agreeing = packument(&tarball_dist(&encoded, &a), &tarball_dist(&plain, &a));
        assert_eq!(scan(agreeing.as_bytes()).unwrap().len(), 1);
        let conflicting = packument(&tarball_dist(&encoded, &a), &tarball_dist(&plain, &b));
        assert!(scan(conflicting.as_bytes()).is_err());
        for tarball in [
            // Whole `%2e` segments are dot segments the URL parser already resolves; an encoded dot
            // anywhere else is refused like the request-side validation refuses it.
            "https://registry.npmjs.org/left-pad/-/left-pad-1%2e0.0.tgz",
            "https://registry.npmjs.org/left-pad/-/left-pad-1%2E0.0.tgz",
            "https://registry.npmjs.org/left-pad/-/left-pad%5c1.0.0.tgz",
            "https://registry.npmjs.org/left-pad/-/left-pad%001.0.0.tgz",
            "https://registry.npmjs.org/@scope%252fpkg/-/pkg-1.0.0.tgz",
            "https://registry.npmjs.org//left-pad/-/left-pad-1.0.0.tgz",
            "https://registry.npmjs.org/left-pad/-/left-pad-%zz.tgz",
        ] {
            let document = one_version(&tarball_dist(tarball, &a));
            assert!(scan(document.as_bytes()).unwrap().is_empty(), "{tarball}");
        }
    }

    #[test]
    fn entries_without_a_usable_digest_or_size_are_omitted() {
        let tarball = left_pad("1.0.0");
        let integrity = sri("sha512", &sha512(b"a"));
        for dist in [
            format!(r#""tarball":"{tarball}""#),
            format!(r#""integrity":"{integrity}""#),
            format!(r#""tarball":"{tarball}","shasum":"86f7e437faa5a7fce15d1ddcb9eaeaea377667b8""#),
            tarball_dist(&tarball, "sha1-hvfkN/qlp/zhXR3cuerq6jd2Z7g="),
            tarball_dist(&tarball, "sha512-not*base64"),
            tarball_dist(&tarball, &sri("sha512", &sha256(b"short"))),
            tarball_dist(&tarball, ""),
            format!(r#""tarball":"{tarball}","integrity":null"#),
            format!(r#""tarball":"{tarball}","integrity":["{integrity}"]"#),
            format!(r#""tarball":{{"url":"{tarball}"}},"integrity":"{integrity}""#),
            format!(r#"{},"size":"12""#, tarball_dist(&tarball, &integrity)),
            format!(r#"{},"size":-1"#, tarball_dist(&tarball, &integrity)),
            format!(r#"{},"size":-0"#, tarball_dist(&tarball, &integrity)),
            format!(r#"{},"size":1.0"#, tarball_dist(&tarball, &integrity)),
            format!(r#"{},"size":1e3"#, tarball_dist(&tarball, &integrity)),
            format!(
                r#"{},"size":18446744073709551616"#,
                tarball_dist(&tarball, &integrity)
            ),
            format!(r#"{},"size":null"#, tarball_dist(&tarball, &integrity)),
        ] {
            let document = one_version(&dist);
            assert!(scan(document.as_bytes()).unwrap().is_empty(), "{dist}");
        }
        let document = one_version(&format!(
            r#"{},"size":18446744073709551615"#,
            tarball_dist(&tarball, &integrity)
        ));
        assert_eq!(
            scan(document.as_bytes()).unwrap()["/left-pad/-/left-pad-1.0.0.tgz"].length,
            u64::MAX
        );
    }

    #[test]
    fn sri_prefers_sha512_and_refuses_ambiguity() {
        let strong = sha512(b"strong");
        let weak = sha256(b"weak");
        let sha512_sri = sri("sha512", &strong);
        let sha256_sri = sri("sha256", &weak);
        for value in [
            sha512_sri.clone(),
            format!("{sha256_sri} {sha512_sri}"),
            format!("{sha512_sri} {sha256_sri}"),
            format!("sha1-abc {sha512_sri}?opt md5-x"),
            format!("  {sha512_sri}\t{sha512_sri}\n"),
            format!("{sha512_sri} {}", sri("sha256", &sha256(b"other"))),
        ] {
            assert_eq!(
                parse_sri(&value),
                Some(ObjectDigest::Sha512(strong)),
                "{value}"
            );
        }
        assert_eq!(parse_sri(&sha256_sri), Some(ObjectDigest::Sha256(weak)));
        assert_eq!(
            parse_sri(&format!("sha1-abc {sha256_sri}")),
            Some(ObjectDigest::Sha256(weak))
        );
        for value in [
            String::new(),
            "sha1-hvfkN/qlp/zhXR3cuerq6jd2Z7g=".to_owned(),
            "sha512".to_owned(),
            "sha512-".to_owned(),
            "sha512-%%%".to_owned(),
            sri("sha512", &weak),
            sri("sha256", &strong),
            format!("{sha512_sri} {}", sri("sha512", &sha512(b"other"))),
            format!("{sha256_sri} {}", sri("sha256", &sha256(b"other"))),
            format!("{sha512_sri} sha256-broken"),
            "SHA512-".to_owned() + &STANDARD.encode(strong),
        ] {
            assert_eq!(parse_sri(&value), None, "{value}");
        }
    }

    #[test]
    fn long_keys_never_match_a_relevant_name() {
        let integrity = sri("sha512", &sha512(b"a"));
        let dist = tarball_dist(&left_pad("1.0.0"), &integrity);
        let mut scanner = NpmIndexScanner::new(target());
        scanner.push(br#"{"versions"#).unwrap();
        scanner.push(&[b'x'; 64]).unwrap();
        scanner
            .push(format!(r#"":{{"1.0.0":{{"dist":{{{dist}}}}}}}}}"#).as_bytes())
            .unwrap();
        assert!(scanner.finish().unwrap().is_empty());
    }

    #[test]
    fn oversized_tarball_urls_are_omitted_without_being_retained() {
        let integrity = sri("sha512", &sha512(b"a"));
        let long = format!("{REGISTRY}/{}.tgz", "a".repeat(MAX_LOCATION_BYTES));
        let mut scanner = NpmIndexScanner::new(target());
        let document = one_version(&tarball_dist(&long, &integrity));
        // Small chunks grow the retained prefix right up to the bound before it overflows.
        for chunk in document.as_bytes().chunks(1000) {
            scanner.push(chunk).unwrap();
        }
        assert!(scanner.text.capacity() <= 2 * MAX_LOCATION_BYTES);
        assert!(scanner.finish().unwrap().is_empty());
    }

    #[test]
    fn huge_irrelevant_strings_stream_in_bounded_memory() {
        const MIB: usize = 1024 * 1024;
        let integrity = sri("sha512", &sha512(b"a"));
        let dist = tarball_dist(&left_pad("1.0.0"), &integrity);
        let filler = "é".repeat(MIB / 2);
        let mut scanner = NpmIndexScanner::new(target());
        scanner.push(br#"{"readme":""#).unwrap();
        // 129 MiB of two-byte characters, with every chunk boundary splitting one of them.
        let (head, tail) = filler.as_bytes().split_at(1);
        scanner.push(head).unwrap();
        for _ in 0..129 {
            scanner.push(tail).unwrap();
            scanner.push(head).unwrap();
        }
        scanner.push(tail).unwrap();
        scanner.push(br#"",""#).unwrap();
        for _ in 0..4 {
            scanner.push(filler.as_bytes()).unwrap();
        }
        scanner.push(br#"":[],"versions":{""#).unwrap();
        for _ in 0..4 {
            scanner.push(filler.as_bytes()).unwrap();
        }
        scanner
            .push(format!(r#"":{{"dist":{{{dist}}}}}}}}}"#).as_bytes())
            .unwrap();
        assert!(scanner.text.capacity() < 1024);
        assert_eq!(scanner.finish().unwrap().len(), 1);
    }
}
