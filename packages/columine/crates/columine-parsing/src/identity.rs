//! Field identity (specs/columine/10-physical-vocabulary.md §6).
//!
//! An Arrow field may carry the metadata key `sb.fields`: a JSON array with
//! one object per declared field the column holds,
//! `{ "lib": text, "n": integer, "name": text, "type": text }`. A column's
//! entries share one type digest, and one `(lib, n)` names one column. A name
//! is display: nothing resolves a field by it.

use crate::json_parser::{JsonParser, ParserError, Token};

/// One declared field a column holds.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FieldIdentity {
    /// The declaring library.
    pub library: String,
    /// The field's number in its library.
    pub number: u32,
    /// The field's local name, for display.
    pub name: String,
    /// The digest of the field's type.
    pub type_digest: String,
}

/// Why a schema's field identities are refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentityError {
    /// The field's `sb.fields` value is not a non-empty array of identity
    /// objects.
    Malformed { field_index: usize },
    /// The field's entries carry two type digests.
    TwoTypes { field_index: usize },
    /// A `(lib, n)` pair this field carries already names an earlier entry,
    /// of this column or an earlier one.
    Repeated { field_index: usize },
    /// The decoded identities could not be backed.
    OutOfMemory,
}

/// Decode every field's `sb.fields` value, by field: `None` metadata is a
/// column that declares no identity.
pub fn decode_field_identities(
    metadata: &[Option<String>],
) -> Result<Vec<Vec<FieldIdentity>>, IdentityError> {
    let mut columns = Vec::new();
    room(&mut columns, metadata.len())?;
    let mut total = 0_usize;
    for (field_index, value) in metadata.iter().enumerate() {
        let entries = match value {
            Some(text) => decode_column(text, field_index)?,
            None => Vec::new(),
        };
        if entries
            .windows(2)
            .any(|pair| pair[0].type_digest != pair[1].type_digest)
        {
            return Err(IdentityError::TwoTypes { field_index });
        }
        total = total.saturating_add(entries.len());
        columns.push(entries);
    }
    // Every `(lib, n)` with its column, sorted: a repeat sits next to the
    // entry it repeats, and the earliest column at which one appears is the
    // one refused.
    let mut keys = Vec::new();
    room(&mut keys, total)?;
    for (field_index, entries) in columns.iter().enumerate() {
        for entry in entries {
            keys.push((entry.library.as_str(), entry.number, field_index));
        }
    }
    keys.sort_unstable();
    let repeated = keys
        .windows(2)
        .filter(|pair| (pair[0].0, pair[0].1) == (pair[1].0, pair[1].1))
        .map(|pair| pair[1].2)
        .min();
    if let Some(field_index) = repeated {
        return Err(IdentityError::Repeated { field_index });
    }
    Ok(columns)
}

/// Back `additional` more elements of `buf`, or refuse as out of memory.
fn room<T>(buf: &mut Vec<T>, additional: usize) -> Result<(), IdentityError> {
    buf.try_reserve_exact(additional)
        .map_err(|_| IdentityError::OutOfMemory)
}

/// A lexer refusal: memory the lexer could not back, else a malformed value.
fn lex(field_index: usize) -> impl Fn(ParserError) -> IdentityError {
    move |error| match error {
        ParserError::OutOfMemory => IdentityError::OutOfMemory,
        ParserError::InvalidJson
        | ParserError::UnexpectedToken
        | ParserError::EndOfInput
        | ParserError::InvalidNumber => IdentityError::Malformed { field_index },
    }
}

/// One field's identity array: at least one entry, and nothing after it.
fn decode_column(text: &str, field_index: usize) -> Result<Vec<FieldIdentity>, IdentityError> {
    let malformed = IdentityError::Malformed { field_index };
    let mut parser = JsonParser::new(text.as_bytes());
    parser.expect_array_begin().map_err(lex(field_index))?;
    let mut entries = Vec::new();
    while !parser.is_array_end() {
        room(&mut entries, 1)?;
        entries.push(decode_entry(&mut parser, field_index)?);
    }
    parser.next_token().map_err(lex(field_index))?;
    match parser.next_token() {
        Err(ParserError::EndOfInput) => {}
        Err(ParserError::OutOfMemory) => return Err(IdentityError::OutOfMemory),
        Ok(_)
        | Err(
            ParserError::InvalidJson | ParserError::UnexpectedToken | ParserError::InvalidNumber,
        ) => return Err(malformed),
    }
    if entries.is_empty() {
        return Err(malformed);
    }
    Ok(entries)
}

/// One identity object: exactly the keys `lib`, `n`, `name` and `type`, in
/// any order, `n` a non-negative integer that fits `u32`.
fn decode_entry(
    parser: &mut JsonParser<'_>,
    field_index: usize,
) -> Result<FieldIdentity, IdentityError> {
    let malformed = IdentityError::Malformed { field_index };
    parser.expect_object_begin().map_err(lex(field_index))?;
    let (mut library, mut number, mut name, mut type_digest) = (None, None, None, None);
    while !parser.is_object_end() {
        let key = parser.expect_field_name().map_err(lex(field_index))?;
        if key.is("n") {
            let Token::Number(digits) = parser.next_token().map_err(lex(field_index))? else {
                return Err(malformed);
            };
            if number
                .replace(digits.parse::<u32>().map_err(|_| malformed)?)
                .is_some()
            {
                return Err(malformed);
            }
            continue;
        }
        let slot = if key.is("lib") {
            &mut library
        } else if key.is("name") {
            &mut name
        } else if key.is("type") {
            &mut type_digest
        } else {
            return Err(malformed);
        };
        let Token::String(value) = parser.next_token().map_err(lex(field_index))? else {
            return Err(malformed);
        };
        if slot
            .replace(value.to_owned_text().map_err(lex(field_index))?)
            .is_some()
        {
            return Err(malformed);
        }
    }
    parser.next_token().map_err(lex(field_index))?;
    match (library, number, name, type_digest) {
        (Some(library), Some(number), Some(name), Some(type_digest)) => Ok(FieldIdentity {
            library,
            number,
            name,
            type_digest,
        }),
        _ => Err(malformed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(metadata: &[Option<&str>]) -> Result<Vec<Vec<FieldIdentity>>, IdentityError> {
        let owned: Vec<Option<String>> = metadata
            .iter()
            .map(|value| value.map(str::to_owned))
            .collect();
        decode_field_identities(&owned)
    }

    fn identity(library: &str, number: u32, name: &str, type_digest: &str) -> FieldIdentity {
        FieldIdentity {
            library: library.into(),
            number,
            name: name.into(),
            type_digest: type_digest.into(),
        }
    }

    #[test]
    fn identities_decode_in_any_key_order_and_absent_metadata_is_empty() {
        assert_eq!(
            decode(&[
                Some(
                    r#"[{"lib":"orders","n":3,"name":"total","type":"d1"},{"type":"d1","name":"sum","n":0,"lib":"billing"}]"#
                ),
                None,
                Some(r#" [ {"n":4294967295,"lib":"orders","type":"d2","name":"a\u00e9"} ] "#),
            ]),
            Ok(vec![
                vec![
                    identity("orders", 3, "total", "d1"),
                    identity("billing", 0, "sum", "d1"),
                ],
                vec![],
                vec![identity("orders", 4_294_967_295, "a\u{e9}", "d2")],
            ])
        );
    }

    #[test]
    fn a_value_that_is_not_an_array_of_identity_objects_is_refused() {
        for text in [
            "",
            "{}",
            "[]",
            r#"[{"lib":"a","n":1,"name":"x"}]"#,
            r#"[{"lib":"a","n":1,"name":"x","type":"d","extra":1}]"#,
            r#"[{"lib":"a","n":1,"name":"x","type":"d","lib":"b"}]"#,
            r#"[{"lib":"a","n":-1,"name":"x","type":"d"}]"#,
            r#"[{"lib":"a","n":1.0,"name":"x","type":"d"}]"#,
            r#"[{"lib":"a","n":4294967296,"name":"x","type":"d"}]"#,
            r#"[{"lib":"a","n":"1","name":"x","type":"d"}]"#,
            r#"[{"lib":1,"n":1,"name":"x","type":"d"}]"#,
            r#"[{"lib":"a","n":1,"name":"x","type":"d"}] []"#,
            r#"[{"lib":"a","n":1,"name":"x","type":"d"},]"#,
            r#"["a"]"#,
        ] {
            assert_eq!(
                decode(&[None, Some(text)]),
                Err(IdentityError::Malformed { field_index: 1 }),
                "{text}"
            );
        }
    }

    #[test]
    fn a_column_with_two_type_digests_is_refused() {
        assert_eq!(
            decode(&[
                Some(r#"[{"lib":"a","n":1,"name":"x","type":"d"}]"#),
                Some(
                    r#"[{"lib":"a","n":2,"name":"y","type":"d"},{"lib":"a","n":3,"name":"z","type":"e"}]"#
                ),
            ]),
            Err(IdentityError::TwoTypes { field_index: 1 })
        );
    }

    #[test]
    fn one_identity_twice_in_a_column_is_refused() {
        assert_eq!(
            decode(&[Some(
                r#"[{"lib":"a","n":1,"name":"x","type":"d"},{"lib":"a","n":1,"name":"y","type":"d"}]"#
            )]),
            Err(IdentityError::Repeated { field_index: 0 })
        );
    }

    #[test]
    fn one_identity_on_two_columns_is_refused_at_the_later_column() {
        assert_eq!(
            decode(&[
                Some(r#"[{"lib":"a","n":1,"name":"x","type":"d"}]"#),
                Some(r#"[{"lib":"b","n":1,"name":"x","type":"d"}]"#),
                Some(r#"[{"lib":"a","n":1,"name":"x","type":"e"}]"#),
            ]),
            Err(IdentityError::Repeated { field_index: 2 })
        );
    }
}
