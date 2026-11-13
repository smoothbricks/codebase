//! The instant grammar (specs/columine/10-physical-vocabulary.md §8, "The
//! instant grammar"): the one reader of an instant's text for every lane and
//! for any host that names it.
//!
//! An instant is RFC 3339 with an uppercase `T`, a four-digit year 0000 to
//! 9999, a real calendar date, hours 00 to 23, minutes and seconds 00 to 59
//! (no leap second), an optional fraction of one or more digits, and `Z` or a
//! `+hh:mm` / `-hh:mm` offset with hours to 23 and minutes to 59. Nothing
//! else is read. Its value is the exact count of nanoseconds since the Unix
//! epoch; a fraction digit past the nanosecond must be zero.

use columine_arrow::{TimeUnit, units_per_second};

/// Why an instant's text is refused.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstantError {
    /// The text is not an instant of the grammar.
    Malformed,
    /// The instant has a non-zero digit finer than the unit it converts to.
    FinerThanUnit,
    /// The instant is outside what the unit's `i64` holds.
    OutOfRange,
}

const NANOS_PER_SECOND: i128 = 1_000_000_000;

/// The exact nanoseconds since the Unix epoch of the instant \`text\` names.
pub fn instant_nanos(text: &str) -> Result<i128, InstantError> {
    let bytes = text.as_bytes();
    let Some((date_time, rest)) = bytes.split_at_checked(19) else {
        return Err(InstantError::Malformed);
    };
    let separators = [(4, b'-'), (7, b'-'), (10, b'T'), (13, b':'), (16, b':')];
    if separators
        .iter()
        .any(|(at, separator)| date_time[*at] != *separator)
    {
        return Err(InstantError::Malformed);
    }
    let year = digits(&date_time[0..4])?;
    let month = digits(&date_time[5..7])?;
    let day = digits(&date_time[8..10])?;
    let hour = digits(&date_time[11..13])?;
    let minute = digits(&date_time[14..16])?;
    let second = digits(&date_time[17..19])?;
    if !(1..=12).contains(&month)
        || !(1..=days_in_month(year, month)).contains(&day)
        || hour > 23
        || minute > 59
        || second > 59
    {
        return Err(InstantError::Malformed);
    }
    let (fraction, offset) = match rest {
        [b'.', after @ ..] => {
            let count = after
                .iter()
                .take_while(|byte| byte.is_ascii_digit())
                .count();
            let (run, offset) = after.split_at(count);
            (fraction_nanos(run)?, offset)
        }
        _ => (0, rest),
    };
    let offset_seconds = match offset {
        [b'Z'] => 0,
        [sign @ (b'+' | b'-'), hours @ .., b':', _, _] if hours.len() == 2 => {
            let hours = digits(hours)?;
            let minutes = digits(&offset[4..6])?;
            if hours > 23 || minutes > 59 {
                return Err(InstantError::Malformed);
            }
            let magnitude = hours * 3_600 + minutes * 60;
            if *sign == b'-' { -magnitude } else { magnitude }
        }
        _ => return Err(InstantError::Malformed),
    };
    // The local time minus its offset is the UTC instant.
    let seconds = epoch_days(year, month, day) * 86_400 + hour * 3_600 + minute * 60 + second
        - offset_seconds;
    Ok(i128::from(seconds) * NANOS_PER_SECOND + i128::from(fraction))
}

/// The instant \`text\` names, in \`unit\` since the Unix epoch: refused when a
/// non-zero digit is finer than \`unit\`, or when \`unit\`'s \`i64\` cannot hold it.
pub fn parse_instant(text: &str, unit: TimeUnit) -> Result<i64, InstantError> {
    let nanos = instant_nanos(text)?;
    let per_unit = NANOS_PER_SECOND / i128::from(units_per_second(unit));
    if nanos % per_unit != 0 {
        return Err(InstantError::FinerThanUnit);
    }
    i64::try_from(nanos / per_unit).map_err(|_| InstantError::OutOfRange)
}

/// A fraction's digit run as nanoseconds: one or more digits, every digit past
/// the ninth zero.
fn fraction_nanos(run: &[u8]) -> Result<i64, InstantError> {
    if run.is_empty() {
        return Err(InstantError::Malformed);
    }
    let (nanos, past) = run.split_at(run.len().min(9));
    if past.iter().any(|digit| *digit != b'0') {
        return Err(InstantError::Malformed);
    }
    let mut padded = [b'0'; 9];
    padded[..nanos.len()].copy_from_slice(nanos);
    digits(&padded)
}

/// A run of ASCII decimal digits; it is at most nine long, so it fits.
fn digits(run: &[u8]) -> Result<i64, InstantError> {
    if run.is_empty() || !run.iter().all(u8::is_ascii_digit) {
        return Err(InstantError::Malformed);
    }
    Ok(run
        .iter()
        .fold(0, |total, digit| total * 10 + i64::from(*digit - b'0')))
}

/// Days in \`month\` (1..=12) of \`year\` in the proleptic Gregorian calendar.
fn days_in_month(year: i64, month: i64) -> i64 {
    match month {
        2 if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) => 29,
        2 => 28,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    }
}

/// Days from 1970-01-01 to a proleptic Gregorian date.
fn epoch_days(year: i64, month: i64, day: i64) -> i64 {
    let (y, m) = if month <= 2 {
        (year - 1, month + 12)
    } else {
        (year, month)
    };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (m - 3) + 2) / 5 + day - 1;
    era * 146_097 + yoe * 365 + yoe / 4 - yoe / 100 + doy - 719_468
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::json_parser::{JsonParser, Token};

    //#region smoo/columine!n/vocab-instant-grammar
    /// Every vector the grammar's readers share reads as exactly its
    /// nanoseconds, or is refused where it has none.
    #[test]
    fn every_shared_vector_reads_as_its_nanoseconds() {
        let bytes = include_bytes!("../test-vectors/instant-grammar.json");
        let mut parser = JsonParser::new(bytes);
        parser.expect_object_begin().unwrap();
        let mut read = 0;
        while !parser.is_object_end() {
            if !parser.expect_field_name().unwrap().is("vectors") {
                parser.skip_value().unwrap();
                continue;
            }
            parser.expect_array_begin().unwrap();
            while !parser.is_array_end() {
                let (mut text, mut nanos, mut case) = (None, None, None);
                parser.expect_object_begin().unwrap();
                while !parser.is_object_end() {
                    let key = parser.expect_field_name().unwrap().to_owned_text().unwrap();
                    match (key.as_str(), parser.next_token().unwrap()) {
                        ("text", Token::String(value)) => {
                            text = Some(value.to_owned_text().unwrap())
                        }
                        ("nanos", Token::String(value)) => {
                            nanos = Some(Some(
                                value.to_owned_text().unwrap().parse::<i128>().unwrap(),
                            ))
                        }
                        ("nanos", Token::Null) => nanos = Some(None),
                        ("case", Token::String(value)) => {
                            case = Some(value.to_owned_text().unwrap())
                        }
                        (key, token) => panic!("a vector member {key} of {token:?}"),
                    }
                }
                parser.next_token().unwrap();
                let (text, nanos, case) = (text.unwrap(), nanos.unwrap(), case.unwrap());
                assert_eq!(instant_nanos(&text).ok(), nanos, "{case}: {text}");
                read += 1;
            }
            parser.next_token().unwrap();
        }
        assert!(read > 0, "the vector file holds vectors");
    }
    //#endregion smoo/columine!n/vocab-instant-grammar

    //#region smoo/columine!n/vocab-temporal-instant
    /// 2024-01-15T10:30:00Z in seconds since the epoch.
    const BASE_SECONDS: i64 = 19_737 * 86_400 + 37_800;

    #[test]
    fn an_instant_converts_exactly_to_each_unit() {
        let text = "2024-01-15T10:30:00.123Z";
        assert_eq!(
            parse_instant(text, TimeUnit::Millisecond),
            Ok(BASE_SECONDS * 1_000 + 123)
        );
        assert_eq!(
            parse_instant(text, TimeUnit::Microsecond),
            Ok(BASE_SECONDS * 1_000_000 + 123_000)
        );
        assert_eq!(
            parse_instant(text, TimeUnit::Nanosecond),
            Ok(BASE_SECONDS * 1_000_000_000 + 123_000_000)
        );
        assert_eq!(
            parse_instant("2024-01-15T11:30:00+01:00", TimeUnit::Second),
            Ok(BASE_SECONDS)
        );
        assert_eq!(
            parse_instant("2024-01-15T10:30:00.123000000Z", TimeUnit::Millisecond),
            Ok(BASE_SECONDS * 1_000 + 123)
        );
    }

    #[test]
    fn an_instant_finer_than_its_unit_is_refused() {
        assert_eq!(
            parse_instant("2024-01-15T10:30:00.123456789Z", TimeUnit::Millisecond),
            Err(InstantError::FinerThanUnit)
        );
        assert_eq!(
            parse_instant("1969-12-31T23:59:59.999999999Z", TimeUnit::Microsecond),
            Err(InstantError::FinerThanUnit)
        );
        assert_eq!(
            parse_instant("2024-01-15T10:30:00.5Z", TimeUnit::Second),
            Err(InstantError::FinerThanUnit)
        );
    }

    #[test]
    fn an_instant_the_unit_cannot_hold_is_refused() {
        assert_eq!(
            parse_instant("9999-12-31T23:59:59Z", TimeUnit::Nanosecond),
            Err(InstantError::OutOfRange)
        );
        assert_eq!(
            parse_instant("9999-12-31T23:59:59Z", TimeUnit::Second),
            Ok(253_402_300_799)
        );
        assert_eq!(
            parse_instant("2262-04-11T23:47:16.854775807Z", TimeUnit::Nanosecond),
            Ok(i64::MAX)
        );
    }
    //#endregion smoo/columine!n/vocab-temporal-instant
}
