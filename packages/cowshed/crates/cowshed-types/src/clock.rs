//! One civil-date and RFC3339 vocabulary over injected time values. Shells read the
//! system clock; these conversions only compute.

use std::time::{SystemTime, UNIX_EPOCH};
/// Days since 1970-01-01 to a proleptic-Gregorian civil date (Howard Hinnant's algorithm,
/// restricted to the non-negative domain). Total over every `u64` second count, unlike
/// `libc::gmtime_r`, which rejects out-of-range `time_t` — a label generator must not fail.
pub const fn civil_from_days(days: u64) -> (u64, u64, u64) {
    let z = days + 719_468;
    let era = z / 146_097;
    let day_of_era = z % 146_097;
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    let year = year_of_era + era * 400 + (month <= 2) as u64;
    (year, month, day)
}

/// Inverse of [`civil_from_days`]. `None` when the triple is not a proleptic-Gregorian
/// date on or after 1970-01-01 — the non-negative domain that function is total over.
pub const fn days_from_civil(year: u64, month: u64, day: u64) -> Option<u64> {
    if year == 0 || month < 1 || month > 12 || day == 0 {
        return None;
    }
    let (era_year, shifted_month) = if month > 2 {
        (year, month - 3)
    } else {
        let Some(previous) = year.checked_sub(1) else {
            return None;
        };
        (previous, month + 9)
    };
    let era = era_year / 400;
    let year_of_era = era_year - era * 400;
    let day_of_year = (153 * shifted_month + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    let Some(z) = era.checked_mul(146_097) else {
        return None;
    };
    let Some(z) = z.checked_add(day_of_era) else {
        return None;
    };
    let Some(days) = z.checked_sub(719_468) else {
        return None;
    };
    let (y, m, d) = civil_from_days(days);
    if y == year && m == month && d == day {
        Some(days)
    } else {
        None
    }
}
pub fn rfc3339_utc(now: SystemTime) -> String {
    let secs = now
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs())
        .unwrap_or(0);
    let (year, month, day) = civil_from_days(secs / 86_400);
    let clock = secs % 86_400;
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        clock / 3_600,
        (clock % 3_600) / 60,
        clock % 60,
    )
}
#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    #[test]
    fn civil_from_days_pins_known_unix_epoch_dates() {
        // Independent of the label formatter: a wrong calendar goes red even if it is
        // self-inverse. Day numbers are unix seconds / 86400.
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(days_from_civil(1970, 1, 1), Some(0));
        assert_eq!(civil_from_days(20_645), (2026, 7, 11));
        assert_eq!(days_from_civil(2026, 7, 11), Some(20_645));
        // 2100 is divisible by 4 but not a leap year.
        assert_eq!(civil_from_days(47_540), (2100, 2, 28));
        assert_eq!(days_from_civil(2100, 2, 28), Some(47_540));
        assert_eq!(days_from_civil(2100, 2, 29), None);
        assert_eq!(civil_from_days(47_541), (2100, 3, 1));
        // 2000 is the 400-year leap exception; 1900 is the other century non-leap and
        // also sits before the unix epoch this pair is total over.
        assert!(days_from_civil(2000, 2, 29).is_some());
        assert_eq!(days_from_civil(1900, 2, 29), None);
        assert_eq!(days_from_civil(1900, 2, 28), None);
        assert!(days_from_civil(2400, 2, 29).is_some());
    }
    proptest! {
    // TelemetryDate::new takes years that fit u16, from unix
    // epoch day 0 (1970-01-01) through 65535-12-31. Pre-1970 dates are outside
    // this pair: civil_from_days is total over u64 days, not over negative time_t.
    #[test]
    fn days_from_civil_inverts_civil_from_days(
        days in 0u64..=days_from_civil(65535, 12, 31).expect("u16 max year is after 1970"),
    ) {
        let (year, month, day) = civil_from_days(days);
        prop_assert_eq!(days_from_civil(year, month, day), Some(days));
    }

    #[test]
    fn civil_from_days_inverts_valid_post_epoch_dates(
        year in 1970u64..=65535,
        month in 1u64..=12,
        day in 1u64..=31,
    ) {
        if let Some(days) = days_from_civil(year, month, day) {
            prop_assert_eq!(civil_from_days(days), (year, month, day));
        }
    }
    }
}
