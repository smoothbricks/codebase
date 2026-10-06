//! Which build a cowshed binary is, as the binary itself records it, and what that says about
//! replacing one build with another (05_gateway.md "The installed cowshed").
//!
//! Every release build carries the commit it was made from and when that commit was committed
//! (`build.rs`). The commit's time is the ordering: it is read from the one commit `HEAD` names,
//! so it survives the shallow clone a release is built from and needs no history to compare, and
//! a checkout whose build is stale is older than the build installed from a newer commit by
//! construction. A build made without git, or made in the debug profile, records nothing.
//!
//! The installing build writes its own record beside the stored copy it installs
//! (`gateway_service`), so the installed build's record is read from a file rather than from a
//! process: nothing is executed to learn it, and a copy installed before records existed simply
//! has none.

use cowshed_core::CowshedError;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// The build a release binary says it is.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BuildRecord {
    /// The commit the build was made from.
    pub commit: String,
    /// When that commit was committed, in seconds since the Unix epoch: what builds are ordered
    /// by.
    pub commit_time: u64,
}

impl BuildRecord {
    /// The record this binary was built with; `None` for a debug build and for one made without
    /// git.
    pub fn running() -> Option<Self> {
        Some(Self {
            commit: option_env!("COWSHED_BUILD_COMMIT")?.to_owned(),
            commit_time: option_env!("COWSHED_BUILD_COMMIT_TIME")?.parse().ok()?,
        })
    }

    /// The commit, as short as a person can use it.
    pub fn short_commit(&self) -> &str {
        self.commit.get(..12).unwrap_or(&self.commit)
    }

    /// `commit 51300433f2a1, committed 2026-10-06T16:02:11Z`.
    pub fn describe(&self) -> String {
        format!(
            "commit {}, committed {}",
            self.short_commit(),
            rfc3339_utc(self.commit_time)
        )
    }
}

/// Whether installing `candidate` over the build `installed` records moves the host to an older
/// build.
///
/// A candidate that records nothing cannot show it is not older, so it is refused: the installed
/// build knows what it is, and a replacement that cannot say is the stale binary this exists to
/// stop. Two builds of the same commit time are not ordered, so neither is older.
pub fn is_downgrade(candidate: Option<&BuildRecord>, installed: &BuildRecord) -> bool {
    candidate.is_none_or(|candidate| candidate.commit_time < installed.commit_time)
}

/// The refusal of a candidate older than the installed build, naming the binary it would have
/// installed and both builds.
pub fn downgrade_refusal(
    source: &Path,
    candidate: Option<&BuildRecord>,
    installed: &BuildRecord,
) -> CowshedError {
    CowshedError::conflict(
        format!(
            "{} would replace the installed cowshed with an older build: it {}, and the installed \
             cowshed is {}",
            source.display(),
            candidate.map_or_else(
                || String::from("records no build"),
                |candidate| format!("is {}", candidate.describe())
            ),
            installed.describe(),
        ),
        "run `cowshed setup` from a build of a newer commit; `cowshed setup --downgrade` from this \
         build installs it anyway",
    )
}

/// `seconds` since the epoch as `YYYY-MM-DDTHH:MM:SSZ`.
fn rfc3339_utc(seconds: u64) -> String {
    // Howard Hinnant's `civil_from_days`: days since 1970-01-01 to a proleptic Gregorian date.
    let days = (seconds / 86_400) as i64 + 719_468;
    let era = days.div_euclid(146_097);
    let day_of_era = days.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);
    let second_of_day = seconds % 86_400;
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}Z",
        second_of_day / 3_600,
        second_of_day % 3_600 / 60,
        second_of_day % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(commit_time: u64) -> BuildRecord {
        BuildRecord {
            commit: "51300433f2a1b0c9d8e7f60514233241506f7e8d".into(),
            commit_time,
        }
    }

    #[test]
    fn an_older_candidate_is_a_downgrade_and_a_newer_or_equal_one_is_not() {
        let installed = record(1_000);
        assert!(is_downgrade(Some(&record(999)), &installed));
        assert!(!is_downgrade(Some(&record(1_000)), &installed));
        assert!(!is_downgrade(Some(&record(1_001)), &installed));
    }

    #[test]
    fn a_candidate_that_records_no_build_cannot_show_it_is_not_older() {
        assert!(is_downgrade(None, &record(1_000)));
    }

    #[test]
    fn the_refusal_names_the_binary_both_builds_and_the_override() {
        let refusal = downgrade_refusal(
            Path::new("/Users/dev/checkout/dist/bin/darwin-arm64/cowshed"),
            Some(&record(1_790_000_000)),
            &record(1_790_100_000),
        );
        assert_eq!(refusal.code.as_str(), "conflict");
        assert!(
            refusal
                .message
                .contains("/Users/dev/checkout/dist/bin/darwin-arm64/cowshed"),
            "{}",
            refusal.message
        );
        assert!(
            refusal.message.contains("commit 51300433f2a1"),
            "{}",
            refusal.message
        );
        assert!(
            refusal.message.contains("2026-09-"),
            "dates are spelled, not counted: {}",
            refusal.message
        );
        assert!(refusal.hint.contains("--downgrade"), "{}", refusal.hint);
        let blind = downgrade_refusal(Path::new("/old/cowshed"), None, &record(1_790_100_000));
        assert!(
            blind.message.contains("records no build"),
            "{}",
            blind.message
        );
    }

    #[test]
    fn epoch_seconds_are_spelled_as_utc_dates() {
        assert_eq!(rfc3339_utc(0), "1970-01-01T00:00:00Z");
        assert_eq!(rfc3339_utc(951_782_400), "2000-02-29T00:00:00Z");
        assert_eq!(rfc3339_utc(1_791_302_531), "2026-10-06T16:02:11Z");
    }
}
