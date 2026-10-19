//! Which build a cowshed binary is, as the binary itself records it, and what that says about
//! replacing one build with another (05_gateway.md "The installed cowshed").
//!
//! Every release build carries the commit it was made from and when that commit was committed
//! (`build.rs`). The commit is what an install is judged by: a build may replace the installed
//! one only when it holds every installed commit, by ancestry or patch identity
//! (`GitRepository::commits_not_held`). The time only describes a build to a person. It never
//! orders two builds: two sibling commits are ordered by clock, and the later one carries none of
//! the earlier one's fixes. A build made without git, or made in the debug profile, records
//! nothing.
//!
//! The installing build writes its own record beside the stored copy it installs
//! (`gateway_service`), so the installed build's record is read from a file rather than from a
//! process: nothing is executed to learn it, and a copy installed before records existed simply
//! has none.

use cowshed_core::CowshedError;
use cowshed_core::git::CommitSubject;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// The build a release binary says it is.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct BuildRecord {
    /// The commit the build was made from.
    pub commit: String,
    /// When that commit was committed, in seconds since the Unix epoch: how a person tells two
    /// builds apart, never how they are ordered.
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

/// How many lost commits a refusal names one by one before it counts the rest.
const NAMED_LOST_COMMITS: usize = 20;

/// The hint every basis refusal ends with: how to make a build that contains the installed one,
/// and the override that goes back on purpose.
fn rebase_hint(installed: &BuildRecord) -> String {
    format!(
        "rebase this build's checkout onto {} (or merge it), rebuild, and run that build's \
         `cowshed setup`; `cowshed setup --downgrade` from this build installs it anyway and \
         removes what it lacks",
        installed.short_commit()
    )
}

/// The refusal of a candidate that lacks changes the installed build carries: the binary it
/// would have installed, both builds, and every commit the install would take off the host, so
/// the reader does not have to go and find out what "older" meant.
pub fn lost_commits_refusal(
    source: &Path,
    candidate: &BuildRecord,
    installed: &BuildRecord,
    lost: &[CommitSubject],
) -> CowshedError {
    let mut message = format!(
        "{} would take {} off this host: it is {}, and does not contain the installed cowshed, \
         {}; these commits of the installed build have no ancestor or patch-identical commit in \
         it:",
        source.display(),
        match lost.len() {
            1 => String::from("1 commit"),
            count => format!("{count} commits"),
        },
        candidate.describe(),
        installed.describe(),
    );
    for commit in lost.iter().take(NAMED_LOST_COMMITS) {
        let oid = commit.oid.as_str();
        message.push_str(&format!(
            "\n  {} {}",
            oid.get(..12).unwrap_or(oid),
            commit.subject
        ));
    }
    if let Some(unnamed) = lost.len().checked_sub(NAMED_LOST_COMMITS)
        && unnamed > 0
    {
        message.push_str(&format!(
            "\n  … and {unnamed} more: git log {}..{}",
            candidate.short_commit(),
            installed.short_commit()
        ));
    }
    CowshedError::conflict(message, rebase_hint(installed))
}

/// The refusal of a candidate that records no build: it cannot show that it contains the
/// installed one, and a replacement that cannot say is the stale binary this exists to stop.
pub fn unrecorded_refusal(source: &Path, installed: &BuildRecord) -> CowshedError {
    CowshedError::conflict(
        format!(
            "{} records no build, so it cannot show that it contains the installed cowshed, {}",
            source.display(),
            installed.describe(),
        ),
        rebase_hint(installed),
    )
}

/// The refusal of a candidate whose containment of the installed build could not be decided:
/// `reason` says which commit could not be read, and where it was looked for. Not deciding is
/// not permission; installing on it is the silent revert this check replaces.
pub fn unproven_refusal(
    source: &Path,
    candidate: &BuildRecord,
    installed: &BuildRecord,
    reason: &str,
) -> CowshedError {
    CowshedError::conflict(
        format!(
            "{} cannot be shown to contain the installed cowshed: it is {}, the installed cowshed \
             is {}, and {reason}",
            source.display(),
            candidate.describe(),
            installed.describe(),
        ),
        rebase_hint(installed),
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
    use cowshed_core::api::GitOid;

    const SOURCE: &str = "/Users/dev/checkout/dist/bin/darwin-arm64/cowshed";

    fn record(commit: &str, commit_time: u64) -> BuildRecord {
        BuildRecord {
            commit: commit.into(),
            commit_time,
        }
    }

    fn lost(oid: &str, subject: &str) -> CommitSubject {
        CommitSubject {
            oid: GitOid::new(oid).unwrap(),
            subject: subject.into(),
        }
    }

    /// Today's collision as the refusal would have said it: the release train's build of
    /// 24acd8e7b684 over the installed f0bfb52de, which carries the gc fix the train lacks.
    #[test]
    fn the_refusal_names_the_binary_both_builds_every_lost_commit_and_the_override() {
        let candidate = record("24acd8e7b684265b49135f2e29e885ab3f547e3b", 1_792_213_208);
        let installed = record("f0bfb52defe2357afc492f6619789607acbc70ca", 1_792_232_045);
        let refusal = lost_commits_refusal(
            Path::new(SOURCE),
            &candidate,
            &installed,
            &[lost(
                "f0bfb52defe2357afc492f6619789607acbc70ca",
                "fix(cowshed): judge strays by the project ignore rules and refuse a failed \
                 judgement",
            )],
        );
        assert_eq!(refusal.code.as_str(), "conflict");
        assert_eq!(
            refusal.message,
            format!(
                "{SOURCE} would take 1 commit off this host: it is commit 24acd8e7b684, committed \
                 2026-10-17T05:00:08Z, and does not contain the installed cowshed, commit \
                 f0bfb52defe2, committed 2026-10-17T10:14:05Z; these commits of the installed \
                 build have no ancestor or patch-identical commit in it:\n  f0bfb52defe2 \
                 fix(cowshed): judge strays by the project ignore rules and refuse a failed \
                 judgement"
            )
        );
        assert_eq!(
            refusal.hint,
            "rebase this build's checkout onto f0bfb52defe2 (or merge it), rebuild, and run that \
             build's `cowshed setup`; `cowshed setup --downgrade` from this build installs it \
             anyway and removes what it lacks"
        );
    }

    /// A long list is named up to a bound and then counted, with the command that lists it all:
    /// the count is never hidden, only the names past the bound.
    #[test]
    fn a_long_loss_names_twenty_commits_and_counts_the_rest() {
        let commits: Vec<CommitSubject> = (0..23)
            .map(|index| lost(&format!("{index:040x}"), &format!("fix: number {index}")))
            .collect();
        let refusal = lost_commits_refusal(
            Path::new(SOURCE),
            &record("a".repeat(40).as_str(), 1),
            &record("b".repeat(40).as_str(), 2),
            &commits,
        );
        assert!(
            refusal.message.contains("23 commits"),
            "{}",
            refusal.message
        );
        assert!(
            refusal.message.contains("fix: number 19"),
            "{}",
            refusal.message
        );
        assert!(
            !refusal.message.contains("fix: number 20"),
            "{}",
            refusal.message
        );
        assert!(
            refusal
                .message
                .ends_with("… and 3 more: git log aaaaaaaaaaaa..bbbbbbbbbbbb"),
            "{}",
            refusal.message
        );
    }

    #[test]
    fn a_candidate_that_records_no_build_cannot_show_it_contains_the_installed_one() {
        let installed = record("f0bfb52defe2357afc492f6619789607acbc70ca", 1_792_232_045);
        let refusal = unrecorded_refusal(Path::new("/old/cowshed"), &installed);
        assert_eq!(refusal.code.as_str(), "conflict");
        assert!(
            refusal.message.contains("records no build")
                && refusal.message.contains("commit f0bfb52defe2"),
            "{}",
            refusal.message
        );
        assert!(refusal.hint.contains("--downgrade"), "{}", refusal.hint);
    }

    #[test]
    fn epoch_seconds_are_spelled_as_utc_dates() {
        assert_eq!(rfc3339_utc(0), "1970-01-01T00:00:00Z");
        assert_eq!(rfc3339_utc(951_782_400), "2000-02-29T00:00:00Z");
        assert_eq!(rfc3339_utc(1_791_302_531), "2026-10-06T16:02:11Z");
    }
}
