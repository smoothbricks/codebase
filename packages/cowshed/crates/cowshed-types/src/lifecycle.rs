//! Lifecycle vocabulary: the checkpoint label grammar and the revision, fact and
//! mount values lifecycle plans are made against. Planners and substrates consume
//! these values; they live with neither.

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thiserror::Error;

use crate::clock::civil_from_days;

/// The label text a checkpoint constructor refused.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("invalid checkpoint label {0:?}")]
pub struct InvalidCheckpointLabel(pub String);
/// The undo-image grammar [`CheckpointLabel`] refuses and the restorer writes. One constant,
/// because a label admitted here that later collides with a controller undo image would
/// silently overwrite the only crash-consistent copy of a workspace.
pub const PRE_RESTORE_PREFIX: &str = "pre-restore-";
/// A validated, path-safe checkpoint label.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct CheckpointLabel(String);

impl CheckpointLabel {
    pub fn new(value: impl Into<String>) -> Result<Self, InvalidCheckpointLabel> {
        let value = value.into();
        let bytes = value.as_bytes();
        let valid = (1..=128).contains(&bytes.len())
            && !value.starts_with(PRE_RESTORE_PREFIX)
            && (bytes[0].is_ascii_lowercase() || bytes[0].is_ascii_digit())
            && bytes.iter().all(|byte| {
                byte.is_ascii_lowercase()
                    || byte.is_ascii_digit()
                    || matches!(byte, b'.' | b'_' | b'-')
            });
        if valid {
            Ok(Self(value))
        } else {
            Err(InvalidCheckpointLabel(value))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The default label for an unlabeled checkpoint: the UTC second of the request, rendered in
    /// the label alphabet (`2026-07-11t120309z`). Workspace revision is the wrong key — it does
    /// not advance between checkpoints, so a revision-derived default collides deterministically
    /// on the second unlabeled checkpoint. A checkpoint image may be the only crash-consistent
    /// copy, so an existing label is never reused or overwritten: a same-second collision takes
    /// `-2`, `-3`, … instead.
    pub fn utc_default(now: SystemTime, mut is_taken: impl FnMut(&str) -> bool) -> Self {
        let seconds = now.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();
        let (year, month, day) = civil_from_days(seconds / 86_400);
        let clock = seconds % 86_400;
        let base = format!(
            "{year:04}-{month:02}-{day:02}t{:02}{:02}{:02}z",
            clock / 3_600,
            clock % 3_600 / 60,
            clock % 60,
        );
        if !is_taken(&base) {
            return Self(base);
        }
        (2_u64..)
            .map(|ordinal| format!("{base}-{ordinal}"))
            .find(|candidate| !is_taken(candidate))
            .map(Self)
            .expect("existing checkpoint labels are finite")
    }
}

impl fmt::Display for CheckpointLabel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl Serialize for CheckpointLabel {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for CheckpointLabel {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(serde::de::Error::custom)
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn labels_are_immutable_validated_components() {
        for valid in ["a", "ci-fail", "2026-07-11t120000z", "release_1.2"] {
            let label = CheckpointLabel::new(valid).unwrap();
            assert_eq!(label.to_string(), valid);
            let encoded = serde_json::to_string(&label).unwrap();
            assert_eq!(
                serde_json::from_str::<CheckpointLabel>(&encoded).unwrap(),
                label
            );
        }
        for invalid in [
            "",
            ".",
            "..",
            "Upper",
            "-leading",
            "slash/name",
            "a b",
            "pre-restore-user",
            "pre-restore-00000000000000000000000000000002",
        ] {
            assert!(
                CheckpointLabel::new(invalid).is_err(),
                "accepted {invalid:?}"
            );
        }
    }

    fn at(seconds: u64) -> SystemTime {
        UNIX_EPOCH + std::time::Duration::from_secs(seconds)
    }

    #[test]
    fn the_default_label_is_the_documented_utc_second() {
        for (seconds, expected) in [
            (0, "1970-01-01t000000z"),
            (1_783_771_200, "2026-07-11t120000z"),
            // Leap day and both neighbours: a leap-year error shifts one of the three.
            (1_835_395_199, "2028-02-28t235959z"),
            (1_835_395_200, "2028-02-29t000000z"),
            (1_835_481_600, "2028-03-01t000000z"),
            // Year boundary at exactly midnight UTC.
            (1_893_455_999, "2029-12-31t235959z"),
            (1_893_456_000, "2030-01-01t000000z"),
            // 2100 is the century rule's first bite: divisible by 4 but not a leap year.
            (4_107_542_399, "2100-02-28t235959z"),
            (4_107_542_400, "2100-03-01t000000z"),
            // 2400 restores the 400-year exception, so its February 29th exists.
            (13_574_585_228, "2400-02-29t060708z"),
        ] {
            let label = CheckpointLabel::utc_default(at(seconds), |_| false);
            assert_eq!(label.as_str(), expected, "at {seconds}s");
            // Every generated default must survive the validator it bypasses.
            assert_eq!(CheckpointLabel::new(expected).unwrap(), label);
        }
    }

    #[test]
    fn a_same_second_collision_takes_the_next_ordinal_and_never_reuses_a_label() {
        let now = at(1_783_771_200);
        let base = CheckpointLabel::utc_default(now, |_| false);
        assert_eq!(base.as_str(), "2026-07-11t120000z");

        let taken = [base.as_str().to_owned()];
        let second =
            CheckpointLabel::utc_default(now, |candidate| taken.iter().any(|l| l == candidate));
        assert_eq!(second.as_str(), "2026-07-11t120000z-2");

        let taken = [taken[0].clone(), second.as_str().to_owned()];
        let third =
            CheckpointLabel::utc_default(now, |candidate| taken.iter().any(|l| l == candidate));
        assert_eq!(third.as_str(), "2026-07-11t120000z-3");
        assert_eq!(CheckpointLabel::new(third.as_str()).unwrap(), third);
    }
}
