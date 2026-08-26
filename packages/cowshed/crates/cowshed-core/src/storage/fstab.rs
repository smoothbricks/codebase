use std::path::PathBuf;
use thiserror::Error;

const COWSHED_FSTAB_TAG: &str = "# cowshed created volume labelled";

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum FstabBuildError {
    #[error("fstab mountpoint is not UTF-8: {0:?}")]
    NonUtf8Mountpoint(PathBuf),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FstabPin {
    pub volume_uuid: String,
    pub mountpoint: PathBuf,
    pub label: String,
}

pub fn build_fstab(existing_text: &str, pins: &[FstabPin]) -> Result<String, FstabBuildError> {
    let mut lines = existing_text
        .lines()
        .filter(|line| {
            !line.contains(COWSHED_FSTAB_TAG)
                && !line.split_whitespace().next().is_some_and(|field| {
                    field
                        .strip_prefix("UUID=")
                        .is_some_and(|uuid| pins.iter().any(|pin| pin.volume_uuid == uuid))
                })
        })
        .map(str::to_owned)
        .collect::<Vec<_>>();
    while lines.last().is_some_and(String::is_empty) {
        lines.pop();
    }
    for pin in pins {
        let mountpoint = pin
            .mountpoint
            .to_str()
            .ok_or_else(|| FstabBuildError::NonUtf8Mountpoint(pin.mountpoint.clone()))?;
        lines.push(format!(
            "UUID={}  {mountpoint}  apfs rw,noatime,noauto,nobrowse,noowners  {COWSHED_FSTAB_TAG} {}",
            pin.volume_uuid, pin.label
        ));
    }
    Ok(format!("{}\n", lines.join("\n")))
}

#[cfg(test)]
mod tests {
    use super::{FstabPin, build_fstab};

    fn pins() -> Vec<FstabPin> {
        vec![
            FstabPin {
                volume_uuid: "STORE-UUID".to_owned(),
                mountpoint: "/private/cowshed/store".into(),
                label: "cowshed.store".to_owned(),
            },
            FstabPin {
                volume_uuid: "CACHES-UUID".to_owned(),
                mountpoint: "/private/cowshed/caches".into(),
                label: "cowshed.caches".to_owned(),
            },
        ]
    }

    #[test]
    fn builds_empty_fstab_with_exact_volume_lines() {
        assert_eq!(
            build_fstab("", &pins()).unwrap(),
            "UUID=STORE-UUID  /private/cowshed/store  apfs rw,noatime,noauto,nobrowse,noowners  # cowshed created volume labelled cowshed.store\n\
UUID=CACHES-UUID  /private/cowshed/caches  apfs rw,noatime,noauto,nobrowse,noowners  # cowshed created volume labelled cowshed.caches\n"
        );
    }

    #[test]
    fn preserves_unrelated_lines_and_normalizes_one_trailing_newline() {
        let existing = "# nix installer entry\nLABEL=nix /nix apfs rw\n\n\n";
        let built = build_fstab(existing, &pins()).unwrap();
        assert!(built.starts_with("# nix installer entry\nLABEL=nix /nix apfs rw\n"));
        assert!(built.ends_with("labelled cowshed.caches\n"));
        assert!(!built.ends_with("\n\n"));
    }

    #[test]
    fn second_run_is_byte_identical() {
        let first = build_fstab("# keep\n", &pins()).unwrap();
        assert_eq!(build_fstab(&first, &pins()).unwrap(), first);
    }

    #[test]
    fn replaces_stale_manual_uuid_line() {
        let existing = "UUID=STORE-UUID /wrong apfs ro\n# keep\n";
        let built = build_fstab(existing, &pins()).unwrap();
        assert!(!built.contains("/wrong"));
        assert!(built.starts_with("# keep\n"));
    }

    #[test]
    fn replaces_every_old_tagged_line() {
        let existing = "UUID=OLD /private/cowshed/store apfs rw # cowshed created volume labelled cowshed.store\n# keep\n";
        let built = build_fstab(existing, &pins()).unwrap();
        assert!(!built.contains("UUID=OLD"));
        assert!(built.starts_with("# keep\n"));
    }

    #[test]
    fn empty_pin_set_removes_only_cowshed_tagged_lines_for_uninstall() {
        let existing = "# nix\nUUID=OLD /private/cowshed/store apfs rw # cowshed created volume labelled cowshed.store\nLABEL=nix /nix apfs rw\n";
        assert_eq!(
            build_fstab(existing, &[]).unwrap(),
            "# nix\nLABEL=nix /nix apfs rw\n"
        );
    }
}
