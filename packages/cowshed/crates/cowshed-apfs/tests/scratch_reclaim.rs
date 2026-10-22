//! Pure proofs of scratch-root cleanup isolation; no filesystem or host operations.

#[path = "support/scratch_reclaim.rs"]
mod reclaim;

use std::collections::BTreeSet;
use std::io;
use std::path::PathBuf;

#[test]
fn an_unreleased_image_retains_only_its_root_and_does_not_block_later_roots() {
    let refused = PathBuf::from("/private/tmp/cowshed-itest-11-0-cli");
    let good = PathBuf::from("/private/tmp/cowshed-itest-12-0-fixture-run");
    let empty = PathBuf::from("/private/tmp/cowshed-itest-13-0-fixture-run");
    let prefix_peer = PathBuf::from("/private/tmp/cowshed-itest-11-0-cl");
    let roots = [
        refused.clone(),
        good.clone(),
        empty.clone(),
        prefix_peer.clone(),
    ];
    let images = BTreeSet::from([
        refused.join("retained/first.dmg"),
        refused.join("second.dmg"),
    ]);
    let mut removed = Vec::new();
    let mut failures = Vec::new();
    reclaim::reclaim_dead_roots(
        roots,
        &images,
        |root| {
            removed.push(root.to_path_buf());
            Ok(())
        },
        |root, error| failures.push((root.to_path_buf(), error.to_string())),
    );
    assert_eq!(removed, [good, empty, prefix_peer]);
    assert_eq!(failures.len(), 1);
    assert_eq!(failures[0].0, refused);
    assert!(failures[0].1.contains("first.dmg could not be released"));
}

#[test]
fn every_refused_root_is_reported_without_stopping_other_reclamation() {
    let first = PathBuf::from("/private/tmp/cowshed-itest-21-0-cli");
    let second = PathBuf::from("/private/tmp/cowshed-itest-22-0-cli");
    let good = PathBuf::from("/private/tmp/cowshed-itest-23-0-cli");
    let roots = [first.clone(), second.clone(), good.clone()];
    let images = BTreeSet::from([first.join("first.dmg"), second.join("second.dmg")]);
    let mut removed = Vec::new();
    let mut failures = Vec::new();
    reclaim::reclaim_dead_roots(
        roots,
        &images,
        |root| {
            removed.push(root.to_path_buf());
            Ok(())
        },
        |root, error| failures.push((root.to_path_buf(), error.to_string())),
    );
    assert_eq!(removed, [good]);
    assert_eq!(
        failures.iter().map(|(root, _)| root).collect::<Vec<_>>(),
        [&first, &second]
    );
    assert!(
        failures
            .iter()
            .all(|(_, error)| error.contains("could not be released"))
    );
}

#[test]
fn a_root_removal_error_is_reported_and_does_not_block_the_next_root() {
    let first = PathBuf::from("/private/tmp/cowshed-itest-31-0-cli");
    let second = PathBuf::from("/private/tmp/cowshed-itest-32-0-cli");
    let mut attempted = Vec::new();
    let mut failures = Vec::new();
    reclaim::reclaim_dead_roots(
        [first.clone(), second.clone()],
        &BTreeSet::new(),
        |root| {
            attempted.push(root.to_path_buf());
            if root == first {
                Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "root removal refused",
                ))
            } else {
                Ok(())
            }
        },
        |root, error| failures.push((root.to_path_buf(), error.kind())),
    );
    assert_eq!(attempted, [first.clone(), second]);
    assert_eq!(failures, [(first, io::ErrorKind::PermissionDenied)]);
}
