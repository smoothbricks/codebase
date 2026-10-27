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
    let counts = reclaim::reclaim_dead_roots(
        roots,
        &images,
        |root| {
            removed.push(root.to_path_buf());
            Ok(())
        },
        |root, error| failures.push((root.to_path_buf(), error.to_string())),
    );
    assert_eq!(
        counts,
        reclaim::Reclaimed {
            removed: 3,
            retained: 1
        }
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
    let counts = reclaim::reclaim_dead_roots(
        roots,
        &images,
        |root| {
            removed.push(root.to_path_buf());
            Ok(())
        },
        |root, error| failures.push((root.to_path_buf(), error.to_string())),
    );
    assert_eq!(
        counts,
        reclaim::Reclaimed {
            removed: 1,
            retained: 2
        }
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
    let counts = reclaim::reclaim_dead_roots(
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
    assert_eq!(
        counts,
        reclaim::Reclaimed {
            removed: 1,
            retained: 1
        }
    );
    assert_eq!(attempted, [first.clone(), second]);
    assert_eq!(failures, [(first, io::ErrorKind::PermissionDenied)]);
}

#[test]
fn the_lane_owner_finishes_every_root_before_recording_and_never_resweeps_its_run() {
    use std::sync::mpsc;
    let (started, running) = mpsc::channel();
    let (release, blocked) = mpsc::channel();
    let (record, recorded) = mpsc::channel();
    let owner = std::thread::spawn(move || {
        reclaim::sweep_once(
            "lane",
            None,
            || {
                let mut count = 0;
                Ok(reclaim::reclaim_dead_roots(
                    (0..64).map(|index| PathBuf::from(format!("namespace-{index}"))),
                    &BTreeSet::new(),
                    |_| {
                        count += 1;
                        if count == 64 {
                            started.send(()).unwrap();
                            blocked.recv().unwrap();
                        }
                        Ok(())
                    },
                    |_, error| panic!("unexpected refusal: {error}"),
                ))
            },
            |bytes| {
                record.send(bytes.to_vec()).unwrap();
                Ok(())
            },
        )
    });
    running.recv().unwrap();
    assert_eq!(recorded.try_recv(), Err(mpsc::TryRecvError::Empty));
    release.send(()).unwrap();
    assert_eq!(
        owner.join().unwrap().unwrap(),
        Some(reclaim::Reclaimed {
            removed: 64,
            retained: 0
        })
    );
    let recorded = recorded.recv().unwrap();
    assert_eq!(recorded, b"lane");
    assert_eq!(
        reclaim::sweep_once(
            "lane",
            Some(&recorded),
            || -> io::Result<()> { panic!("a completed run must not sweep twice") },
            |_| panic!("a completed run must not rewrite its record")
        )
        .unwrap(),
        None
    );
}

#[test]
fn a_failed_pass_never_records_completion_and_a_failed_record_refuses_setup() {
    for failure in [io::ErrorKind::PermissionDenied, io::ErrorKind::Other] {
        let result = reclaim::sweep_once(
            "lane",
            Some(b"previous"),
            || -> io::Result<()> { Err(io::Error::from(failure)) },
            |_| panic!("failed cleanup must not record completion"),
        );
        assert_eq!(result.unwrap_err().kind(), failure);
    }
    let result = reclaim::sweep_once(
        "lane",
        None,
        || Ok(()),
        |_| Err(io::Error::from(io::ErrorKind::PermissionDenied)),
    );
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::PermissionDenied);
}
