//! Cross-filesystem custody of a recovered image, exercised through the forced-copy seam on one
//! filesystem: the copy transport, its manifest state and every refusal are the production ones,
//! only the device comparison that selects the transport is bypassed (the host smoke owns that).
//! The image is a real sparse regular file in a provisioned project's trash with its real grants
//! sidecar and CA companion, so every byte, hole and record the recovery carries is observed.

use std::fs;
use std::io::{Seek as _, SeekFrom, Write as _};
use std::os::unix::fs::MetadataExt as _;
use std::path::{Path, PathBuf};

use super::{COPY_PROOF_FILE, CopyCheckpoint, MANIFEST_FILE, recover, recover_by_copy};
use crate::api::dto::{RecoveredImage, RecoveryCause, RecoveryRetention};
use crate::error::{CowshedError, ErrorCode, Result};
use crate::metadata::{DetachedWorkspaceMetadata, sidecar_path};
use crate::repository::RepoId;
use crate::storage::apfs::{ApfsExecutionHost as _, companion_path};
use crate::storage::deletion_log::{self, Caller, KeptRecovery, RetiredBy};
use crate::storage::directory::DirectoryExecutionHost;
use crate::storage::directory::tests::{Scratch, published_directory};
use crate::storage::{StorageLayout, WORKSPACE_MARKER_PATH};

const IMAGE_LEN: u64 = 32 * 1024 * 1024;
const PAYLOAD_OFFSET: u64 = 16 * 1024 * 1024;
const PAYLOAD: &[u8] = b"recovery payload that must survive the copy";
const INTERRUPT: &str = "interrupted by the test observer";

struct Fixture {
    host: DirectoryExecutionHost,
    storage: StorageLayout,
    repo: RepoId,
    metadata: DetachedWorkspaceMetadata,
    trash: PathBuf,
    trash_dir: PathBuf,
    sources: Vec<PathBuf>,
    originals: Vec<Vec<u8>>,
    image_blocks: u64,
    to: PathBuf,
    directory: PathBuf,
}

/// A sparse recovery image and its companions in the trash of a provisioned directory store:
/// the layout, lifecycle lock, sidecar record and CA bytes the directory fixture publishes,
/// with the trash payload a native retirement leaves (one regular sparse file).
fn file_fixture(scratch: &Scratch) -> Fixture {
    let (host, canonical, metadata) = published_directory(scratch, "selected", 1);
    let storage = StorageLayout::new(scratch.path().join("store"), &metadata.repo_id).unwrap();
    let trash = storage.project().sessions.join(format!(
        ".trash/selected-{}.dmg",
        metadata.workspace_incarnation
    ));
    let trash_dir = trash.parent().unwrap().to_owned();
    fs::create_dir_all(&trash_dir).unwrap();
    let mut image = fs::File::create(&trash).unwrap();
    image.set_len(IMAGE_LEN).unwrap();
    image.seek(SeekFrom::Start(PAYLOAD_OFFSET)).unwrap();
    image.write_all(PAYLOAD).unwrap();
    image.seek(SeekFrom::Start(0)).unwrap();
    image.write_all(b"raw image head").unwrap();
    image.sync_all().unwrap();
    drop(image);
    metadata.write_for_image(&trash).unwrap();
    let key = fs::read(companion_path(&canonical)).unwrap();
    fs::write(companion_path(&trash), &key).unwrap();
    let sources = vec![sidecar_path(&trash), companion_path(&trash), trash.clone()];
    let originals = sources.iter().map(|path| fs::read(path).unwrap()).collect();
    let image_blocks = fs::metadata(&trash).unwrap().blocks();
    let to = scratch.path().join("recovered");
    let directory = to.join(format!("selected-{}", metadata.workspace_incarnation));
    Fixture {
        repo: metadata.repo_id.clone(),
        host,
        storage,
        metadata,
        trash,
        trash_dir,
        sources,
        originals,
        image_blocks,
        to,
        directory,
    }
}

fn finals(fixture: &Fixture) -> Vec<PathBuf> {
    fixture
        .sources
        .iter()
        .map(|source| fixture.directory.join(source.file_name().unwrap()))
        .collect()
}

fn copy_ok(fixture: &Fixture) -> RecoveredImage {
    recover_by_copy(
        &fixture.storage,
        &fixture.repo,
        &fixture.trash,
        &fixture.to,
        &mut |_, _| Ok(()),
    )
    .unwrap()
}

fn interrupted() -> Result<()> {
    Err(CowshedError::internal(INTERRUPT))
}

fn checkpoint_name(checkpoint: CopyCheckpoint) -> &'static str {
    match checkpoint {
        CopyCheckpoint::ManifestDurable => "manifest",
        CopyCheckpoint::CopyCreated => "copy-created",
        CopyCheckpoint::Staged => "staged",
        CopyCheckpoint::Published => "published",
        CopyCheckpoint::BeforeRetire => "before-retire",
        CopyCheckpoint::BeforeSourceRetire => "before-source-retire",
        CopyCheckpoint::RetirementStaged => "retirement-staged",
        CopyCheckpoint::Retired => "retired",
    }
}

fn assert_sources_intact(fixture: &Fixture) {
    for (source, original) in fixture.sources.iter().zip(&fixture.originals) {
        assert_eq!(
            &fs::read(source).unwrap(),
            original,
            "the source {} is retained byte for byte",
            source.display()
        );
    }
}

fn assert_finals(fixture: &Fixture, recovered: &RecoveredImage) {
    let expected = finals(fixture);
    assert_eq!(recovered.moved, expected);
    for (moved, original) in expected.iter().zip(&fixture.originals) {
        assert_eq!(
            &fs::read(moved).unwrap(),
            original,
            "the recovered {} carries the source bytes",
            moved.display()
        );
    }
    assert_eq!(
        fs::read_dir(&fixture.trash_dir).unwrap().count(),
        0,
        "the trash keeps nothing once the copy is complete"
    );
}

fn partial_entries(directory: &Path) -> Vec<PathBuf> {
    let mut partials: Vec<PathBuf> = fs::read_dir(directory)
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".partial"))
        })
        .collect();
    partials.sort();
    partials
}

fn overwrite_at(path: &Path, offset: u64, bytes: &[u8]) {
    let mut file = fs::OpenOptions::new().write(true).open(path).unwrap();
    file.seek(SeekFrom::Start(offset)).unwrap();
    file.write_all(bytes).unwrap();
    file.sync_all().unwrap();
}

fn record_retention(fixture: &Fixture, scratch: &Scratch) -> Vec<RecoveryCause> {
    let because = vec![RecoveryCause::OnlyCopy {
        path: PathBuf::from("notes/only.txt"),
    }];
    deletion_log::log_retirement(
        &fixture.storage.project().project_root,
        "selected",
        &scratch.path().join("mnt/selected"),
        None,
        RetiredBy {
            caller: Caller {
                pid: 1,
                argv: vec!["cowshed".to_owned(), "rm".to_owned()],
                parent: None,
            },
            forced: None,
        },
        Some(KeptRecovery {
            image: fixture.trash.clone(),
            because: because.clone(),
        }),
    );
    because
}

/// The copy transport carries a sparse image, its grants sidecar and its CA companion out of
/// the store beside an immutable admission manifest and a final copy proof recording every artifact as published: bytes and holes
/// survive, no source byte is dropped before the whole set is durable at the destination, and
/// the finished recovery answers the same on repeat. A destination inside the store and an
/// occupied one refuse the copy exactly as they refuse the rename, before anything moves.
#[test]
fn a_recovery_image_copies_out_of_the_store_sparsely_with_its_companions_and_a_manifest() {
    let scratch = Scratch::new("copy-recover-image");
    let fixture = file_fixture(&scratch);
    let because = record_retention(&fixture, &scratch);

    let inside = fixture.storage.project().store_root.join("kept");
    let refused = recover_by_copy(
        &fixture.storage,
        &fixture.repo,
        &fixture.trash,
        &inside,
        &mut |_, _| Ok(()),
    )
    .unwrap_err();
    assert_eq!(refused.code, ErrorCode::Usage);
    assert!(
        !inside.exists(),
        "a destination inside the store is never made"
    );
    fs::create_dir_all(&fixture.directory).unwrap();
    let refused = recover_by_copy(
        &fixture.storage,
        &fixture.repo,
        &fixture.trash,
        &fixture.to,
        &mut |_, _| Ok(()),
    )
    .unwrap_err();
    assert_eq!(
        refused.code,
        ErrorCode::Conflict,
        "an occupied destination is never merged into"
    );
    fs::remove_dir(&fixture.directory).unwrap();
    assert_sources_intact(&fixture);

    let mut events: Vec<(&'static str, PathBuf)> = Vec::new();
    let recovered = recover_by_copy(
        &fixture.storage,
        &fixture.repo,
        &fixture.trash,
        &fixture.to,
        &mut |checkpoint, path| {
            let name = checkpoint_name(checkpoint);
            if name == "before-retire" {
                assert_sources_intact(&fixture);
            }
            if name == "published" && path == fixture.trash.as_path() {
                assert!(
                    fixture.trash.exists(),
                    "publishing the image never retires its source early"
                );
            }
            events.push((name, path.to_owned()));
            Ok(())
        },
    )
    .unwrap();

    assert_eq!(events.first().map(|(name, _)| *name), Some("manifest"));
    let last_published = events
        .iter()
        .rposition(|(name, _)| *name == "published")
        .unwrap();
    let before_retire = events
        .iter()
        .position(|(name, _)| *name == "before-retire")
        .unwrap();
    let first_retired = events
        .iter()
        .position(|(name, _)| *name == "retired")
        .unwrap();
    assert!(
        last_published < before_retire && before_retire < first_retired,
        "every artifact is published before any source is retired: {events:?}"
    );

    assert_eq!(recovered.workspace, fixture.metadata.workspace);
    assert_eq!(
        recovered.incarnation,
        fixture.metadata.workspace_incarnation
    );
    assert_eq!(recovered.source, fixture.trash);
    assert_eq!(recovered.directory, fixture.directory);
    assert_eq!(recovered.manifest, fixture.directory.join(MANIFEST_FILE));
    assert!(recovered.grants);
    assert!(matches!(
        &recovered.retained,
        RecoveryRetention::Recorded { because: recorded, .. } if *recorded == because
    ));
    assert_finals(&fixture, &recovered);
    let moved = &recovered.moved[2];
    assert_eq!(
        DetachedWorkspaceMetadata::read_for_image(moved).unwrap(),
        fixture.metadata
    );
    let copied = fs::metadata(moved).unwrap();
    assert_eq!(copied.len(), IMAGE_LEN);
    assert!(
        copied.blocks() * 512 <= fixture.image_blocks * 512 + 65536,
        "sparse holes were materialized: source {} bytes, copied {} bytes",
        fixture.image_blocks * 512,
        copied.blocks() * 512
    );
    assert!(
        partial_entries(&fixture.directory).is_empty(),
        "a published stage is renamed away, never left behind"
    );
    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&recovered.manifest).unwrap()).unwrap();
    assert_eq!(manifest["workspace"], "selected");
    assert_eq!(
        manifest["incarnation"],
        fixture.metadata.workspace_incarnation.as_str()
    );
    assert_eq!(manifest["source"], fixture.trash.to_str().unwrap());
    assert_eq!(manifest["retained"]["because"][0]["kind"], "onlyCopy");
    let artifacts = manifest["copy"]["artifacts"].as_array().unwrap();
    assert_eq!(artifacts.len(), fixture.sources.len());
    for artifact in artifacts {
        assert_eq!(
            artifact["state"]["phase"], "pending",
            "the admission manifest is immutable: {artifact}"
        );
    }
    let proof: serde_json::Value =
        serde_json::from_slice(&fs::read(fixture.directory.join(COPY_PROOF_FILE)).unwrap())
            .unwrap();
    assert_eq!(proof["source"], fixture.trash.to_str().unwrap());
    let artifacts = proof["copy"]["artifacts"].as_array().unwrap();
    assert_eq!(artifacts.len(), fixture.sources.len());
    for artifact in artifacts {
        assert_eq!(artifact["state"]["phase"], "published", "{artifact}");
    }

    assert_eq!(
        copy_ok(&fixture),
        recovered,
        "a finished copy answers the same on repeat"
    );
    assert_finals(&fixture, &recovered);
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Stop {
    Manifest,
    CopyCreated,
    StagedImage,
    PublishedSidecar,
    BeforeRetire,
    FirstRetired,
}

fn stop_at(stop: Stop, checkpoint: CopyCheckpoint, path: &Path, fixture: &Fixture) -> bool {
    match (stop, checkpoint) {
        (Stop::Manifest, CopyCheckpoint::ManifestDurable) => true,
        (Stop::CopyCreated, CopyCheckpoint::CopyCreated) => path == fixture.trash.as_path(),
        (Stop::StagedImage, CopyCheckpoint::Staged) => path == fixture.trash.as_path(),
        (Stop::PublishedSidecar, CopyCheckpoint::Published) => {
            path == sidecar_path(&fixture.trash).as_path()
        }
        (Stop::BeforeRetire, CopyCheckpoint::BeforeRetire) => true,
        (Stop::FirstRetired, CopyCheckpoint::Retired) => true,
        _ => false,
    }
}

/// An interruption at every publication boundary leaves custody unambiguous - the complete
/// source set in the trash until retirement begins, afterwards each artifact in exactly one
/// place - and the same request finishes the recovery its manifest describes, keeping the
/// identity of every artifact an earlier attempt already published.
#[test]
fn an_interrupted_copy_keeps_custody_and_finishes_on_retry() {
    for (index, stop) in [
        Stop::Manifest,
        Stop::CopyCreated,
        Stop::StagedImage,
        Stop::PublishedSidecar,
        Stop::BeforeRetire,
        Stop::FirstRetired,
    ]
    .into_iter()
    .enumerate()
    {
        let scratch = Scratch::new(&format!("copy-stop-{index}"));
        let fixture = file_fixture(&scratch);
        let refused = recover_by_copy(
            &fixture.storage,
            &fixture.repo,
            &fixture.trash,
            &fixture.to,
            &mut |checkpoint, path| {
                if stop_at(stop, checkpoint, path, &fixture) {
                    interrupted()
                } else {
                    Ok(())
                }
            },
        )
        .unwrap_err();
        assert_eq!(refused.code, ErrorCode::Internal, "{stop:?}");
        assert!(
            refused.message.contains(INTERRUPT),
            "{stop:?}: {}",
            refused.message
        );

        let expected_finals = finals(&fixture);
        if stop == Stop::FirstRetired {
            assert_eq!(
                fixture
                    .sources
                    .iter()
                    .filter(|source| source.exists())
                    .count(),
                fixture.sources.len() - 1,
                "{stop:?}: exactly the retired artifact has left the trash"
            );
            for ((source, moved), original) in fixture
                .sources
                .iter()
                .zip(&expected_finals)
                .zip(&fixture.originals)
            {
                let held = if source.exists() { source } else { moved };
                assert_eq!(
                    &fs::read(held).unwrap(),
                    original,
                    "{stop:?}: custody of {}",
                    held.display()
                );
            }
        } else {
            assert_sources_intact(&fixture);
        }
        let published_identities: Vec<(PathBuf, u64)> = expected_finals
            .iter()
            .filter(|moved| moved.exists())
            .map(|moved| (moved.clone(), fs::metadata(moved).unwrap().ino()))
            .collect();

        let recovered = copy_ok(&fixture);
        assert_finals(&fixture, &recovered);
        for (moved, inode) in &published_identities {
            assert_eq!(
                fs::metadata(moved).unwrap().ino(),
                *inode,
                "{stop:?}: an already published artifact is never recopied on resume"
            );
        }
    }
}

/// A source that changes under the copy is never recovered as if it had not: drift observed
/// against the stamp captured before any effect refuses, the changed source stays in the
/// trash, and a retry refuses the same drift instead of adopting the new bytes.
#[test]
fn source_drift_during_a_copy_refuses_and_keeps_the_source() {
    let drift = vec![b'D'; PAYLOAD.len()];

    let scratch = Scratch::new("copy-drift-staged");
    let fixture = file_fixture(&scratch);
    let refused = recover_by_copy(
        &fixture.storage,
        &fixture.repo,
        &fixture.trash,
        &fixture.to,
        &mut |checkpoint, path| {
            if matches!(checkpoint, CopyCheckpoint::Staged) && path == fixture.trash.as_path() {
                overwrite_at(&fixture.trash, PAYLOAD_OFFSET, &drift);
            }
            Ok(())
        },
    )
    .unwrap_err();
    assert_eq!(refused.code, ErrorCode::Integrity);
    let kept = fs::read(&fixture.trash).unwrap();
    assert_eq!(
        &kept[PAYLOAD_OFFSET as usize..PAYLOAD_OFFSET as usize + PAYLOAD.len()],
        drift.as_slice(),
        "the drifted source itself is retained, not silently replaced"
    );
    let moved = &finals(&fixture)[2];
    if moved.exists() {
        let placed = fs::read(moved).unwrap();
        assert_ne!(
            &placed[PAYLOAD_OFFSET as usize..PAYLOAD_OFFSET as usize + PAYLOAD.len()],
            drift.as_slice(),
            "the drifted bytes were never claimed as recovered"
        );
    }
    let refused = recover_by_copy(
        &fixture.storage,
        &fixture.repo,
        &fixture.trash,
        &fixture.to,
        &mut |_, _| Ok(()),
    )
    .unwrap_err();
    assert_eq!(
        refused.code,
        ErrorCode::Integrity,
        "a retry refuses the same drift"
    );
    assert!(fixture.trash.exists());

    let scratch = Scratch::new("copy-drift-retire");
    let fixture = file_fixture(&scratch);
    let companion = companion_path(&fixture.trash);
    let refused = recover_by_copy(
        &fixture.storage,
        &fixture.repo,
        &fixture.trash,
        &fixture.to,
        &mut |checkpoint, _| {
            if matches!(checkpoint, CopyCheckpoint::BeforeRetire) {
                overwrite_at(&companion, 0, b"drifted");
            }
            Ok(())
        },
    )
    .unwrap_err();
    assert_eq!(refused.code, ErrorCode::Integrity);
    for source in &fixture.sources {
        assert!(
            source.exists(),
            "no source is retired over drift: {}",
            source.display()
        );
    }
    assert_eq!(
        &fs::read(&fixture.sources[0]).unwrap(),
        &fixture.originals[0]
    );
    assert_eq!(
        &fs::read(&fixture.sources[2]).unwrap(),
        &fixture.originals[2]
    );
}

/// A staged copy whose bytes changed after its durable record is evidence of tampering, not
/// an invitation to copy over it: resume verifies the very inode the manifest admitted,
/// refuses the corruption, and leaves the complete source in the trash on every retry.
#[test]
fn a_corrupted_staged_copy_is_never_published() {
    let scratch = Scratch::new("copy-corrupt-stage");
    let fixture = file_fixture(&scratch);
    let refused = recover_by_copy(
        &fixture.storage,
        &fixture.repo,
        &fixture.trash,
        &fixture.to,
        &mut |checkpoint, path| {
            if matches!(checkpoint, CopyCheckpoint::Staged) && path == fixture.trash.as_path() {
                interrupted()
            } else {
                Ok(())
            }
        },
    )
    .unwrap_err();
    assert!(refused.message.contains(INTERRUPT));

    let image_name = fixture.trash.file_name().unwrap().to_str().unwrap();
    let staged = partial_entries(&fixture.directory)
        .into_iter()
        .find(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.contains(image_name))
        })
        .expect("the interrupted image copy retains its stage");
    let corruption = vec![b'X'; PAYLOAD.len()];
    overwrite_at(&staged, PAYLOAD_OFFSET, &corruption);

    for _ in 0..2 {
        let refused = recover_by_copy(
            &fixture.storage,
            &fixture.repo,
            &fixture.trash,
            &fixture.to,
            &mut |_, _| Ok(()),
        )
        .unwrap_err();
        assert_eq!(refused.code, ErrorCode::Integrity);
        assert_sources_intact(&fixture);
    }
    assert!(
        !fixture.directory.join(COPY_PROOF_FILE).exists(),
        "the corrupted stage never counts as a completed copy"
    );
}

/// Nothing foreign at the destination is merged into, followed or overwritten: an occupied
/// directory, a symlink where the directory belongs and a file planted at a name the manifest
/// expects to publish all refuse with both sides byte-identical, while a stage nobody admitted
/// is left untouched and never trusted as this recovery's own.
#[test]
fn a_foreign_destination_is_never_merged_into_or_overwritten() {
    let scratch = Scratch::new("copy-foreign-occupied");
    let fixture = file_fixture(&scratch);
    fs::create_dir_all(&fixture.directory).unwrap();
    let foreign = fixture.directory.join("someone-elses-work");
    fs::write(&foreign, b"not this recovery's").unwrap();
    let refused = recover_by_copy(
        &fixture.storage,
        &fixture.repo,
        &fixture.trash,
        &fixture.to,
        &mut |_, _| Ok(()),
    )
    .unwrap_err();
    assert_eq!(refused.code, ErrorCode::Conflict);
    assert_eq!(fs::read(&foreign).unwrap(), b"not this recovery's");
    assert_sources_intact(&fixture);

    let scratch = Scratch::new("copy-foreign-symlink");
    let fixture = file_fixture(&scratch);
    let elsewhere = scratch.path().join("elsewhere");
    fs::create_dir_all(&elsewhere).unwrap();
    fs::create_dir_all(&fixture.to).unwrap();
    std::os::unix::fs::symlink(&elsewhere, &fixture.directory).unwrap();
    let refused = recover_by_copy(
        &fixture.storage,
        &fixture.repo,
        &fixture.trash,
        &fixture.to,
        &mut |_, _| Ok(()),
    )
    .unwrap_err();
    assert_eq!(refused.code, ErrorCode::Conflict);
    assert!(
        fs::symlink_metadata(&fixture.directory)
            .unwrap()
            .file_type()
            .is_symlink()
    );
    assert_eq!(
        fs::read_dir(&elsewhere).unwrap().count(),
        0,
        "the link is never followed"
    );
    assert_sources_intact(&fixture);

    let scratch = Scratch::new("copy-foreign-final");
    let fixture = file_fixture(&scratch);
    let refused = recover_by_copy(
        &fixture.storage,
        &fixture.repo,
        &fixture.trash,
        &fixture.to,
        &mut |checkpoint, path| {
            if matches!(checkpoint, CopyCheckpoint::Staged) && path == fixture.trash.as_path() {
                interrupted()
            } else {
                Ok(())
            }
        },
    )
    .unwrap_err();
    assert!(refused.message.contains(INTERRUPT));
    let planted = &finals(&fixture)[2];
    fs::write(planted, b"foreign bytes at the recovered name").unwrap();
    let refused = recover_by_copy(
        &fixture.storage,
        &fixture.repo,
        &fixture.trash,
        &fixture.to,
        &mut |_, _| Ok(()),
    )
    .unwrap_err();
    assert_eq!(refused.code, ErrorCode::Conflict);
    assert_eq!(
        fs::read(planted).unwrap(),
        b"foreign bytes at the recovered name"
    );
    assert_sources_intact(&fixture);

    let scratch = Scratch::new("copy-foreign-stage");
    let fixture = file_fixture(&scratch);
    let refused = recover_by_copy(
        &fixture.storage,
        &fixture.repo,
        &fixture.trash,
        &fixture.to,
        &mut |checkpoint, path| {
            if matches!(checkpoint, CopyCheckpoint::CopyCreated) && path == fixture.trash.as_path()
            {
                interrupted()
            } else {
                Ok(())
            }
        },
    )
    .unwrap_err();
    assert!(refused.message.contains(INTERRUPT));
    let image_name = fixture.trash.file_name().unwrap().to_str().unwrap();
    let fake_stage = fixture.directory.join(format!(
        ".{image_name}.recovery-00000000000000000000000000000000.partial"
    ));
    fs::write(&fake_stage, b"a stage nobody admitted").unwrap();
    let recovered = copy_ok(&fixture);
    assert_finals(&fixture, &recovered);
    assert_eq!(fs::read(&fake_stage).unwrap(), b"a stage nobody admitted");
}

/// The copy transport is only for the native payload of regular files: a directory payload
/// refuses before the recovered directory or its manifest is made, and the same-filesystem
/// rename that payload has always used still takes it out of the store afterwards.
#[test]
fn a_directory_payload_refuses_the_copy_before_any_effect_and_still_renames() {
    let scratch = Scratch::new("copy-directory-payload");
    let (host, image, metadata) = published_directory(&scratch, "selected", 1);
    let storage = StorageLayout::new(scratch.path().join("store"), &metadata.repo_id).unwrap();
    let trash = storage.project().sessions.join(format!(
        ".trash/selected-{}.dmg",
        metadata.workspace_incarnation
    ));
    fs::create_dir_all(trash.parent().unwrap()).unwrap();
    fs::hard_link(sidecar_path(&image), sidecar_path(&trash)).unwrap();
    fs::hard_link(companion_path(&image), companion_path(&trash)).unwrap();
    host.retire_image(&image, &trash).unwrap();

    let to = scratch.path().join("recovered");
    let directory = to.join(format!("selected-{}", metadata.workspace_incarnation));
    let refused = recover_by_copy(&storage, &metadata.repo_id, &trash, &to, &mut |_, _| Ok(()));
    assert!(
        refused.is_err(),
        "a directory payload has no copy transport"
    );
    assert!(
        !directory.exists(),
        "the refusal precedes the recovered directory and manifest"
    );
    assert!(trash.join(WORKSPACE_MARKER_PATH).exists());
    assert!(sidecar_path(&trash).exists());
    assert!(companion_path(&trash).exists());

    let recovered = recover(&storage, &metadata.repo_id, &trash, &to, &host).unwrap();
    assert!(recovered.moved[2].join(WORKSPACE_MARKER_PATH).exists());
    assert!(!trash.exists());
}

/// On the store's own filesystem the production entry still moves by rename, never by copy:
/// every artifact arrives with the very inode it held in the trash.
#[test]
fn the_same_filesystem_path_still_renames_instead_of_copying() {
    let scratch = Scratch::new("copy-same-fs-rename");
    let fixture = file_fixture(&scratch);
    let identities: Vec<u64> = fixture
        .sources
        .iter()
        .map(|source| fs::metadata(source).unwrap().ino())
        .collect();
    let recovered = recover(
        &fixture.storage,
        &fixture.repo,
        &fixture.trash,
        &fixture.to,
        &fixture.host,
    )
    .unwrap();
    assert_finals(&fixture, &recovered);
    for (moved, inode) in recovered.moved.iter().zip(&identities) {
        assert_eq!(
            fs::metadata(moved).unwrap().ino(),
            *inode,
            "a rename keeps the inode"
        );
    }
}

fn identity_json(path: &Path) -> serde_json::Value {
    let metadata = fs::metadata(path).unwrap();
    serde_json::json!({
        "device": metadata.dev(),
        "inode": metadata.ino(),
        "size": metadata.len(),
        "mode": metadata.mode(),
        "mtime": [metadata.mtime(), metadata.mtime_nsec()],
        "ctime": [metadata.ctime(), metadata.ctime_nsec()],
    })
}

fn directory_identity_json(path: &Path) -> serde_json::Value {
    let metadata = fs::metadata(path).unwrap();
    serde_json::json!({ "device": metadata.dev(), "inode": metadata.ino() })
}

/// The two destination documents one genuine completed recovery writes, harvested as the
/// template a forger with stat access to both directories could copy field for field.
fn completed_documents() -> (serde_json::Value, serde_json::Value) {
    let scratch = Scratch::new("copy-forge-template");
    let fixture = file_fixture(&scratch);
    let recovered = copy_ok(&fixture);
    let admission = serde_json::from_slice(&fs::read(&recovered.manifest).unwrap()).unwrap();
    let proof = serde_json::from_slice(&fs::read(fixture.directory.join(COPY_PROOF_FILE)).unwrap())
        .unwrap();
    (admission, proof)
}

/// Point a harvested document at this fixture: its source image, both directory identities,
/// every source stamp copied from stat, and - for a proof - a target stamp copied from each
/// planted destination file, so every self-reported identity in the forgery is true.
fn retarget_document(
    document: &mut serde_json::Value,
    fixture: &Fixture,
    targets: Option<&[PathBuf]>,
) {
    document["source"] = serde_json::json!(fixture.trash.to_str().unwrap());
    document["copy"]["sourceDirectory"] = directory_identity_json(&fixture.trash_dir);
    document["copy"]["destinationDirectory"] = directory_identity_json(&fixture.directory);
    for (index, artifact) in document["copy"]["artifacts"]
        .as_array_mut()
        .unwrap()
        .iter_mut()
        .enumerate()
    {
        artifact["source"] = identity_json(&fixture.sources[index]);
        if let Some(targets) = targets {
            artifact["state"]["target"] = identity_json(&targets[index]);
        }
    }
}

/// A destination planted before any recovery ran, holding wrong bytes under every final name
/// plus a fully forged published proof whose source and target identities are copied from
/// stat, authorizes nothing: without a source-held journal record the directory is merely
/// occupied, and neither the real sources nor the planted bytes and documents are touched.
#[test]
fn a_fully_forged_published_proof_without_source_authority_deletes_nothing() {
    let (mut admission, mut proof) = completed_documents();
    let scratch = Scratch::new("copy-forged-cold");
    let fixture = file_fixture(&scratch);
    fs::create_dir_all(&fixture.directory).unwrap();
    let planted = finals(&fixture);
    let wrong: Vec<Vec<u8>> = vec![
        b"forged sidecar".to_vec(),
        b"forged key".to_vec(),
        b"forged image bytes".to_vec(),
    ];
    for (path, bytes) in planted.iter().zip(&wrong) {
        fs::write(path, bytes).unwrap();
    }
    retarget_document(&mut admission, &fixture, None);
    retarget_document(&mut proof, &fixture, Some(&planted));
    let admission_bytes = serde_json::to_vec_pretty(&admission).unwrap();
    let proof_bytes = serde_json::to_vec_pretty(&proof).unwrap();
    fs::write(fixture.directory.join(MANIFEST_FILE), &admission_bytes).unwrap();
    fs::write(fixture.directory.join(COPY_PROOF_FILE), &proof_bytes).unwrap();

    let refused = recover_by_copy(
        &fixture.storage,
        &fixture.repo,
        &fixture.trash,
        &fixture.to,
        &mut |_, _| Ok(()),
    )
    .unwrap_err();
    assert_eq!(
        refused.code,
        ErrorCode::Conflict,
        "no source-held record admits the planted directory"
    );
    assert_sources_intact(&fixture);
    for (path, bytes) in planted.iter().zip(&wrong) {
        assert_eq!(
            &fs::read(path).unwrap(),
            bytes,
            "planted bytes are preserved"
        );
    }
    assert_eq!(
        fs::read(fixture.directory.join(MANIFEST_FILE)).unwrap(),
        admission_bytes
    );
    assert_eq!(
        fs::read(fixture.directory.join(COPY_PROOF_FILE)).unwrap(),
        proof_bytes
    );
}

/// Over a genuine journal whose publications are durable, replacing two destination finals
/// with wrong inodes and forging the complete proof around their real stat identities still
/// authorizes nothing: the journal's own progress witnesses refuse the resume, and the real
/// sources, the genuine sidecar copy, the planted bytes and the forged proof all survive.
#[test]
fn a_forged_proof_over_a_genuine_journal_deletes_nothing() {
    let (_admission, mut proof) = completed_documents();
    let scratch = Scratch::new("copy-forged-proof");
    let fixture = file_fixture(&scratch);
    let refused = recover_by_copy(
        &fixture.storage,
        &fixture.repo,
        &fixture.trash,
        &fixture.to,
        &mut |checkpoint, path| {
            if matches!(checkpoint, CopyCheckpoint::Published) && path == fixture.trash.as_path() {
                interrupted()
            } else {
                Ok(())
            }
        },
    )
    .unwrap_err();
    assert!(refused.message.contains(INTERRUPT));
    assert!(
        !fixture.directory.join(COPY_PROOF_FILE).exists(),
        "the interruption precedes the genuine proof"
    );

    let planted = finals(&fixture);
    let genuine_sidecar = fs::read(&planted[0]).unwrap();
    let wrong_companion = b"planted companion bytes".to_vec();
    let wrong_image = b"planted image bytes".to_vec();
    fs::remove_file(&planted[1]).unwrap();
    fs::write(&planted[1], &wrong_companion).unwrap();
    fs::remove_file(&planted[2]).unwrap();
    fs::write(&planted[2], &wrong_image).unwrap();
    retarget_document(&mut proof, &fixture, Some(&planted));
    let proof_bytes = serde_json::to_vec_pretty(&proof).unwrap();
    fs::write(fixture.directory.join(COPY_PROOF_FILE), &proof_bytes).unwrap();

    let refused = recover_by_copy(
        &fixture.storage,
        &fixture.repo,
        &fixture.trash,
        &fixture.to,
        &mut |_, _| Ok(()),
    )
    .unwrap_err();
    assert_eq!(refused.code, ErrorCode::Integrity);
    assert_sources_intact(&fixture);
    assert_eq!(fs::read(&planted[0]).unwrap(), genuine_sidecar);
    assert_eq!(fs::read(&planted[1]).unwrap(), wrong_companion);
    assert_eq!(fs::read(&planted[2]).unwrap(), wrong_image);
    assert_eq!(
        fs::read(fixture.directory.join(COPY_PROOF_FILE)).unwrap(),
        proof_bytes,
        "the forged proof is evidence, never rewritten"
    );
}

/// The destination admission is never replaced in place: swapping its inode for a parseable
/// but different manifest after the image is staged leaves the foreign document byte for
/// byte where the writer put it and refuses before any source is retired, because only the
/// source journal's admitted document and identity can authorize the retirement.
#[test]
fn a_swapped_admission_manifest_is_preserved_and_refused() {
    let scratch = Scratch::new("copy-manifest-swap");
    let fixture = file_fixture(&scratch);
    let manifest_path = fixture.directory.join(MANIFEST_FILE);
    let mut forged_bytes: Vec<u8> = Vec::new();
    let refused = recover_by_copy(
        &fixture.storage,
        &fixture.repo,
        &fixture.trash,
        &fixture.to,
        &mut |checkpoint, path| {
            if matches!(checkpoint, CopyCheckpoint::Staged) && path == fixture.trash.as_path() {
                let mut forged: serde_json::Value =
                    serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
                forged["copy"]["artifacts"][2]["source"]["size"] = serde_json::json!(IMAGE_LEN + 1);
                forged_bytes = serde_json::to_vec_pretty(&forged).unwrap();
                let planted = fixture.directory.join("planted-manifest");
                fs::write(&planted, &forged_bytes).unwrap();
                fs::rename(&planted, &manifest_path).unwrap();
            }
            Ok(())
        },
    )
    .unwrap_err();
    assert_eq!(refused.code, ErrorCode::Integrity);
    assert_eq!(
        fs::read(&manifest_path).unwrap(),
        forged_bytes,
        "the swapped manifest inode is never overwritten"
    );
    assert_sources_intact(&fixture);
}

/// The entry for the source a retirement has staged inside its private directory, if any.
fn retirement_stage(fixture: &Fixture, source: &Path) -> Option<PathBuf> {
    let name = source.file_name().unwrap();
    for entry in fs::read_dir(&fixture.trash_dir).unwrap() {
        let path = entry.unwrap().path();
        let is_stage = path
            .file_name()
            .and_then(|entry_name| entry_name.to_str())
            .is_some_and(|entry_name| {
                entry_name.contains(".recovery-retire-") && entry_name.ends_with(".partial")
            });
        if is_stage {
            let staged = path.join(name);
            if staged.exists() {
                return Some(staged);
            }
        }
    }
    None
}

/// Retirement deletes by inode, never by name: a writer that renames the validated sidecar
/// aside and plants a foreign file at its name in the window after validation loses neither
/// file. The staged foreign inode is refused and preserved, the validated inode survives
/// where the writer moved it, and the remaining sources stay in the trash untouched.
#[test]
fn a_source_name_swap_at_retirement_deletes_neither_inode() {
    let scratch = Scratch::new("copy-retire-swap");
    let fixture = file_fixture(&scratch);
    let sidecar = sidecar_path(&fixture.trash);
    let kept_aside = fixture.trash_dir.join("kept-aside-by-writer");
    let foreign = b"foreign file at the sidecar name".to_vec();
    let refused = recover_by_copy(
        &fixture.storage,
        &fixture.repo,
        &fixture.trash,
        &fixture.to,
        &mut |checkpoint, path| {
            if matches!(checkpoint, CopyCheckpoint::BeforeSourceRetire) && path == sidecar.as_path()
            {
                fs::rename(&sidecar, &kept_aside).unwrap();
                fs::write(&sidecar, &foreign).unwrap();
            }
            Ok(())
        },
    )
    .unwrap_err();
    assert_eq!(refused.code, ErrorCode::Integrity);
    assert_eq!(
        fs::read(&kept_aside).unwrap(),
        fixture.originals[0],
        "the validated inode survives where the writer moved it"
    );
    let foreign_preserved = (sidecar.exists() && fs::read(&sidecar).unwrap() == foreign)
        || retirement_stage(&fixture, &sidecar)
            .is_some_and(|staged| fs::read(staged).unwrap() == foreign);
    assert!(foreign_preserved, "the foreign inode was not deleted");
    for index in 1..fixture.sources.len() {
        assert_eq!(
            &fs::read(&fixture.sources[index]).unwrap(),
            &fixture.originals[index],
            "no later source is retired over the swap"
        );
    }
    for (moved, original) in finals(&fixture).iter().zip(&fixture.originals) {
        assert_eq!(&fs::read(moved).unwrap(), original);
    }
}

/// A retirement interrupted after its source was moved into the private stage and journaled
/// there resumes against exactly that inode: the companion waits under its own name inside
/// the stage directory, the sidecar is already retired, the image is untouched, and the
/// same request then finishes leaving the trash - stage directory included - empty.
#[test]
fn an_interrupted_retirement_resumes_from_the_private_stage() {
    let scratch = Scratch::new("copy-retire-staged");
    let fixture = file_fixture(&scratch);
    let refused = recover_by_copy(
        &fixture.storage,
        &fixture.repo,
        &fixture.trash,
        &fixture.to,
        &mut |checkpoint, path| {
            if matches!(checkpoint, CopyCheckpoint::RetirementStaged)
                && path == fixture.sources[1].as_path()
            {
                interrupted()
            } else {
                Ok(())
            }
        },
    )
    .unwrap_err();
    assert!(refused.message.contains(INTERRUPT));

    let expected = finals(&fixture);
    assert!(
        !fixture.sources[0].exists(),
        "the sidecar source is retired"
    );
    assert_eq!(&fs::read(&expected[0]).unwrap(), &fixture.originals[0]);
    assert!(
        !fixture.sources[1].exists(),
        "the companion has left its original name"
    );
    let staged = retirement_stage(&fixture, &fixture.sources[1])
        .expect("the companion waits in the private retirement stage");
    assert_eq!(fs::read(&staged).unwrap(), fixture.originals[1]);
    assert_eq!(
        &fs::read(&fixture.sources[2]).unwrap(),
        &fixture.originals[2],
        "the image source is untouched"
    );

    let recovered = copy_ok(&fixture);
    assert_finals(&fixture, &recovered);
}
