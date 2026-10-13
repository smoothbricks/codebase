//! Journal proof for the explicit re-adoption intent, `LifecycleIntent::ReAdopt`.
//!
//! Governing specs: `specs/cowshed/02_workspaces.md` "Re-adoption: format transition and space
//! reclamation" steps 1-5 (the durable intent records the exact source backing path, incarnation,
//! checkout, build identity, kernel capacities and retained paths before anything changes, and an
//! existing `<checkout>.pre-cowshed` is never overwritten); `specs/cowshed/01_storage.md` "Images"
//! (capacity is recorded from the kernel before detach); `specs/cowshed/16_build_volumes.md`
//! "Targets and seeds" (owner record preserved verbatim; old-incarnation seeds stay retained).
//!
//! Git evidence is captured from a real temporary repository. Images, sidecars and the
//! pre-cowshed tree are sentinel bytes at the exact paths the intent names: the journal must never
//! rewrite them, and no disk image is attached or minted here.

use std::fs;
use std::path::{Path, PathBuf};

use serde_json::{Value, json};

use crate::ErrorCode;
use crate::api::GitOid;
use crate::api::dto::{AdoptOptions, RemoveReport};
use crate::build_volume::{BuildVolumeId, BuildVolumeRecord, BuildVolumeRole};
use crate::git_preservation::{self, GitPreservationSnapshot};
use crate::metadata::{
    DetachedWorkspaceMetadata, GrantSet, MACOS_PORT_MIN, NEW_PORT_BLOCK_SIZE, Platform, PortBlock,
    PublicationState, SIDECAR_VERSION, WorkspaceIncarnation, WorkspaceInfoSnapshot, WorkspaceName,
    WorkspaceRole, sidecar_path,
};
use crate::repository::RepoId;
use crate::temp_root::TempRoot;
use cowshed_os::fork_lock::Run as _;

use super::{
    LIFECYCLE_INTENTS_FILE, LifecycleIntent, LifecycleIntentCompletion, LifecycleIntentJournal,
    LifecycleIntentPhase, MissingMainBuild, ReAdoptBuild, ReAdoptEmptyBuild, ReAdoptImage,
    ReAdoptKeptBuild, ReAdoptMain, ReAdoptMissingMain, ReAdoptRetained, ReAdoptSource,
    STAGING_NAMESPACE, preserved_checkout,
};

const OLD_INCARNATION: &str = "0198f2c0b7e34dc795f17b238b331c80";
const NEW_INCARNATION: &str = "0198f2c0b7e34dc795f17b238b331c81";
const BUILD_ID: &str = "0123456789abcdef0123456789abcdef";
const SEED_IDS: [&str; 2] = [
    "fedcba9876543210fedcba9876543210",
    "00000000000000000000000000000001",
];
/// 100 GiB, the default image capacity (`01_storage.md` "Images").
const MAIN_CAPACITY_BYTES: u64 = 100 * 1024 * 1024 * 1024;
const BUILD_CAPACITY_BYTES: u64 = 50 * 1024 * 1024 * 1024;

/// A real Git checkout, its captured state, and a project directory to record a re-adoption of
/// it under. Other modules' tests (staging collection) build their journals from it, so the
/// record they protect is the validated one, never a stand-in.
pub(crate) struct Fixture {
    _scratch: TempRoot,
    pub(crate) checkout: PathBuf,
    pub(crate) project: PathBuf,
    /// The repository's staging directory its re-adoption mounts sit in: `<project>/.staging`
    /// unless [`Self::with_staging`] names the host's `<mount root>/.staging`.
    staging: PathBuf,
    git_before: GitPreservationSnapshot,
    head_tree: GitOid,
}

impl Fixture {
    pub(crate) fn new() -> Self {
        let scratch = TempRoot::new("cowshed-readopt-journal");
        let checkout = scratch.join("checkout");
        fs::create_dir(&checkout).expect("create checkout");
        git(&checkout, &["init", "-q", "-b", "main"]);
        git(&checkout, &["config", "user.name", "Cowshed Test"]);
        git(&checkout, &["config", "user.email", "test@example.invalid"]);
        fs::write(checkout.join("README"), b"re-adoption fixture\n").expect("write README");
        git(&checkout, &["add", "README"]);
        git(&checkout, &["commit", "-qm", "first"]);
        let git_before =
            git_preservation::capture(&checkout, &checkout).expect("capture real Git state");
        let head_tree =
            GitOid::new(git_text(&checkout, &["rev-parse", "HEAD^{tree}"])).expect("tree oid");
        let project = scratch.join("store").join("acme").join("widget");
        fs::create_dir_all(&project).expect("create project directory");
        let staging = project.join(STAGING_NAMESPACE);
        Self {
            _scratch: scratch,
            checkout,
            project,
            staging,
            git_before,
            head_tree,
        }
    }

    /// The same fixture recording its three re-adoption mounts in `staging`, a repository's
    /// staging directory as the host layout names it.
    #[cfg(target_os = "macos")]
    pub(crate) fn with_staging(mut self, staging: PathBuf) -> Self {
        self.staging = staging;
        self
    }

    /// The store the project directory sits in, `<store>/acme/widget`.
    #[cfg(target_os = "macos")]
    pub(crate) fn store(&self) -> &Path {
        self.project
            .parent()
            .and_then(Path::parent)
            .expect("the fixture project is <store>/<owner>/<repo>")
    }

    pub(crate) fn journal_path(&self) -> PathBuf {
        self.project.join(LIFECYCLE_INTENTS_FILE)
    }

    /// The old main's sidecar, with every required field set to a real value: the checkout's
    /// own root and HEAD, and the closed baseline grants with the macOS port block.
    fn main_metadata(&self) -> DetachedWorkspaceMetadata {
        let block = PortBlock::new(MACOS_PORT_MIN, NEW_PORT_BLOCK_SIZE).expect("port block");
        let mut grants = GrantSet::closed_baseline(Some(block)).expect("closed baseline grants");
        grants.revision = 7;
        DetachedWorkspaceMetadata {
            version: SIDECAR_VERSION,
            repo_id: RepoId::parse("acme/widget").expect("fixture repo id"),
            workspace: WorkspaceName::main(),
            workspace_incarnation: WorkspaceIncarnation::new(OLD_INCARNATION)
                .expect("old incarnation"),
            platform: Platform::Macos,
            publication_state: PublicationState::Active,
            updated_at: "2026-07-11T12:34:56Z".to_owned(),
            grants,
            info_snapshot: WorkspaceInfoSnapshot {
                project_root: self.checkout.clone(),
                role: WorkspaceRole::Main,
                base_commit: self.git_before.head().as_str().to_owned(),
                branch: Some("main".to_owned()),
                created_at: "2026-07-11T12:00:00Z".to_owned(),
                forked_from: None,
                captured_at: "2026-07-11T12:34:00Z".to_owned(),
                stale: false,
                git_worktree: false,
            },
        }
    }

    pub(crate) fn main_image(&self) -> ReAdoptImage {
        ReAdoptImage {
            original: self.project.join("main.dmg"),
            retained: self
                .project
                .join("retained")
                .join(format!("main-{OLD_INCARNATION}.dmg")),
            source_mount: self.staging.join(format!("readopt-{OLD_INCARNATION}-main")),
            capacity_bytes: MAIN_CAPACITY_BYTES,
        }
    }

    /// The live build volume of main, linked to main's checkout. Its owner record is minted once
    /// here and then compared verbatim against what the journal hands back.
    pub(crate) fn build(&self) -> ReAdoptBuild {
        ReAdoptBuild {
            id: BuildVolumeId::parse(BUILD_ID).expect("fixture build id"),
            image: ReAdoptImage {
                original: self.project.join("build").join(format!("{BUILD_ID}.dmg")),
                retained: self
                    .project
                    .join("retained")
                    .join(format!("build-{BUILD_ID}.dmg")),
                source_mount: self
                    .staging
                    .join(format!("readopt-{OLD_INCARNATION}-build")),
                capacity_bytes: BUILD_CAPACITY_BYTES,
            },
            owner_record: BuildVolumeRecord::new(
                Some(self.head_tree.clone()),
                BuildVolumeRole::Linked {
                    checkout: WorkspaceName::main(),
                },
            ),
            replacement_image: self
                .project
                .join(STAGING_NAMESPACE)
                .join(format!("readopt-{NEW_INCARNATION}-build.dmg")),
            replacement_mount: self
                .staging
                .join(format!("readopt-{NEW_INCARNATION}-build")),
        }
    }

    fn old_seeds(&self) -> Vec<ReAdoptRetained> {
        SEED_IDS
            .iter()
            .map(|id| ReAdoptRetained {
                original: self.project.join("build").join(format!("{id}.dmg")),
                retained: self.project.join("retained").join(format!("seed-{id}.dmg")),
            })
            .collect()
    }

    pub(crate) fn source(&self, build: Option<ReAdoptBuild>) -> ReAdoptSource {
        ReAdoptSource {
            checkout: self.checkout.clone(),
            main: ReAdoptMain::Retained(self.main_image()),
            metadata: self.main_metadata(),
            git_before: self.git_before.clone(),
            build,
            old_seeds: self.old_seeds(),
            replacement: WorkspaceIncarnation::new(NEW_INCARNATION)
                .expect("replacement incarnation"),
        }
    }

    pub(crate) fn intent(&self, build: Option<ReAdoptBuild>) -> LifecycleIntent {
        self.intent_for(self.source(build))
    }

    fn intent_for(&self, source: ReAdoptSource) -> LifecycleIntent {
        LifecycleIntent::ReAdopt {
            options: AdoptOptions {
                path: Some(self.checkout.clone()),
                re_adopt: true,
                ..AdoptOptions::default()
            },
            source: Box::new(source),
        }
    }

    /// The tree adopt preserves beside the checkout: `<checkout>.pre-cowshed`.
    fn pre_cowshed(&self) -> PathBuf {
        let mut name = self.checkout.as_os_str().to_owned();
        name.push(".pre-cowshed");
        PathBuf::from(name)
    }
}

fn git(root: &Path, args: &[&str]) -> Vec<u8> {
    let output = crate::git::git_command_at(root)
        .env("GIT_OPTIONAL_LOCKS", "0")
        .args(args)
        .output_locked()
        .expect("run real git fixture command");
    assert!(
        output.status.success(),
        "git {args:?}: {}",
        crate::git::git_ended(&output)
    );
    output.stdout
}

fn git_text(root: &Path, args: &[&str]) -> String {
    String::from_utf8(git(root, args))
        .expect("git output is UTF-8")
        .trim_end()
        .to_owned()
}

fn path_str(path: &Path) -> &str {
    path.to_str().expect("fixture paths are UTF-8")
}

/// Writes a sentinel at `path` and returns what the journal must leave there.
fn plant(path: &Path, bytes: &[u8]) -> (PathBuf, Vec<u8>) {
    fs::create_dir_all(path.parent().expect("planted input has a parent"))
        .expect("create planted parent");
    fs::write(path, bytes).expect("plant input");
    (path.to_owned(), bytes.to_vec())
}

/// `valid` with one field removed at `path` (object keys from the root).
fn without(valid: &Value, path: &[&str], field: &str) -> Value {
    let mut damaged = valid.clone();
    let mut node = &mut damaged;
    for key in path {
        node = &mut node[*key];
    }
    node.as_object_mut()
        .expect("object on the fixture path")
        .remove(field);
    damaged
}

/// `damaged` is refused before anything could act on it: it does not decode, or a journal
/// recording it refuses to persist, so it never reaches disk.
fn assert_refused(damaged: Value, label: &str) {
    let Ok(intent) = serde_json::from_value::<LifecycleIntent>(damaged) else {
        return;
    };
    let scratch = TempRoot::new("cowshed-readopt-refusal");
    let journal = scratch.join(LIFECYCLE_INTENTS_FILE);
    let recorded = LifecycleIntentJournal::update(&journal, |state| {
        state.begin(intent);
        Ok(())
    });
    assert!(recorded.is_err(), "{label} was recorded");
    assert!(!journal.exists(), "{label} reached disk");
}

fn begin_and_mark_mutating(journal: &Path, intent: LifecycleIntent) {
    LifecycleIntentJournal::update(journal, |state| {
        state.begin(intent);
        Ok(())
    })
    .expect("record ReAdopt intent before its first mutation");
    LifecycleIntentJournal::update(journal, |state| state.mark_mutating(&WorkspaceName::main()))
        .expect("mark ReAdopt intent mutating");
}

#[test]
fn retained_input_refuses_an_unrecorded_path() {
    let fixture = Fixture::new();
    let source = fixture.source(Some(fixture.build()));
    source.validate().expect("valid retained source record");
    let main = source.main.retained().expect("retained main");
    let build = source.build.as_ref().expect("retained build");
    for path in [
        &main.original,
        &main.retained,
        &build.image.original,
        &build.image.retained,
    ]
    .into_iter()
    .chain(
        source
            .old_seeds
            .iter()
            .flat_map(|seed| [&seed.original, &seed.retained]),
    ) {
        source
            .retained_input(path)
            .expect("recorded original and retained paths are inputs");
    }
    let unrecorded = fixture.project.join("unrecorded.dmg");
    let error = source
        .retained_input(&unrecorded)
        .expect_err("an unrecorded image is not an input");
    assert_eq!(error.code, ErrorCode::Integrity);
    assert!(
        error
            .message
            .contains("is not an input named by this re-adoption record")
    );
}

#[test]
fn fixture_git_before_is_captured_from_the_real_checkout() {
    let fixture = Fixture::new();
    assert_eq!(
        fixture.git_before.head().as_str(),
        git_text(&fixture.checkout, &["rev-parse", "HEAD"])
    );
    assert!(
        fixture.git_before.fsck_succeeded(),
        "the captured repository passes git fsck"
    );
    let recaptured =
        git_preservation::capture(&fixture.checkout, &fixture.checkout).expect("recapture");
    fixture
        .git_before
        .compare(&recaptured)
        .expect("an unchanged checkout compares equal to its capture");
}

#[test]
fn re_adopt_intent_round_trips_retained_source_capacity_owner_record_and_seed_list() {
    let fixture = Fixture::new();
    let build = fixture.build();
    let intent = fixture.intent(Some(build.clone()));

    let encoded = serde_json::to_value(&intent).expect("encode ReAdopt intent");
    let decoded: LifecycleIntent =
        serde_json::from_value(encoded.clone()).expect("decode ReAdopt intent");
    assert_eq!(decoded, intent);
    assert_eq!(
        serde_json::to_value(&decoded).expect("re-encode ReAdopt intent"),
        encoded
    );

    assert_eq!(encoded["kind"], "reAdopt");
    assert_eq!(encoded["options"]["reAdopt"], true);
    let source = &encoded["source"];
    let main_image = fixture.main_image();
    assert_eq!(source["checkout"], path_str(&fixture.checkout));
    assert_eq!(source["image"]["original"], path_str(&main_image.original));
    assert_eq!(source["image"]["retained"], path_str(&main_image.retained));
    assert_eq!(
        source["image"]["sourceMount"],
        path_str(&main_image.source_mount)
    );
    assert_eq!(source["image"]["capacityBytes"], MAIN_CAPACITY_BYTES);
    assert_eq!(source["build"]["id"], BUILD_ID);
    assert_eq!(
        source["build"]["image"]["retained"],
        path_str(&build.image.retained)
    );
    assert_eq!(
        source["build"]["image"]["capacityBytes"],
        BUILD_CAPACITY_BYTES
    );
    assert_eq!(
        source["build"]["ownerRecord"],
        serde_json::to_value(&build.owner_record).expect("encode owner record")
    );
    assert_eq!(
        source["oldSeeds"],
        serde_json::to_value(fixture.old_seeds()).expect("encode seed list")
    );
    assert!(source["gitBefore"].is_object(), "git evidence is recorded");
}

#[test]
fn re_adopt_without_build_link_round_trips_through_journal_as_null() {
    let fixture = Fixture::new();
    let intent = fixture.intent(None);
    let encoded = serde_json::to_value(&intent).expect("encode ReAdopt intent");
    assert!(
        encoded["source"]["build"].is_null(),
        "an absent build link is null, never an empty stand-in"
    );

    let journal = fixture.journal_path();
    LifecycleIntentJournal::update(&journal, |state| {
        state.begin(intent.clone());
        Ok(())
    })
    .expect("persist ReAdopt intent");
    let loaded = LifecycleIntentJournal::load(&journal).expect("reload journal");
    let record = loaded
        .get(&WorkspaceName::main())
        .expect("main intent is recorded");
    assert_eq!(record.operation, intent);
    assert_eq!(record.completion, None);
}

#[test]
fn re_adopt_intent_targets_main_and_reports_the_adopt_verb() {
    let fixture = Fixture::new();
    let intent = fixture.intent(None);
    assert_eq!(intent.target(), &WorkspaceName::main());
    assert_eq!(intent.verb(), "adopt");
    match &intent {
        LifecycleIntent::ReAdopt { options, .. } => {
            assert!(options.re_adopt, "re-adoption is explicit");
        }
        other => panic!("expected a ReAdopt intent, got {other:?}"),
    }

    let mut journal = LifecycleIntentJournal::default();
    journal.begin(intent);
    assert!(
        journal.get(&WorkspaceName::main()).is_some(),
        "the intent is filed under main"
    );
}

#[test]
fn re_adopt_mark_mutating_puts_main_in_forming_only_after_the_fence() {
    let fixture = Fixture::new();
    let journal = fixture.journal_path();
    LifecycleIntentJournal::update(&journal, |state| {
        state.begin(fixture.intent(Some(fixture.build())));
        Ok(())
    })
    .expect("record ReAdopt intent");

    let prepared = LifecycleIntentJournal::load(&journal).expect("load prepared journal");
    assert_eq!(
        prepared.forming().count(),
        0,
        "a prepared re-adoption has made nothing yet"
    );

    LifecycleIntentJournal::update(&journal, |state| {
        state.mark_mutating(&WorkspaceName::main())
    })
    .expect("mark ReAdopt mutating");
    let mutating = LifecycleIntentJournal::load(&journal).expect("load mutating journal");
    let forming: Vec<WorkspaceName> = mutating.forming().cloned().collect();
    assert_eq!(forming, vec![WorkspaceName::main()]);
}

#[test]
fn re_adopt_completion_with_the_exact_new_incarnation_is_accepted() {
    let fixture = Fixture::new();
    let journal = fixture.journal_path();
    begin_and_mark_mutating(&journal, fixture.intent(None));

    let replacement = WorkspaceIncarnation::new(NEW_INCARNATION).expect("replacement incarnation");
    assert_ne!(
        replacement,
        fixture.main_metadata().workspace_incarnation,
        "the replacement is a new incarnation, not the retained one"
    );
    let completion = LifecycleIntentCompletion::Workspace(replacement);
    LifecycleIntentJournal::update(&journal, |state| {
        state.complete(&WorkspaceName::main(), completion.clone())
    })
    .expect("the exact replacement incarnation completes the intent");

    let completed = LifecycleIntentJournal::load(&journal).expect("reload completed journal");
    let record = completed.get(&WorkspaceName::main()).expect("main record");
    assert_eq!(record.completion, Some(completion));
    assert_eq!(
        completed.forming().count(),
        0,
        "a completed re-adoption no longer forms"
    );
}

#[test]
fn re_adopt_retire_completion_is_refused_and_never_reaches_disk() {
    let fixture = Fixture::new();
    let journal = fixture.journal_path();
    begin_and_mark_mutating(&journal, fixture.intent(None));
    let before = fs::read(&journal).expect("read journal before refusal");

    let error = LifecycleIntentJournal::update(&journal, |state| {
        state.complete(
            &WorkspaceName::main(),
            LifecycleIntentCompletion::Retire(RemoveReport::default()),
        )
    })
    .expect_err("a retire result cannot complete a re-adoption");
    assert_eq!(error.code, ErrorCode::Integrity);
    assert_eq!(
        fs::read(&journal).expect("reread journal"),
        before,
        "the refused completion was not persisted"
    );
}

#[test]
fn re_adopt_wrong_completion_kind_on_disk_is_refused_at_load() {
    let fixture = Fixture::new();
    let journal = fixture.journal_path();
    begin_and_mark_mutating(&journal, fixture.intent(None));

    let mut document: Value =
        serde_json::from_slice(&fs::read(&journal).expect("read journal")).expect("journal JSON");
    let record = document["entries"]
        .as_object_mut()
        .expect("entries object")
        .values_mut()
        .next()
        .expect("main record");
    record["completion"] = json!({ "kind": "retire", "result": {} });
    fs::write(
        &journal,
        serde_json::to_vec(&document).expect("encode damaged journal"),
    )
    .expect("write damaged journal");

    let error = LifecycleIntentJournal::load(&journal)
        .expect_err("recovery must refuse a retire result on a re-adoption");
    assert_eq!(error.code, ErrorCode::Integrity);
}

#[test]
fn re_adopt_source_missing_a_required_field_is_refused() {
    let fixture = Fixture::new();
    let valid =
        serde_json::to_value(fixture.intent(Some(fixture.build()))).expect("encode ReAdopt");
    for field in [
        "checkout",
        "image",
        "metadata",
        "gitBefore",
        "build",
        "oldSeeds",
        "replacement",
    ] {
        assert_refused(
            without(&valid, &["source"], field),
            &format!("source without {field}"),
        );
    }
    for field in ["original", "retained", "sourceMount", "capacityBytes"] {
        assert_refused(
            without(&valid, &["source", "image"], field),
            &format!("image without {field}"),
        );
    }
    for field in [
        "id",
        "image",
        "ownerRecord",
        "replacementImage",
        "replacementMount",
    ] {
        assert_refused(
            without(&valid, &["source", "build"], field),
            &format!("build without {field}"),
        );
    }
    for field in [
        "version",
        "repoId",
        "workspace",
        "workspaceIncarnation",
        "platform",
        "publicationState",
        "updatedAt",
        "infoSnapshot",
    ] {
        assert_refused(
            without(&valid, &["source", "metadata"], field),
            &format!("metadata without {field}"),
        );
    }
}

#[test]
fn re_adopt_source_with_malformed_fields_is_refused() {
    let fixture = Fixture::new();
    let valid =
        serde_json::to_value(fixture.intent(Some(fixture.build()))).expect("encode ReAdopt");
    let project = fixture.project.display().to_string();
    type MalformedField = (&'static str, fn(&mut Value, &str));
    let cases: [MalformedField; 21] = [
        ("image capacity as text", |v: &mut Value, _: &str| {
            v["source"]["image"]["capacityBytes"] = json!("100GiB");
        }),
        (
            "build id that is not a BuildVolumeId",
            |v: &mut Value, _: &str| {
                v["source"]["build"]["id"] = json!("not-a-build-volume-id");
            },
        ),
        (
            "metadata from another sidecar schema",
            |v: &mut Value, _: &str| {
                v["source"]["metadata"]["version"] = json!(SIDECAR_VERSION + 1);
            },
        ),
        (
            "main metadata claiming the workspace role",
            |v: &mut Value, _: &str| {
                v["source"]["metadata"]["infoSnapshot"]["role"] = json!("workspace");
            },
        ),
        (
            "publication state outside the grammar",
            |v: &mut Value, _: &str| {
                v["source"]["metadata"]["publicationState"] = json!("published");
            },
        ),
        ("old seeds not a list", |v: &mut Value, _: &str| {
            v["source"]["oldSeeds"] = json!("build/seed.dmg");
        }),
        ("unknown field on source", |v: &mut Value, _: &str| {
            v["source"]["unexpected"] = json!(true);
        }),
        ("unknown field on image", |v: &mut Value, _: &str| {
            v["source"]["image"]["unexpected"] = json!(true);
        }),
        ("unknown field on build", |v: &mut Value, _: &str| {
            v["source"]["build"]["unexpected"] = json!(true);
        }),
        ("unknown field on git evidence", |v: &mut Value, _: &str| {
            v["source"]["gitBefore"]["unexpected"] = json!(true);
        }),
        ("relative checkout", |v: &mut Value, _: &str| {
            v["source"]["checkout"] = json!("checkout");
        }),
        ("relative main image", |v: &mut Value, _: &str| {
            v["source"]["image"]["original"] = json!("main.dmg");
        }),
        (
            "main image not named main",
            |v: &mut Value, project: &str| {
                v["source"]["image"]["original"] = json!(format!("{project}/other.dmg"))
            },
        ),
        (
            "retained main outside the retained namespace",
            |v: &mut Value, project: &str| {
                v["source"]["image"]["retained"] =
                    json!(format!("{project}/main-{OLD_INCARNATION}.dmg"));
            },
        ),
        (
            "retained main naming another incarnation",
            |v: &mut Value, project: &str| {
                v["source"]["image"]["retained"] =
                    json!(format!("{project}/retained/main-{NEW_INCARNATION}.dmg"));
            },
        ),
        (
            "source mount outside the staging namespace",
            |v: &mut Value, project: &str| {
                v["source"]["image"]["sourceMount"] =
                    json!(format!("{project}/readopt-{OLD_INCARNATION}-main"));
            },
        ),
        (
            "build image of another id",
            |v: &mut Value, project: &str| {
                v["source"]["build"]["image"]["original"] =
                    json!(format!("{project}/build/{}.dmg", SEED_IDS[0]));
            },
        ),
        (
            "old seed outside the project's build images",
            |v: &mut Value, _: &str| {
                v["source"]["oldSeeds"][0]["original"] = json!(format!("/tmp/{}.dmg", SEED_IDS[0]));
            },
        ),
        ("zero main capacity", |v: &mut Value, _: &str| {
            v["source"]["image"]["capacityBytes"] = json!(0);
        }),
        (
            "replacement reusing the replaced incarnation",
            |v: &mut Value, _: &str| {
                v["source"]["replacement"] = json!(OLD_INCARNATION);
            },
        ),
        (
            "legacy main retained as if it were RAW",
            |v: &mut Value, project: &str| {
                v["source"]["image"]["original"] = json!(format!("{project}/main.asif"));
            },
        ),
    ];
    for (label, mutate) in cases {
        let mut damaged = valid.clone();
        mutate(&mut damaged, &project);
        assert_refused(damaged, label);
    }
}

#[test]
fn re_adopt_completion_with_any_incarnation_but_the_recorded_replacement_is_refused() {
    let fixture = Fixture::new();
    let journal = fixture.journal_path();
    begin_and_mark_mutating(&journal, fixture.intent(Some(fixture.build())));
    let before = fs::read(&journal).expect("read journal before refusal");
    for incarnation in [OLD_INCARNATION, "0198f2c0b7e34dc795f17b238b331c82"] {
        let error = LifecycleIntentJournal::update(&journal, |state| {
            state.complete(
                &WorkspaceName::main(),
                LifecycleIntentCompletion::Workspace(
                    WorkspaceIncarnation::new(incarnation).expect("incarnation"),
                ),
            )
        })
        .expect_err("only the recorded replacement completes a re-adoption");
        assert_eq!(error.code, ErrorCode::Integrity, "{incarnation}: {error:?}");
        assert_eq!(
            fs::read(&journal).expect("reread journal"),
            before,
            "the refused completion {incarnation} was not persisted"
        );
    }
}

#[test]
fn re_adopt_journal_updates_never_overwrite_original_retained_or_pre_cowshed_inputs() {
    let fixture = Fixture::new();
    let build = fixture.build();
    let source = fixture.source(Some(build.clone()));
    let intent = fixture.intent_for(source.clone());

    let mut planted = vec![
        plant(
            &fixture.main_image().original,
            b"original main backing image, pre-cowshed",
        ),
        plant(
            &sidecar_path(&fixture.main_image().original),
            b"original main grants sidecar",
        ),
        plant(
            &fixture.main_image().retained,
            b"retained main backing image",
        ),
        plant(&build.image.original, b"original build volume image"),
        plant(&build.image.retained, b"retained build volume image"),
        plant(
            &fixture.pre_cowshed().join("README"),
            b"pre-cowshed checkout tree",
        ),
    ];
    for seed in &source.old_seeds {
        planted.push(plant(&seed.original, b"old-incarnation seed image"));
        planted.push(plant(
            &seed.retained,
            b"retained old-incarnation seed image",
        ));
    }

    let journal = fixture.journal_path();
    begin_and_mark_mutating(&journal, intent.clone());
    let replacement = WorkspaceIncarnation::new(NEW_INCARNATION).expect("replacement incarnation");
    LifecycleIntentJournal::update(&journal, |state| {
        state.complete(
            &WorkspaceName::main(),
            LifecycleIntentCompletion::Workspace(replacement),
        )
    })
    .expect("complete re-adoption");

    for (path, bytes) in &planted {
        assert_eq!(
            &fs::read(path).expect("retained input still readable"),
            bytes,
            "journal update rewrote {}",
            path.display()
        );
    }
    let loaded = LifecycleIntentJournal::load(&journal).expect("reload journal");
    let record = loaded.get(&WorkspaceName::main()).expect("main record");
    assert_eq!(
        record.operation, intent,
        "the recorded source is exactly the one captured before mutation"
    );
}

/// Staging collection reads exactly the paths a forming re-adoption recorded, and nothing of
/// one that is still `Prepared` or already complete.
#[test]
fn forming_re_adoptions_name_exactly_their_recorded_staging_paths() {
    let fixture = Fixture::new();
    let journal = fixture.journal_path();
    let build = fixture.build();
    LifecycleIntentJournal::update(&journal, |state| {
        state.begin(fixture.intent(Some(build.clone())));
        Ok(())
    })
    .expect("record ReAdopt intent");
    let prepared = LifecycleIntentJournal::load(&journal).expect("load prepared journal");
    assert_eq!(prepared.forming_re_adoptions().count(), 0);

    LifecycleIntentJournal::update(&journal, |state| {
        state.mark_mutating(&WorkspaceName::main())
    })
    .expect("mark ReAdopt intent mutating");
    let mutating = LifecycleIntentJournal::load(&journal).expect("load mutating journal");
    let sources: Vec<&ReAdoptSource> = mutating.forming_re_adoptions().collect();
    let [source] = sources.as_slice() else {
        panic!("one forming re-adoption: {sources:?}");
    };
    assert_eq!(
        source.staging_mounts().collect::<Vec<_>>(),
        [
            fixture.main_image().source_mount.as_path(),
            build.image.source_mount.as_path(),
            build.replacement_mount.as_path(),
        ]
    );
    assert_eq!(
        source.staging_images().collect::<Vec<_>>(),
        [build.replacement_image.as_path()]
    );

    LifecycleIntentJournal::update(&journal, |state| {
        state.complete(
            &WorkspaceName::main(),
            LifecycleIntentCompletion::Workspace(
                WorkspaceIncarnation::new(NEW_INCARNATION).expect("replacement incarnation"),
            ),
        )
    })
    .expect("complete re-adoption");
    let complete = LifecycleIntentJournal::load(&journal).expect("load completed journal");
    assert_eq!(complete.forming_re_adoptions().count(), 0);
}

/// A missing main's source as its re-adoption records it (02_workspaces.md, "Missing main
/// image"): the plain checkout is the copy source, the replacement is published at `main.dmg`,
/// and the checkout moves aside to its readopt sibling at handoff.
fn missing_source(fixture: &Fixture, build: Option<MissingMainBuild>) -> ReAdoptSource {
    let mut source = fixture.source(None);
    source.main = ReAdoptMain::Missing(ReAdoptMissingMain {
        canonical: fixture.project.join("main.dmg"),
        preserved: preserved_checkout(&fixture.checkout, &source.replacement)
            .expect("the fixture checkout has a name"),
        capacity_bytes: MAIN_CAPACITY_BYTES,
        build,
    });
    source
}

fn linked_main(tree: Option<GitOid>) -> BuildVolumeRecord {
    BuildVolumeRecord::new(
        tree,
        BuildVolumeRole::Linked {
            checkout: WorkspaceName::main(),
        },
    )
}

/// While nothing is published, a missing main's replay records the checkout's Git state again:
/// a commit made after an earlier attempt refused replaces only `git_before`, and the record stays
/// unfinished and mutating. A replay holding a stale record, or a retained main's record, writes
/// nothing.
#[test]
fn a_missing_main_replay_records_the_checkouts_git_state_again_and_nothing_else() {
    let fixture = Fixture::new();
    let journal = fixture.journal_path();
    let source = missing_source(&fixture, None);
    begin_and_mark_mutating(&journal, fixture.intent_for(source.clone()));

    fs::write(
        fixture.checkout.join("README"),
        b"committed after a refusal\n",
    )
    .expect("change README");
    git(&fixture.checkout, &["commit", "-qam", "after a refusal"]);
    let recaptured = git_preservation::capture(&fixture.checkout, &fixture.checkout)
        .expect("capture the moved checkout");
    assert!(
        source.git_before.compare(&recaptured).is_err(),
        "the commit moved the checkout's Git state"
    );

    let (_, rerecorded) = LifecycleIntentJournal::update(&journal, |state| {
        state.rerecord_missing_main_git(&source, recaptured.clone())
    })
    .expect("record the checkout's Git state again");
    let mut expected = source.clone();
    expected.git_before = recaptured.clone();
    assert_eq!(rerecorded, expected, "only git_before changes");
    let loaded = LifecycleIntentJournal::load(&journal).expect("reload the journal");
    let record = loaded
        .get(&WorkspaceName::main())
        .expect("main's re-adoption is recorded");
    assert!(record.completion.is_none(), "the record stays unfinished");
    assert_eq!(record.phase, LifecycleIntentPhase::Mutating);
    let LifecycleIntent::ReAdopt { source: stored, .. } = &record.operation else {
        panic!("main's record is a re-adoption")
    };
    assert_eq!(stored.as_ref(), &expected);

    let stale = LifecycleIntentJournal::update(&journal, |state| {
        state.rerecord_missing_main_git(&source, recaptured.clone())
    });
    assert!(
        stale.is_err(),
        "a replay holding a stale record writes nothing"
    );

    let retained = Fixture::new();
    let retained_journal = retained.journal_path();
    begin_and_mark_mutating(&retained_journal, retained.intent(None));
    let refused = LifecycleIntentJournal::update(&retained_journal, |state| {
        state.rerecord_missing_main_git(&retained.source(None), retained.git_before.clone())
    });
    assert!(
        refused.is_err(),
        "a retained main's recorded Git state is never replaced"
    );
}

/// A missing main's build volume decodes in both shapes: the empty volume journals held before a
/// surviving one could be kept (a golden record of that shape), and a kept volume. Each round-trips
/// and validates; a kept volume that is not main's is refused.
#[test]
fn a_missing_mains_build_volume_round_trips_empty_or_kept() {
    let fixture = Fixture::new();
    let id = BuildVolumeId::parse(BUILD_ID).expect("fixture build id");
    let empty_record = linked_main(None);
    let empty = MissingMainBuild::Empty(ReAdoptEmptyBuild {
        id: id.clone(),
        record: empty_record.clone(),
        capacity_bytes: BUILD_CAPACITY_BYTES,
    });
    let golden = json!({
        "id": BUILD_ID,
        "record": serde_json::to_value(&empty_record).expect("encode the empty record"),
        "capacityBytes": BUILD_CAPACITY_BYTES,
    });
    assert_eq!(
        serde_json::from_value::<MissingMainBuild>(golden).expect("decode the empty shape"),
        empty,
        "an empty-volume record written before kept volumes still decodes as empty"
    );
    let kept = MissingMainBuild::Kept(ReAdoptKeptBuild {
        kept: BuildVolumeId::parse(SEED_IDS[0]).expect("kept build id"),
        record: linked_main(Some(fixture.head_tree.clone())),
    });
    for build in [empty, kept] {
        let source = missing_source(&fixture, Some(build));
        source.validate().expect("a missing main's record is valid");
        let encoded = serde_json::to_value(&source).expect("encode the source");
        let decoded: ReAdoptSource = serde_json::from_value(encoded).expect("decode the source");
        assert_eq!(decoded, source);
    }
    let foreign = missing_source(
        &fixture,
        Some(MissingMainBuild::Kept(ReAdoptKeptBuild {
            kept: id,
            record: BuildVolumeRecord::new(
                None,
                BuildVolumeRole::Linked {
                    checkout: WorkspaceName::new("topic").expect("a session name"),
                },
            ),
        })),
    );
    assert!(
        foreign.validate().is_err(),
        "a kept volume another checkout owns is refused"
    );
}
