use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use cowshed_core::api::{AdoptOptions, RemoveOptions, RemoveReport};
use cowshed_core::metadata::{WorkspaceIncarnation, WorkspaceName};
use cowshed_core::storage::recovery::{
    IntentLease, KeptAbandonBundles, LIFECYCLE_INTENTS_FILE, LifecycleIntent,
    LifecycleIntentCompletion, LifecycleIntentJournal, LifecycleIntentPhase, MintOptions,
};
use cowshed_os::fork_lock::{Run as _, Spawn as _};

const CHILD_MODE: &str = "COWSHED_LIFECYCLE_CRASH_CHILD";
const CHILD_ROOT: &str = "COWSHED_LIFECYCLE_CRASH_ROOT";
const LEASE_CHILD_ROOT: &str = "COWSHED_LIFECYCLE_LEASE_CHILD_ROOT";

struct TestRoot(PathBuf);

impl TestRoot {
    fn new(operation: &str) -> Self {
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock after epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "cowshed-lifecycle-intent-{operation}-{}-{nonce}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create crash fixture root");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestRoot {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// What the trash keeps of `rm --abandon` is counted by the rule a sweep uses: only regular
/// `*.bundle` files directly in it, never a staged `.partial`, a directory or a symlink.
#[test]
fn kept_abandon_bundles_count_only_regular_bundles_in_the_trash() {
    let root = TestRoot::new("kept-bundles");
    let trash = root.path().join("sessions/.trash");
    assert_eq!(
        KeptAbandonBundles::in_trash(&trash).expect("absent trash reads"),
        None,
        "a project that never abandoned anything keeps nothing"
    );
    fs::create_dir_all(&trash).expect("trash");
    fs::write(trash.join("raven-1111.bundle"), vec![7_u8; 9000]).expect("first bundle");
    fs::write(trash.join("crow-2222.bundle"), b"bundle").expect("second bundle");
    fs::write(trash.join(".crow-2222.bundle.0abc.partial"), b"staged").expect("partial");
    fs::create_dir(trash.join("not-a-file.bundle")).expect("directory");
    std::os::unix::fs::symlink(trash.join("crow-2222.bundle"), trash.join("link.bundle"))
        .expect("symlink");
    fs::write(
        trash.join("raven-0198f2c0b7e34dc795f17b238b331c80.dmg"),
        b"image",
    )
    .expect("image");

    let kept = KeptAbandonBundles::in_trash(&trash)
        .expect("trash reads")
        .expect("two bundles");
    assert_eq!(kept.trash, trash);
    assert_eq!(kept.count, 2);
    let allocated = |name: &str| {
        use std::os::unix::fs::MetadataExt as _;
        fs::metadata(trash.join(name))
            .expect("bundle metadata")
            .blocks()
            * 512
    };
    assert_eq!(
        kept.bytes,
        allocated("raven-1111.bundle") + allocated("crow-2222.bundle")
    );
    assert!(
        kept.to_string().contains("keeps 2 abandon bundles"),
        "{kept}"
    );
}

fn workspace(value: &str) -> WorkspaceName {
    WorkspaceName::new(value).expect("fixture workspace name")
}
#[test]
fn historical_create_and_fork_tags_remain_readable_without_public_aliases() {
    let old_create = serde_json::json!({
        "kind": "create", "workspace": "raven",
        "options": { "revision": { "branch": "main" }, "fromWorkspace": "main",
            "browse": false, "register": false, "gitWorktree": false }
    });
    let LifecycleIntent::Create { options, .. } =
        serde_json::from_value(old_create).expect("historical create")
    else {
        panic!("expected historical create")
    };
    assert_eq!(
        options.clean, None,
        "the old main-only clean rule remains distinguishable"
    );
    let old_fork = serde_json::json!({ "kind": "fork", "source": "main", "destination": "raven" });
    let fork: LifecycleIntent = serde_json::from_value(old_fork.clone()).expect("historical fork");
    assert_eq!(serde_json::to_value(fork).unwrap(), old_fork);
}

#[test]
fn rename_flag_is_distinct_and_an_absent_flag_stays_a_raw_fork() {
    let old_fork = serde_json::json!({ "kind": "fork", "source": "main", "destination": "raven" });
    let historical: LifecycleIntent =
        serde_json::from_value(old_fork.clone()).expect("historical fork");
    assert_eq!(
        serde_json::to_value(&historical).unwrap(),
        old_fork,
        "an absent flag decodes false and re-encodes without the field"
    );
    assert_eq!(historical.verb(), "fork");

    let explicit: LifecycleIntent = serde_json::from_value(serde_json::json!({
        "kind": "fork",
        "source": "main",
        "destination": "raven",
        "rename": false,
    }))
    .expect("explicit false fork");
    assert_eq!(explicit, historical);

    let rename = LifecycleIntent::Fork {
        source: workspace("main"),
        destination: workspace("raven"),
        rename: true,
    };
    let encoded = serde_json::to_value(&rename).expect("encode rename");
    assert_eq!(
        encoded,
        serde_json::json!({
            "kind": "fork",
            "source": "main",
            "destination": "raven",
            "rename": true,
        })
    );
    let decoded: LifecycleIntent = serde_json::from_value(encoded).expect("decode rename");
    assert_eq!(decoded, rename);
    assert_eq!(decoded.verb(), "rename");
    assert_ne!(
        decoded, historical,
        "replay must not confuse a rename with a fork of the same names"
    );
}

fn intent(operation: &str) -> LifecycleIntent {
    match operation {
        "create" => LifecycleIntent::Create {
            workspace: workspace("created"),
            options: MintOptions::default(),
        },
        "fork" => LifecycleIntent::Fork {
            source: workspace("main"),
            destination: workspace("forked"),
            rename: false,
        },
        "remove" => LifecycleIntent::Retire {
            workspace: workspace("removed"),
            options: RemoveOptions {
                force: true,
                ..RemoveOptions::default()
            },
            origin: None,
        },
        other => panic!("unknown crash fixture operation {other}"),
    }
}
fn retire(name: &str, options: RemoveOptions) -> LifecycleIntent {
    LifecycleIntent::Retire {
        workspace: workspace(name),
        options,
        origin: None,
    }
}

fn state_path(root: &Path, operation: &str) -> PathBuf {
    match operation {
        "create" => root.join("created"),
        "fork" => root.join("forked"),
        "remove" => root.join("removed"),
        other => panic!("unknown crash fixture operation {other}"),
    }
}

fn sync_directory(path: &Path) {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .expect("sync fixture directory");
}

#[test]
fn lifecycle_intent_child() {
    let Ok(operation) = std::env::var(CHILD_MODE) else {
        return;
    };
    let root = PathBuf::from(std::env::var_os(CHILD_ROOT).expect("child fixture root"));
    let journal_path = root.join(LIFECYCLE_INTENTS_FILE);
    let operation_intent = intent(&operation);
    LifecycleIntentJournal::update(&journal_path, |journal| {
        journal.begin(operation_intent);
        Ok(())
    })
    .expect("persist intent before mutation");

    let state = state_path(&root, &operation);
    if operation == "remove" {
        fs::remove_dir_all(&state).expect("retire workspace state");
    } else {
        fs::create_dir(&state).expect("publish workspace state");
        fs::write(
            state.join("incarnation"),
            b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\n",
        )
        .expect("write workspace incarnation");
        sync_directory(&state);
    }
    let effects_path = root.join("effects");
    let mut effects = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&effects_path)
        .expect("open effect log");
    effects.write_all(b"effect\n").expect("record effect");
    effects.sync_all().expect("sync effect log");
    sync_directory(&root);

    std::process::abort();
}

fn crash_then_recover(operation: &str) {
    let root = TestRoot::new(operation);
    let state = state_path(root.path(), operation);
    if operation == "remove" {
        fs::create_dir(&state).expect("create removable workspace state");
        sync_directory(root.path());
    }

    let status = Command::new(std::env::current_exe().expect("current test executable"))
        .args(["--exact", "lifecycle_intent_child", "--nocapture"])
        .env(CHILD_MODE, operation)
        .env(CHILD_ROOT, root.path())
        .status_locked()
        .expect("spawn crash child");
    assert!(
        !status.success(),
        "the child must die between mutation and acknowledgement"
    );

    let journal_path = root.path().join(LIFECYCLE_INTENTS_FILE);
    let reopened = LifecycleIntentJournal::load(&journal_path).expect("reopen durable intent");
    let target = intent(operation).target().clone();
    let record = reopened
        .get(&target)
        .expect("pending intent survived process death");
    assert_eq!(record.operation, intent(operation));
    assert_eq!(record.completion, None);

    let completion = if operation == "remove" {
        assert!(!state.exists(), "retirement reached its publication fence");
        LifecycleIntentCompletion::Retire(RemoveReport::default())
    } else {
        assert!(state.is_dir(), "workspace publication reached its fence");
        LifecycleIntentCompletion::Workspace(
            WorkspaceIncarnation::new("a".repeat(32)).expect("fixture incarnation"),
        )
    };
    LifecycleIntentJournal::update(&journal_path, |journal| {
        journal.complete(&target, completion.clone())
    })
    .expect("reconcile published state");

    let retried = LifecycleIntentJournal::load(&journal_path).expect("retry reload");
    assert_eq!(
        retried
            .get(&target)
            .and_then(|record| record.completion.clone()),
        Some(completion),
        "a retry observes the first operation's exact durable result"
    );
    assert_eq!(
        fs::read_to_string(root.path().join("effects")).expect("read effect log"),
        "effect\n",
        "recovery and retry must not apply the lifecycle mutation twice"
    );
}
#[test]
fn read_only_startup_discards_a_refused_retirement_before_listing() {
    let mut journal = LifecycleIntentJournal::default();
    journal.begin(retire("dirty", RemoveOptions::default()));

    let discarded = journal.discard_prepared_retirement(&workspace("dirty"));

    assert!(discarded);
    assert!(
        journal.records().next().is_none(),
        "a safety refusal must not become read-only startup work"
    );
}

#[test]
fn refused_retirement_cannot_hide_a_later_force_authorization() {
    let mut journal = LifecycleIntentJournal::default();
    journal.begin(retire("dirty", RemoveOptions::default()));
    assert!(
        journal.discard_prepared_retirement(&workspace("dirty")),
        "startup must discard the earlier unforced request before dispatch"
    );

    journal.begin(retire(
        "dirty",
        RemoveOptions {
            force: true,
            ..RemoveOptions::default()
        },
    ));
    let record = journal.get(&workspace("dirty")).expect("forced request");
    let LifecycleIntent::Retire { options, .. } = &record.operation else {
        panic!("retire request")
    };
    assert!(options.force);
}

#[test]
fn refused_retirement_cannot_hide_a_later_abandon_authorization() {
    let mut journal = LifecycleIntentJournal::default();
    journal.begin(retire("unlanded", RemoveOptions::default()));
    assert!(
        journal.discard_prepared_retirement(&workspace("unlanded")),
        "startup must discard the earlier non-abandoning request before dispatch"
    );

    journal.begin(retire(
        "unlanded",
        RemoveOptions {
            abandon: true,
            ..RemoveOptions::default()
        },
    ));
    let record = journal
        .get(&workspace("unlanded"))
        .expect("abandon request");
    let LifecycleIntent::Retire { options, .. } = &record.operation else {
        panic!("retire request")
    };
    assert!(options.abandon);
}

#[test]
fn prepared_retirement_of_one_workspace_cannot_block_an_unrelated_command() {
    let mut journal = LifecycleIntentJournal::default();
    journal.begin(retire("unrelated", RemoveOptions::default()));
    journal.begin(LifecycleIntent::Create {
        workspace: workspace("requested"),
        options: MintOptions::default(),
    });

    assert!(journal.discard_prepared_retirement(&workspace("unrelated")));
    assert!(
        journal.get(&workspace("requested")).is_some(),
        "discarding an unstarted retirement must preserve unrelated lifecycle work"
    );
}
#[test]
fn mutating_retirement_remains_recoverable_after_a_crash() {
    let name = workspace("retiring");
    let mut journal = LifecycleIntentJournal::default();
    journal.begin(retire("retiring", RemoveOptions::default()));
    journal
        .mark_mutating(&name)
        .expect("cross the retirement mutation fence");

    assert!(!journal.discard_prepared_retirement(&name));
    assert!(
        journal.get(&name).is_some(),
        "a crash after mutation begins must retain its recovery record"
    );
}

#[test]
fn a_refused_clone_puts_back_the_record_it_superseded() {
    let name = workspace("created");
    let mut journal = LifecycleIntentJournal::default();
    journal.begin(retire("created", RemoveOptions::default()));
    journal
        .complete(
            &name,
            LifecycleIntentCompletion::Retire(RemoveReport::default()),
        )
        .expect("an earlier lifecycle of the name finished");
    let earlier = journal.get(&name).cloned();

    let superseded = journal.begin(intent("create"));
    assert_eq!(superseded, earlier);
    assert!(journal.discard_prepared_clone_intent(&name, superseded));
    assert_eq!(
        journal.get(&name).cloned(),
        earlier,
        "a refusal leaves the journal as the verb found it"
    );

    let fork = intent("fork");
    let destination = fork.target().clone();
    let superseded = journal.begin(fork);
    assert_eq!(superseded, None);
    assert!(journal.discard_prepared_clone_intent(&destination, superseded));
    assert!(journal.get(&destination).is_none());
}

#[test]
fn only_a_prepared_clone_intent_can_be_withdrawn() {
    let mut journal = LifecycleIntentJournal::default();
    let created = workspace("created");
    journal.begin(intent("create"));
    journal
        .mark_mutating(&created)
        .expect("cross the clone mutation fence");
    assert!(!journal.discard_prepared_clone_intent(&created, None));
    assert_eq!(
        journal.get(&created).map(|record| record.phase),
        Some(LifecycleIntentPhase::Mutating),
        "a clone that may have mutated stays recoverable"
    );

    let removed = workspace("removed");
    journal.begin(intent("remove"));
    assert!(
        !journal.discard_prepared_clone_intent(&removed, None),
        "retirements have their own discard rule"
    );
    assert!(journal.get(&removed).is_some());
}

/// Build-volume collection keeps what a create or fork past its mutation fence may have forked
/// into its unpublished clone, and the first build volume an adopt past its fence may have minted
/// beside main's image: only those intents name a forming workspace. A prepared verb has made
/// nothing, a completed one is published, and a retirement forms nothing.
#[test]
fn only_a_mutating_unfinished_clone_or_adoption_is_forming() {
    let mut journal = LifecycleIntentJournal::default();
    let forming = |journal: &LifecycleIntentJournal| journal.forming().cloned().collect::<Vec<_>>();
    journal.begin(LifecycleIntent::Adopt {
        options: AdoptOptions::default(),
    });
    journal.begin(intent("create"));
    journal.begin(intent("fork"));
    journal.begin(intent("remove"));
    assert_eq!(forming(&journal), Vec::<WorkspaceName>::new());

    for name in ["main", "created", "forked", "removed"] {
        journal
            .mark_mutating(&workspace(name))
            .expect("cross the mutation fence");
    }
    assert_eq!(
        forming(&journal),
        [workspace("created"), workspace("forked"), workspace("main")]
    );

    for name in ["created", "main"] {
        journal
            .complete(
                &workspace(name),
                LifecycleIntentCompletion::Workspace(
                    WorkspaceIncarnation::new("a".repeat(32)).expect("fixture incarnation"),
                ),
            )
            .expect("publish the workspace");
    }
    assert_eq!(forming(&journal), [workspace("forked")]);
}

#[test]
fn refused_pending_retirement_restores_the_clone_and_a_new_removal_can_be_authorized() {
    let root = TestRoot::new("pending-refusal");
    let path = root.path().join(LIFECYCLE_INTENTS_FILE);
    let name = workspace("pending");
    let clone = LifecycleIntent::Fork {
        source: workspace("main"),
        destination: name.clone(),
        rename: false,
    };
    LifecycleIntentJournal::update(&path, |journal| {
        journal.begin(clone.clone());
        journal.begin(LifecycleIntent::Retire {
            workspace: name.clone(),
            options: RemoveOptions::default(),
            origin: Some(Box::new(clone.clone())),
        });
        Ok(())
    })
    .expect("persist interrupted retirement");

    LifecycleIntentJournal::update(&path, |reopened| {
        assert!(reopened.restore_prepared_clone_intent(&name));
        assert_eq!(reopened.get(&name).unwrap().operation, clone);
        reopened.begin(LifecycleIntent::Retire {
            workspace: name.clone(),
            options: RemoveOptions {
                abandon: true,
                ..RemoveOptions::default()
            },
            origin: Some(Box::new(clone.clone())),
        });
        reopened
            .mark_mutating(&name)
            .expect("begin authorized retirement");
        assert!(!reopened.restore_prepared_clone_intent(&name));
        Ok(())
    })
    .expect("persist mutating retirement");
    let recovered = LifecycleIntentJournal::load(&path).expect("reopen mutating retirement");
    let LifecycleIntent::Retire { options, .. } = &recovered.get(&name).unwrap().operation else {
        panic!("retirement remains recoverable");
    };
    assert!(options.abandon);
}

#[test]
fn killed_create_reopens_reconciles_and_retries_exactly_once() {
    crash_then_recover("create");
}

#[test]
fn killed_fork_reopens_reconciles_and_retries_exactly_once() {
    crash_then_recover("fork");
}

#[test]
fn killed_remove_reopens_reconciles_and_retries_exactly_once() {
    crash_then_recover("remove");
}

#[test]
fn a_killed_create_keeps_its_intent_when_another_process_updates_the_journal() {
    // Another cowshed process opened this project before `fork` began and records its own
    // lifecycle step after `fork` was killed mid-flight. The killed create's intent is the only
    // authority that can finish or retire its unpublished clone, so an update about a different
    // workspace must not erase it.
    let root = TestRoot::new("concurrent-update");
    let journal_path = root.path().join(LIFECYCLE_INTENTS_FILE);

    let status = Command::new(std::env::current_exe().expect("current test executable"))
        .args(["--exact", "lifecycle_intent_child", "--nocapture"])
        .env(CHILD_MODE, "create")
        .env(CHILD_ROOT, root.path())
        .status_locked()
        .expect("spawn create child");
    assert!(!status.success(), "the create child is killed mid-flight");

    LifecycleIntentJournal::update(&journal_path, |journal| {
        journal.begin(retire("unrelated", RemoveOptions::default()));
        Ok(())
    })
    .expect("record the other process's step");

    let reopened = LifecycleIntentJournal::load(&journal_path).expect("reopen");
    let killed = reopened
        .get(&workspace("created"))
        .expect("the killed create's intent survives the other process's update");
    assert_eq!(killed.operation, intent("create"));
    assert_eq!(killed.completion, None);
    assert!(
        reopened.get(&workspace("unrelated")).is_some(),
        "the other process's step is recorded too"
    );
}

#[test]
fn lifecycle_lease_child() {
    let Some(root) = std::env::var_os(LEASE_CHILD_ROOT).map(PathBuf::from) else {
        return;
    };
    let name = workspace("removed");
    let _lease = IntentLease::try_claim(&root.join("sessions"), &name)
        .expect("claim lease")
        .expect("nobody else runs this removal");
    LifecycleIntentJournal::update(&root.join(LIFECYCLE_INTENTS_FILE), |journal| {
        journal.begin(retire(
            "removed",
            RemoveOptions {
                abandon: true,
                ..RemoveOptions::default()
            },
        ));
        journal.mark_mutating(&name)
    })
    .expect("record the running removal");
    fs::write(root.join("ready"), b"bundling\n").expect("announce the running removal");
    // Still bundling: the parent kills this process while it holds the lease.
    std::thread::sleep(Duration::from_secs(60));
    std::process::exit(1);
}

#[test]
fn recovery_leaves_a_running_removal_to_its_process_and_takes_it_over_once_that_dies() {
    // `cowshed rm --abandon` in one process is still writing its bundle when another process
    // opens the project. Its intent is unfinished and mutating — exactly what a crash leaves —
    // but its process is alive, and running it a second time put a second bundle writer on the
    // same trash path and failed that open outright. Once the process dies, the same intent is
    // crash residue that recovery must take over.
    let root = TestRoot::new("live-lease");
    let sessions = root.path().join("sessions");
    let name = workspace("removed");
    let mut child = Command::new(std::env::current_exe().expect("current test executable"))
        .args(["--exact", "lifecycle_lease_child", "--nocapture"])
        .env(LEASE_CHILD_ROOT, root.path())
        .spawn_locked()
        .expect("spawn removal child");
    let deadline = Instant::now() + Duration::from_secs(30);
    while !root.path().join("ready").exists() {
        if let Some(status) = child.try_wait().expect("poll removal child") {
            panic!("the removal child ended before announcing itself: {status}");
        }
        assert!(
            Instant::now() < deadline,
            "the removal child never announced itself"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let journal =
        LifecycleIntentJournal::load(&root.path().join(LIFECYCLE_INTENTS_FILE)).expect("open");
    let record = journal.get(&name).expect("the running removal is recorded");
    assert_eq!(record.completion, None);
    assert_eq!(record.phase, LifecycleIntentPhase::Mutating);

    assert!(
        IntentLease::try_claim(&sessions, &name)
            .expect("probe lease")
            .is_none(),
        "a removal its process is still running is not recoverable"
    );

    child.kill().expect("kill removal child");
    child.wait().expect("reap removal child");
    assert!(
        IntentLease::try_claim(&sessions, &name)
            .expect("claim lease")
            .is_some(),
        "a removal whose process died is recoverable"
    );
}

#[test]
fn malformed_record_names_its_project_and_record() {
    let root = TestRoot::new("malformed");
    let project = root.path().join("acme/widget");
    fs::create_dir_all(&project).expect("create project fixture");
    let path = project.join(LIFECYCLE_INTENTS_FILE);
    fs::write(
        &path,
        r#"{
            "version": 1,
            "entries": {
                "raven": {
                    "operation": {
                        "kind": "fork",
                        "source": "main",
                        "destination": "owl"
                    }
                }
            }
        }"#,
    )
    .expect("write malformed journal fixture");

    let error = LifecycleIntentJournal::load(&path).unwrap_err();
    assert_eq!(
        error.message,
        format!(
            "invalid lifecycle intent journal for project {} in {}: lifecycle intent record \
             raven disagrees with target owl",
            project.display(),
            path.display()
        )
    );
}
