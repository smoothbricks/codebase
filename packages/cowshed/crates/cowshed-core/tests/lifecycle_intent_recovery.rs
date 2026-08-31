use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use cowshed_core::api::{CreateOptions, RemoveOptions, RemoveReport};
use cowshed_core::metadata::{WorkspaceIncarnation, WorkspaceName};
use cowshed_core::storage::recovery::{
    LIFECYCLE_INTENTS_FILE, LifecycleIntent, LifecycleIntentCompletion, LifecycleIntentJournal,
};

const CHILD_MODE: &str = "COWSHED_LIFECYCLE_CRASH_CHILD";
const CHILD_ROOT: &str = "COWSHED_LIFECYCLE_CRASH_ROOT";

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

fn workspace(value: &str) -> WorkspaceName {
    WorkspaceName::new(value).expect("fixture workspace name")
}

fn intent(operation: &str) -> LifecycleIntent {
    match operation {
        "create" => LifecycleIntent::Create {
            workspace: workspace("created"),
            options: CreateOptions::default(),
        },
        "fork" => LifecycleIntent::Fork {
            source: workspace("main"),
            destination: workspace("forked"),
        },
        "remove" => LifecycleIntent::Retire {
            workspace: workspace("removed"),
            options: RemoveOptions {
                force: true,
                ..RemoveOptions::default()
            },
        },
        other => panic!("unknown crash fixture operation {other}"),
    }
}
fn retire(name: &str, options: RemoveOptions) -> LifecycleIntent {
    LifecycleIntent::Retire {
        workspace: workspace(name),
        options,
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
    let mut journal = LifecycleIntentJournal::default();
    journal.begin(operation_intent);
    journal
        .persist(&journal_path)
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
        .status()
        .expect("spawn crash child");
    assert!(
        !status.success(),
        "the child must die between mutation and acknowledgement"
    );

    let journal_path = root.path().join(LIFECYCLE_INTENTS_FILE);
    let mut reopened = LifecycleIntentJournal::load(&journal_path).expect("reopen durable intent");
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
    reopened
        .complete(&target, completion.clone())
        .expect("reconcile published state");
    reopened
        .persist(&journal_path)
        .expect("persist recovery result");

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
        options: CreateOptions::default(),
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
