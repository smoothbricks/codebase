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
