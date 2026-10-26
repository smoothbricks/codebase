//! `cowshed tree-holders`, run as the binary a caller runs: a process whose working directory is
//! inside the tree is named with that hold and the tree path's exact bytes, one outside it is not
//! named at all, and every line is one whole `TreeHolder` whose pid and hold lead it, the prefix a
//! shell reader matches. Each resident leads its own process group and stays unreaped until its
//! guard kills that group and reaps it, so a failed assertion leaks neither it nor the tree.
#![cfg(unix)]

use cowshed_os::test_backstop::{ChildBackstop as _, CommandBackstop as _};
use std::fs;
use std::io::{BufRead as _, BufReader};
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;

use cowshed_core::api::tree_holder::TreeHolder;
use cowshed_core::runtime::job_groups::signal_unreaped_group;
use serde_json::Value;

/// The `cowshed` binary under test, read when the test runs (see `disk_command.rs`).
fn cowshed() -> PathBuf {
    PathBuf::from(
        std::env::var_os("CARGO_BIN_EXE_cowshed")
            .expect("cargo and nextest set CARGO_BIN_EXE_cowshed for an integration test"),
    )
}

/// A canonical directory of this test's own, removed when it drops.
struct Tree {
    root: PathBuf,
}

impl Tree {
    fn new(name: &str) -> Self {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!(
            "cowshed-tree-holders-{name}-{}-{n}",
            std::process::id()
        ));
        fs::create_dir_all(&root).expect("create the test directory");
        let root = fs::canonicalize(&root).expect("canonicalize the test directory");
        Self { root }
    }

    /// The directory `name` inside this one, created.
    fn dir(&self, name: &std::ffi::OsStr) -> PathBuf {
        let dir = self.root.join(name);
        fs::create_dir(&dir)
            .unwrap_or_else(|error| panic!("create a directory in the tree at {dir:?}: {error}"));
        dir
    }
}

impl Drop for Tree {
    fn drop(&mut self) {
        let removed = fs::remove_dir_all(&self.root);
        if !thread::panicking() {
            removed.expect("remove the test directory");
        }
    }
}

/// A `sleep` whose working directory is `dir`, running once it has said so. It leads its own
/// process group and stays unreaped until it drops, so its pid and group id name nothing else.
struct Resident {
    child: Child,
}

impl Resident {
    fn start(dir: &Path) -> Self {
        let child = Command::new("sh")
            .args(["-c", "echo ready; exec sleep 600"])
            .current_dir(dir)
            .stdout(Stdio::piped())
            .process_group(0)
            .spawn()
            .expect("spawn the resident");
        let mut resident = Self { child };
        let mut line = String::new();
        BufReader::new(resident.child.stdout.take().expect("stdout"))
            .read_line(&mut line)
            .expect("read readiness");
        assert_eq!(line, "ready\n");
        resident
    }

    fn id(&self) -> u32 {
        self.child.id()
    }
}

impl Drop for Resident {
    fn drop(&mut self) {
        let pid = i32::try_from(self.id()).expect("a pid fits in pid_t");
        let result = signal_unreaped_group(pid, libc::SIGKILL)
            .and_then(|()| self.child.wait_bounded().map(|_| ()));
        if let Err(error) = result {
            if thread::panicking() {
                cowshed_os::eprintln!("resident process group {pid} cleanup failed: {error}");
            } else {
                panic!("resident process group {pid} cleanup failed: {error}");
            }
        }
    }
}

/// Every line cowshed tree-holders <dir> writes, read whole by the TreeHolder the command prints:
/// its reader refuses a stray field, a missing one and a path that is not tagged bytes, so no key
/// or kind list is retyped here. Each line also leads with the {"pid":<n>,"hold":"<kind>", prefix
/// a shell reader may match (02_workspaces.md, "Holders cowshed does not run"). The lines stay
/// JSON values for the cases that name what a line holds.
fn holders(dir: &Path) -> Vec<Value> {
    let output = Command::new(cowshed())
        .arg("tree-holders")
        .arg(dir)
        .output_bounded()
        .expect("run cowshed tree-holders");
    assert!(
        output.status.success(),
        "tree-holders failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("UTF-8 lines");
    assert!(
        stdout.is_empty() || stdout.ends_with('\n'),
        "a last line is cut: {stdout:?}"
    );
    stdout
        .lines()
        .map(|line| {
            let holder: TreeHolder = serde_json::from_str(line)
                .unwrap_or_else(|error| panic!("not one whole TreeHolder ({error}): {line}"));
            let hold = serde_json::to_string(&holder.hold).expect("a hold kind encodes");
            assert!(
                line.starts_with(&format!(r#"{{"pid":{},"hold":{hold},"#, holder.pid)),
                "{line}"
            );
            serde_json::from_str(line).expect("one JSON object per line")
        })
        .collect()
}

/// The holder line naming `resident`, which must be among `named`.
fn line_of<'a>(named: &'a [Value], resident: &Resident) -> &'a Value {
    named
        .iter()
        .find(|holder| holder["pid"] == resident.id())
        .unwrap_or_else(|| panic!("resident {} is not named: {named:?}", resident.id()))
}

#[test]
fn a_process_working_in_the_tree_is_named_with_that_hold_and_one_outside_is_not() {
    let tree = Tree::new("plain");
    let inside = tree.dir("shed".as_ref());
    let outside = tree.dir("elsewhere".as_ref());
    let within = Resident::start(&inside);
    let beside = Resident::start(&outside);

    let named = holders(&inside);
    let hold = line_of(&named, &within);
    assert_eq!(hold["hold"], "working-directory", "{hold}");
    assert_eq!(
        hold["path"],
        serde_json::json!({
            "encoding": "utf8",
            "data": inside.to_str().expect("a UTF-8 path"),
        }),
        "{hold}"
    );
    assert!(
        named.iter().all(|holder| holder["pid"] != beside.id()),
        "a process outside the tree was named: {named:?}"
    );
}

/// A directory whose name is not UTF-8 is named by its exact bytes beside one whose name is, and
/// neither cuts the other's line short. Linux only: Darwin's file systems (APFS, HFS+) refuse a
/// name that is not UTF-8, so no Darwin census meets one made here.
#[cfg(target_os = "linux")]
#[test]
fn a_holder_in_a_directory_whose_name_is_not_utf8_is_named_by_its_exact_bytes() {
    use base64::Engine as _;
    use std::os::unix::ffi::OsStrExt as _;

    let tree = Tree::new("non-utf8");
    let plain = tree.dir("plain".as_ref());
    let odd = tree.dir(std::ffi::OsStr::from_bytes(b"shed-\xff\xfe"));
    let in_plain = Resident::start(&plain);
    let in_odd = Resident::start(&odd);

    let named = holders(&tree.root);
    let odd_hold = line_of(&named, &in_odd);
    assert_eq!(odd_hold["hold"], "working-directory", "{odd_hold}");
    assert_eq!(odd_hold["path"]["encoding"], "base64", "{odd_hold}");
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(
            odd_hold["path"]["data"]
                .as_str()
                .expect("base64 path bytes"),
        )
        .expect("canonical base64");
    assert_eq!(bytes, odd.as_os_str().as_bytes(), "{odd_hold}");
    let plain_hold = line_of(&named, &in_plain);
    assert_eq!(
        plain_hold["path"],
        serde_json::json!({
            "encoding": "utf8",
            "data": plain.to_str().expect("a UTF-8 path"),
        }),
        "{plain_hold}"
    );
}
