//! The stock-Nx API bridge (`build_volume/nx_publication.js`) against a real Nx 23.2.1 workspace.
//!
//! Each step runs the stock `nx` under a scrubbed environment with this repository's plugin
//! registered. Only a run given a controller-selected receipt leaves a task hint, and the bridge's
//! `inspect` mode must qualify exactly the checked successful task while refusing every other state
//! without executing a task. This is a focused proof of the bridge only: it is neither a protected
//! land, a publication, nor a cutover claim.

use cowshed_os::fork_lock::{Run as _, Spawn as _};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::mpsc;
use std::time::{SystemTime, UNIX_EPOCH};

use cowshed_core::build_volume::cache_adapter;
use serde_json::{Value, json};

#[path = "support/temp_parent.rs"]
mod temp_parent;
#[path = "support/temp_root.rs"]
mod temp_root;
use temp_root::TempRoot;

const BRIDGE: &str = include_str!("../src/build_volume/nx_publication.js");
const CHECK: &str = "00000000000000000000000000000001";
const REFUSED: &str = "00000000000000000000000000000002";
const MISSING: &str = "00000000000000000000000000000003";
const FAILED: &str = "00000000000000000000000000000004";
const SOURCE_CHECK: &str = "00000000000000000000000000000005";
const READER_CHECK: &str = "00000000000000000000000000000006";
const READER_WIP: &str = "00000000000000000000000000000007";
const CHECKED_COMMAND: &str = "nx run app:build --outputStyle=static";
const ANSWER_LIMIT: usize = 1024 * 1024;
const READY_FRAME_LIMIT: u64 = 4096;
const MAX_UPLOAD_BYTES: &str = "1048576";
const CAP_BYTES: &str = "16777216";
const WRITER_TOKEN: &str = "writer-token-for-the-host-lane-proof";
const READER_TOKEN: &str = "reader-token-for-the-host-lane-proof";
const UNKNOWN_HASH: &str = "18446744073709551615";

/// A fixture workspace under the scratch parent. Its child is owned by this guard from the moment
/// it is created, so a failed check below removes it as the panic unwinds.
fn fixture() -> TempRoot {
    TempRoot::new_in(&temp_parent::scratch_parent(), "cowshed-nx-publication")
}

/// A remote cache endpoint and the one token a process is given against it.
struct Remote {
    origin: String,
    token: String,
}

/// The production `nx-cache-server` bundle this repository builds, run by the `bun` on `PATH` and
/// supervised by the test: its stdin is the lease, and a panic kills and reaps it rather than
/// leaving an orphan.
struct Server {
    child: Option<Child>,
    origin: String,
}

impl Server {
    /// Starts the server on an ephemeral loopback port, named by its one-line readiness frame.
    fn start(root: &Path, writer: &str, reader: &str) -> Self {
        let script =
            repository_root().join("packages/nx-cache-server/dist/server/nx-cache-server.js");
        let entries = root.join("entries");
        std::fs::create_dir(&entries)
            .unwrap_or_else(|error| panic!("create {}: {error}", entries.display()));
        let mut child = Command::new("bun")
            .arg(&script)
            .arg("--exit-on-stdin-eof")
            .env_clear()
            .env(
                "PATH",
                std::env::var_os("PATH").expect("PATH is set for the managed toolchain"),
            )
            .env("NX_CACHE_SERVER_DIR", &entries)
            .env("NX_CACHE_SERVER_HOST", "127.0.0.1")
            .env("NX_CACHE_SERVER_PORT", "0")
            .env("NX_CACHE_SERVER_MAX_BYTES", MAX_UPLOAD_BYTES)
            .env("NX_CACHE_SERVER_CAP_BYTES", CAP_BYTES)
            .env("NX_CACHE_SERVER_WRITE_TOKENS", format!("writer:{writer}"))
            .env("NX_CACHE_SERVER_READ_TOKENS", format!("reader:{reader}"))
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn_locked()
            .unwrap_or_else(|error| panic!("start bun {}: {error}", script.display()));
        let stdout = child.stdout.take().expect("server stdout");
        let (sender, receiver) = mpsc::channel();
        std::thread::spawn(move || {
            let mut frame = String::new();
            let outcome = BufReader::new(stdout.take(READY_FRAME_LIMIT))
                .read_line(&mut frame)
                .map(|_| frame);
            // The receiver is gone only when the test stopped waiting, so the frame has no reader.
            if sender.send(outcome).is_err() {
                cowshed_os::eprintln!(
                    "nx-cache-server readiness frame arrived after the test stopped waiting"
                );
            }
        });
        // Owned from here: a failed readiness check below kills the server as the panic unwinds.
        let mut server = Self {
            child: Some(child),
            origin: String::new(),
        };
        let frame = receiver
            .recv()
            .expect("the readiness reader reports")
            .expect("read the readiness frame");
        let ready: Value = serde_json::from_str(&frame).expect("readiness frame is JSON");
        assert_eq!(ready["kind"], json!("ready"), "{frame}");
        server.origin = ready["origin"]
            .as_str()
            .expect("readiness origin")
            .to_owned();
        assert!(
            server.origin.starts_with("http://127.0.0.1:"),
            "{}",
            server.origin
        );
        server
    }

    /// Closes the lease: stdin EOF makes the server drain and exit, which must succeed. The child
    /// stays owned until that exit is confirmed, so a failed close is still killed and reaped.
    fn close(mut self) {
        let child = self.child.as_mut().expect("the server is running");
        drop(child.stdin.take());
        let status = child.wait().expect("reap nx-cache-server");
        assert!(status.success(), "nx-cache-server exited with {status}");
        self.child = None;
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        // Reached with the child still held only on a panic before a confirmed close: kill, then reap.
        let Some(child) = self.child.as_mut() else {
            return;
        };
        match child.kill() {
            Ok(()) => {}
            // The child already exited and was reaped, so there is nothing left to kill.
            Err(error) if error.kind() == std::io::ErrorKind::InvalidInput => {}
            Err(error) => cowshed_os::eprintln!("kill nx-cache-server {}: {error}", self.origin),
        }
        if let Err(error) = child.wait() {
            cowshed_os::eprintln!("reap nx-cache-server {}: {error}", self.origin);
        }
    }
}

fn repository_root() -> PathBuf {
    let manifest = std::env::var_os("CARGO_MANIFEST_DIR")
        .expect("cargo or nextest supplies CARGO_MANIFEST_DIR");
    std::fs::canonicalize(Path::new(&manifest).join("../../../.."))
        .expect("repository root from the crate manifest")
}

fn now_ms() -> u64 {
    let elapsed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock after the epoch");
    u64::try_from(elapsed.as_millis()).expect("millisecond clock fits in u64")
}

fn write_file(path: &Path, contents: impl AsRef<[u8]>) {
    std::fs::write(path, contents)
        .unwrap_or_else(|error| panic!("write {}: {error}", path.display()));
}

/// `app:build` caches a copy of `source.txt` and appends one `run` line per real execution;
/// `app:other` is an unrelated cached task; `app:fail` always exits 3 and is never cached.
fn write_workspace(root: &Path) {
    let repository = repository_root();
    let plugin = std::fs::canonicalize(repository.join("packages/nx-plugin/loader.js"))
        .expect("this repository's nx-plugin loader");
    std::os::unix::fs::symlink(repository.join("node_modules"), root.join("node_modules"))
        .expect("link the repository's node_modules");
    std::fs::create_dir(root.join("app")).expect("create the app project");
    std::fs::create_dir(root.join("sockets")).expect("create the Nx socket directory");
    write_file(
        &root.join("package.json"),
        json!({"name": "nx-publication-fixture", "private": true}).to_string(),
    );
    write_file(
        &root.join("nx.json"),
        json!({"cacheDirectory": ".nx/cache", "plugins": [plugin.display().to_string()]})
            .to_string(),
    );
    write_file(
        &root.join("app/project.json"),
        json!({
            "name": "app",
            "targets": {
                "build": {
                    "executor": "nx:run-commands",
                    "cache": true,
                    "inputs": ["{projectRoot}/source.txt"],
                    "outputs": ["{projectRoot}/result.txt"],
                    "options": {"cwd": "app", "command": "echo run >> runs.txt && cp source.txt result.txt"}
                },
                "other": {
                    "executor": "nx:run-commands",
                    "cache": true,
                    "outputs": ["{projectRoot}/other.txt"],
                    "options": {"cwd": "app", "command": "echo other > other.txt"}
                },
                "fail": {
                    "executor": "nx:run-commands",
                    "cache": false,
                    "options": {"cwd": "app", "command": "exit 3"}
                }
            }
        })
        .to_string(),
    );
    write_file(&root.join("app/source.txt"), "one");
}

/// The environment of every Nx process here: nothing inherited but the managed `PATH` and a home
/// inside the fixture, and every `NX_*` the bridge or Nx reads named explicitly.
fn scrub(command: &mut Command, root: &Path) {
    command
        .env_clear()
        .env(
            "PATH",
            std::env::var_os("PATH").expect("PATH is set for the managed toolchain"),
        )
        .env("HOME", root)
        .env("NO_COLOR", "1")
        .env("NX_DAEMON", "false")
        .env("NX_ISOLATE_PLUGINS", "false")
        .env("NX_USE_LOCAL", "true")
        .env("NX_WORKSPACE_ROOT_PATH", root)
        .env("NX_SOCKET_DIR", root.join("sockets"))
        .env(
            "NX_WORKSPACE_DATA_DIRECTORY",
            root.join(".nx/workspace-data"),
        )
        .env("NX_CACHE_DIRECTORY", root.join(".nx/cache"));
}

/// Runs the stock `nx run <target>`; the receipt is set only when the run is a checked run.
fn nx(root: &Path, receipt: Option<&str>, target: &str) -> Output {
    nx_remote(root, receipt, target, None)
}

/// Runs the stock `nx run <target>` against one remote cache endpoint, when one is given.
fn nx_remote(root: &Path, receipt: Option<&str>, target: &str, remote: Option<&Remote>) -> Output {
    let mut command = Command::new("node");
    command
        .arg(root.join("node_modules/.bin/nx"))
        .args(["run", target, "--outputStyle=static"])
        .current_dir(root);
    scrub(&mut command, root);
    if let Some(id) = receipt {
        command.env("COWSHED_NX_TASK_RECEIPT_ID", id);
    }
    if let Some(remote) = remote {
        command
            .env("NX_SELF_HOSTED_REMOTE_CACHE_SERVER", &remote.origin)
            .env("NX_SELF_HOSTED_REMOTE_CACHE_ACCESS_TOKEN", &remote.token);
    }
    command.output_locked().expect("run the stock Nx")
}

/// Runs one bridge mode from the workspace, its production prefix and body on stdin. The remote
/// endpoint and its token reach this child only.
fn bridge(root: &Path, mode: &str, input: &Value, remote: Option<&Remote>) -> Output {
    let mut command = Command::new("node");
    command
        .arg("-")
        .arg(root)
        .arg(mode)
        .arg(input.to_string())
        .current_dir(root)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    scrub(&mut command, root);
    if let Some(remote) = remote {
        command
            .env("NX_SELF_HOSTED_REMOTE_CACHE_SERVER", &remote.origin)
            .env("NX_SELF_HOSTED_REMOTE_CACHE_ACCESS_TOKEN", &remote.token);
    }
    let mut child = command
        .spawn_locked()
        .expect("run the Nx publication bridge with node");
    child
        .stdin
        .take()
        .expect("bridge stdin")
        .write_all(&cache_adapter::script(BRIDGE))
        .expect("send the bridge script");
    child.wait_with_output().expect("collect the bridge output")
}

fn inspect(root: &Path, input: &Value) -> Output {
    bridge(root, "inspect", input, None)
}

/// The bridge's bounded JSON answer, from a run that succeeded.
fn answer(output: &Output) -> Value {
    assert!(output.status.success(), "{}", text(output));
    assert!(
        output.stdout.len() <= ANSWER_LIMIT,
        "bridge answer is {} bytes, over the {ANSWER_LIMIT}-byte limit",
        output.stdout.len()
    );
    serde_json::from_slice(&output.stdout).expect("bridge answers JSON")
}

/// The status a direct GET of one hash returns from the server, with the read token.
fn direct_status(origin: &str, token: &str, hash: &str) -> u16 {
    let address = origin
        .strip_prefix("http://")
        .expect("loopback http origin");
    let mut stream = TcpStream::connect(address).expect("connect to the server");
    write!(
        stream,
        "GET /v1/cache/{hash} HTTP/1.1\r\nHost: {address}\r\nAuthorization: Bearer {token}\r\nConnection: close\r\n\r\n"
    )
    .expect("send the GET");
    let mut response = String::new();
    (&mut stream)
        .take(64 * 1024)
        .read_to_string(&mut response)
        .expect("read the GET response");
    response
        .split_whitespace()
        .nth(1)
        .expect("status code")
        .parse()
        .expect("numeric status")
}

fn text(output: &Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

/// Real executions of `app:build`: one `run` line each, never a cache hit. The marker exists once
/// the first build has run, so a read failure here is a fixture failure, not zero executions.
fn executions(root: &Path) -> usize {
    std::fs::read_to_string(root.join("app/runs.txt"))
        .unwrap_or_else(|error| panic!("read the execution marker: {error}"))
        .lines()
        .count()
}

/// The single hint in a receipt's namespace, parsed.
fn hint(root: &Path, receipt: &str) -> Value {
    let namespace = root.join(".nx/cache/.cowshed-task-results").join(receipt);
    let mut files: Vec<PathBuf> = std::fs::read_dir(&namespace)
        .unwrap_or_else(|error| panic!("hint namespace {}: {error}", namespace.display()))
        .map(|entry| entry.expect("hint entry").path())
        .collect();
    assert_eq!(files.len(), 1, "{}", namespace.display());
    let file = files.pop().expect("one hint file");
    serde_json::from_slice(&std::fs::read(&file).expect("read hint")).expect("hint is JSON")
}

#[test]
fn stock_nx_bridge_qualifies_only_the_checked_successful_task() {
    let fixture = fixture();
    let root: &Path = &fixture;
    write_workspace(root);

    // An unrelated cached task, run without a receipt: no checked run ever names it.
    assert!(nx(root, None, "app:other").status.success());
    let built = nx(root, None, "app:build");
    assert!(built.status.success(), "{}", text(&built));
    assert_eq!(executions(root), 1);

    // The checked run: unchanged, so a local hit, under its own receipt.
    let start = now_ms();
    let check = nx(root, Some(CHECK), "app:build");
    let end = now_ms();
    assert!(check.status.success(), "{}", text(&check));
    assert_eq!(executions(root), 1);
    assert_eq!(
        hint(root, CHECK)["tasks"][0]["status"],
        json!("local-cache")
    );

    let qualified = answer(&inspect(
        root,
        &json!({"requestId": CHECK, "startMs": start, "endMs": end}),
    ));
    assert_eq!(qualified["commands"], json!([CHECKED_COMMAND]));
    assert_eq!(qualified["entries"].as_array().map(Vec::len), Some(1));
    assert_eq!(qualified["entries"][0]["task"], json!("app:build"));
    assert_eq!(qualified["entries"][0]["code"], json!(0));
    assert_eq!(qualified["entries"][0]["cacheStatus"], json!("local-cache"));
    assert!(
        qualified["entries"][0]["hash"]
            .as_str()
            .is_some_and(|hash| !hash.is_empty() && hash.bytes().all(|byte| byte.is_ascii_digit()))
    );
    assert_eq!(executions(root), 1, "inspection must execute no task");
    assert!(
        !root
            .join(".nx/cache/.cowshed-task-results")
            .join(CHECK)
            .exists(),
        "a qualified namespace is consumed"
    );

    // Refusal: the source changes after the checked run, so the current hash no longer matches.
    let start = now_ms();
    let refused_run = nx(root, Some(REFUSED), "app:build");
    let end = now_ms();
    assert!(refused_run.status.success(), "{}", text(&refused_run));
    write_file(&root.join("app/source.txt"), "two");
    let refused = inspect(
        root,
        &json!({"requestId": REFUSED, "startMs": start, "endMs": end}),
    );
    assert!(!refused.status.success(), "{}", text(&refused));
    assert!(
        text(&refused).contains("differs from its result hint"),
        "{}",
        text(&refused)
    );
    assert_eq!(executions(root), 1);
    assert_eq!(hint(root, REFUSED)["requestId"], json!(REFUSED));

    // Missing artifact: a fresh checked run executes the changed source; its cached outputs are then removed.
    let start = now_ms();
    let fresh = nx(root, Some(MISSING), "app:build");
    let end = now_ms();
    assert!(fresh.status.success(), "{}", text(&fresh));
    assert_eq!(executions(root), 2);
    let hash = hint(root, MISSING)["tasks"][0]["task"]["hash"]
        .as_str()
        .expect("checked task hash")
        .to_owned();
    std::fs::remove_dir_all(root.join(".nx/cache").join(&hash))
        .expect("remove the cached artifact of the checked hash");
    let missing = inspect(
        root,
        &json!({"requestId": MISSING, "startMs": start, "endMs": end}),
    );
    assert!(!missing.status.success(), "{}", text(&missing));
    assert!(
        text(&missing).contains("has no successful complete local cache artifact"),
        "{}",
        text(&missing)
    );
    assert_eq!(executions(root), 2);
    assert_eq!(hint(root, MISSING)["requestId"], json!(MISSING));

    // Failed result: a non-cached task that exits 3 leaves a failure hint, which never qualifies.
    let start = now_ms();
    let failed_run = nx(root, Some(FAILED), "app:fail");
    let end = now_ms();
    assert!(!failed_run.status.success(), "{}", text(&failed_run));
    assert_eq!(hint(root, FAILED)["tasks"][0]["status"], json!("failure"));
    let failure = inspect(
        root,
        &json!({"requestId": FAILED, "startMs": start, "endMs": end}),
    );
    assert!(!failure.status.success(), "{}", text(&failure));
    assert!(
        text(&failure).contains("has no successful result"),
        "{}",
        text(&failure)
    );
    assert_eq!(executions(root), 2);
}

/// The production publish mode and the read path, through the production server: publication
/// uploads the checked hash once and then sees it existing; a reader's ordinary run restores it
/// remotely without executing; a changed read is local work whose hash the server never stores.
#[test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
fn host_controller_bridge_publishes_and_reads_through_the_production_server() {
    let source = fixture();
    let reader = fixture();
    let backend = fixture();
    let source_root: &Path = &source;
    let reader_root: &Path = &reader;
    write_workspace(source_root);
    write_workspace(reader_root);

    let server = Server::start(&backend, WRITER_TOKEN, READER_TOKEN);
    let writer = Remote {
        origin: server.origin.clone(),
        token: WRITER_TOKEN.to_owned(),
    };
    let reading = Remote {
        origin: server.origin.clone(),
        token: READER_TOKEN.to_owned(),
    };

    // The source builds and checks locally; nothing has been published yet.
    let built = nx(source_root, None, "app:build");
    assert!(built.status.success(), "{}", text(&built));
    assert_eq!(executions(source_root), 1);
    let start = now_ms();
    let check = nx(source_root, Some(SOURCE_CHECK), "app:build");
    let end = now_ms();
    assert!(check.status.success(), "{}", text(&check));
    assert_eq!(executions(source_root), 1);
    let selected = answer(&inspect(
        source_root,
        &json!({"requestId": SOURCE_CHECK, "startMs": start, "endMs": end}),
    ));
    assert_eq!(selected["entries"].as_array().map(Vec::len), Some(1));
    let hash = selected["entries"][0]["hash"]
        .as_str()
        .expect("selected hash")
        .to_owned();

    // Publication uploads the selected hash once, then finds it already present.
    let marker_before_publish = executions(source_root);
    let first = answer(&bridge(
        source_root,
        "publish",
        &json!([hash]),
        Some(&writer),
    ));
    assert_eq!(first["published"], json!(1));
    assert_eq!(first["existing"], json!(0));
    assert_eq!(
        executions(source_root),
        1,
        "publication uploads and executes nothing"
    );
    let second = answer(&bridge(
        source_root,
        "publish",
        &json!([hash]),
        Some(&writer),
    ));
    assert_eq!(second["published"], json!(0));
    assert_eq!(second["existing"], json!(1));
    assert_eq!(executions(source_root), 1);

    // The stored entry is the server's file for the key, no extension; its allocation is the blocks it occupies.
    use std::os::unix::fs::MetadataExt as _;
    let stored = backend.join("entries").join(&hash);
    let stored_metadata = std::fs::metadata(&stored)
        .unwrap_or_else(|error| panic!("stat {}: {error}", stored.display()));
    let allocated = stored_metadata
        .blocks()
        .checked_mul(512)
        .expect("allocated bytes fit in u64");

    // The reader restores the selected hash remotely: no execution, the same hash, the same bytes.
    let read = nx_remote(reader_root, Some(READER_CHECK), "app:build", Some(&reading));
    assert!(read.status.success(), "{}", text(&read));
    assert_eq!(
        std::fs::read_to_string(reader_root.join("app/result.txt")).expect("restored result"),
        "one"
    );
    let reader_executed_on_restore = reader_root.join("app/runs.txt").exists();
    assert!(
        !reader_root.join("app/runs.txt").exists(),
        "the reader's executor never ran"
    );
    let restored = hint(reader_root, READER_CHECK);
    assert_eq!(restored["tasks"][0]["status"], json!("remote-cache"));
    assert_eq!(restored["tasks"][0]["task"]["hash"], json!(hash));

    // A cold read of a changed source executes once, locally; the read token cannot store it.
    write_file(&reader_root.join("app/source.txt"), "three");
    let cold_started = std::time::Instant::now();
    let cold = nx_remote(reader_root, Some(READER_WIP), "app:build", Some(&reading));
    let cold_wall_ms =
        u64::try_from(cold_started.elapsed().as_millis()).expect("wall time fits in u64");
    assert!(cold.status.success(), "{}", text(&cold));
    assert_eq!(
        executions(reader_root),
        1,
        "the cold read executes once, locally"
    );
    let wip = hint(reader_root, READER_WIP);
    assert_eq!(wip["tasks"][0]["status"], json!("success"));
    let wip_hash = wip["tasks"][0]["task"]["hash"]
        .as_str()
        .expect("wip hash")
        .to_owned();
    assert_ne!(wip_hash, hash);
    let wip_status = direct_status(&server.origin, READER_TOKEN, &wip_hash);
    assert_eq!(wip_status, 404);
    let unknown_status = direct_status(&server.origin, READER_TOKEN, UNKNOWN_HASH);
    assert_eq!(unknown_status, 404);

    // One structured record of what this case measured; nothing here is a protected or cutover claim.
    let proof = json!({
        "publication": {
            "elapsedMs": first["elapsedMs"],
            "published": first["published"],
            "existing": first["existing"],
            "repeatPublished": second["published"],
            "repeatExisting": second["existing"],
        },
        "storedEntry": {
            "path": format!("entries/{hash}"),
            "bytes": stored_metadata.len(),
            "allocatedBytes": allocated,
        },
        "cap": {
            "capBytes": CAP_BYTES.parse::<u64>().expect("decimal cap"),
            "maxUploadBytes": MAX_UPLOAD_BYTES.parse::<u64>().expect("decimal upload limit"),
        },
        "sourceMarker": {
            "beforePublish": marker_before_publish,
            "afterPublish": executions(source_root),
        },
        "readerRemoteHit": {
            "result": restored["tasks"][0]["status"],
            "taskHash": hash,
            "executed": reader_executed_on_restore,
        },
        "coldReadOnly": {
            "wallMs": cold_wall_ms,
            "marker": executions(reader_root),
            "getStatus": {"wip": wip_status, "unknown": unknown_status},
            "putObserved": "not measured",
        },
    });
    println!("nx-publication-proof {proof}");
    // Closing the lease ends the server with success: no retry, no sleep, no poll.
    server.close();
}

const ROOT_PROBE: &str = "COWSHED_NX_PUBLICATION_ROOT_PROBE";

/// The repository root is resolved from the manifest directory at runtime, never baked in at
/// compile time: a child test process whose `CARGO_MANIFEST_DIR` names another fixture resolves
/// that fixture. A private file carries the exact native path bytes independently of libtest's
/// stdout formatting. The child gets its environment on its own `Command`, so no test mutates
/// the shared environment that parallel tests read.
#[test]
fn repository_root_resolves_the_manifest_at_runtime_not_at_compile_time() {
    if let Some(probe) = std::env::var_os(ROOT_PROBE) {
        std::fs::write(probe, repository_root().as_os_str().as_encoded_bytes())
            .expect("write the resolved root probe");
        return;
    }
    let fixture = fixture();
    let manifest = fixture.join("packages/cowshed/crates/cowshed-core");
    std::fs::create_dir_all(&manifest).expect("fixture manifest directory");
    let expected = std::fs::canonicalize(&*fixture).expect("canonical fixture root");
    let probe = fixture.join("resolved-root");
    let output = Command::new(std::env::current_exe().expect("this test binary"))
        .args([
            "--exact",
            "repository_root_resolves_the_manifest_at_runtime_not_at_compile_time",
            "--nocapture",
        ])
        .env("CARGO_MANIFEST_DIR", &manifest)
        .env(ROOT_PROBE, &probe)
        .output_locked()
        .expect("run the root probe child");
    assert!(output.status.success(), "{}", text(&output));
    let resolved = std::fs::read(&probe)
        .unwrap_or_else(|error| panic!("read root probe {}: {error}", probe.display()));
    assert_eq!(resolved.as_slice(), expected.as_os_str().as_encoded_bytes());
}

fn read_manifest(path: &Path) -> Value {
    let bytes =
        std::fs::read(path).unwrap_or_else(|error| panic!("read {}: {error}", path.display()));
    serde_json::from_slice(&bytes)
        .unwrap_or_else(|error| panic!("parse {}: {error}", path.display()))
}

/// The server ships as one platform-independent bundle that Bun runs from `PATH` (tg#7504): the
/// nx-cache-server package publishes `dist/server` and no `dist/bin`, and no cowshed CLI build
/// depends on, hashes, or copies an nx-cache-server artifact. All four platforms are checked; a
/// single target is not enough.
#[test]
fn cli_targets_carry_no_nx_cache_server_and_the_server_ships_its_bundle() {
    let root = repository_root();
    let cowshed = read_manifest(&root.join("packages/cowshed/package.json"));
    let server = read_manifest(&root.join("packages/nx-cache-server/package.json"));
    let files = server["files"]
        .as_array()
        .expect("nx-cache-server declares its published files");
    assert!(files.contains(&json!("dist/server")), "{files:?}");
    assert!(!files.contains(&json!("dist/bin")), "{files:?}");
    assert_eq!(
        server["nx"]["targets"]["bundle"]["outputs"],
        json!(["{projectRoot}/dist/server/nx-cache-server.js"])
    );
    for cli in [
        "cli-arm64-macos",
        "cli-x64-macos",
        "cli-arm64-linux",
        "cli-x64-linux",
    ] {
        let target = &cowshed["nx"]["targets"][cli];
        assert!(target.is_object(), "{cli} is a cowshed target");
        let declared = serde_json::to_string(target).expect("serialize the target");
        assert!(
            !declared.contains("nx-cache-server"),
            "{cli} must not depend on, hash, or copy an nx-cache-server artifact: {declared}"
        );
    }
}
