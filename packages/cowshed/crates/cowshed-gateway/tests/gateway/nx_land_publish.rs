//! A land's publish end to end (05_gateway.md "`/nx/v1/cache/{hash}`"; the owner's rulings,
//! 2026-10-25 and 2026-11-10). Shed A's Nx runs under the environment every workspace carries: it
//! reads the project's cache and stores only a result worth its bytes. The land of A then runs the
//! publish `cowshed land` runs after its gate passed: the same script, `node - <root> <hashes>`,
//! over the task hashes `attribute` gives the land for A's runs, which stores what the client left
//! unstored. Shed B of the project, at the same inputs, then restores the task from the cache and
//! executes nothing. One real `Gateway` with one real nx-cache-server, the repository's Nx client
//! in two fixture checkouts, and no credential anywhere.
use super::*;
use cowshed_os::test_backstop::AsyncChildBackstop as _;

use super::nx_outage::{executions, gateway_with, nx_events, run_text, scrub, write_workspace};
use super::nx_process::{server_path, server_script};
use cowshed_core::build_volume::nx::{Attribution, Published, attribute, publish_script};
use cowshed_gateway::NxCacheServiceConfig;
use cowshed_os::fork_lock::Spawn as _;
use std::os::unix::fs::DirBuilderExt as _;
use std::path::Path;
use std::process::{Output, Stdio};
use tokio::io::AsyncWriteExt as _;
use tokio::process::Command;

const REPO: &str = "owner/nx-land-publish";
/// The land check A's gate ran for `project`, as `cowshed land --check` would name it.
fn check(project: &str) -> String {
    format!("nx run {project}:build --outputStyle=static")
}

/// The endpoint and store bound every workspace's environment sets (03_caches.md, Wiring).
fn workspace_nx(command: &mut Command, origin: &str) {
    command
        .env(
            cowshed_core::workspace_environment::NX_REMOTE_SERVER_ENV,
            origin,
        )
        .env(
            cowshed_core::workspace_environment::NX_REMOTE_STORE_BOUND_ENV,
            cowshed_core::workspace_environment::NX_REMOTE_STORE_SECONDS_PER_MIB,
        );
}

/// One shed's `nx run <project>:build`, in the workspace's Nx environment.
async fn shed_build(root: &Path, origin: &str, project: &str) -> Output {
    let mut command = Command::new("node");
    command
        .arg(root.join("node_modules/.bin/nx"))
        .args(["run", &format!("{project}:build"), "--outputStyle=static"])
        .current_dir(root);
    scrub(&mut command, root);
    workspace_nx(&mut command, origin);
    command
        .output_bounded()
        .await
        .expect("run the repository's Nx")
}

/// A second project whose build takes longer than the workspace's bound asks of its few bytes,
/// so the client's own store keeps it (the build nx-remote-cache-outage.test.ts calls slow).
fn write_slow_project(root: &Path) {
    std::fs::create_dir(root.join("slow")).expect("create the slow project");
    std::fs::write(
        root.join("slow/project.json"),
        serde_json::json!({
            "name": "slow",
            "targets": {
                "build": {
                    "executor": "nx:run-commands",
                    "cache": true,
                    "inputs": ["{projectRoot}/source.txt"],
                    "outputs": ["{projectRoot}/result.txt"],
                    "options": {"cwd": "slow", "command": "sleep 1.5 && cp source.txt result.txt"}
                }
            }
        })
        .to_string(),
    )
    .expect("write the slow project");
    std::fs::write(root.join("slow/source.txt"), "slow").expect("write the slow source");
}

/// The one hash the land attributes to shed A's run of `project` between the two instants.
fn attributed(
    root: &Path,
    project: &str,
    spawned: std::time::SystemTime,
    exited: std::time::SystemTime,
) -> String {
    let Attribution::Ours(run) =
        attribute(&root.join(".nx/cache"), &check(project), spawned, exited)
    else {
        panic!("the land attributes shed A's run of {project} to its check");
    };
    let hashes: Vec<String> = run.tasks.into_iter().map(|task| task.hash).collect();
    let [hash] = hashes.as_slice() else {
        panic!("one cacheable task: {hashes:?}");
    };
    hash.clone()
}

/// The gateway's audited `(status, bytes)` of each `HEAD` for `hash`: what the proxy put on the wire.
fn head_events(audit: &super::NxRecordingAudit, hash: &str) -> Vec<(Option<u16>, u64)> {
    let path = format!("/nx/v1/cache/{hash}");
    audit
        .events
        .lock()
        .expect("audit events")
        .iter()
        .filter(|event| {
            event.path.as_deref() == Some(path.as_str()) && event.method.as_deref() == Some("HEAD")
        })
        .map(|event| (event.http_status, event.bytes))
        .collect()
}

/// Whether the store's entry for `hash` carries SIEVE's visited bit (`atime > mtime`, 03_caches.md).
fn visited(store: &Path, hash: &str) -> bool {
    use std::os::unix::fs::MetadataExt as _;
    let mut pending = vec![store.to_path_buf()];
    while let Some(directory) = pending.pop() {
        for entry in std::fs::read_dir(&directory).expect("list the Nx store") {
            let path = entry.expect("a store entry").path();
            if path.is_dir() {
                pending.push(path);
            } else if path.file_name().is_some_and(|name| name == hash) {
                let metadata = std::fs::metadata(&path).expect("stat the stored archive");
                return (metadata.atime(), metadata.atime_nsec())
                    > (metadata.mtime(), metadata.mtime_nsec());
            }
        }
    }
    panic!("no stored archive for {hash} under {}", store.display());
}
/// The land's publish job for `hashes` of the checkout at `root`: the script on stdin, the
/// root and the JSON hashes as arguments, the workspace's own Nx environment.
async fn land_publish(root: &Path, origin: &str, hashes: &[String]) -> Published {
    let mut command = Command::new("node");
    command
        .arg("-")
        .arg(root)
        .arg(serde_json::to_string(hashes).expect("hashes as JSON"))
        .current_dir(root);
    scrub(&mut command, root);
    workspace_nx(&mut command, origin);
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn_locked().expect("start the publish");
    let mut stdin = child.stdin.take().expect("publish stdin");
    stdin
        .write_all(&publish_script())
        .await
        .expect("write the publish script");
    drop(stdin);
    let output = child
        .wait_with_output_bounded()
        .await
        .expect("wait for the publish");
    assert!(output.status.success(), "{}", run_text(&output));
    cowshed_core::build_volume::nx::published(&output.stdout).expect("the publish's count")
}

#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_landed_gate_publishes_and_another_shed_restores_it() {
    let root = secure_fixture_dir(&format!("cowshed-nx-land-publish-{}", std::process::id()));
    let shed_a = root.path().join("shed-a");
    let shed_b = root.path().join("shed-b");
    let store = root.path().join("nx");
    for path in [&shed_a, &shed_b, &store] {
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(path)
            .expect("create a fixture directory");
    }
    write_workspace(&shed_a);
    write_workspace(&shed_b);
    write_slow_project(&shed_a);
    let (mut config, _cache) = test_config();
    config.nx_cache = Some(NxCacheServiceConfig {
        script: server_script(),
        path: server_path(),
        directory: store.clone(),
        max_bytes: 64 * 1024 * 1024,
        cap_bytes: 96 * 1024 * 1024,
    });
    let (gateway, audit) = gateway_with(config, UpstreamHealth::Healthy).await;
    let handle = gateway.handle();
    let mut origins = Vec::new();
    let mut endpoints = Vec::new();
    for (workspace, byte) in [("nx-land-a", 91), ("nx-land-b", 92)] {
        let (installed, _, _) = session(
            workspace,
            REPO,
            block_endpoint(free_endpoint()),
            byte,
            1,
            WorkspacePolicy::default(),
        );
        let endpoint = install_in_free_block(&handle, installed).await;
        origins.push(format!("http://{endpoint}/nx"));
        endpoints.push(endpoint);
    }

    // Shed A builds a cheap task: its lookup misses, the task executes, and the bound keeps the
    // client from storing a result that took milliseconds.
    let spawned = std::time::SystemTime::now();
    let built = shed_build(&shed_a, &origins[0], "app").await;
    let exited = std::time::SystemTime::now();
    assert!(built.status.success(), "{}", run_text(&built));
    assert_eq!(executions(&shed_a), 1, "shed A executes its task");
    let cheap = attributed(&shed_a, "app", spawned, exited);
    let hash = &cheap;
    let lookups = nx_events(&audit, hash, "GET");
    assert!(
        !lookups.is_empty() && lookups.iter().all(|lookup| *lookup == (Some(404), None)),
        "shed A's lookups all miss: {lookups:?}"
    );
    assert!(
        nx_events(&audit, hash, "PUT").is_empty(),
        "a result not worth its bytes is not stored by the shed's own run"
    );

    // Shed A builds a task that took longer than the bound asks: its own client stores it, once.
    let spawned = std::time::SystemTime::now();
    let built = shed_build(&shed_a, &origins[0], "slow").await;
    let exited = std::time::SystemTime::now();
    assert!(built.status.success(), "{}", run_text(&built));
    let worthwhile = attributed(&shed_a, "slow", spawned, exited);
    assert_eq!(
        nx_events(&audit, &worthwhile, "PUT"),
        [(Some(200), None)],
        "a result worth its bytes is stored by the shed's own run"
    );

    // The land publishes what its gate attributed: the cheap result is stored now, by one PUT,
    // and the worthwhile one is found stored.
    let hashes = [cheap.clone(), worthwhile.clone()];
    let published = land_publish(&shed_a, &origins[0], &hashes).await;
    assert_eq!(
        published,
        Published {
            published: 1,
            existing: 1,
            skipped: 0
        }
    );
    assert_eq!(nx_events(&audit, hash, "PUT"), [(Some(200), None)]);
    assert_eq!(nx_events(&audit, &worthwhile, "PUT"), [(Some(200), None)]);
    // A HEAD through the gateway is a membership check: the stored key answers 200 with the
    // archive's length and no body, a key never stored 404, and the archive the publish just
    // stored, which nothing has read, stays unvisited.
    let held =
        super::proxy_request(endpoints[0], super::nx_request(endpoints[0], "HEAD", hash)).await;
    assert!(held.starts_with("HTTP/1.1 200 "), "{held}");
    assert!(held.ends_with("\r\n\r\n"), "a HEAD answers no body: {held}");
    let missing = "404040404040404040";
    let absent = super::proxy_request(
        endpoints[0],
        super::nx_request(endpoints[0], "HEAD", missing),
    )
    .await;
    assert!(absent.starts_with("HTTP/1.1 404 "), "{absent}");
    assert_eq!(head_events(&audit, hash), [(Some(200), 0)]);
    assert_eq!(head_events(&audit, missing), [(Some(404), 0)]);
    assert!(!visited(&store, hash), "a membership check is not a use");
    // Publishing again finds both results stored and replaces nothing.
    let again = land_publish(&shed_a, &origins[0], &hashes).await;
    assert_eq!(
        again,
        Published {
            published: 0,
            existing: 2,
            skipped: 0
        }
    );

    // Shed B, at the same inputs, restores the task from the project's cache and executes nothing.
    let restored = shed_build(&shed_b, &origins[1], "app").await;
    assert!(restored.status.success(), "{}", run_text(&restored));
    assert_eq!(
        executions(&shed_b),
        0,
        "shed B restores instead of executing"
    );
    assert_eq!(
        std::fs::read_to_string(shed_b.join("app/result.txt")).expect("restored output"),
        "one"
    );
    assert!(
        nx_events(&audit, hash, "GET").contains(&(Some(200), None)),
        "shed B's lookup is a hit: {:?}",
        nx_events(&audit, hash, "GET")
    );
    assert!(
        String::from_utf8_lossy(&restored.stdout).contains("remote cache"),
        "{}",
        run_text(&restored)
    );
    assert!(
        visited(&store, hash),
        "shed B's restore is a use, and marks the archive visited"
    );
    gateway.drain().await.expect("drain the gateway");
}
